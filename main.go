package main

import (
	"log"
	"scraper/config"
	"scraper/crawler"
	"scraper/redis_client"
	"os"
	"sync"
	"time"
	"strings"
	"net/url"
	"fmt"

	"github.com/IBM/sarama"
)

const (
	configDir            = "sources"
	redisAddr            = "localhost:6379" 
	redisPassword        = ""              
	redisDB              = 0
	redisKeyPrefix       = "article_links"
	requestTimeout       = 10 * time.Second
	delayBetweenRequests = 500 * time.Millisecond 
	kafkaBroker          = "localhost:9092"
	userAgent            = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
var rc *redisclient.Client
var producer sarama.SyncProducer

type ArticleMeta struct {
	Domain   string
	Category string
	URL      string
}
func processDomain(domainCfg config.DomainConfig, rc *redisclient.Client, producer sarama.SyncProducer, newArticlesChan chan<- ArticleMeta, wg *sync.WaitGroup) { // Bỏ error return nếu muốn xử lý lỗi bên trong
	defer wg.Done()

	log.Printf("[%s] Bắt đầu crawl...", domainCfg.DomainName)
	var totalNewArticlesInDomain int 

	for _, category := range domainCfg.Categories {
		log.Printf("[%s - %s] Đang crawl trang chủ đề: %s", domainCfg.DomainName, category.Name, category.URL)

		doc, err := crawler.FetchPageContent(category.URL, userAgent, requestTimeout)
		if err != nil {
			log.Printf("[%s - %s] LỖI KHI FETCH PAGE: %s. Lỗi: %v. Bỏ qua category này.", domainCfg.DomainName, category.Name, category.URL, err)
			continue
		}

		articleLinks := crawler.ExtractArticleLinks(doc, domainCfg.BaseURL, category.ArticleLinkSelector, category.ArticleURLRegex)
		// log.Printf("[%s - %s] Tìm thấy %d link bài viết (sau khi lọc regex nếu có).", domainCfg.DomainName, category.Name, len(articleLinks))

		var newArticlesInCategory int
		for _, link := range articleLinks {
			linkDomain, topicName, err := getDomainAndTopicName(link) // Lấy domain của chính link đó
			if err != nil {
				log.Printf("[%s - %s] Lỗi lấy tên miền/topic cho URL '%s': %v. Bỏ qua link này.", domainCfg.DomainName, category.Name, link, err)
				continue 
			}

			// Quan trọng: Kiểm tra link này với domain của chính nó (linkDomain),
			isProcessed, err := rc.IsLinkProcessed(linkDomain, link)
			if err != nil {
				log.Printf("[%s - %s] Lỗi khi kiểm tra URL '%s' (domain link: %s) trong Redis: %v. Bỏ qua link này.", domainCfg.DomainName, category.Name, link, linkDomain, err)
				continue // Tiếp tục với link tiếp theo
			}

			if isProcessed {
				// log.Printf("[%s - %s] Link '%s' (domain link: %s) đã được xử lý. Bỏ qua.", domainCfg.DomainName, category.Name, link, linkDomain)
				continue // Link đã xử lý, tiếp tục với link tiếp theo
			}

			// Nếu đến đây, link này là MỚI
			log.Printf("[%s - %s] Link MỚI cần xử lý: %s (domain link: %s, topic: %s)", domainCfg.DomainName, category.Name, link, linkDomain, topicName)

			// 2. Gửi link mới vào Kafka
			msg := &sarama.ProducerMessage{
				Topic: topicName, // Gửi vào topic của domain của link
				Value: sarama.StringEncoder(link),
			}

			_, _, kafkaErr := producer.SendMessage(msg)
			if kafkaErr != nil {
				log.Printf("[%s - %s] LỖI KHI GỬI MESSAGE tới Kafka topic '%s' cho URL '%s': %v. Bỏ qua đánh dấu Redis cho link này.", domainCfg.DomainName, category.Name, topicName, link, kafkaErr)
				continue 
			}
			log.Printf("[%s - %s] Message cho URL '%s' đã được gửi tới Kafka topic '%s'", domainCfg.DomainName, category.Name, link, topicName)

			addedToRedis, redisErr := rc.MarkLinkAsProcessed(linkDomain, link)
			if redisErr != nil {
				log.Printf("[%s - %s] LỖI KHI ĐÁNH DẤU URL '%s' (domain link: %s) là đã xử lý trong Redis: %v. Kafka message đã được gửi!", domainCfg.DomainName, category.Name, link, linkDomain, redisErr)
				continue
			}

			if addedToRedis {
				log.Printf("[%s - %s] Đã thêm link MỚI '%s' vào Redis và gửi Kafka.", domainCfg.DomainName, category.Name, link)
				newArticlesChan <- ArticleMeta{
					Domain:   domainCfg.DomainName, 
					Category: category.Name,
					URL:      link,                
				}
				newArticlesInCategory++
				totalNewArticlesInDomain++
			} else {
				log.Printf("[%s - %s] CẢNH BÁO: URL '%s' (domain link: %s) không được thêm mới vào Redis mặc dù kiểm tra ban đầu là chưa xử lý.", domainCfg.DomainName, category.Name, link, linkDomain)
			}
        time.Sleep(50 * time.Millisecond) 
		} 
		if newArticlesInCategory > 0 {
			log.Printf("[%s - %s] Hoàn thành crawl category. Tìm thấy %d bài viết mới trong category này.", domainCfg.DomainName, category.Name, newArticlesInCategory)
		} else {
			log.Printf("[%s - %s] Hoàn thành crawl category. Không tìm thấy bài viết mới nào.", domainCfg.DomainName, category.Name)
		}
	} // Kết thúc vòng lặp qua các category

	if totalNewArticlesInDomain > 0 {
		log.Printf("[%s] HOÀN THÀNH CRAWL DOMAIN. Tổng cộng tìm thấy %d bài viết mới.", domainCfg.DomainName, totalNewArticlesInDomain)
	} else {
		log.Printf("[%s] HOÀN THÀNH CRAWL DOMAIN. Không tìm thấy bài viết mới nào.", domainCfg.DomainName, totalNewArticlesInDomain)
	}
	// Không cần return error nữa nếu bạn muốn goroutine này tự xử lý lỗi và tiếp tục
	// Nếu bạn muốn báo lỗi lên cấp cao hơn, hãy thiết kế lại cách trả về lỗi (ví dụ qua một error channel)
}
func getDomainAndTopicName(rawURL string) (string, string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("không thể phân tích URL '%s': %w", rawURL, err)
	}

	host := parsedURL.Hostname()
	if host == "" {
		return "", "", fmt.Errorf("không thể trích xuất hostname từ URL '%s'", rawURL)
	}

	domainName := strings.TrimPrefix(host, "www.")

	parts := strings.Split(domainName, ".")
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	topicName := strings.Join(parts, "_")
	topicName = strings.ReplaceAll(topicName, "-", "_")

	return domainName, topicName, nil
}
func initializeRedisAndKafka() error { 
	var err error
	rc, err = redisclient.New(redisAddr, redisPassword, redisDB, redisKeyPrefix)
	if err != nil {
		return fmt.Errorf("khởi tạo Redis client thất bại: %w", err)
	}
	log.Println("Redis client đã được khởi tạo thành công.") 

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err = sarama.NewSyncProducer([]string{kafkaBroker}, config)
	if err != nil {
		if rc != nil {
			rc.Close()
		}
		return fmt.Errorf("không thể khởi tạo Sarama producer: %w", err)
	}
	log.Println("Đã kết nối thành công tới Kafka với vai trò producer")
	return nil
}
func main() {
	log.SetOutput(os.Stdout) // Đảm bảo log ra stdout
	log.Println("Service crawl bắt đầu...")

	// 1. Khởi tạo Redis client và Kafka Producer
	err := initializeRedisAndKafka()
	if err != nil {
		log.Fatalf("Lỗi khởi tạo clients: %v", err)
	}
	// Đảm bảo các client được đóng khi main kết thúc
	defer func() {
		if rc != nil {
			if err := rc.Close(); err != nil {
				log.Printf("Lỗi khi đóng Redis client: %v", err)
			}
		}
		if producer != nil {
			if err := producer.Close(); err != nil {
				log.Printf("Lỗi khi đóng Kafka producer: %v", err)
			}
		}
		log.Println("Đã đóng các kết nối Redis và Kafka.")
	}()

	// 2. Load tất cả các cấu hình domain
	domainConfigs, err := config.LoadAllConfigs(configDir)
	if err != nil {
		log.Fatalf("Không thể load cấu hình domains từ '%s': %v", configDir, err)
	}
	if len(domainConfigs) == 0 {
		log.Println("Không tìm thấy file config nào trong thư mục 'sources'. Kết thúc.")
		return
	}
	log.Printf("Đã load %d cấu hình domain.", len(domainConfigs))

	// 3. Sử dụng goroutines và WaitGroup để xử lý song song các domain
	var wg sync.WaitGroup
	newArticlesChan := make(chan ArticleMeta, 100) // Buffered channel

	log.Println("Bắt đầu quá trình crawl các domain...")
	for _, domainCfg := range domainConfigs {
		wg.Add(1)
		// Truyền producer đã khởi tạo vào processDomain
		go processDomain(domainCfg, rc, producer, newArticlesChan, &wg)
	}

	// Goroutine để đóng channel khi tất cả các processDomain goroutines hoàn thành
	go func() {
		wg.Wait()
		close(newArticlesChan)
		log.Println("Tất cả các domain đã được crawl. Channel newArticlesChan đã đóng.")
	}()

	// 4. Thu thập và xử lý các link mới từ channel
	var allNewArticles []ArticleMeta
	log.Println("Đang chờ nhận bài viết mới từ channel...")
	for article := range newArticlesChan { 
		allNewArticles = append(allNewArticles, article)
		log.Printf("-> Nhận được bài viết mới: Domain=%s, Category=%s, URL=%s", article.Domain, article.Category, article.URL)
	}

	log.Printf("Tổng cộng tìm thấy %d bài viết mới từ tất cả các domain.", len(allNewArticles))

	if len(allNewArticles) > 0 {
		log.Println("\n--- DANH SÁCH TỔNG HỢP BÀI VIẾT MỚI ---")
		for i, article := range allNewArticles {
			log.Printf("%d. Domain: %s, Category: %s, URL: %s", i+1, article.Domain, article.Category, article.URL)
		}
	} else {
		log.Println("Không tìm thấy bài viết mới nào trong lần chạy này.")
	}

	log.Println("Service crawl hoàn thành.")
}