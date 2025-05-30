package main

import (
	"log"
	"scraper/config"
	"scraper/crawler"
	"scraper/redis_client"
	"os"
	"sync"
	"time"
)

const (
	configDir            = "sources"
	redisAddr            = "localhost:6379" 
	redisPassword        = ""              
	redisDB              = 0
	redisKeyPrefix       = "article_links"
	requestTimeout       = 10 * time.Second
	delayBetweenRequests = 500 * time.Millisecond // Độ trễ giữa các request
	userAgent            = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

// ArticleMeta chứa thông tin cơ bản của bài viết mới
type ArticleMeta struct {
	Domain   string
	Category string
	URL      string
}
func processDomain(domainCfg config.DomainConfig, rc *redisclient.Client, newArticlesChan chan<- ArticleMeta, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("[%s] Bắt đầu crawl...", domainCfg.DomainName)
	var articlesFoundInDomain int

	for _, category := range domainCfg.Categories {
		log.Printf("[%s - %s] Đang crawl trang chủ đề: %s", domainCfg.DomainName, category.Name, category.URL)
		time.Sleep(delayBetweenRequests)

		doc, err := crawler.FetchPageContent(category.URL, userAgent, requestTimeout)
		if err != nil {
			log.Printf("[%s - %s] Lỗi khi fetch page: %v", domainCfg.DomainName, category.Name, err)
			continue
		}

		// Truyền ArticleURLRegex vào hàm ExtractArticleLinks
		articleLinks := crawler.ExtractArticleLinks(doc, domainCfg.BaseURL, category.ArticleLinkSelector, category.ArticleURLRegex)
		log.Printf("[%s - %s] Tìm thấy %d link bài viết (sau khi lọc regex nếu có).", domainCfg.DomainName, category.Name, len(articleLinks))

		for _, link := range articleLinks {
			isNew, err := rc.AddLinkIfNotExists(domainCfg.DomainName, link)
			if err != nil {
				continue
			}
			if isNew {
				log.Printf("[%s] Link MỚI: %s", domainCfg.DomainName, link)
				newArticlesChan <- ArticleMeta{
					Domain:   domainCfg.DomainName,
					Category: category.Name,
					URL:      link,
				}
				articlesFoundInDomain++
			}
		}
	}
	log.Printf("[%s] Hoàn thành crawl. Tìm thấy %d bài viết mới.", domainCfg.DomainName, articlesFoundInDomain)
}

func main() {
	log.SetOutput(os.Stdout) 
	log.Println("Service crawl bắt đầu...")

	// 1. Khởi tạo Redis client
	rc, err := redisclient.New(redisAddr, redisPassword, redisDB, redisKeyPrefix)
	if err != nil {
		log.Fatalf("Không thể khởi tạo Redis client: %v", err)
	}
	defer rc.Close()

	// 2. Load tất cả các cấu hình domain
	domainConfigs, err := config.LoadAllConfigs(configDir)
	if err != nil {
		log.Fatalf("Không thể load cấu hình domains: %v", err)
	}
	if len(domainConfigs) == 0 {
		log.Println("Không tìm thấy file config nào. Kết thúc.")
		return
	}
	log.Printf("Đã load %d cấu hình domain.", len(domainConfigs))

	// 3. Sử dụng goroutines và WaitGroup để xử lý song song các domain
	var wg sync.WaitGroup
	newArticlesChan := make(chan ArticleMeta, 100) // Buffered channel để chứa các link mới tìm được

	for _, domainCfg := range domainConfigs {
		wg.Add(1)
		go processDomain(domainCfg, rc, newArticlesChan, &wg)
	}

	// Goroutine để đóng channel khi tất cả các processDomain goroutines hoàn thành
	go func() {
		wg.Wait()
		close(newArticlesChan)
	}()

	// 4. Thu thập và xử lý các link mới từ channel
	var allNewArticles []ArticleMeta
	for article := range newArticlesChan {
		allNewArticles = append(allNewArticles, article)
	}

	log.Printf("Tổng cộng tìm thấy %d bài viết mới từ tất cả các domain.", len(allNewArticles))

	// Tại đây, bạn có thể làm gì đó với `allNewArticles`
	if len(allNewArticles) > 0 {
		log.Println("\n--- DANH SÁCH BÀI VIẾT MỚI ---")
		for _, article := range allNewArticles {
			log.Printf("Domain: %s, Category: %s, URL: %s", article.Domain, article.Category, article.URL)
		}
	}

	log.Println("Service crawl hoàn thành.")
}