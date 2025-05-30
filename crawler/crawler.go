package crawler

import (
	"fmt"
	"log"
	"net/http"
	"net/http/cookiejar"
	"regexp"
	"scraper/utils"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/publicsuffix"
)

func FetchPageContent(url string, userAgent string, timeout time.Duration) (*goquery.Document, error) {
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return nil, fmt.Errorf("failed to create cookie jar: %w", err)
	}

	client := &http.Client{
		Timeout: timeout,
		Jar:     jar, 
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			log.Printf("Redirecting to: %s (from %s)", req.URL, via[len(via)-1].URL)
			return nil 
		},
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	uaToSet := "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
	if userAgent != "" {
		uaToSet = userAgent
	}
	req.Header.Set("User-Agent", uaToSet)

	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9,vi;q=0.8") // Prioritize English for broader compatibility, then Vietnamese
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Referer", "https://www.google.com/")
	req.Header.Set("Sec-Ch-Ua", `"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"`)
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", `"Windows"`)
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "cross-site") 
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")

	log.Printf("Fetching URL: %s with User-Agent: %s", url, uaToSet)

	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request for %s: %w", url, err)
	}
	defer res.Body.Close()

	log.Printf("Response Status: %s, Code: %d for URL: %s", res.Status, res.StatusCode, url)

	// Check for successful status codes (2xx range)
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		// Log response body for debugging non-2xx responses, if small
		// bodyBytes, _ := io.ReadAll(res.Body)
		// log.Printf("Error response body: %s", string(bodyBytes))
		return nil, fmt.Errorf("bad status for %s: %s (Code: %d)", url, res.Status, res.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML from %s: %w", url, err)
	}

	return doc, nil
}

// ExtractArticleLinks trích xuất các link bài viết từ tài liệu HTML
func ExtractArticleLinks(doc *goquery.Document, baseURL, selector, articleURLRegexStr string) []string {
	var links []string
	seenLinks := make(map[string]bool)

	var compiledRegex *regexp.Regexp
	var regexErr error
	if articleURLRegexStr != "" {
		compiledRegex, regexErr = regexp.Compile(articleURLRegexStr)
		if regexErr != nil {
			log.Printf("Lỗi compile regex '%s': %v. Sẽ không sử dụng regex này.", articleURLRegexStr, regexErr)
			compiledRegex = nil // Đảm bảo không dùng regex nếu lỗi
		}
	}

	doc.Find(selector).Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists {
			fullURL, err := utils.NormalizeURL(baseURL, href)
			if err == nil && fullURL != "" && !strings.HasPrefix(fullURL, "javascript:") {
				// Áp dụng regex filter nếu có và regex hợp lệ
				if compiledRegex != nil {
					if compiledRegex.MatchString(fullURL) {
						if !seenLinks[fullURL] {
							links = append(links, fullURL)
							seenLinks[fullURL] = true
						}
					}
					// else {
					// log.Printf("URL '%s' không khớp regex.", fullURL)
					// }
				} else {
					if !seenLinks[fullURL] {
						links = append(links, fullURL)
						seenLinks[fullURL] = true
					}
				}
			} else if err != nil {
				log.Printf("Lỗi khi chuẩn hóa URL '%s' với base '%s': %v", href, baseURL, err)
			}
		}
	})
	return links
}
