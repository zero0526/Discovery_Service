package redisclient

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type Client struct {
	rdb            *redis.Client
	redisKeyPrefix string
}

func New(addr, password string, db int, keyPrefix string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("không thể kết nối tới Redis tại %s: %w", addr, err)
	}
	log.Printf("Đã kết nối thành công tới Redis tại %s, DB %d", addr, db)

	cleanKeyPrefix := keyPrefix
	if cleanKeyPrefix != "" && !strings.HasSuffix(cleanKeyPrefix, ":") {
		cleanKeyPrefix += ":"
	}

	return &Client{rdb: rdb, redisKeyPrefix: cleanKeyPrefix}, nil
}

func (c *Client) getSetKey(domainName string) string {
	safeDomainName := strings.ReplaceAll(domainName, ".", "_")
	return fmt.Sprintf("%s:%s:processed", c.redisKeyPrefix, safeDomainName)
}

func (c *Client) IsLinkProcessed(domainName, link string) (bool, error) {
	setKey := c.getSetKey(domainName)
	isMember, err := c.rdb.SIsMember(ctx, setKey, link).Result()
	if err != nil {
		log.Printf("Lỗi khi kiểm tra link '%s' trong Redis set '%s' cho domain '%s': %v", link, setKey, domainName, err)
		return false, fmt.Errorf("lỗi SIsMember: %w", err)
	}
	if isMember {
		log.Printf("Link '%s' đã tồn tại trong Redis set '%s' (domain: %s)", link, setKey, domainName)
	}
	return isMember, nil
}

func (c *Client) MarkLinkAsProcessed(domainName, link string) (bool, error) {
	setKey := c.getSetKey(domainName)
	addedCount, err := c.rdb.SAdd(ctx, setKey, link).Result()
	if err != nil {
		log.Printf("Lỗi khi thêm link '%s' vào Redis set '%s' cho domain '%s': %v", link, setKey, domainName, err)
		return false, fmt.Errorf("lỗi SAdd: %w", err)
	}

	if addedCount > 0 {
		log.Printf("Đã thêm link mới '%s' vào Redis set '%s' (domain: %s)", link, setKey, domainName)
		return true, nil
	}
	return false, nil
}
// Trả về true nếu link được thêm mới, false nếu đã tồn tại hoặc lỗi
func (c *Client) AddLinkIfNotExists(domainName, link string) (bool, error) {
	setKey := c.getSetKey(domainName)
	// SAdd trả về số lượng element được thêm mới (0 hoặc 1 trong trường hợp này)
	added, err := c.rdb.SAdd(ctx, setKey, link).Result()
	if err != nil {
		log.Printf("[%s] Lỗi khi thêm link vào Redis %s: %v", domainName, link, err)
		return false, err
	}
	if added > 0 {
		log.Printf("[%s] Đã thêm link mới vào Redis: %s", domainName, link)
		return true, nil
	}
	log.Printf("[%s] Link đã tồn tại trong Redis: %s", domainName, link)
	return false, nil
}

func (c *Client) Close() error {
	if c.rdb != nil {
		log.Println("Đang đóng kết nối Redis...")
		return c.rdb.Close()
	}
	return nil
}

