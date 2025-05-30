package redisclient

import (
	"context"
	"fmt"
	"log"

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
		Password: password, // no password set
		DB:       db,       // use default DB
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối Redis: %w", err)
	}
	log.Println("Kết nối Redis thành công.")
	return &Client{rdb: rdb, redisKeyPrefix: keyPrefix}, nil
}

func (c *Client) getSetKey(domainName string) string {
	return fmt.Sprintf("%s:%s:processed", c.redisKeyPrefix, domainName)
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
	return c.rdb.Close()
}

