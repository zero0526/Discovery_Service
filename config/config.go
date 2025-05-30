package config

import (
	"encoding/json"
	"os"
	"log"
	"path/filepath"
)

// CategoryConfig định nghĩa cấu trúc cho một chủ đề
type CategoryConfig struct {
	Name                string `json:"name"`
	URL                 string `json:"url"`
	ArticleLinkSelector string `json:"articleLinkSelector"`
	ArticleURLRegex     string `json:"articleUrlRegex,omitempty"`
}

// DomainConfig định nghĩa cấu trúc cho một domain báo
type DomainConfig struct {
	DomainName string           `json:"domainName"`
	BaseURL    string           `json:"baseURL"`
	Categories []CategoryConfig `json:"categories"`
}

func LoadDomainConfig(filePath string) (*DomainConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var cfg DomainConfig
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func LoadAllConfigs(configDir string) ([]DomainConfig, error) {
	var configs []DomainConfig
	files, err := os.ReadDir(configDir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			filePath := filepath.Join(configDir, file.Name())
			cfg, err := LoadDomainConfig(filePath)
			if err != nil {
				log.Printf("Lỗi khi load config file %s: %v. Bỏ qua.", filePath, err)
				continue
			}
			configs = append(configs, *cfg)
		}
	}
	return configs, nil
}