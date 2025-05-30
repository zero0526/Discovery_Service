package utils

import (
	"net/url"
	"path"
)

func NormalizeURL(baseURL string, link string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	// Nếu link đã là URL tuyệt đối
	u, err := url.Parse(link)
	if err == nil && u.IsAbs() {
		return u.String(), nil
	}

	// Ghép với base URL
	// Xử lý trường hợp link bắt đầu bằng '/' hoặc không
	if len(link) > 0 && link[0] == '/' {
		base.Path = link
	} else {
		base.Path = path.Join(base.Path, link)
	}
	return base.String(), nil
}