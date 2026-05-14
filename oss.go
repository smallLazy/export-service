package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

type OSSUploader interface {
	Upload(ctx context.Context, localPath, exportType, taskID string, expiresAt time.Time) (OSSUploadResult, error)
}

type OSSUploadResult struct {
	Key      string
	URL      string
	FileSize int64
}

type AliyunOSSUploader struct {
	endpoint        string
	bucket          string
	accessKeyID     string
	accessKeySecret string
	basePath        string
	client          *http.Client
}

func newOSSUploader(cfg OSSConfig) OSSUploader {
	if !cfg.Enabled() {
		return nil
	}
	return &AliyunOSSUploader{
		endpoint:        strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/"),
		bucket:          strings.TrimSpace(cfg.Bucket),
		accessKeyID:     strings.TrimSpace(cfg.AccessKeyID),
		accessKeySecret: strings.TrimSpace(cfg.AccessKeySecret),
		basePath:        strings.Trim(strings.TrimSpace(cfg.BasePath), "/"),
		client:          &http.Client{Timeout: 10 * time.Minute},
	}
}

func (u *AliyunOSSUploader) Upload(ctx context.Context, localPath, exportType, taskID string, expiresAt time.Time) (OSSUploadResult, error) {
	info, err := os.Stat(localPath)
	if err != nil {
		return OSSUploadResult{}, err
	}
	file, err := os.Open(localPath)
	if err != nil {
		return OSSUploadResult{}, err
	}
	defer file.Close()

	now := time.Now().UTC()
	key := u.objectKey(exportType, taskID, now)
	putURL := u.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL, file)
	if err != nil {
		return OSSUploadResult{}, err
	}
	req.ContentLength = info.Size()
	req.Header.Set("Date", now.Format(http.TimeFormat))
	req.Header.Set("Content-Type", "text/csv; charset=utf-8")
	req.Header.Set("Authorization", u.authorization(http.MethodPut, key, req.Header.Get("Date"), req.Header.Get("Content-Type")))

	resp, err := u.client.Do(req)
	if err != nil {
		return OSSUploadResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return OSSUploadResult{}, fmt.Errorf("oss upload failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return OSSUploadResult{
		Key:      key,
		URL:      u.signedURL(key, expiresAt),
		FileSize: info.Size(),
	}, nil
}

func (u *AliyunOSSUploader) objectKey(exportType, taskID string, now time.Time) string {
	items := []string{u.basePath, normalizeExportType(exportType), now.Format("2006"), now.Format("01"), now.Format("02"), taskID + ".csv"}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.Trim(item, "/")
		if item != "" {
			parts = append(parts, item)
		}
	}
	return path.Join(parts...)
}

func (u *AliyunOSSUploader) objectURL(key string) string {
	endpoint := u.endpointWithScheme()
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return strings.TrimRight(endpoint, "/") + "/" + key
	}
	parsed.Host = u.bucket + "." + parsed.Host
	parsed.Path = "/" + key
	parsed.RawQuery = ""
	return parsed.String()
}

func (u *AliyunOSSUploader) signedURL(key string, expiresAt time.Time) string {
	expires := expiresAt.Unix()
	signature := u.sign(fmt.Sprintf("GET\n\n\n%d\n/%s/%s", expires, u.bucket, key))
	rawURL := u.objectURL(key)
	separator := "?"
	if strings.Contains(rawURL, "?") {
		separator = "&"
	}
	values := url.Values{}
	values.Set("OSSAccessKeyId", u.accessKeyID)
	values.Set("Expires", fmt.Sprintf("%d", expires))
	values.Set("Signature", signature)
	return rawURL + separator + values.Encode()
}

func (u *AliyunOSSUploader) authorization(method, key, date, contentType string) string {
	stringToSign := fmt.Sprintf("%s\n\n%s\n%s\n/%s/%s", method, contentType, date, u.bucket, key)
	return "OSS " + u.accessKeyID + ":" + u.sign(stringToSign)
}

func (u *AliyunOSSUploader) sign(value string) string {
	mac := hmac.New(sha1.New, []byte(u.accessKeySecret))
	_, _ = mac.Write([]byte(value))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (u *AliyunOSSUploader) endpointWithScheme() string {
	if strings.HasPrefix(u.endpoint, "http://") || strings.HasPrefix(u.endpoint, "https://") {
		return u.endpoint
	}
	return "https://" + u.endpoint
}
