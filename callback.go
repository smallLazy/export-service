package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type CallbackClient interface {
	Notify(ctx context.Context, callbackURL string, payload ExportCallbackPayload) error
}

type ExportCallbackPayload struct {
	TaskID       string    `json:"task_id"`
	RequestID    string    `json:"request_id,omitempty"`
	UserID       string    `json:"user_id,omitempty"`
	CompanyUUID  string    `json:"company_uuid,omitempty"`
	SourceSystem string    `json:"source_system,omitempty"`
	ExportType   string    `json:"export_type"`
	FileName     string    `json:"file_name,omitempty"`
	Status       string    `json:"status"`
	FileSize     int64     `json:"file_size,omitempty"`
	OSSKey       string    `json:"oss_key,omitempty"`
	OSSURL       string    `json:"oss_url,omitempty"`
	ExpiredAt    time.Time `json:"expired_at,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
}

type HTTPCallbackClient struct {
	client  *http.Client
	timeout time.Duration
}

func newCallbackClient() CallbackClient {
	return &HTTPCallbackClient{
		client:  &http.Client{Timeout: 15 * time.Second},
		timeout: 15 * time.Second,
	}
}

func (c *HTTPCallbackClient) Notify(ctx context.Context, callbackURL string, payload ExportCallbackPayload) error {
	callbackURL = strings.TrimSpace(callbackURL)
	if callbackURL == "" {
		return nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("callback failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}
