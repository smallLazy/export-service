package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	exportpkg "export-service/internal/export"
	taskpkg "export-service/internal/task"
)

type HTTPCallbackClient struct {
	client  *http.Client
	timeout time.Duration
}

func NewHTTPClient() exportpkg.CallbackClient {
	return &HTTPCallbackClient{
		client:  &http.Client{Timeout: 15 * time.Second},
		timeout: 15 * time.Second,
	}
}

func (c *HTTPCallbackClient) Notify(ctx context.Context, callbackURL string, payload taskpkg.ExportCallbackPayload) error {
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
