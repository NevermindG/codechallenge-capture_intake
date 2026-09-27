package downstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type HTTPClient struct {
	url    string
	client *http.Client
}

func NewHTTPClient(url string, timeout time.Duration) *HTTPClient {
	return &HTTPClient{url: url, client: &http.Client{Timeout: timeout}}
}

func (c *HTTPClient) Deliver(ctx context.Context, deliveryKey string, eventType string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build downstream request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Delivery-Key", deliveryKey)
	req.Header.Set("X-Event-Type", eventType)
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("downstream request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("downstream status %d", resp.StatusCode)
	}
	return nil
}
