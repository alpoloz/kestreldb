package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

const defaultRPCTimeout = 5 * time.Second

type Client struct {
	httpClient *resty.Client
	timeout    time.Duration
}

func Dial(addr string) (*Client, error) {
	baseURL := normalizeBaseURL(addr)
	httpClient := resty.New().
		SetBaseURL(baseURL).
		SetHeader("Content-Type", "application/json")

	return &Client{
		httpClient: httpClient,
		timeout:    defaultRPCTimeout,
	}, nil
}

func (c *Client) Close() error {
	c.httpClient.GetClient().CloseIdleConnections()
	return nil
}

func (c *Client) Ping(ctx context.Context) (*PingResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &PingResponse{}
	httpResp, err := c.request(callCtx).
		SetResult(resp).
		Get("/v1/ping")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) HashSet(ctx context.Context, key string, field string, value []byte) (*HashSetResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &HashSetResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetPathParam("field", field).
		SetBody(hashSetRequest{Value: string(value)}).
		SetResult(resp).
		Put("/v1/hashes/{key}/fields/{field}")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) HashGet(ctx context.Context, key string, field string) (*HashGetResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &HashGetResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetPathParam("field", field).
		SetResult(resp).
		Get("/v1/hashes/{key}/fields/{field}")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) HashDelete(ctx context.Context, key string, field string) (*HashDeleteResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &HashDeleteResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetPathParam("field", field).
		SetResult(resp).
		Delete("/v1/hashes/{key}/fields/{field}")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) HashLen(ctx context.Context, key string) (*HashLenResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &HashLenResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetResult(resp).
		Get("/v1/hashes/{key}/length")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) HashGetAll(ctx context.Context, key string) (*HashGetAllResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &HashGetAllResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetResult(resp).
		Get("/v1/hashes/{key}")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) SortedSetAdd(ctx context.Context, key string, score float64, member string) (*SortedSetAddResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &SortedSetAddResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetPathParam("member", member).
		SetBody(sortedSetAddRequest{Score: score}).
		SetResult(resp).
		Put("/v1/sorted-sets/{key}/members/{member}")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) SortedSetRemove(ctx context.Context, key string, member string) (*SortedSetRemoveResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &SortedSetRemoveResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetPathParam("member", member).
		SetResult(resp).
		Delete("/v1/sorted-sets/{key}/members/{member}")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) SortedSetScore(ctx context.Context, key string, member string) (*SortedSetScoreResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &SortedSetScoreResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetPathParam("member", member).
		SetResult(resp).
		Get("/v1/sorted-sets/{key}/members/{member}")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) SortedSetCardinality(ctx context.Context, key string) (*SortedSetCardinalityResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &SortedSetCardinalityResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetResult(resp).
		Get("/v1/sorted-sets/{key}/cardinality")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) SortedSetRange(ctx context.Context, key string, start int64, stop int64) (*SortedSetRangeResponse, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()

	resp := &SortedSetRangeResponse{}
	httpResp, err := c.request(callCtx).
		SetPathParam("key", key).
		SetQueryParams(map[string]string{
			"start": fmt.Sprintf("%d", start),
			"stop":  fmt.Sprintf("%d", stop),
		}).
		SetResult(resp).
		Get("/v1/sorted-sets/{key}/range")
	if err != nil {
		return nil, err
	}
	if err := checkHTTPError(httpResp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) request(ctx context.Context) *resty.Request {
	return c.httpClient.R().SetContext(ctx)
}

func (c *Client) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, c.timeout)
}

func normalizeBaseURL(addr string) string {
	trimmed := strings.TrimSpace(addr)
	trimmed = strings.TrimRight(trimmed, "/")
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		return trimmed
	}
	return "http://" + trimmed
}

func checkHTTPError(resp *resty.Response) error {
	if resp == nil {
		return errors.New("request failed: no response")
	}
	if resp.IsSuccess() {
		return nil
	}

	var errResp errorResponse
	if err := json.Unmarshal(resp.Body(), &errResp); err == nil && errResp.Error != "" {
		return fmt.Errorf("server error (%d): %s", resp.StatusCode(), errResp.Error)
	}
	return fmt.Errorf("server error (%d): %s", resp.StatusCode(), strings.TrimSpace(resp.String()))
}

type PingResponse struct {
	Message string `json:"message"`
}

func (r *PingResponse) GetMessage() string {
	if r == nil {
		return ""
	}
	return r.Message
}

type hashSetRequest struct {
	Value string `json:"value"`
}

type HashSetResponse struct {
	Created bool `json:"created"`
}

func (r *HashSetResponse) GetCreated() bool {
	if r == nil {
		return false
	}
	return r.Created
}

type HashGetResponse struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

func (r *HashGetResponse) GetValue() []byte {
	if r == nil {
		return nil
	}
	return []byte(r.Value)
}

func (r *HashGetResponse) GetFound() bool {
	if r == nil {
		return false
	}
	return r.Found
}

type HashDeleteResponse struct {
	Deleted bool `json:"deleted"`
}

func (r *HashDeleteResponse) GetDeleted() bool {
	if r == nil {
		return false
	}
	return r.Deleted
}

type HashLenResponse struct {
	Length uint64 `json:"length"`
}

func (r *HashLenResponse) GetLength() uint64 {
	if r == nil {
		return 0
	}
	return r.Length
}

type HashEntry struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

func (e *HashEntry) GetField() string {
	if e == nil {
		return ""
	}
	return e.Field
}

func (e *HashEntry) GetValue() []byte {
	if e == nil {
		return nil
	}
	return []byte(e.Value)
}

type HashGetAllResponse struct {
	Entries []*HashEntry `json:"entries"`
}

func (r *HashGetAllResponse) GetEntries() []*HashEntry {
	if r == nil {
		return nil
	}
	return r.Entries
}

type sortedSetAddRequest struct {
	Score float64 `json:"score"`
}

type SortedSetAddResponse struct {
	Added bool `json:"added"`
}

func (r *SortedSetAddResponse) GetAdded() bool {
	if r == nil {
		return false
	}
	return r.Added
}

type SortedSetRemoveResponse struct {
	Removed bool `json:"removed"`
}

func (r *SortedSetRemoveResponse) GetRemoved() bool {
	if r == nil {
		return false
	}
	return r.Removed
}

type SortedSetScoreResponse struct {
	Score float64 `json:"score"`
	Found bool    `json:"found"`
}

func (r *SortedSetScoreResponse) GetScore() float64 {
	if r == nil {
		return 0
	}
	return r.Score
}

func (r *SortedSetScoreResponse) GetFound() bool {
	if r == nil {
		return false
	}
	return r.Found
}

type SortedSetCardinalityResponse struct {
	Count uint64 `json:"count"`
}

func (r *SortedSetCardinalityResponse) GetCount() uint64 {
	if r == nil {
		return 0
	}
	return r.Count
}

type SortedSetMember struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

func (m *SortedSetMember) GetMember() string {
	if m == nil {
		return ""
	}
	return m.Member
}

func (m *SortedSetMember) GetScore() float64 {
	if m == nil {
		return 0
	}
	return m.Score
}

type SortedSetRangeResponse struct {
	Items []*SortedSetMember `json:"items"`
}

func (r *SortedSetRangeResponse) GetItems() []*SortedSetMember {
	if r == nil {
		return nil
	}
	return r.Items
}

type errorResponse struct {
	Error string `json:"error"`
}
