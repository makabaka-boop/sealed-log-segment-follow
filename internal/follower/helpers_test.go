package follower

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newHTTPTestServer(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

// syncBarrierHTTP drives the test barrier endpoint so each assertion happens
// strictly after the followers have processed the on-disk change.
func syncBarrierHTTP(t *testing.T, baseURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/test/read-barrier", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("read barrier: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("read barrier status: %d", resp.StatusCode)
	}
	// Server-side flush of the SSE responses happens after the same completed
	// poll, but give writes a brief scheduling window without weakening
	// assertions on event content or offsets.
	time.Sleep(2 * time.Millisecond)
}
