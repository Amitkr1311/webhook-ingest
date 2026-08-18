package ingest_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestRecordingProcessingOutlivesWebhookRequest verifies that asynchronous
// recording work is allowed to complete after the webhook handler responds.
func TestRecordingProcessingOutlivesWebhookRequest(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	if resp := post(t, srv.URL+"/webhooks/calls", eventJSON(eventID, callID, accountID)); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var processed bool
		err := st.Pool().QueryRow(context.Background(),
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed)
		if err != nil {
			t.Fatalf("read call: %v", err)
		}
		if processed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("recording was never marked processed after the webhook request completed")
}
