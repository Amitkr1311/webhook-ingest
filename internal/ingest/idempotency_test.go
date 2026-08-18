package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestDuplicateWebhookDoesNotDoubleCountStats verifies that processing the
// same event_id twice does not increment account_stats more than once.
// This reproduces the production symptom: "Account call counts drift higher
// than the actual number of calls."
func TestDuplicateWebhookDoesNotDoubleCountStats(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  100,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)

	// Send the same event 3 times sequentially.
	for i := 0; i < 3; i++ {
		resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	// There must be exactly 1 event row.
	var eventCount int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Errorf("events: stored %d copies, want 1", eventCount)
	}

	// The account stats must show exactly 1 call, not 3.
	var callCount int64
	var totalDuration int64
	row = st.Pool().QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`, accountID)
	if err := row.Scan(&callCount, &totalDuration); err != nil {
		t.Fatalf("read account_stats: %v", err)
	}
	if callCount != 1 {
		t.Errorf("account_stats.call_count = %d, want 1", callCount)
	}
	if totalDuration != 100 {
		t.Errorf("account_stats.total_duration_sec = %d, want 100", totalDuration)
	}
}

// TestConcurrentDuplicateWebhooks verifies that concurrent submissions of the
// same event_id result in exactly one logical processing. This reproduces the
// TOCTOU race between EventExists and InsertEvent.
func TestConcurrentDuplicateWebhooks(t *testing.T) {
	srv, st := testutil.NewServer(t)
	ctx := context.Background()

	// Use many distinct event IDs each submitted concurrently to increase the
	// chance of hitting the TOCTOU race.
	const numEvents = 5
	const concurrency = 10

	type eventInfo struct {
		eventID   string
		callID    string
		accountID string
	}
	events := make([]eventInfo, numEvents)
	for i := 0; i < numEvents; i++ {
		events[i] = eventInfo{
			eventID:   fmt.Sprintf("evt_conc_%s_%d", t.Name(), i),
			callID:    fmt.Sprintf("call_conc_%s_%d", t.Name(), i),
			accountID: fmt.Sprintf("acc_conc_%s_%d", t.Name(), i),
		}
		// Clean up these rows before and after.
		acctID := events[i].accountID
		evtID := events[i].eventID
		callID := events[i].callID
		clean := func() {
			st.Pool().Exec(ctx, `DELETE FROM events WHERE event_id = $1`, evtID)
			st.Pool().Exec(ctx, `DELETE FROM calls WHERE call_id = $1`, callID)
			st.Pool().Exec(ctx, `DELETE FROM account_stats WHERE account_id = $1`, acctID)
		}
		clean()
		t.Cleanup(clean)
	}

	var wg sync.WaitGroup
	// For each event, submit it `concurrency` times concurrently.
	for _, e := range events {
		body := fmt.Sprintf(`{
		  "event_id":      %q,
		  "call_id":       %q,
		  "account_id":    %q,
		  "status":        "completed",
		  "duration_sec":  60,
		  "recording_url": "https://recordings.example.com/%s.wav",
		  "occurred_at":   "2026-08-13T09:12:00Z"
		}`, e.eventID, e.callID, e.accountID, e.callID)

		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
				if err != nil {
					t.Errorf("post: %v", err)
					return
				}
				resp.Body.Close()
			}()
		}
	}
	wg.Wait()

	// Verify each event has exactly 1 row and exactly 1 stat increment.
	for _, e := range events {
		var eventCount int
		row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, e.eventID)
		if err := row.Scan(&eventCount); err != nil {
			t.Fatalf("count events for %s: %v", e.eventID, err)
		}
		if eventCount != 1 {
			t.Errorf("events for %s: stored %d copies, want 1", e.eventID, eventCount)
		}

		var statsCallCount int64
		row = st.Pool().QueryRow(ctx,
			`SELECT call_count FROM account_stats WHERE account_id = $1`, e.accountID)
		if err := row.Scan(&statsCallCount); err != nil {
			t.Fatalf("read account_stats for %s: %v", e.accountID, err)
		}
		if statsCallCount != 1 {
			t.Errorf("account_stats.call_count for %s = %d, want 1", e.accountID, statsCallCount)
		}
	}
}
