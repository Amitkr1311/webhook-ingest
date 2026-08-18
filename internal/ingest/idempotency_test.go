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

// TestConcurrentDuplicateWebhookIsProcessedOnce keeps concurrent inserts open
// long enough for every request to finish the non-atomic duplicate check. It
// verifies the provider's at-least-once delivery contract under contention.
func TestConcurrentDuplicateWebhookIsProcessedOnce(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	_, err := st.Pool().Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION delay_test_event_insert() RETURNS trigger AS $$
		BEGIN
			IF NEW.event_id = '%s' THEN
				PERFORM pg_sleep(0.25);
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER delay_test_event_insert_trigger
		BEFORE INSERT ON events
		FOR EACH ROW EXECUTE FUNCTION delay_test_event_insert();`, eventID))
	if err != nil {
		t.Fatalf("install insert delay: %v", err)
	}
	t.Cleanup(func() {
		if _, err := st.Pool().Exec(context.Background(), `
			DROP TRIGGER IF EXISTS delay_test_event_insert_trigger ON events;
			DROP FUNCTION IF EXISTS delay_test_event_insert();`); err != nil {
			t.Errorf("remove insert delay: %v", err)
		}
	})

	body := fmt.Sprintf(`{
	  "event_id": %q,
	  "call_id": %q,
	  "account_id": %q,
	  "status": "completed",
	  "duration_sec": 100,
	  "occurred_at": "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID)

	const deliveries = 10
	start := make(chan struct{})
	errCh := make(chan error, deliveries)
	var wg sync.WaitGroup
	for range deliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				errCh <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("got status %d, want 200", resp.StatusCode)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	var eventCount int
	if err := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Errorf("stored %d event rows, want 1", eventCount)
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("read account stats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 100 {
		t.Errorf("account stats = %+v, want one 100-second call", got)
	}
}
