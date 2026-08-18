package ingest_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/testutil"
)

type gracefulShutdown interface {
	Shutdown(context.Context) error
}

// TestShutdownWaitsForRecordingProcessing verifies that the service drains
// accepted recording work before its Postgres dependency is closed.
func TestShutdownWaitsForRecordingProcessing(t *testing.T) {
	observer := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, observer)
	st := testutil.NewStore(t)

	cfg := config.Load()
	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	svc := ingest.New(st, stats.NewCache(), rdb, slog.New(slog.NewTextHandler(io.Discard, nil)))
	event := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  100,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Now().UTC(),
	}
	if err := svc.Ingest(context.Background(), event); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	if svc, ok := any(svc).(gracefulShutdown); ok {
		if err := svc.Shutdown(context.Background()); err != nil {
			t.Fatalf("drain service: %v", err)
		}
	}
	st.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var processed bool
		err := observer.Pool().QueryRow(context.Background(),
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed)
		if err != nil {
			t.Fatalf("read call: %v", err)
		}
		if processed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("recording work was lost when the service dependency closed")
}
