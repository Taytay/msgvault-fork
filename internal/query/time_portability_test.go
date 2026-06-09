package query_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	requirepkg "github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/query"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

// TestQueryEngine_TimeAggregatePortability exercises the ViewTime aggregate,
// whose grouping key is the dialect's time-truncation expression (SQLite
// strftime, PostgreSQL to_char, Dolt/MySQL DATE_FORMAT). It runs against
// whatever backend testutil.NewTestStore selects: SQLite by default, and the
// Dolt/PostgreSQL path when MSGVAULT_TEST_DB points at one. A dialect mistake
// surfaces as a wrong bucket key or a Scan/Exec error.
func TestQueryEngine_TimeAggregatePortability(t *testing.T) {
	require := requirepkg.New(t)
	st := testutil.NewTestStore(t)
	src, err := st.GetOrCreateSource("gmail", "time@example.com")
	require.NoError(err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(src.ID, "thread-time", "Thread")
	require.NoError(err, "EnsureConversation")

	// Two messages in 2024-01, one in 2024-02.
	months := []time.Time{
		time.Date(2024, 1, 5, 9, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 20, 9, 0, 0, 0, time.UTC),
		time.Date(2024, 2, 3, 9, 0, 0, 0, time.UTC),
	}
	for i, when := range months {
		_, err := st.UpsertMessage(&store.Message{
			ConversationID:  convID,
			SourceID:        src.ID,
			SourceMessageID: gmailSourceID(i),
			MessageType:     "email",
			SentAt:          sql.NullTime{Time: when, Valid: true},
			Subject:         sql.NullString{String: subjectFor(i), Valid: true},
			SizeEstimate:    1000,
		})
		require.NoError(err, "UpsertMessage")
	}

	eng := query.NewEngineForStore(st)
	rows, err := eng.Aggregate(context.Background(), query.ViewTime, query.AggregateOptions{
		TimeGranularity: query.TimeMonth,
		SortField:       query.SortByName,
		SortDirection:   query.SortAsc,
		Limit:           10,
	})
	require.NoError(err, "Aggregate ViewTime")

	got := map[string]int64{}
	for _, r := range rows {
		got[r.Key] = r.Count
	}
	require.Equal(int64(2), got["2024-01"], "two messages bucket into 2024-01")
	require.Equal(int64(1), got["2024-02"], "one message buckets into 2024-02")
}
