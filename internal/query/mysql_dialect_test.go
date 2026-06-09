package query_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.kenn.io/msgvault/internal/query"
)

// TestMySQLQueryDialect_SQL pins the SQL fragments the Dolt query engine emits.
// These run without a database so a dialect typo (a stray %, the wrong time
// function) is caught even when no Dolt server is available.
func TestMySQLQueryDialect_SQL(t *testing.T) {
	d := query.MySQLQueryDialect{}

	// ? placeholders are native to MySQL — Rebind is a no-op.
	assert.Equal(t, "SELECT ? AND ?", d.Rebind("SELECT ? AND ?"))

	// Booleans are TINYINT(1); compare to 1 (not the bare column as on PG).
	assert.Equal(t, "m.has_attachments = 1", d.BoolTrueExpr("m.has_attachments"))

	// Time bucketing uses DATE_FORMAT with literal % specifiers.
	assert.Equal(t, "DATE_FORMAT(m.sent_at, '%Y')", d.TimeTruncExpression("m.sent_at", "year"))
	assert.Equal(t, "DATE_FORMAT(m.sent_at, '%Y-%m')", d.TimeTruncExpression("m.sent_at", "month"))
	assert.Equal(t, "DATE_FORMAT(m.sent_at, '%Y-%m-%d')", d.TimeTruncExpression("m.sent_at", "day"))
	assert.Equal(t, "DATE_FORMAT(m.sent_at, '%Y-%m')", d.TimeTruncExpression("m.sent_at", "unknown"))

	// FTS is reported absent so free-text degrades to LIKE on subject/snippet.
	assert.Equal(t, "SELECT 0", d.HasFTSTableSQL())
	assert.Equal(t, "", d.FTSJoin())
	expr, arg := d.BuildFTSTerm([]string{"hello"})
	assert.Equal(t, "FALSE", expr)
	assert.Equal(t, "", arg)
	assert.Equal(t, "", d.SanitizeFTSQuery("hello"))
}
