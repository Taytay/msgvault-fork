package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"go.kenn.io/msgvault/internal/store"
)

var createSubsetCmd = &cobra.Command{
	Use:   "create-subset",
	Short: "Create a smaller database from the archive",
	Long: `Create a new msgvault database containing a subset of the
most recent messages. Useful for testing, demos, or sharing.

The destination directory will contain a complete msgvault.db with
all referenced data (conversations, participants, labels, etc.)
and can be used directly:

  MSGVAULT_HOME=/path/to/subset msgvault tui`,
	RunE: runCreateSubset,
}

var (
	subsetOutput string
	subsetRows   int
)

func init() {
	createSubsetCmd.Flags().StringVarP(
		&subsetOutput, "output", "o", "",
		"destination: an output directory (SQLite) or a new database name (Dolt)",
	)
	createSubsetCmd.Flags().IntVar(
		&subsetRows, "rows", 0,
		"number of most recent messages to copy",
	)
	_ = createSubsetCmd.MarkFlagRequired("output")
	_ = createSubsetCmd.MarkFlagRequired("rows")
	rootCmd.AddCommand(createSubsetCmd)
}

func runCreateSubset(cmd *cobra.Command, _ []string) error {
	if err := MustBeLocal("create-subset"); err != nil {
		return err
	}

	if subsetRows <= 0 {
		return usageErr(cmd, errors.New("--rows must be a positive integer"))
	}

	// Open the source store and ask it to subset itself. The backend's
	// SubsetExporter owns how the copy happens and what `dest` means; the
	// command never branches on backend type.
	s, err := store.OpenExisting(cfg.DatabaseDSN())
	if errors.Is(err, store.ErrDatabaseNotFound) {
		return fmt.Errorf("source database not found: %s\nRun 'msgvault init-db' and sync first", cfg.DatabaseDSN())
	}
	if err != nil {
		return fmt.Errorf("open source database: %w", err)
	}
	defer func() { _ = s.Close() }()

	exporter, ok := s.SubsetExporter()
	if !ok {
		return fmt.Errorf("create-subset is not supported on the %s backend", s.Backend())
	}

	// For SQLite, dest is a directory; resolve it to an absolute path so the
	// "to use" hint is unambiguous. For other backends dest is opaque (e.g. a
	// Dolt database name) and passed through verbatim.
	dest := subsetOutput
	if s.Backend() == store.BackendSQLite {
		if abs, err := filepath.Abs(subsetOutput); err == nil {
			dest = abs
		}
	}

	fmt.Fprintf(os.Stderr, "Copying %d messages to %s...\n", subsetRows, dest)

	result, err := exporter.ExportSubset(cmd.Context(), subsetRows, dest)
	if err != nil {
		return fmt.Errorf("create subset (destination is %s): %w", exporter.DestinationHint(), err)
	}

	fmt.Fprintf(os.Stderr,
		"Created subset (%s)\n", result.Elapsed.Round(time.Millisecond),
	)
	fmt.Printf("Sources:       %d\n", result.Sources)
	fmt.Printf("Messages:      %d\n", result.Messages)
	fmt.Printf("Conversations: %d\n", result.Conversations)
	fmt.Printf("Participants:  %d\n", result.Participants)
	fmt.Printf("Labels:        %d\n", result.Labels)
	if result.DBSize > 0 {
		fmt.Printf("Database size: %s\n", formatSize(result.DBSize))
	}

	if int64(subsetRows) > result.Messages {
		fmt.Fprintf(os.Stderr,
			"Note: requested %d messages but source only had %d\n",
			subsetRows, result.Messages,
		)
	}

	if s.Backend() == store.BackendSQLite {
		fmt.Fprintf(os.Stderr, "\nTo use: MSGVAULT_HOME=%s msgvault tui\n", dest)
	}

	return nil
}
