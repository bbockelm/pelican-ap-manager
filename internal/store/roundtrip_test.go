package store

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/pelican-ap-manager/internal/control"
	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/google/go-cmp/cmp"
)

// serveDB runs a real dbrpc server over an in-process pipe and returns a client
// speaking to it.
//
// This is the test that was missing. Everything else in this package exercises
// pure functions -- what a rule serializes to, which rows changed -- or the
// path where the connection fails. Neither can see whether the server accepts
// what we send, and it did not: writes went out in the bracketed new-ClassAd
// form while dbrpc parses written ads with classad.ParseOld, so every write
// failed at the server with a syntax error. Nothing here could fail until a
// real server was on the other end.
func serveDB(t *testing.T) (*dbrpc.Client, string) {
	t.Helper()

	d, err := db.Open("")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	srv := dbrpc.NewServer(d)
	c1, c2 := net.Pipe()
	go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(c2)) }()

	client := dbrpc.NewClient(dbrpc.NewStreamConn(c1))
	t.Cleanup(func() {
		_ = client.Close()
		srv.Close()
		_ = d.Close()
	})
	// A single-table server serves exactly dbrpc.DefaultTable, which is all these
	// tests need: the table name is the store's business, the wire format is
	// what is under test.
	return client, dbrpc.DefaultTable
}

// TestRuleStoreRoundTripsThroughARealServer: write a rule, read it back, get the
// same rule.
func TestRuleStoreRoundTripsThroughARealServer(t *testing.T) {
	client, table := serveDB(t)
	s := &DBStore{table: table, client: client}
	ctx := context.Background()

	want := ratelimit.Rule{
		Name: "bbockelm", Origin: ratelimit.OriginStatic,
		User: "bbockelm", RateCount: 1, RateWindow: time.Minute,
		ConfigManaged: true, UpdatedAt: time.Unix(1787431818, 0),
	}
	if err := s.PutRule(ctx, want); err != nil {
		t.Fatalf("PutRule: %v", err)
	}

	got, err := s.ListRules(ctx)
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("%d rules stored, want 1", len(got))
	}
	if diff := cmp.Diff(want, got[0]); diff != "" {
		t.Errorf("rule did not survive the round trip:\n%s", diff)
	}

	// And it is an upsert, not an append.
	want.RateCount = 5
	if err := s.PutRule(ctx, want); err != nil {
		t.Fatalf("PutRule (update): %v", err)
	}
	got, _ = s.ListRules(ctx)
	if len(got) != 1 || got[0].RateCount != 5 {
		t.Errorf("after update: %d rules, first rate %d; want 1 rule at 5", len(got), got[0].RateCount)
	}

	if err := s.DeleteRule(ctx, want.Name); err != nil {
		t.Fatalf("DeleteRule: %v", err)
	}
	if got, _ = s.ListRules(ctx); len(got) != 0 {
		t.Errorf("%d rules after delete, want 0", len(got))
	}
}

// TestStateStoreRoundTripsThroughARealServer: the whole sectioned state has to
// survive a save and a load against a server that actually parses it.
func TestStateStoreRoundTripsThroughARealServer(t *testing.T) {
	client, table := serveDB(t)
	s := &DBStateStore{table: table, client: client, written: map[string]string{}}
	ctx := context.Background()

	st := state.New()
	st.RestoreSections(populatedSections())
	if err := s.Save(ctx, st); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// A fresh store, so nothing is served from the write cache.
	reader := &DBStateStore{table: s.table, client: s.client, written: map[string]string{}}
	loaded, err := reader.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if diff := cmp.Diff(st.Sections(), loaded.Sections()); diff != "" {
		t.Errorf("state did not survive a real save/load:\n%s", diff)
	}
}

// TestLoadSeedsTheCacheWithWhatSaveWouldWrite: Load re-serializes what it read to
// seed the dirty-row cache, and Save compares against it byte for byte. If the
// two serializations differ at all, the first Save after a restart rewrites
// every row -- correct, but it silently gives up the reason the rows exist.
func TestLoadSeedsTheCacheWithWhatSaveWouldWrite(t *testing.T) {
	client, table := serveDB(t)
	ctx := context.Background()

	writer := &DBStateStore{table: table, client: client, written: map[string]string{}}
	st := state.New()
	st.RestoreSections(populatedSections())
	if err := writer.Save(ctx, st); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reader := &DBStateStore{table: table, client: client, written: map[string]string{}}
	loaded, err := reader.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Saving the state we just loaded must be a complete no-op.
	rows, err := stateRows(loaded.Sections())
	if err != nil {
		t.Fatalf("stateRows: %v", err)
	}
	changed, gone := diffRows(reader.written, rows)
	if len(changed) != 0 || len(gone) != 0 {
		t.Errorf("re-saving a just-loaded state would write %d rows and delete %d; want none.\nchanged: %v",
			len(changed), len(gone), rowKeys(changed))
	}
}

// TestPairStatesAreQueryableOnTheServer: storing per-pair rows is only worth it
// if the server can select them, which needs the attributes to have survived as
// attributes rather than as text.
func TestPairStatesAreQueryableOnTheServer(t *testing.T) {
	client, table := serveDB(t)
	ctx := context.Background()

	s := &DBStateStore{table: table, client: client, written: map[string]string{}}
	st := state.New()
	st.RestoreSections(state.Sections{
		PairStates: map[string]control.PairState{
			"alice|UCSD": {CapacityGBPerMin: 4.5, LastUpdated: time.Unix(1_700_000_000, 0)},
			"bob|PSU":    {CapacityGBPerMin: 2, LastUpdated: time.Unix(1_700_000_000, 0)},
		},
	})
	if err := s.Save(ctx, st); err != nil {
		t.Fatalf("Save: %v", err)
	}

	rows, err := client.QueryTable(ctx, table, `Kind == "pair"`, 0)
	if err != nil {
		t.Fatalf("QueryTable: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("%d pair rows matched Kind == \"pair\", want 2", len(rows))
	}

	rows, err = client.QueryTable(ctx, table, `Kind == "pair" && CapacityGBPerMin > 3.0`, 0)
	if err != nil {
		t.Fatalf("QueryTable: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("%d rows over 3 GB/min, want 1 -- CapacityGBPerMin is not queryable as a number", len(rows))
	}
}

// TestRowsCarryTheirKey: htcondordb's REPL takes "a row's primary key lives in
// the Key attribute" as its convention. Rows written without one show up as
// `Key | undefined` in SELECT *, and UPDATE/DELETE by constraint has to resolve
// keys server-side instead of reading them off the matched rows. Since managing
// rules by hand in SQL is the supported path, the rows have to be addressable
// that way.
func TestRowsCarryTheirKey(t *testing.T) {
	client, table := serveDB(t)
	ctx := context.Background()

	// Rules.
	rs := &DBStore{table: table, client: client}
	if err := rs.PutRule(ctx, ratelimit.Rule{
		Name: "bbockelm", Origin: ratelimit.OriginStatic,
		User: "bbockelm", RateCount: 1, RateWindow: time.Minute,
	}); err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	rows, err := client.QueryTable(ctx, table, `Key == "bbockelm"`, 0)
	if err != nil {
		t.Fatalf("QueryTable: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("%d rules matched Key == \"bbockelm\", want 1 -- the rule row carries no Key", len(rows))
	}

	// State rows, whose keys are structured (cursor, pair:..., scratch:...).
	client2, table2 := serveDB(t)
	ss := &DBStateStore{table: table2, client: client2, written: map[string]string{}}
	st := state.New()
	st.RestoreSections(populatedSections())
	if err := ss.Save(ctx, st); err != nil {
		t.Fatalf("Save: %v", err)
	}

	for _, key := range []string{"cursor", "pair:alice|UCSD", "scratch:recent_transfers"} {
		got, qerr := client2.QueryTable(ctx, table2, `Key == "`+key+`"`, 0)
		if qerr != nil {
			t.Fatalf("QueryTable %s: %v", key, qerr)
		}
		if len(got) != 1 {
			t.Errorf("%d rows matched Key == %q, want 1", len(got), key)
		}
	}

	// And the key is still what Load reads rows back by, so stamping it did not
	// change which row is which.
	reader := &DBStateStore{table: table2, client: client2, written: map[string]string{}}
	loaded, err := reader.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if diff := cmp.Diff(st.Sections(), loaded.Sections()); diff != "" {
		t.Errorf("state changed once rows carried a Key:\n%s", diff)
	}
}
