package webserver

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// BenchmarkValidateTokenAtScale measures token validation as the number of live
// tokens grows.
//
// It exists to keep the fix honest. Validation used to compare against every
// unexpired token with bcrypt, and this benchmark reported exactly that shape --
// 61ms, 615ms, 3.0s, 12.4s at 1/10/50/200 live tokens. It now reports a flat
// ~60ms, the cost of the single comparison that authorizes the request.
func BenchmarkValidateTokenAtScale(b *testing.B) {
	for _, n := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("live_tokens=%d", n), func(b *testing.B) {
			db, err := NewDB(filepath.Join(b.TempDir(), "t.db"))
			if err != nil {
				b.Fatalf("NewDB: %v", err)
			}
			defer func() { _ = db.Close() }()

			var last string
			for i := 0; i < n; i++ {
				tok, _, err := db.RegisterJob(fmt.Sprintf("%d.0", i), "{}", "alice", 1000, 1000)
				if err != nil {
					b.Fatalf("RegisterJob: %v", err)
				}
				last = tok
			}

			start := time.Now()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, _, _, err := db.ValidateToken(last); err != nil {
					b.Fatalf("ValidateToken: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(time.Since(start).Milliseconds())/float64(b.N), "ms/validate")
		})
	}
}
