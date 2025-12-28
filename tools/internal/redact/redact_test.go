package redact

import (
	"testing"
)

func TestExprPathsAreNotSanitized(t *testing.T) {
	r := NewRedactor()
	path := "/Expr((ExitCode isnt 0))/"

	if got := r.redactPath(path, false); got != path {
		t.Fatalf("/Expr path should pass through, got %s", got)
	}
}
