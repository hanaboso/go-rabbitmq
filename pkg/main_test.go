package pkg_test

import (
	"os"
	"testing"
)

func DSNForTest(t *testing.T) string {
	return os.Getenv("RABBIT_DSN")
}
