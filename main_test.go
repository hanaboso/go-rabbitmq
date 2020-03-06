package rabbitmq_test

import (
	"os"
	"testing"
)

func DSNForTest(t *testing.T) string {
	return os.Getenv("RABBITMQ_DSN")
}
