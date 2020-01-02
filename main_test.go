package rabbitmq_test

import (
	"log"
	"os"
	"testing"

	dockertest "github.com/hanaboso-go/rabbitmq/dockertest"
)

var rb *dockertest.RabbitMQ

func TestMain(m *testing.M) {
	var err error

	rb, err = dockertest.GetRabbitMQ("go-libs_rabbitmq")
	if err != nil {
		log.Fatalf("Failed to create docker resource: %v", err)
	}
	code := m.Run()
	// Don't defer. os.Exit do not call defer.
	if err := rb.Close(); err != nil {
		log.Println(err)
	}
	os.Exit(code)
}

func DSNForTest(t *testing.T) string {
	// TODO connect to vhost
	return rb.DSN()
}
