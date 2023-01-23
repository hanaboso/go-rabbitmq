package rabbitmq

import (
	"fmt"
	"github.com/hanaboso/go-log/pkg/zap"
	"github.com/hanaboso/go-rabbitmq/pkg/rabbitmq"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"time"
)

var client *rabbitmq.Client

func init() {
	client = newClient()
}

func newClient() *rabbitmq.Client {
	return rabbitmq.NewClient(os.Getenv("RABBITMQ_DSN"), zap.NewLogger(), true)
}

func closeConnection() {
	host := os.Getenv("RABBITMQ_API")
	httpClient := &http.Client{}

	for i := 0; i < 10; i++ {
		<-time.After(1 * time.Second)
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/connections", host), nil)
		req.Header.Set("Authorization", "Basic Z3Vlc3Q6Z3Vlc3Q=")
		res, err := httpClient.Do(req)
		if err != nil {
			continue
		}

		body, _ := ioutil.ReadAll(res.Body)
		regular := regexp.MustCompile("name\":\\s*\"(.+?)\"")
		name := regular.FindStringSubmatch(string(body))
		if len(name) <= 0 {
			continue
		}

		req, _ = http.NewRequest("DELETE", fmt.Sprintf("%s/api/connections/%s", host, name[1]), nil)
		req.Header.Set("Authorization", "Basic Z3Vlc3Q6Z3Vlc3Q=")
		_, _ = httpClient.Do(req)
		client.AwaitConnect()
		break
	}
}
