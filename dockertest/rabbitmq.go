package dockertest

import (
	"fmt"
	"os"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	dockerResource
}

// GetMariaDB returns MariaDB instance as singleton
func GetRabbitMQ(containerName string, opts ...func(*dockerResource) error) (*RabbitMQ, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("failed to create a docker pool: %w", err)
	}
	pool.MaxWait = 2 * time.Minute // default is 1min

	mq := RabbitMQ{
		dockerResource: dockerResource{
			pool:       pool,
			networkID:  os.Getenv("TEST_NETWORK"),
			repository: "rabbitmq",
			tag:        "3.7.14-alpine",
		},
	}

	for _, opt := range opts {
		if err := opt(&mq.dockerResource); err != nil {
			return nil, err
		}
	}

	if err := mq.LoadOrCreateResource(containerName); err != nil {
		return nil, fmt.Errorf("failed to get docker resource: %w", err)
	}

	if err := mq.pool.Retry(rabbitMQReadyCheck(&mq)); err != nil {
		if purgeErr := mq.pool.Purge(mq.resource); purgeErr != nil {
			return nil, fmt.Errorf("failed to start resource and also failed to purge it: %v: %w", purgeErr, err)
		}
		return nil, fmt.Errorf("failed to start mariadb resource: %w", err)
	}

	return &mq, nil
}

func (r *RabbitMQ) DSN() string {
	return fmt.Sprintf("amqp://guest:guest@%s/", r.Addr(5672))
}

func (r *RabbitMQ) LoadOrCreateResource(containerName string) error {
	// TODO load

	res, err := r.pool.RunWithOptions(&dockertest.RunOptions{
		Repository: r.repository,
		Tag:        r.tag,
		Name:       containerName,
	})
	if err != nil {
		return fmt.Errorf("failed to create new container: %w", err)
	}

	r.resource = res

	return nil
}

func rabbitMQReadyCheck(md *RabbitMQ) func() error {
	return func() (err error) {
		conn, err := amqp.Dial(md.DSN())
		if err != nil {
			return fmt.Errorf("failed to open DB connection: %w", err)
		}

		defer func() {
			if conn.Close() != nil && err == nil {
				err = fmt.Errorf("failed to close database connection: %w", err)
			}
		}()

		return nil
	}
}
