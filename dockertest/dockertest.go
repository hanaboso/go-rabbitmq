package dockertest

import (
	"fmt"
	"runtime"

	"github.com/ory/dockertest/v3"
)

func WithTag(tag string) func(d *dockerResource) error {
	return func(d *dockerResource) error {
		d.tag = tag
		return nil
	}
}

type dockerResource struct {
	pool       *dockertest.Pool
	resource   *dockertest.Resource
	networkID  string
	repository string
	tag        string
}

func (d *dockerResource) Image() string {
	return fmt.Sprintf("%s:%s", d.repository, d.tag)
}

func (d *dockerResource) GetContainerIP() string {
	if d.networkID != "" {
		container, ok := d.resource.Container.NetworkSettings.Networks[d.networkID]
		if ok {
			return container.IPAddress
		}
	}

	return d.resource.Container.NetworkSettings.IPAddress
}

func (d *dockerResource) Addr(tcpPort int) string {
	if runtime.GOOS == "darwin" {
		port := fmt.Sprintf("%d/tcp", tcpPort)
		return fmt.Sprintf("localhost:%s", d.resource.GetPort(port))
	}

	return fmt.Sprintf("%s:%d", d.GetContainerIP(), tcpPort)
}

func (d *dockerResource) Close() error {
	if d.pool == nil || d.resource == nil {
		return nil
	}

	if err := d.pool.Purge(d.resource); err != nil {
		return fmt.Errorf("failed to purge docker dockerResource: %w", err)
	}

	return nil
}
