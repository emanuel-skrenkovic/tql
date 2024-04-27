package main

import (
	"fmt"
	"log"
	"os"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

type LocalTestFixture struct {
	dockerComposePath string
	compose           tc.DockerCompose
	waitParams        map[string]waitParams
}

type waitParams struct {
	service          string
	connectionString string
	driver           string
	port             int
}

type TestFixtureOpt func(*LocalTestFixture)

func WithWaitDBFunc(serviceName, connectionString, driver string, port int) TestFixtureOpt {
	return func(f *LocalTestFixture) {
		f.waitParams[serviceName] = waitParams{
			service:          serviceName,
			connectionString: connectionString,
			driver:           driver,
			port:             port,
		}
	}
}

func NewLocalTestFixture(dockerComposePath string, opts ...TestFixtureOpt) (LocalTestFixture, error) {
	compose := tc.NewLocalDockerCompose(
		[]string{dockerComposePath},
		uuid.New().String(),
	)

	fixture := LocalTestFixture{
		dockerComposePath: dockerComposePath,
		waitParams:        make(map[string]waitParams, len(compose.Services)),
	}

	for _, opt := range opts {
		opt(&fixture)
	}

	for serviceName := range compose.Services {
		if params, ok := fixture.waitParams[serviceName]; ok {
			p := fmt.Sprintf("%d", params.port)

			compose.WithExposedService(
				serviceName,
				params.port,
				wait.ForSQL(nat.Port(p), params.driver, func(string, nat.Port) string {
					return params.connectionString
				}),
			)
		}
	}

	fixture.compose = compose.WithCommand([]string{"up", "--build", "-d"})

	return fixture, nil
}

func (f *LocalTestFixture) Start() error {
	log.Println(os.Getenv("SKIP_INFRASTRUCTURE"))
	if skip := os.Getenv("SKIP_INFRASTRUCTURE"); skip == "true" {
		return nil
	}

	execErr := f.compose.Invoke()
	return execErr.Error
}

func (f *LocalTestFixture) Stop() error {
	if skip := os.Getenv("SKIP_INFRASTRUCTURE"); skip == "true" {
		return nil
	}

	return f.compose.Down().Error
}
