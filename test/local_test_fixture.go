package main

import (
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"os"
)

type LocalTestFixture struct {
	dockerComposePath string
	compose           testcontainers.DockerCompose
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
	compose := testcontainers.NewLocalDockerCompose(
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
			compose.WithExposedService(
				serviceName,
				params.port,
				wait.ForSQL(nat.Port(fmt.Sprintf("%d", params.port)), params.driver, func(nat.Port) string {
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
