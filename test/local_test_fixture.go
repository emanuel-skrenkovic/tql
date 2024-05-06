package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/docker/go-connections/nat"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

type LocalTestFixture struct {
	dockerComposePath string
	compose           tc.ComposeStack
	waitParams        map[string]waitParams
	opts              []TestFixtureOpt
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
	compose, err := tc.NewDockerCompose(dockerComposePath)
	if err != nil {
		return LocalTestFixture{}, err
	}

	fixture := LocalTestFixture{
		dockerComposePath: dockerComposePath,
		compose:           compose,
		waitParams:        make(map[string]waitParams, len(opts)),
		opts:              opts,
	}

	for _, opt := range opts {
		opt(&fixture)
	}

	for _, params := range fixture.waitParams {
		fixture.compose = fixture.compose.WaitForService(
			params.service,
			wait.ForSQL(
				nat.Port(fmt.Sprintf("%d", params.port)),
				params.driver,
				func(string, nat.Port) string {
					return params.connectionString
				},
			),
		)
	}

	return fixture, nil
}

func (f *LocalTestFixture) Start(ctx context.Context) error {
	log.Println(os.Getenv("SKIP_INFRASTRUCTURE"))
	if skip := os.Getenv("SKIP_INFRASTRUCTURE"); skip == "true" {
		return nil
	}

	return f.compose.Up(ctx)
}

func (f *LocalTestFixture) Stop(ctx context.Context) error {
	if skip := os.Getenv("SKIP_INFRASTRUCTURE"); skip == "true" {
		return nil
	}

	return f.compose.Down(ctx)
}
