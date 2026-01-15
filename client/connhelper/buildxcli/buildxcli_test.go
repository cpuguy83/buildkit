package buildxcli

import (
	"context"
	"net"
	"os/exec"
	"testing"

	"github.com/moby/buildkit/identity"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestCLI(t *testing.T) {
	t.Parallel()

	spec, shutdown := setupTestBuilder(t)
	defer shutdown()

	h, err := Helper(spec.ToURL())
	require.NoError(t, err)

	client, err := grpc.NewClient("passthrough://", grpc.WithContextDialer(h.ContextDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Close()

	hpbClient := healthpb.NewHealthClient(client)
	t.Log("checking health...")
	_, err = hpbClient.Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	t.Log("ok...")

	conn, err := h.ContextDialer(context.Background(), "")
	require.NoError(t, err)
	defer conn.Close()

	shutdown()

	// Buildx tries *really* hard to connect
	// Even if the service isn't there, or we reject new conns... it still tries to connect.
	// So the only way to test the error condition without hitting a 30s timeout is to cancel the context.
	// Technically we could use a timeout, but this are in affect testing the same thing.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = h.ContextDialer(ctx, "")
	require.ErrorIs(t, err, context.Canceled)
}

func setupTestBuilder(t *testing.T) (_ Spec, rejectClients func()) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		l.Close() //nolint:errcheck
	})

	srv := grpc.NewServer()
	t.Cleanup(srv.Stop)

	healthpb.RegisterHealthServer(srv, &testHealthServer{})

	go func() {
		srv.Serve(l) //nolint:errcheck
	}()

	id := "test-" + identity.NewID()
	cfgDir := t.TempDir()

	cmd := exec.Command("docker", "--config", cfgDir, "buildx", "create", "--name", id, "--driver=remote")
	cmd.Env = []string{"BUILDKIT_HOST=tcp://" + l.Addr().String()}
	dt, err := cmd.CombinedOutput()
	require.NoError(t, err, string(dt))

	spec := Spec{
		Builder:   id,
		ConfigDir: cfgDir,
	}

	return spec, srv.Stop
}

type testHealthServer struct {
	healthpb.HealthServer
}

func (s *testHealthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{}, nil
}
