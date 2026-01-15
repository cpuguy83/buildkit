package buildxcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"net/url"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/client/connhelper"
)

const (
	scheme       = "buildx"
	configDirOpt = "config"
)

func init() {
	connhelper.Register(scheme, Helper)
}

func Helper(u *url.URL) (*connhelper.ConnectionHelper, error) {
	var spec Spec
	if err := spec.FromURL(u); err != nil {
		return nil, err
	}

	return &connhelper.ConnectionHelper{
		ContextDialer: func(ctx context.Context, addr string) (_ net.Conn, retErr error) {
			cmd := spec.cmd()

			c1, c2 := net.Pipe()
			defer func() {
				if retErr != nil {
					c1.Close()
					c2.Close()
				}
			}()

			cmd.Stdin = c1
			cmd.Stdout = c1

			// Handle event stream encoded over stderr
			r, w := io.Pipe()
			cmd.Stderr = w
			defer func() {
				if retErr != nil {
					r.CloseWithError(retErr)
				}
			}()

			chWait := make(chan struct{})

			if err := cmd.Start(); err != nil {
				return nil, fmt.Errorf("failed to start buildx dialer: %w", err)
			}

			go func() {
				err := cmd.Wait()
				close(chWait)

				if err != nil {
					r.CloseWithError(fmt.Errorf("buildx dialer exited with error: %w", err))
				} else {
					r.Close()
				}

				c2.Close() //nolint:errcheck
			}()

			chDone := make(chan struct{})
			defer close(chDone)

			go func() {
				select {
				case <-ctx.Done():
					r.CloseWithError(ctx.Err())
				case <-chDone:
				}
			}()

			outConn := &buildxConn{
				Conn: c2,
				wait: chWait,
				cmd:  cmd.Process,
			}

			defer func() {
				if retErr != nil {
					outConn.Close()
				}
			}()

			for evt, err := range scanEvents(r) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}

				if err != nil {
					return nil, err
				}

				for _, v := range evt.Vertexes {
					if v.Error != "" {
						return nil, fmt.Errorf("error connecting to builder: %s", v.Error)
					}

					if v.Completed == nil {
						continue
					}

					// Successfully connected
					// We need to continue draining the pipe to ensure stderr is not blocked
					go io.Copy(io.Discard, r) //nolint:errcheck
					fmt.Fprintln(os.Stderr, "have conn")
					return outConn, nil
				}
			}

			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			return nil, errors.New("buildx dialer exited without completing dialing")
		},
	}, nil
}

type buildxConn struct {
	net.Conn
	wait <-chan struct{}

	shutdownOnce sync.Once
	cmd          *os.Process
}

func (c *buildxConn) Close() error {
	c.shutdownOnce.Do(c.shutdownBuildx)
	c.Conn.Close() //nolint:errcheck
	fmt.Fprintln(os.Stderr, "buildx dialer connection closed")
	return nil
}

func (c *buildxConn) shutdownBuildx() {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	// Try to shutdown gracefully For this docker wants 3 SIGINT's, which is how
	// docker cli plugins (which buildx is) get terminated.
	// Just sending signals rapidly is likely to cause some signals to be missed, so we need to send
	// signals then check if the process has exited.
	//
	// Once a timeout is hit (and the process hasn't exited), we'll send SIGKILL
	// If this happens it is not ideal since the docker CLI's sub-process will often
	// be left behind.

	for {
		select {
		case <-c.wait:
			return
		case <-timer.C:
			c.cmd.Kill() //nolint:errcheck
			return
		default:
		}

		err := c.cmd.Signal(os.Interrupt)
		if err != nil {
			// If we are getting any error, we probably can't signal the process.
			if !errors.Is(err, os.ErrProcessDone) {
				c.cmd.Kill() //nolint:errcheck
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func scanEvents(r io.Reader) iter.Seq2[*client.SolveStatus, error] {
	return func(yield func(*client.SolveStatus, error) bool) {
		dec := json.NewDecoder(r)
		var evt client.SolveStatus

		first := true
		for {
			err := dec.Decode(&evt)
			if err != nil {
				if errors.Is(err, io.EOF) {
					// Nothing else to yield
					return
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					yield(nil, err)
					return
				}

				// Read any buffered data for better error context
				dt, _ := io.ReadAll(dec.Buffered())

				if first {
					var jse *json.SyntaxError
					if errors.As(err, &jse) {
						// Probably this is data before first JSON object
						// Just return the raw buffered data
						yield(nil, errors.New(string(dt)))
						return
					}
				}

				yield(nil, fmt.Errorf("error reading json events: %w: %s", err, string(dt)))
				return
			}

			first = false

			if !yield(&evt, nil) {
				return
			}
		}
	}
}

type Spec struct {
	Builder   string
	ConfigDir string
}

func (s *Spec) ToURL() *url.URL {
	u := &url.URL{
		Host: s.Builder,
	}

	v := url.Values{}
	v.Add(configDirOpt, s.ConfigDir)
	u.RawQuery = v.Encode()

	return u
}

func (s *Spec) FromURL(u *url.URL) error {
	if u.Scheme != "" && u.Scheme != scheme {
		return fmt.Errorf("invalid scheme %q, expected %q", u.Scheme, scheme)
	}

	q := u.Query()
	switch len(q) {
	case 0:
	case 1:
		_, ok := q[configDirOpt]
		if !ok {
			return fmt.Errorf("unknown query options %q", u.RawQuery)
		}
	default:
		return fmt.Errorf("unknown query options %q", u.RawQuery)
	}

	s.Builder = u.Host
	s.ConfigDir = q.Get(configDirOpt)

	return nil
}

func (s *Spec) cmd() *exec.Cmd {
	args := make([]string, 0, 4)
	if s.ConfigDir != "" {
		// this flag is for the docker CLI itself, so it needs to go before buildx
		args = append(args, "--config", s.ConfigDir)
	}

	args = append(args, "buildx")

	if s.Builder != "" {
		args = append(args, "--builder", s.Builder)
	}
	args = append(args, []string{"dial-stdio", "--progress=rawjson"}...)

	// NOTE: Do *not* use exec.CommandContext here as it will prevent proper cleanup of the process
	// or more specifically, the subprocess it spawns.
	// This is because go sends SIGKILL forcing the process to exit immediately, which prevents
	// the buildx dial-stdio process from cleaning up its resources properly.
	cmd := exec.Command("docker", args...)
	cmd.Env = os.Environ()

	return cmd
}
