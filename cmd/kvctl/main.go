// Command kvctl is the single tool for operating SamanvayDB: data ops,
// cluster introspection, lifecycle, and fault simulation.
//
// Design principles:
//   - every command supports --output json alongside human-readable tables;
//   - exit codes are meaningful, so scripts can branch on them (see below);
//   - kvctl is stateless with respect to topology: it discovers the cluster
//     fresh from the Admin API on every invocation. The only local state it
//     reads is the process table of a `kvctl cluster start` cluster
//     (--local-dir), used for starting/killing processes and as default seeds.
//
// Exit codes:
//
//	0 success
//	1 error (unclassified)
//	2 usage error
//	3 key not found (get)
//	4 unavailable (no quorum / no node reachable) — retry later
//	5 invalid request — do not retry
//	6 precondition failed (CAS mismatch, lock held / not owner / expired)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"samanvaydb/internal/faultinjector"
	"samanvaydb/pkg/client"
)

// Exit codes.
const (
	exitOK           = 0
	exitError        = 1
	exitUsage        = 2
	exitNotFound     = 3
	exitUnavailable  = 4
	exitInvalid      = 5
	exitPrecondition = 6
)

// exitError carries a specific exit code.
type codedError struct {
	code int
	err  error
}

func (e *codedError) Error() string { return e.err.Error() }
func (e *codedError) Unwrap() error { return e.err }

func withCode(code int, err error) error { return &codedError{code: code, err: err} }

// exitCodeFor classifies an error into an exit code.
func exitCodeFor(err error) int {
	var ce *codedError
	switch {
	case err == nil:
		return exitOK
	case errors.As(err, &ce):
		return ce.code
	case errors.Is(err, client.ErrUnavailable):
		return exitUnavailable
	case errors.Is(err, client.ErrInvalidRequest):
		return exitInvalid
	case errors.Is(err, client.ErrCASFailed), errors.Is(err, client.ErrAlreadyHeld),
		errors.Is(err, client.ErrNotOwner), errors.Is(err, client.ErrLockExpired):
		return exitPrecondition
	}
	return exitError
}

// app holds global flag values and output streams.
type app struct {
	servers  string
	output   string
	timeout  time.Duration
	localDir string
	out      io.Writer
	errOut   io.Writer
}

func (a *app) json() bool { return a.output == "json" }

// emit prints v as JSON (json mode) or calls human() (table mode).
func (a *app) emit(v any, human func(w io.Writer)) {
	if a.json() {
		enc := json.NewEncoder(a.out)
		enc.SetIndent("", "  ")
		enc.Encode(v)
		return
	}
	human(a.out)
}

// seeds resolves the seed addresses: --server, $SAMANVAY_SERVERS, the local
// cluster state file, then the single-node default.
func (a *app) seeds() []string {
	s := a.servers
	if s == "" {
		s = os.Getenv("SAMANVAY_SERVERS")
	}
	if s == "" {
		if lc, err := faultinjector.OpenLocal(a.localDir); err == nil {
			return lc.Seeds()
		}
		s = "127.0.0.1:7000"
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (a *app) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), a.timeout)
}

// client connects to the cluster (fresh topology discovery every time).
func (a *app) client(ctx context.Context) (*client.Client, error) {
	cl, err := client.New(ctx, client.Config{Seeds: a.seeds(), RequestTimeout: a.timeout})
	if err != nil {
		return nil, withCode(exitUnavailable, err)
	}
	return cl, nil
}

func newRootCmd(out, errOut io.Writer) (*cobra.Command, *app) {
	a := &app{out: out, errOut: errOut}
	root := &cobra.Command{
		Use:           "kvctl",
		Short:         "kvctl — operate a SamanvayDB cluster",
		Long:          "kvctl is the command-line tool for SamanvayDB: data operations, cluster introspection, lifecycle management and fault simulation.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(out)
	root.SetErr(errOut)
	pf := root.PersistentFlags()
	pf.StringVarP(&a.servers, "server", "s", "", "comma-separated node gRPC addresses (default: $SAMANVAY_SERVERS, the local cluster, or 127.0.0.1:7000)")
	pf.StringVarP(&a.output, "output", "o", "table", "output format: table | json")
	pf.DurationVar(&a.timeout, "timeout", 10*time.Second, "overall timeout per command")
	pf.StringVar(&a.localDir, "local-dir", ".samanvay-local", "state directory of a local cluster created by \"kvctl cluster start\"")

	root.AddCommand(putCmd(a), getCmd(a), deleteCmd(a), casCmd(a), lockCmd(a),
		clusterCmd(a), shardCmd(a), nodeCmd(a), partitionCmd(a), netCmd(a), rebalanceCmd(a))
	return root, a
}

func main() {
	root, _ := newRootCmd(os.Stdout, os.Stderr)
	err := root.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		if strings.Contains(err.Error(), "unknown command") || strings.Contains(err.Error(), "accepts ") ||
			strings.Contains(err.Error(), "required flag") || strings.Contains(err.Error(), "unknown flag") {
			os.Exit(exitUsage)
		}
	}
	os.Exit(exitCodeFor(err))
}

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"
