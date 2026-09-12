// Package logging provides structured logging for SamanvayDB nodes.
//
// Every log line carries at minimum a node_id field from startup. As the system
// grows, additional context fields (term, role) will be added for Raft-aware
// logging. This package wraps Go's standard log/slog so that unstructured
// fmt.Println debugging never creeps in — interleaved logs from multiple nodes
// are unreadable without structured fields.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// Logger wraps slog.Logger with SamanvayDB-specific context (node_id, and
// eventually term/role once Raft exists).
type Logger struct {
	*slog.Logger
}

// Options configures the logger.
type Options struct {
	// NodeID is attached to every log line.
	NodeID string

	// Level sets the minimum log level. Defaults to slog.LevelInfo.
	Level slog.Level

	// Output is the writer for log output. Defaults to os.Stdout.
	Output io.Writer

	// JSON controls whether output is JSON (true) or text (false).
	JSON bool
}

// NewLogger creates a structured logger that carries node_id on every line.
// Additional context (term, role) can be added later via WithRaftContext.
func NewLogger(opts Options) *Logger {
	if opts.Output == nil {
		opts.Output = os.Stdout
	}

	handlerOpts := &slog.HandlerOptions{
		Level: opts.Level,
	}

	var handler slog.Handler
	if opts.JSON {
		handler = slog.NewJSONHandler(opts.Output, handlerOpts)
	} else {
		handler = slog.NewTextHandler(opts.Output, handlerOpts)
	}

	// Attach node_id to every log line from the start.
	logger := slog.New(handler).With("node_id", opts.NodeID)

	return &Logger{Logger: logger}
}

// WithRaftContext returns a new Logger that also carries Raft-specific fields
// (term and role) on every log line. Called when Raft is initialized (Phase 3+).
func (l *Logger) WithRaftContext(term uint64, role string) *Logger {
	return &Logger{
		Logger: l.Logger.With("term", term, "role", role),
	}
}

// WithComponent returns a new Logger scoped to a specific component
// (e.g. "storage", "raft", "transport").
func (l *Logger) WithComponent(component string) *Logger {
	return &Logger{
		Logger: l.Logger.With("component", component),
	}
}

// Ctx returns the logger's context-aware variant for use with context
// propagation patterns.
func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.Logger.InfoContext(ctx, msg, args...)
}
