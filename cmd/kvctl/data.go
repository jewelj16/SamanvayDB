package main

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"samanvaydb/pkg/client"
	"samanvaydb/proto/kvpb"
)

type dataResult struct {
	OK       bool   `json:"ok"`
	Op       string `json:"op"`
	Key      string `json:"key,omitempty"`
	Value    string `json:"value,omitempty"`
	Found    *bool  `json:"found,omitempty"`
	Shard    int32  `json:"shard"`
	Leader   string `json:"leader,omitempty"`
	Error    string `json:"error,omitempty"`
	Token    uint64 `json:"fencing_token,omitempty"`
	Holder   string `json:"holder,omitempty"`
	Released *bool  `json:"released,omitempty"`
}

func (a *app) result(r dataResult, err error, human func(w io.Writer)) error {
	if err != nil {
		r.OK, r.Error = false, err.Error()
	}
	a.emit(r, func(w io.Writer) {
		if err != nil {
			return // the error is printed by main
		}
		human(w)
	})
	return err
}

func putCmd(a *app) *cobra.Command {
	var ttl time.Duration
	var direct bool
	c := &cobra.Command{
		Use:   "put <key> <value>",
		Short: "Write a key-value pair",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if direct {
				return a.directPut(args[0], args[1], ttl)
			}
			ctx, cancel := a.ctx()
			defer cancel()
			cl, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer cl.Close()
			key := []byte(args[0])
			err = cl.PutTTL(ctx, key, []byte(args[1]), ttl)
			sid := cl.ShardFor(key)
			return a.result(dataResult{OK: true, Op: "put", Key: args[0], Shard: sid, Leader: cl.LeaderHint(sid)}, err,
				func(w io.Writer) { fmt.Fprintf(w, "OK (shard=%d leader=%s)\n", sid, cl.LeaderHint(sid)) })
		},
	}
	c.Flags().DurationVar(&ttl, "ttl", 0, "expire the key after this duration (e.g. 30s)")
	c.Flags().BoolVar(&direct, "direct", false, "send to the first --server node ONLY: no redirects, no retries (shows what that node itself answers)")
	return c
}

// directPut sends one Put to exactly one node and reports its raw answer.
func (a *app) directPut(key, value string, ttl time.Duration) error {
	ctx, cancel := a.ctx()
	defer cancel()
	seeds := a.seeds()
	cc, err := grpc.NewClient(seeds[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer cc.Close()
	resp, err := kvpb.NewKVClient(cc).Put(ctx, &kvpb.PutRequest{Key: []byte(key), Value: []byte(value), TtlMs: uint64(ttl.Milliseconds())})
	if err != nil {
		return withCode(exitUnavailable, fmt.Errorf("%s: %w", seeds[0], err))
	}
	r := dataResult{OK: resp.Ok, Op: "put", Key: key, Shard: resp.ShardId, Leader: resp.RedirectLeader}
	if !resp.Ok {
		r.Error = resp.Code.String() + ": " + resp.Error
		a.emit(r, func(w io.Writer) {})
		code := exitError
		switch resp.Code {
		case kvpb.ErrorCode_UNAVAILABLE, kvpb.ErrorCode_NOT_LEADER:
			code = exitUnavailable
		case kvpb.ErrorCode_INVALID_REQUEST:
			code = exitInvalid
		}
		return withCode(code, fmt.Errorf("%s answered %s (redirect_leader=%q): %s", seeds[0], resp.Code, resp.RedirectLeader, resp.Error))
	}
	a.emit(r, func(w io.Writer) { fmt.Fprintf(w, "OK (shard=%d, served directly by %s)\n", resp.ShardId, seeds[0]) })
	return nil
}

func getCmd(a *app) *cobra.Command {
	var consistency string
	c := &cobra.Command{
		Use:   "get <key>",
		Short: "Read the value for a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var opts []client.GetOption
			switch consistency {
			case "leader", "":
			case "follower-ok", "follower_ok":
				opts = append(opts, client.WithFollowerOK())
			default:
				return withCode(exitUsage, fmt.Errorf("--consistency must be leader or follower-ok"))
			}
			ctx, cancel := a.ctx()
			defer cancel()
			cl, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer cl.Close()
			key := []byte(args[0])
			v, found, err := cl.Get(ctx, key, opts...)
			sid := cl.ShardFor(key)
			r := dataResult{OK: true, Op: "get", Key: args[0], Value: string(v), Found: &found, Shard: sid, Leader: cl.LeaderHint(sid)}
			if err == nil && !found {
				a.emit(r, func(w io.Writer) { fmt.Fprintln(w, "(not found)") })
				return withCode(exitNotFound, errors.New("key not found"))
			}
			return a.result(r, err, func(w io.Writer) { fmt.Fprintf(w, "%s\n", v) })
		},
	}
	c.Flags().StringVar(&consistency, "consistency", "leader", "leader (linearizable) | follower-ok (may be stale)")
	return c
}

func deleteCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := a.ctx()
			defer cancel()
			cl, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer cl.Close()
			key := []byte(args[0])
			err = cl.Delete(ctx, key)
			sid := cl.ShardFor(key)
			return a.result(dataResult{OK: true, Op: "delete", Key: args[0], Shard: sid}, err,
				func(w io.Writer) { fmt.Fprintf(w, "OK (shard=%d)\n", sid) })
		},
	}
}

func casCmd(a *app) *cobra.Command {
	var absent bool
	c := &cobra.Command{
		Use:   "cas <key> <expected> <new>",
		Short: "Compare-and-swap: set key to <new> only if it currently equals <expected>",
		Args: func(cmd *cobra.Command, args []string) error {
			if absent {
				return cobra.ExactArgs(2)(cmd, args)
			}
			return cobra.ExactArgs(3)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := a.ctx()
			defer cancel()
			cl, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer cl.Close()
			key := []byte(args[0])
			var expected, newV []byte
			if absent {
				newV = []byte(args[1])
			} else {
				expected, newV = []byte(args[1]), []byte(args[2])
			}
			res, err := cl.CompareAndSwap(ctx, key, expected, newV)
			r := dataResult{OK: true, Op: "cas", Key: args[0], Shard: cl.ShardFor(key)}
			if errors.Is(err, client.ErrCASFailed) {
				r.Value, r.Found = string(res.CurrentValue), &res.CurrentFound
			}
			return a.result(r, err, func(w io.Writer) { fmt.Fprintln(w, "OK") })
		},
	}
	c.Flags().BoolVar(&absent, "expect-absent", false, "succeed only if the key does not exist (args: <key> <new>)")
	return c
}

func lockCmd(a *app) *cobra.Command {
	lock := &cobra.Command{Use: "lock", Short: "Distributed locks (acquire/renew/release/wait)"}
	var owner string
	var ttl, wait time.Duration
	run := func(op string) func(cmd *cobra.Command, args []string) error {
		return func(cmd *cobra.Command, args []string) error {
			ctx, cancel := a.ctx()
			defer cancel()
			cl, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer cl.Close()
			name := args[0]
			r := dataResult{OK: true, Op: "lock_" + op, Key: name, Shard: cl.ShardFor([]byte("__lock__:" + name))}
			var info client.LockInfo
			switch op {
			case "acquire":
				info, err = cl.TryLock(ctx, name, owner, ttl)
			case "renew":
				info, err = cl.RenewLock(ctx, name, owner, ttl)
			case "release":
				err = cl.Unlock(ctx, name, owner)
			case "wait":
				var released bool
				released, err = cl.WaitLock(ctx, name, wait)
				r.Released = &released
			}
			r.Token, r.Holder = info.FencingToken, info.Holder
			return a.result(r, err, func(w io.Writer) {
				switch op {
				case "acquire", "renew":
					fmt.Fprintf(w, "OK fencing_token=%d expires_at_ms=%d\n", info.FencingToken, info.ExpiresAtMs)
				case "wait":
					fmt.Fprintf(w, "released=%v\n", *r.Released)
				default:
					fmt.Fprintln(w, "OK")
				}
			})
		}
	}
	for _, op := range []string{"acquire", "renew", "release", "wait"} {
		c := &cobra.Command{Use: op + " <name>", Short: op + " a lock", Args: cobra.ExactArgs(1), RunE: run(op)}
		if op != "wait" {
			c.Flags().StringVar(&owner, "owner", "", "owner ID")
			c.MarkFlagRequired("owner")
		}
		if op == "acquire" || op == "renew" {
			c.Flags().DurationVar(&ttl, "ttl", 10*time.Second, "lease length")
		}
		if op == "wait" {
			c.Flags().DurationVar(&wait, "wait", 5*time.Second, "maximum time to wait")
		}
		lock.AddCommand(c)
	}
	return lock
}
