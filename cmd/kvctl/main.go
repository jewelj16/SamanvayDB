package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"samanvaydb/proto/kvpb"
)

var serverAddr string

func main() {
	root := &cobra.Command{
		Use:   "kvctl",
		Short: "kvctl — command-line client for SamanvayDB",
		Long:  "kvctl is a CLI for interacting with a SamanvayDB node over gRPC.",
	}

	root.PersistentFlags().StringVarP(&serverAddr, "server", "s", "127.0.0.1:7000",
		"address of the SamanvayDB node (host:port)")

	root.AddCommand(putCmd(), getCmd(), deleteCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func dial() (*grpc.ClientConn, error) {
	return grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func putCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "put <key> <value>",
		Short: "Write a key-value pair",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := dial()
			if err != nil {
				return fmt.Errorf("connect: %w", err)
			}
			defer conn.Close()

			resp, err := kvpb.NewKVClient(conn).Put(context.Background(), &kvpb.PutRequest{
				Key:   []byte(args[0]),
				Value: []byte(args[1]),
			})
			if err != nil {
				return err
			}
			if !resp.Ok {
				return fmt.Errorf("server error: %s", resp.Error)
			}
			fmt.Println("OK")
			return nil
		},
	}
}

func getCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <key>",
		Short: "Read the value for a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := dial()
			if err != nil {
				return fmt.Errorf("connect: %w", err)
			}
			defer conn.Close()

			resp, err := kvpb.NewKVClient(conn).Get(context.Background(), &kvpb.GetRequest{
				Key: []byte(args[0]),
			})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("server error: %s", resp.Error)
			}
			if !resp.Found {
				fmt.Println("(not found)")
				return nil
			}
			fmt.Printf("%s\n", resp.Value)
			return nil
		},
	}
}

func deleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := dial()
			if err != nil {
				return fmt.Errorf("connect: %w", err)
			}
			defer conn.Close()

			resp, err := kvpb.NewKVClient(conn).Delete(context.Background(), &kvpb.DeleteRequest{
				Key: []byte(args[0]),
			})
			if err != nil {
				return err
			}
			if !resp.Ok {
				return fmt.Errorf("server error: %s", resp.Error)
			}
			fmt.Println("OK")
			return nil
		},
	}
}
