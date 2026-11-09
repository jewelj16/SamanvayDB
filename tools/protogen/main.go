// Command protogen compiles the project's .proto files without needing the
// protoc C++ binary. It parses them with bufbuild/protocompile (a pure-Go
// protobuf compiler), then drives the standard protoc-gen-go and
// protoc-gen-go-grpc plugins over the protoc plugin protocol (a
// CodeGeneratorRequest on stdin, a CodeGeneratorResponse on stdout).
//
// Usage (from the repository root):
//
//	protogen -root . -module samanvaydb proto/kv.proto proto/raft.proto proto/admin.proto
//
// The plugins must be on PATH (or in $GOPATH/bin):
//
//	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
//	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

var goModule *string

func main() {
	root := flag.String("root", ".", "import root for .proto files")
	goModule = flag.String("module", "samanvaydb", "Go module path; outputs are written relative to it")
	flag.Parse()
	files := flag.Args()
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "usage: protogen -root <dir> file.proto...")
		os.Exit(2)
	}
	if err := run(*root, files); err != nil {
		fmt.Fprintln(os.Stderr, "protogen:", err)
		os.Exit(1)
	}
}

func run(root string, files []string) error {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: []string{root},
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	compiled, err := compiler.Compile(context.Background(), files...)
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}

	// Collect every file (dependencies first) for the request.
	var protoFiles []*descriptorpb.FileDescriptorProto
	seen := map[string]bool{}
	var add func(fd protoreflect.FileDescriptor)
	add = func(fd protoreflect.FileDescriptor) {
		if seen[fd.Path()] {
			return
		}
		seen[fd.Path()] = true
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			add(imports.Get(i).FileDescriptor)
		}
		protoFiles = append(protoFiles, protodesc.ToFileDescriptorProto(fd))
	}
	for _, f := range compiled {
		add(f)
	}

	for _, plugin := range []string{"protoc-gen-go", "protoc-gen-go-grpc"} {
		req := &pluginpb.CodeGeneratorRequest{
			FileToGenerate: files,
			Parameter:      proto.String("module=" + *goModule),
			ProtoFile:      protoFiles,
		}
		if err := invoke(plugin, req, root); err != nil {
			return err
		}
	}
	return nil
}

func pluginPath(name string) string {
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	if p, err := exec.LookPath(name); err == nil {
		return p
	}
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		home, _ := os.UserHomeDir()
		gopath = filepath.Join(home, "go")
	}
	return filepath.Join(gopath, "bin", name)
}

func invoke(plugin string, req *pluginpb.CodeGeneratorRequest, outRoot string) error {
	in, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	cmd := exec.Command(pluginPath(plugin))
	cmd.Stdin = bytes.NewReader(in)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w: %s", plugin, err, stderr.String())
	}
	var resp pluginpb.CodeGeneratorResponse
	if err := proto.Unmarshal(out.Bytes(), &resp); err != nil {
		return fmt.Errorf("%s: decode response: %w", plugin, err)
	}
	if resp.Error != nil {
		return fmt.Errorf("%s: %s", plugin, resp.GetError())
	}
	for _, f := range resp.File {
		path := filepath.Join(outRoot, f.GetName())
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(f.GetContent()), 0o644); err != nil {
			return err
		}
		fmt.Println("wrote", path)
	}
	return nil
}
