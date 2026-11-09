//go:build windows

package faultinjector

import (
	"os/exec"
	"syscall"
)

// detach starts the node in a new process group so Ctrl+C in the kvctl
// console is not delivered to it. (os.Process.Kill on Windows calls
// TerminateProcess: an immediate, ungraceful crash, like SIGKILL.)
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}
