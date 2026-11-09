//go:build !windows

package faultinjector

import (
	"os/exec"
	"syscall"
)

// detach puts the node in its own process group so it outlives (and is not
// signalled with) the kvctl process that started it.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
