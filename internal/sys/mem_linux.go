//go:build linux

package sys

import "syscall"

// TotalMemory returns the total physical memory of the system in bytes.
func TotalMemory() uint64 {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0
	}
	return info.Totalram * uint64(info.Unit)
}
