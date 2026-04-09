//go:build darwin

package sys

import (
	"syscall"
	"unsafe"
)

// TotalMemory returns the total physical memory of the system in bytes.
func TotalMemory() uint64 {
	s, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0
	}
	// sysctl returns a null-terminated string whose bytes encode a uint64.
	b := []byte(s)
	if len(b) < 8 {
		return 0
	}
	return *(*uint64)(unsafe.Pointer(&b[0]))
}
