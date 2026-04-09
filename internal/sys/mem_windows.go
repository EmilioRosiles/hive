//go:build windows

package sys

import (
	"syscall"
	"unsafe"
)

// TotalMemory returns the total physical memory of the system in bytes.
func TotalMemory() uint64 {
	kernel32 := syscall.MustLoadDLL("kernel32.dll")
	globalMemoryStatusEx := kernel32.MustFindProc("GlobalMemoryStatusEx")

	var memStatus struct {
		dwLength                uint32
		dwMemoryLoad            uint32
		ullTotalPhys            uint64
		ullAvailPhys            uint64
		ullTotalPageFile        uint64
		ullAvailPageFile        uint64
		ullTotalVirtual         uint64
		ullAvailVirtual         uint64
		ullAvailExtendedVirtual uint64
	}
	memStatus.dwLength = uint32(unsafe.Sizeof(memStatus))
	ret, _, _ := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memStatus)))
	if ret == 0 {
		return 0
	}
	return memStatus.ullTotalPhys
}
