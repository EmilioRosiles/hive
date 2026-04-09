//go:build !linux && !darwin && !windows

package sys

// TotalMemory returns 0 on unsupported platforms.
func TotalMemory() uint64 { return 0 }
