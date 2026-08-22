package transport

// Op identifies the operation being forwarded.
// Shared ops (Del, Expire) work for any DataStructure kind.
// Kind-specific ops are grouped in ranges: Value=10-19, Set=50-59, Hash=100-109.
type Op uint8

const (
	// Shared ops — apply to any DataStructure kind.
	OpDel    Op = 1
	OpExpire Op = 2
	OpLock   Op = 3
	OpUnlock Op = 4
	OpRenew  Op = 5

	// Value ops.
	OpValueSet Op = 10
	OpValueGet Op = 11

	// Set ops.
	OpSAdd      Op = 50
	OpSRem      Op = 51
	OpSIsMember Op = 52
	OpSMembers  Op = 53
	OpSCard     Op = 54

	// Hash ops.
	OpHSet    Op = 100
	OpHGet    Op = 101
	OpHDel    Op = 102
	OpHGetAll Op = 103
	OpHKeys   Op = 104

	// List ops.
	OpLPush  Op = 150
	OpRPush  Op = 151
	OpLPop   Op = 152
	OpRPop   Op = 153
	OpLLen   Op = 154
	OpLIndex Op = 155
	OpLRange Op = 156
	OpLSet   Op = 157

	// ZSet ops.
	OpZAdd          Op = 170
	OpZRem          Op = 171
	OpZScore        Op = 172
	OpZRank         Op = 173
	OpZCard         Op = 174
	OpZRange        Op = 175
	OpZRangeByScore Op = 176
	OpZRevRank      Op = 177
)
