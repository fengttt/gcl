package gcl

import "github.com/google/uuid"

func UUIDCmp(a, b uuid.UUID) int {
	for i := 0; i < 16; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

func UUIDLess(a, b uuid.UUID) bool {
	return UUIDCmp(a, b) < 0
}

func UUIDLessEqual(a, b uuid.UUID) bool {
	return UUIDCmp(a, b) <= 0
}

func UUIDGreater(a, b uuid.UUID) bool {
	return UUIDCmp(a, b) > 0
}

func UUIDGreaterEqual(a, b uuid.UUID) bool {
	return UUIDCmp(a, b) >= 0
}
