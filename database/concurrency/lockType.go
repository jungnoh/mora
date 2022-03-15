package concurrency

type LockType uint8

const (
	NoLock  LockType = 0 // No lock held
	ISLock  LockType = 1 // Intention shared
	IXLock  LockType = 2 // Intention exclusive
	SLock   LockType = 3 // Shared
	SIXLock LockType = 4 // Shared intention exclusive
	XLock   LockType = 5 // Exclusive
)

// LockTypesCompatible returns if locks of type l1 and l2 can applied to a resource at the same time.
func LockTypesCompatible(l1, l2 LockType) bool {
	if l1 == NoLock || l2 == NoLock {
		return true
	}
	switch l1 {
	case XLock:
		return false
	case SIXLock:
		return l2 == ISLock
	case SLock:
		return l2 == SLock || l2 == ISLock
	case IXLock:
		return l2 == ISLock || l2 == IXLock
	case ISLock:
		return l2 != XLock
	}
	panic("invalid lock types")
}

// LockTypesCanBeParent returns if a parent with the given LockType can grant the given LockType to its child
func LockTypesCanBeParent(parent, child LockType) bool {
	if child == NoLock {
		return true
	}
	switch parent {
	case NoLock:
		return false
	case ISLock:
		return child == SLock || child == ISLock
	case IXLock:
		return true
	case SLock:
		return !child.IsIntent()
	case SIXLock:
		return child == IXLock || child == XLock
	case XLock:
		return child == XLock || child == SLock
	}
	return false
}

// LockTypesSubstitutable returns if a lock with type 'have' can substitute a lock with type 'want'.
func LockTypesSubstitutable(have, want LockType) bool {
	if have == want {
		return true
	}
	switch want {
	case NoLock:
		return true
	case ISLock:
		return have == SLock || have == IXLock
	case IXLock:
		return have == XLock
	case SLock:
		return have == SIXLock || have == XLock
	case SIXLock:
		return have == SLock || have == IXLock
	case XLock:
		return false
	}
	return false
}

func (l LockType) Compatible(other LockType) bool {
	return LockTypesCompatible(l, other)
}

func (l LockType) LockOfParent() LockType {
	switch l {
	case NoLock:
		return NoLock
	case ISLock:
		return ISLock
	case IXLock:
		return IXLock
	case SLock:
		return ISLock
	case SIXLock:
		return IXLock
	case XLock:
		return IXLock
	}
	panic("unknown lock type")
}

func (l LockType) CanBeChildOf(parent LockType) bool {
	return LockTypesCanBeParent(parent, l)
}

func (l LockType) CanBeParentOf(child LockType) bool {
	return LockTypesCanBeParent(l, child)
}

func (l LockType) CanSubsitute(other LockType) bool {
	return LockTypesSubstitutable(l, other)
}

func (l LockType) IsIntent() bool {
	return l == IXLock || l == ISLock || l == SIXLock
}

func (l LockType) String() string {
	switch l {
	case NoLock:
		return "NO"
	case ISLock:
		return "IS"
	case IXLock:
		return "IX"
	case SLock:
		return "S"
	case SIXLock:
		return "SIX"
	case XLock:
		return "X"
	}
	panic("unknown lock type")
}
