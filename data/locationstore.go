package data

// Represents a location in the ring

type LocationStore struct {
	Key   int
	Value string
}

func NewLocationStore(Key int, Value string) *LocationStore {
	store := new(LocationStore)
	store.Key = Key
	store.Value = Value
	return store
}

func NilLocationStore() LocationStore {
	return *NewLocationStore(-1, "")
}
