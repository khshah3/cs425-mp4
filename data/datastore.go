package data

//Value should be changed to be generic
type DataStore struct {
	Key   int
	Value string
}

func NewDataStore(key int, value string) *DataStore {
	store := new(DataStore)
	store.Key = key
	store.Value = value
	return store
}

func NilDataStore() DataStore {
	return *NewDataStore(-1, "")
}
