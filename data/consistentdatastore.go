package data

type ConsistentOpArgs struct {
	Consistency int
	DataStore   *DataStore
}

func NewConsistentDataStore(data *DataStore, consistency int) *ConsistentOpArgs {
	store := new(ConsistentOpArgs)
	store.Consistency = consistency
	store.DataStore = data
	return store
}
