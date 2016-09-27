package swift

// Account models all necessary info regarding an account event
type Account struct {
	*Partition
	Path           string
	SizeKB         int64
	Account        string
	Status         string
	ContainerCount int64
	ObjectCount    int64
	BytesUsedMB    int64
}
