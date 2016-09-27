package swift

// Container models all necessary info regarding an container event
type Container struct {
	*Partition
	Path        string
	SizeKB      int64
	Account     string
	Container   string
	Status      string
	ObjectCount int64
	BytesUsedMB int64
	PolicyIndex int64
}
