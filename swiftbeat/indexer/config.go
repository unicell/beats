package indexer

var (
	defaultConfig = indexerConfig{
		EnableObjectPartitionIndex: true,
		EnableDatafileIndex:        false,
		ObjectIndexHandoffOnly:     true,
		EnableContainerIndex:       true,
	}
)

type indexerConfig struct {
	EnableObjectPartitionIndex bool `config:"enable_object_partition_index"`
	EnableDatafileIndex        bool `config:"enable_datafile_index"`
	ObjectIndexHandoffOnly     bool `config:"object_index_handoff_only"`
	EnableContainerIndex       bool `config:"enable_container_index"`
}
