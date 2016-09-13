package indexer

var (
	defaultConfig = indexerConfig{
		EnableObjectPartitionIndex: true,
		EnableDatafileIndex:        false,
		ObjectIndexHandoffOnly:     true,
		EnableAccountIndex:         true,
		EnableContainerIndex:       true,
	}
)

type indexerConfig struct {
	EnableObjectPartitionIndex bool `config:"enable_object_partition_index"`
	EnableDatafileIndex        bool `config:"enable_datafile_index"`
	ObjectIndexHandoffOnly     bool `config:"object_index_handoff_only"`
	EnableAccountIndex         bool `config:"enable_account_index"`
	EnableContainerIndex       bool `config:"enable_container_index"`
}
