package indexer

var (
	defaultConfig = indexerConfig{
		EnableObjectPartitionIndex: true,
		EnableDatafileIndex:        false,
		EnableAccountIndex:         true,
		EnableContainerIndex:       true,
		PartitionIndexOnly:         true,
	}
)

type indexerConfig struct {
	EnableObjectPartitionIndex bool `config:"enable_object_partition_index"`
	EnableDatafileIndex        bool `config:"enable_datafile_index"`
	EnableAccountIndex         bool `config:"enable_account_index"`
	EnableContainerIndex       bool `config:"enable_container_index"`
	PartitionIndexOnly         bool `config:"partition_index_only"`
}
