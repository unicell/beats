package indexer

var (
	defaultConfig = indexerConfig{
		EnablePartitionIndex: true,
		EnableDatafileIndex:  false,
		IndexHandoffOnly:     true,
	}
)

type indexerConfig struct {
	EnablePartitionIndex bool `config:"enable_partition_index"`
	EnableDatafileIndex  bool `config:"enable_datafile_index"`
	IndexHandoffOnly     bool `config:"index_handoff_only"`
}
