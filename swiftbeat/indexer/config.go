package indexer

var (
	defaultConfig = indexerConfig{
		EnablePartitionIndex: true,
		EnableDatafileIndex:  false,
	}
)

type indexerConfig struct {
	EnablePartitionIndex bool `config:"enable_partition_index"`
	EnableDatafileIndex  bool `config:"enable_datafile_index"`
}
