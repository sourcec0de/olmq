package lmq

type OwnerMeta struct{}
type PartitionMeta struct{}

type Topic interface {
	OwnerMeta() OwnerMeta
	UpdatOwnerMeta(om OwnerMeta)
	PartitionMeta() PartitionMeta
	UpdatePartitionMeta(pm PartitionMeta) bool
	RemoveOldPartitions()
	PersistedToPartition(msg []Message) bool
	ConsumerFromPartition() []Message
}
