package lmq

import (
	"fmt"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

const (
	ownerMetaName     = "ownerMeta"
	partitionMetaName = "partitionMeta"
)

var (
	keyProducerBytes = []byte("producer_head")
	preConsumerStr   = "consumer_head_"
)

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

type lmdbTopic struct {
	env                 *lmdb.Env
	name                string
	ownerMeta           lmdb.DBI
	partitionMeta       lmdb.DBI
	partitionMetaInited bool
}

func newLmdbTopic(env *lmdb.Env, name string) *lmdbTopic {
	topic := &lmdbTopic{
		env:  env,
		name: name,
	}
	topic.env.Update(func(txn *lmdb.Txn) error {
		topic.initOwerMeta(txn)
		topic.initPartitionMeta(txn)
		return nil
	})
	return topic
}

func (topic *lmdbTopic) initOwerMeta(txn *lmdb.Txn) {
	ownerDBName := fmt.Sprintf("%s-%s", topic.name, ownerMetaName)
	ownerMeta, err := txn.CreateDBI(ownerDBName)
	if err != nil {
		panic(err)
	}
	topic.ownerMeta = ownerMeta
	initOffset := uInt64ToBytes(0)
	err = txn.Put(topic.ownerMeta, keyProducerBytes, initOffset, lmdb.NoOverwrite)
	if err != nil {
		if err, ok := err.(*lmdb.OpError); ok {
			if err.Errno == lmdb.KeyExist {
				topic.partitionMetaInited = true
			} else {
				panic(err)
			}
		} else {
			panic(err)
		}
	} else {
		topic.partitionMetaInited = false
	}
}

func (topic *lmdbTopic) initPartitionMeta(txn *lmdb.Txn) {
	partitionDBName := fmt.Sprintf("%s-%s", topic.name, partitionMetaName)
	partitionMeta, err := txn.CreateDBI(partitionDBName)
	if err != nil {
		panic(err)
	}
	topic.partitionMeta = partitionMeta
	if topic.partitionMetaInited {
		return
	}
	initOffset := uInt64ToBytes(0)
	initpartitionID := uInt64ToBytes(0)
	err = txn.Put(topic.partitionMeta, initpartitionID, initOffset, lmdb.NoOverwrite)
	if err != nil {
		panic(err)
	}
}
