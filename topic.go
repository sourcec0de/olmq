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

type Topic interface {
	OwnerMeta() OwnerMeta
	UpdatOwnerMeta(om OwnerMeta)
	PartitionMeta() PartitionMeta
	UpdatePartitionMeta(pm PartitionMeta) bool
	RemoveOldPartitions()
	PersistedToPartition(msg []Message) bool
	ConsumerFromPartition() []Message
}

type PartitionMeta struct {
	id     uint64
	offset uint64
}

type lmdbTopic struct {
	env                 *lmdb.Env
	envPath             string
	name                string
	ownerMeta           lmdb.DBI
	partitionMeta       lmdb.DBI
	partitionMetaInited bool
	currentPartitionID  uint64
	currentPartitionDB  lmdb.DBI
}

func newLmdbTopic(env *lmdb.Env, name string) *lmdbTopic {
	topic := &lmdbTopic{
		env:  env,
		name: name,
	}
	topic.envPath, _ = env.Path()
	err := topic.env.Update(func(txn *lmdb.Txn) error {
		if err := topic.initOwnerMeta(txn); err != nil {
			return err
		}
		if err := topic.initPartitionMeta(txn); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return topic
}

func (topic *lmdbTopic) initOwnerMeta(txn *lmdb.Txn) error {
	ownerDBName := fmt.Sprintf("%s-%s", topic.name, ownerMetaName)
	ownerMeta, err := txn.CreateDBI(ownerDBName)
	if err != nil {
		return err
	}
	topic.ownerMeta = ownerMeta
	initOffset := uInt64ToBytes(0)
	err = txn.Put(topic.ownerMeta, keyProducerBytes, initOffset, lmdb.NoOverwrite)
	if err != nil {
		if err, ok := err.(*lmdb.OpError); ok {
			if err.Errno == lmdb.KeyExist {
				topic.partitionMetaInited = true
			} else {
				return err
			}
		}
		return err
	}
	topic.partitionMetaInited = false
	return nil
}

func (topic *lmdbTopic) initPartitionMeta(txn *lmdb.Txn) error {
	partitionDBName := fmt.Sprintf("%s-%s", topic.name, partitionMetaName)
	partitionMeta, err := txn.CreateDBI(partitionDBName)
	if err != nil {
		return err
	}
	topic.partitionMeta = partitionMeta
	if topic.partitionMetaInited {
		return nil
	}
	initOffset := uInt64ToBytes(0)
	initpartitionID := uInt64ToBytes(0)
	return txn.Put(topic.partitionMeta, initpartitionID, initOffset, lmdb.NoOverwrite)
}

func (topic *lmdbTopic) openPartitionForPersisted(txn *lmdb.Txn, rotating bool) {
	partitionMeta := topic.getLatestPartitionMeta(txn)
	if rotating && topic.currentPartitionID == partitionMeta.id {

	}
	topic.currentPartitionID = partitionMeta.id
	path := topic.getPartitionPath(topic.currentPartitionID)
	topic.openPartitionDB(path)
}

func (topic *lmdbTopic) openPartitionDB(path string) {
	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}
	if err := env.SetMapSize(100); err != nil {
		panic(err)
	}
	if err := env.SetMaxDBs(1); err != nil {
		panic(err)
	}
	if err := env.Open(path, lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
		panic(err)
	}
	if _, err = env.ReaderCheck(); err != nil {
		panic(err)
	}
	err = env.Update(func(txn *lmdb.Txn) error {
		partitionName := uInt64ToString(topic.currentPartitionID)
		topic.currentPartitionDB, err = txn.CreateDBI(partitionName)
		return err
	})
	if err != nil {
		panic(err)
	}
}

func (topic *lmdbTopic) getPartitionPath(id uint64) string {
	return fmt.Sprintf("%s/%s.%d", topic.envPath, topic.name, id)
}

func (topic *lmdbTopic) getLatestPartitionMeta(txn *lmdb.Txn) *PartitionMeta {
	cur, err := txn.OpenCursor(topic.partitionMeta)
	if err != nil {
		panic(err)
	}
	idBuf, offsetBuf, err := cur.Get(nil, nil, lmdb.Last)
	if err != nil {
		panic(err)
	}
	partitionMeta := &PartitionMeta{
		id:     bytesToUInt64(idBuf),
		offset: bytesToUInt64(offsetBuf),
	}
	return partitionMeta
}
