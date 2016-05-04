package lmq

import (
	"fmt"
	"os"

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
	persistedEnv        *lmdb.Env
	consumedEnv         *lmdb.Env
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

func (topic *lmdbTopic) PersistedToPartition(msgs []Message) {
	isFull := false
	err := topic.env.Update(func(txn *lmdb.Txn) error {
		offset, err := topic.persistedOffset(txn)
		if err != nil {
			return err
		}
		offset, err = topic.persistedToPartitionDB(txn, offset, msgs)
		if err == nil {
			return topic.updatePersistedOffset(txn, offset)
		}
		return err
	})
	if err == nil {
		return
	}
	if err, ok := err.(*lmdb.OpError); ok {
		if err.Errno == lmdb.MapFull {
			isFull = true
		} else {
			panic(err.Errno)
		}
	}
	if isFull {
		topic.rotate()
		topic.PersistedToPartition(msgs)
	}
}

func (topic *lmdbTopic) persistedToPartitionDB(txn *lmdb.Txn, offset uint64, msgs []Message) (uint64, error) {
	err := topic.persistedEnv.Update(func(txn *lmdb.Txn) error {
		for _, v := range msgs {
			offset++
			k := uInt64ToBytes(offset)
			if err := txn.Put(topic.currentPartitionDB, k, v, lmdb.Append); err != nil {
				return err
			}
		}
		return nil
	})
	return offset, err
}

func (topic *lmdbTopic) updatePersistedOffset(txn *lmdb.Txn, offset uint64) error {
	err := txn.Put(topic.ownerMeta, keyProducerBytes, uInt64ToBytes(offset), 0)
	return err
}

func (topic *lmdbTopic) rotate() {
	err := topic.env.Update(func(txn *lmdb.Txn) error {
		if err := topic.closeCurrentPartition(txn); err != nil {
			return err
		}
		expiredCount, err := topic.countExpiredPartitions(txn)
		if err != nil {
			return err
		}
		if err := topic.removeExpiredPartitions(expiredCount); err != nil {
			return err
		}
		return topic.openPartitionForPersisted(txn, true)
	})
	if err != nil {
		panic(err)
	}
}

func (topic *lmdbTopic) closeCurrentPartition(txn *lmdb.Txn) error {
	topic.env.CloseDBI(topic.currentPartitionDB)
	return topic.env.Close()
}

func (topic *lmdbTopic) countExpiredPartitions(txn *lmdb.Txn) (uint64, error) {
	cursor, err := txn.OpenCursor(topic.partitionMeta)
	if err != nil {
		return 0, err
	}
	beginIDBbuf, _, err := cursor.Get(nil, nil, lmdb.First)
	if err != nil {
		return 0, err
	}
	endIDBbuf, _, err := cursor.Get(nil, nil, lmdb.Last)
	if err != nil {
		return 0, err
	}
	return bytesToUInt64(endIDBbuf) - bytesToUInt64(beginIDBbuf), nil
}

func (topic *lmdbTopic) removeExpiredPartitions(expiredCount uint64) error {
	err := topic.persistedEnv.Update(func(txn *lmdb.Txn) error {
		cursor, err := txn.OpenCursor(topic.partitionMeta)
		if err != nil {
			return err
		}
		i := uint64(0)
		for idBuf, _, err := cursor.Get(nil, nil, lmdb.First); err != nil && i < expiredCount; i++ {
			id := bytesToUInt64(idBuf)
			if err := cursor.Del(0); err != nil {
				return err
			}
			partitionPath := topic.partitionPath(id)
			if err := os.Remove(partitionPath); err != nil {
				return err
			}
			if err := os.Remove(fmt.Sprintf("%s-lock", partitionPath)); err != nil {
				return err
			}
			idBuf, _, err = cursor.Get(nil, nil, lmdb.Next)
		}
		return nil
	})
	return err
}

func (topic *lmdbTopic) persistedOffset(txn *lmdb.Txn) (uint64, error) {
	offsetBuf, err := txn.Get(topic.ownerMeta, keyProducerBytes)
	if err != nil {
		return 0, err
	}
	return bytesToUInt64(offsetBuf), err
}

func (topic *lmdbTopic) openPartitionForPersisted(txn *lmdb.Txn, rotating bool) error {
	partitionMeta, err := topic.latestPartitionMeta(txn)
	if err != nil {
		return err
	}
	if rotating && topic.currentPartitionID == partitionMeta.id {
		partitionMeta.id++
		partitionMeta.offset++
		if err := topic.updatePartitionMeta(txn, partitionMeta); err != nil {
			return err
		}
	}
	topic.currentPartitionID = partitionMeta.id
	path := topic.partitionPath(topic.currentPartitionID)
	return topic.openPartitionDB(path)
}

func (topic *lmdbTopic) updatePartitionMeta(txn *lmdb.Txn, partitionMeta *PartitionMeta) error {
	idBuf := uInt64ToBytes(partitionMeta.id)
	offsetBuf := uInt64ToBytes(partitionMeta.offset)
	return txn.Put(topic.partitionMeta, idBuf, offsetBuf, lmdb.Append)
}

func (topic *lmdbTopic) openPartitionDB(path string) error {
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	if err := env.SetMapSize(100); err != nil {
		return err
	}
	if err := env.SetMaxDBs(1); err != nil {
		return err
	}
	if err := env.Open(path, lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
		return err
	}
	if _, err = env.ReaderCheck(); err != nil {
		return err
	}
	err = env.Update(func(txn *lmdb.Txn) error {
		partitionName := uInt64ToString(topic.currentPartitionID)
		topic.currentPartitionDB, err = txn.CreateDBI(partitionName)
		return err
	})
	if err != nil {
		return err
	}
	topic.persistedEnv = env
	return nil
}

func (topic *lmdbTopic) partitionPath(id uint64) string {
	return fmt.Sprintf("%s/%s.%d", topic.envPath, topic.name, id)
}

func (topic *lmdbTopic) latestPartitionMeta(txn *lmdb.Txn) (*PartitionMeta, error) {
	cur, err := txn.OpenCursor(topic.partitionMeta)
	if err != nil {
		return nil, err
	}
	idBuf, offsetBuf, err := cur.Get(nil, nil, lmdb.Last)
	if err != nil {
		return nil, err
	}
	partitionMeta := &PartitionMeta{
		id:     bytesToUInt64(idBuf),
		offset: bytesToUInt64(offsetBuf),
	}
	return partitionMeta, nil
}
