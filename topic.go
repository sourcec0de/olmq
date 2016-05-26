package lmq

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

const (
	ownerMetaName     = "ownerMeta"
	partitionMetaName = "partitionMeta"

	defaultPartitionSize    = 1024 * 1024 * 1024
	defaultPartitionsToKeep = 8
)

var (
	keyProducerBytes = []byte("producer_head")
	preConsumerStr   = "consumer_head"
)

type Topic interface {
	OpenPartitionForPersist() (uint64, error)
	PersistToPartition(msgs []Message)
	ConsumFromPartition() <-chan Message
	OpenPartitionForConsum(consumerTag string)
}

type PartitionMeta struct {
	id     uint64
	offset uint64
}

type lmdbTopic struct {
	env                 *lmdb.Env
	envPath             string
	root                string
	name                string
	conf                *Config
	currentPartitionID  uint64
	currentPartitionDB  lmdb.DBI
	ownerMeta           lmdb.DBI
	partitionMeta       lmdb.DBI
	partitionMetaInited bool
	persistEnv          *lmdb.Env
	consumedEnv         *lmdb.Env
	consumCursor        *lmdb.Cursor
	consumTxn           *lmdb.Txn
	consumerTag         string
}

func newLmdbTopic(env *lmdb.Env, name string, conf *Config) *lmdbTopic {
	topic := &lmdbTopic{
		env:  env,
		name: name,
		conf: conf,
	}
	topic.envPath, _ = env.Path()
	topic.root = strings.TrimRight(topic.envPath, envMetaName)
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
				return nil
			}
			return err
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

func (topic *lmdbTopic) PersistToPartition(msgs []Message) {
	isFull := false
	err := topic.env.Update(func(txn *lmdb.Txn) error {
		offset, err := topic.persistOffset(txn)
		if err != nil {
			return err
		}
		offset, err = topic.persistToPartitionDB(offset, msgs)
		if err == nil {
			return topic.updatePersistOffset(txn, offset)
		}
		return err
	})
	if err == nil {
		return
	}
	if lmdb.IsMapFull(err) {
		isFull = true
	} else {
		panic(err)
	}
	if isFull {
		topic.persistRotate()
		topic.PersistToPartition(msgs)
	}
}

func (topic *lmdbTopic) persistToPartitionDB(offset uint64, msgs []Message) (uint64, error) {
	err := topic.persistEnv.Update(func(txn *lmdb.Txn) error {
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

func (topic *lmdbTopic) updatePersistOffset(txn *lmdb.Txn, offset uint64) error {
	return txn.Put(topic.ownerMeta, keyProducerBytes, uInt64ToBytes(offset+1), 0)
}

func (topic *lmdbTopic) OpenPartitionForPersist() (uint64, error) {
	partitionID := uint64(0)
	err := topic.env.Update(func(txn *lmdb.Txn) (err error) {
		partitionID, err = topic.openPartitionForPersist(txn, false)
		return
	})
	if err != nil {
		return 0, err
	}
	return partitionID, nil
}

func (topic *lmdbTopic) OpenPartitionForConsum(consumerTag string) {
	topic.consumerTag = consumerTag
	err := topic.env.Update(func(txn *lmdb.Txn) error {
		return topic.openPartitionForConsum(txn, consumerTag)
	})
	if err != nil {
		panic(err)
	}
}

func (topic *lmdbTopic) persistRotate() {
	err := topic.env.Update(func(txn *lmdb.Txn) error {
		if err := topic.closeCurrentPersistPartition(); err != nil {
			return err
		}
		count, err := topic.countPartitions(txn)
		if err != nil {
			return err
		}
		if count > topic.conf.Topic.partitionsToKeep {
			expiredCount := count - topic.conf.Topic.partitionsToKeep
			if err := topic.removeExpiredPartitions(txn, expiredCount); err != nil {
				return err
			}
		}
		_, err = topic.openPartitionForPersist(txn, true)
		return err
	})
	if err != nil {
		panic(err)
	}
}

func (topic *lmdbTopic) consumRotate() {
	err := topic.env.Update(func(txn *lmdb.Txn) error {
		if err := topic.closeCurrentConsumPartition(); err != nil {
			return err
		}
		return topic.openPartitionForConsum(txn, topic.consumerTag)
	})
	if err != nil {
		panic(err)
	}
}

func (topic *lmdbTopic) closeCurrentPersistPartition() error {
	topic.persistEnv.CloseDBI(topic.currentPartitionDB)
	return topic.persistEnv.Close()
}

func (topic *lmdbTopic) closeCurrentConsumPartition() error {
	topic.consumCursor.Close()
	topic.consumTxn.Abort()
	topic.consumedEnv.CloseDBI(topic.currentPartitionDB)
	return topic.consumedEnv.Close()
}

func (topic *lmdbTopic) countPartitions(txn *lmdb.Txn) (uint64, error) {
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
	return bytesToUInt64(endIDBbuf) - bytesToUInt64(beginIDBbuf) + 1, nil
}

func (topic *lmdbTopic) removeExpiredPartitions(txn *lmdb.Txn, expiredCount uint64) error {
	cursor, err := txn.OpenCursor(topic.partitionMeta)
	if err != nil {
		return err
	}
	i := uint64(0)
	for idBuf, _, err := cursor.Get(nil, nil, lmdb.First); err == nil && i < expiredCount; i++ {
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
	return err
}

func (topic *lmdbTopic) persistOffset(txn *lmdb.Txn) (uint64, error) {
	offsetBuf, err := txn.Get(topic.ownerMeta, keyProducerBytes)
	if err != nil {
		return 0, err
	}
	return bytesToUInt64(offsetBuf), err
}

func (topic *lmdbTopic) openPartitionForPersist(txn *lmdb.Txn, rotating bool) (uint64, error) {
	partitionMeta, err := topic.latestPartitionMeta(txn)
	if err != nil {
		return 0, err
	}
	if rotating && topic.currentPartitionID == partitionMeta.id {
		partitionMeta.id++
		partitionMeta.offset++
		if err := topic.updatePartitionMeta(txn, partitionMeta); err != nil {
			return 0, err
		}
	}
	topic.currentPartitionID = partitionMeta.id
	path := topic.partitionPath(topic.currentPartitionID)
	if err = topic.openPersistDB(path); err != nil {
		return 0, err
	}
	return topic.currentPartitionID, nil
}

func (topic *lmdbTopic) updatePartitionMeta(txn *lmdb.Txn, partitionMeta *PartitionMeta) error {
	idBuf := uInt64ToBytes(partitionMeta.id)
	offsetBuf := uInt64ToBytes(partitionMeta.offset)
	return txn.Put(topic.partitionMeta, idBuf, offsetBuf, lmdb.Append)
}

func (topic *lmdbTopic) openPersistDB(path string) error {
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	if err = env.SetMapSize(topic.conf.Topic.partitionSize); err != nil {
		return err
	}
	if err = env.SetMaxDBs(1); err != nil {
		return err
	}
	if err = env.Open(path, lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
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
	topic.persistEnv = env
	return nil
}

func (topic *lmdbTopic) partitionPath(id uint64) string {
	return fmt.Sprintf("%s/%s.%d", topic.root, topic.name, id)
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

func (topic *lmdbTopic) ConsumFromPartition() <-chan Message {
	buf := make(chan Message, topic.conf.ChannelBufferSize)
	topic.consumFromPartition(buf)
	return buf
}

func (topic *lmdbTopic) consumFromPartition(out chan<- Message) {
	shouldRotate := false
	err := topic.env.Update(func(txn *lmdb.Txn) error {
		pOffset, err := topic.persistOffset(txn)
		if err != nil {
			return err
		}
		cOffset, err := topic.consumOffset(txn, topic.consumerTag)
		if err != nil {
			return err
		}
		if pOffset-cOffset == 1 || pOffset == 0 {
			return nil
		}
		offsetBuf, payload, err := topic.consumCursor.Get(uInt64ToBytes(cOffset), nil, lmdb.SetRange)
		if err == nil {
			i := 0
			offset := bytesToUInt64(offsetBuf)
			for cnt := cap(out); err == nil && cnt > 0; cnt-- {
				out <- payload
				i++
				offset = bytesToUInt64(offsetBuf)
				offsetBuf, payload, err = topic.consumCursor.Get(nil, nil, lmdb.Next)
				if err != nil {
					log.Println("err: ", err)
				}
			}
			if offset > 0 {
				err = topic.updateConsumOffset(txn, topic.consumerTag, offset+1)
				if err != nil {
					return err
				}
			}
		} else {
			if !lmdb.IsNotFound(err) {
				log.Println("Consumer seek error: ", err)
			}
			pOffset, err := topic.persistOffset(txn)
			if err != nil {
				return err
			}
			if cOffset <= pOffset {
				shouldRotate = true
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	if shouldRotate {
		topic.consumRotate()
		topic.consumFromPartition(out)
	}
}

func (topic *lmdbTopic) updateConsumOffset(txn *lmdb.Txn, consumerTag string, offset uint64) error {
	keyConsumerStr := fmt.Sprintf("%s_%s", preConsumerStr, consumerTag)
	return txn.Put(topic.ownerMeta, []byte(keyConsumerStr), uInt64ToBytes(offset), 0)
}

func (topic *lmdbTopic) openPartitionForConsum(txn *lmdb.Txn, consumerTag string) error {
	currentPartitionID, err := topic.consumPartitionID(txn, consumerTag, topic.currentPartitionID)
	if err != nil {
		return err
	}
	topic.currentPartitionID = currentPartitionID
	path := topic.partitionPath(topic.currentPartitionID)
	return topic.openConsumDB(path)
}

func (topic *lmdbTopic) openConsumDB(path string) error {
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	topic.consumedEnv = env
	if err = env.SetMaxDBs(1); err != nil {
		return err
	}
	if err = env.SetMapSize(topic.conf.Topic.partitionSize); err != nil {
		return err
	}
	if err = env.Open(path, lmdb.Readonly|lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
		return err
	}
	if _, err = env.ReaderCheck(); err != nil {
		return err
	}
	err = env.View(func(txn *lmdb.Txn) error {
		topic.currentPartitionDB, err = txn.CreateDBI(uInt64ToString(topic.currentPartitionID))
		return err
	})
	if err != nil {
		return err
	}
	rtxn, err := env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {

		return err
	}
	topic.consumTxn = rtxn
	cursor, err := rtxn.OpenCursor(topic.currentPartitionDB)
	if err != nil {

		return err
	}
	topic.consumCursor = cursor
	rtxn.Reset()
	return rtxn.Renew()
}

func (topic *lmdbTopic) consumPartitionID(txn *lmdb.Txn, consumerTag string, searchFrom uint64) (uint64, error) {
	offset, err := topic.consumOffset(txn, consumerTag)
	if err != nil {
		return 0, err
	}
	cursor, err := txn.OpenCursor(topic.partitionMeta)
	if err != nil {
		return 0, err
	}
	idBuf, eoffsetBuf, err := cursor.Get(uInt64ToBytes(searchFrom), nil, lmdb.SetRange)
	if err != nil {
		eoffset := bytesToUInt64(eoffsetBuf)
		for offset > eoffset {
			idBuf, eoffsetBuf, err = cursor.Get(nil, nil, lmdb.Next)
			if err != nil {
				if lmdb.IsNotFound(err) {
					break
				} else {
					return 0, err
				}
			}
			eoffset = bytesToUInt64(eoffsetBuf)
		}
		return 0, err
	}
	eoffset := bytesToUInt64(eoffsetBuf)
	preIDBuf := idBuf
	for offset > eoffset {
		idBuf, eoffsetBuf, err = cursor.Get(nil, nil, lmdb.Next)
		if err != nil {
			break
		}
		preIDBuf = idBuf
		eoffset = bytesToUInt64(eoffsetBuf)
	}
	return bytesToUInt64(preIDBuf), nil
}

func (topic *lmdbTopic) consumOffset(txn *lmdb.Txn, consumerTag string) (uint64, error) {
	keyConsumserStr := fmt.Sprintf("%s_%s", preConsumerStr, consumerTag)
	offsetBuf, err := txn.Get(topic.ownerMeta, []byte(keyConsumserStr))
	if err == nil {
		offset := bytesToUInt64(offsetBuf)
		return offset, nil
	}
	if !lmdb.IsNotFound(err) {
		return 0, err
	}
	cursor, err := txn.OpenCursor(topic.partitionMeta)
	if err != nil {

		return 0, err
	}
	_, offsetBuf, err = cursor.Get(nil, nil, lmdb.First)
	if err != nil {
		return 0, err
	}
	return bytesToUInt64(offsetBuf), nil
}
