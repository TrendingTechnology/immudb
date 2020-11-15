package store

import (
	"bytes"
	"crypto/sha256"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/dgraph-io/badger/v2"
	"math"
)

// SetBatch adds many entries at once
func (t *Store) SetBatch(list schema.KVList, options ...WriteOption) (index *schema.Index, err error) {
	if err = list.Validate(); err != nil {
		return nil, err
	}
	opts := makeWriteOptions(options...)
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	tsEntries := t.tree.NewBatch(&list)

	for i, kv := range list.KVs {
		if err = checkKey(kv.Key); err != nil {
			return nil, err
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:   kv.Key,
			Value: WrapValueWithTS(kv.Value, tsEntries[i].ts),
		}); err != nil {
			return nil, mapError(err)
		}
	}

	ts := tsEntries[len(tsEntries)-1].ts
	index = &schema.Index{
		Index: ts - 1,
	}

	for _, leafEntry := range tsEntries {
		if err = txn.SetEntry(&badger.Entry{
			Key:      treeKey(uint8(0), leafEntry.ts-1),
			Value:    refTreeKey(*leafEntry.h, *leafEntry.r),
			UserMeta: bitTreeEntry,
		}); err != nil {
			return nil, mapError(err)
		}
	}

	cb := func(err error) {
		if err == nil {
			for _, entry := range tsEntries {
				t.tree.Commit(entry)
			}
		} else {
			for _, entry := range tsEntries {
				t.tree.Discard(entry)
			}
		}

		if opts.asyncCommit {
			t.wg.Done()
		}
	}

	if opts.asyncCommit {
		t.wg.Add(1)
		err = mapError(txn.CommitAt(ts, cb)) // cb will be executed in a new goroutine
	} else {
		err = mapError(txn.CommitAt(ts, nil))
		cb(err)
	}
	return
}

// SetBatchAtomicOperations ...
func (t *Store) SetBatchAtomicOperations(operations *schema.AtomicOperations, options ...WriteOption) (index *schema.Index, err error) {

	opts := makeWriteOptions(options...)
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	var listKv schema.KVList
	var listZAdd schema.KVList

	var tsEntriesKv []*treeStoreEntry

	kmap := make(map[[32]byte]bool)

	for _, kv := range operations.KVs {
		if kv != nil {
			kmap[sha256.Sum256(kv.Key)] = true
			listKv.KVs = append(listKv.KVs, kv)
		}
	}
	tsEntriesKv = t.tree.NewBatch(&listKv)
	for _, zOpt := range operations.ZOpts {
		if zOpt != nil {
			skipPersistenceCheck := false
			if kmap[sha256.Sum256(zOpt.Key)] == true {
				skipPersistenceCheck = true
				for _, e := range tsEntriesKv {
					if bytes.Compare(*e.r, zOpt.Key) == 0 {
						zOpt.Index = &schema.Index{Index: e.Index()}
					}
				}
			}
			// if skipPersistenceCheck is true it means that the reference will be done with a key value that is not yet persisted in the store, but it's present in the previous key value list.
			// if skipPersistenceCheck is false it means that the reference is already persisted on disk.
			k, v, err := t.getSortedSetKeyVal(zOpt, skipPersistenceCheck)
			if err != nil {
				return nil, err
			}
			kv := &schema.KeyValue{
				Key:   k,
				Value: v,
			}
			listZAdd.KVs = append(listZAdd.KVs, kv)
		}
	}
	tsEntriesZAdd := t.tree.NewBatch(&listZAdd)

	if err = listKv.Validate(); err != nil {
		return nil, err
	}
	for i, kv := range listKv.KVs {
		if err := checkKey(kv.Key); err != nil {
			return nil, err
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:   kv.Key,
			Value: WrapValueWithTS(kv.Value, tsEntriesKv[i].ts),
		}); err != nil {
			return nil, mapError(err)
		}
	}

	if err = listZAdd.Validate(); err != nil {
		return nil, err
	}
	for i, kv := range listZAdd.KVs {
		if err := checkKey(kv.Key); err != nil {
			return nil, err
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:      kv.Key,
			Value:    WrapValueWithTS(kv.Value, tsEntriesZAdd[i].ts),
			UserMeta: bitReferenceEntry,
		}); err != nil {
			return nil, mapError(err)
		}
	}

	tsEntriesKv = append(tsEntriesKv, tsEntriesZAdd...)

	listKv.KVs = append(listKv.KVs, listZAdd.KVs...)

	ts := tsEntriesKv[len(tsEntriesKv)-1].ts
	index = &schema.Index{
		Index: ts - 1,
	}

	for _, leafEntry := range tsEntriesKv {
		if err = txn.SetEntry(&badger.Entry{
			Key:      treeKey(uint8(0), leafEntry.ts-1),
			Value:    refTreeKey(*leafEntry.h, *leafEntry.r),
			UserMeta: bitTreeEntry,
		}); err != nil {
			return nil, mapError(err)
		}
	}

	cb := func(err error) {
		if err == nil {
			for _, entry := range tsEntriesKv {
				t.tree.Commit(entry)
			}
		} else {
			for _, entry := range tsEntriesKv {
				t.tree.Discard(entry)
			}
		}

		if opts.asyncCommit {
			t.wg.Done()
		}
	}

	if opts.asyncCommit {
		t.wg.Add(1)
		err = mapError(txn.CommitAt(ts, cb)) // cb will be executed in a new goroutine
	} else {
		err = mapError(txn.CommitAt(ts, nil))
		cb(err)
	}
	return
}
