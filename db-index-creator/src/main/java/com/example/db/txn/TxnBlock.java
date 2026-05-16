package com.example.db.txn;

import com.example.db.StorageEngine;

@FunctionalInterface
public interface TxnBlock<R> {
    R execute(StorageEngine storageEngine) throws Exception;
}
