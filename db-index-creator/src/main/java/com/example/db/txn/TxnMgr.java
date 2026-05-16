package com.example.db.txn;

import java.util.concurrent.CompletableFuture;

public interface TxnMgr {
    <R> CompletableFuture<R> executeInTxn(TxnBlock<R> block) throws Exception;
}
