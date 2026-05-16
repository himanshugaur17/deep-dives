package com.example.db.txn;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.example.db.StorageEngine;

public class TxnMgrImpl implements TxnMgr {
    private final AtomicInteger txnIdGenerator = new AtomicInteger(1);
    private final StorageEngine storageEngine;
    private final Executor vExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public TxnMgrImpl(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;

    }

    @Override
    public <R> CompletableFuture<R> executeInTxn(TxnBlock<R> block) throws Exception {
        CompletableFuture<R> future = CompletableFuture.supplyAsync(()->{
            int txnId= txnIdGenerator.getAndIncrement();
            System.out.println("Starting transaction with ID: " + txnId);
            
            
        }, vExecutor);
        return null;
    }
}
