package com.example.db.txn;

@FunctionalInterface
public interface TxnBlock<R> {
    R execute() throws Exception;
}
