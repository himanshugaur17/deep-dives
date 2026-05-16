package com.example.db.txn;

public interface TxnMgr {
        <R> R executeInTxn(TxnBlock<R> block) throws Exception;
}
