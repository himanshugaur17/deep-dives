package com.example;

import com.example.db.InMemDbEngine;
import com.example.db.models.Row;
import com.example.db.txn.TxnBlock;
import com.example.db.txn.TxnMgr;
import com.example.db.txn.TxnMgrImpl;

/**
 * Hello world!
 *
 */
public class DbClient {
    private static final TxnMgr txnMgr = new TxnMgrImpl(new InMemDbEngine());
    private static int txnRanCount = 0;

    public static void main(String[] args) throws Exception {

        runTxn((engine) -> {
            String key = "key1";

            Row row = engine.load(key);
            Thread.sleep(3000);
            return row.getValue();
        });
    }

    private static void txnRanCount() {
        txnRanCount++;
        System.out.println("Transaction ran count: " + txnRanCount);
    }

    private static void runTxn(TxnBlock<String> txnBlock) throws Exception {
        txnMgr.executeInTxn(txnBlock)
                .thenAccept(result -> {
                    System.out.println("running on thread: " + Thread.currentThread().getName());
                    System.out.println("Transaction result: " + result);
                    txnRanCount();
                });
    }
}
