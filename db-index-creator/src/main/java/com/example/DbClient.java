package com.example;

import com.example.db.InMemDbEngine;
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

        TxnBlock<String> txnBlock=(storageEngine)->{
            
        }
        txnMgr.executeInTxn(txnBlock);
    }

    private static void txnRanCount() {
        txnRanCount++;
        System.out.println("Transaction ran count: " + txnRanCount);
    }
}
