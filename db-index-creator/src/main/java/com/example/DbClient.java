package com.example;

import com.example.db.txn.TxnBlock;

/**
 * Hello world!
 *
 */
public class DbClient {
    public static void main(String[] args) {
        TxnBlock<String> txnBlock = () -> {
            System.out.println("Executing transaction block...");
            // Simulate some work
            Thread.sleep(1000);
            System.out.println("Transaction block executed successfully.");
            return "Transaction Result";
        };
        txnBlock.execute()
    }
}
