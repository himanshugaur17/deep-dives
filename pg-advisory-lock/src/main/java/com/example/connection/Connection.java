package com.example.connection;

public class Connection implements IConnection {
    private boolean isTxn;
    private String txnId;
    private boolean isClosed;

    @Override
    public void executeQuery(String query) {
        System.out.println("Executing query: " + query);
        try {
            // a rest could be made to actual psotgres server to execute the query and
            // return results
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Query executed successfully.");
    }

    @Override
    public void commit() {
        // Implementation for committing a transaction
        if (isTxn) {
            System.out.println("Transaction committed with ID: " + txnId);
            isTxn = false;
            txnId = null;
        } else {
            throw new IllegalStateException("No transaction in progress to commit");
        }
    }

    @Override
    public void rollback() {
        // Implementation for rolling back a transaction
        if (isTxn) {
            System.out.println("Transaction rolled back with ID: " + txnId);
            isTxn = false;
            txnId = null;
        } else {
            throw new IllegalStateException("No transaction in progress to rollback");
        }
    }

    @Override
    public void beginTransaction() {
        if (isTxn) {
            throw new IllegalStateException("Transaction already in progress");
        }
        this.isTxn = true;
        this.txnId = "txn_" + System.currentTimeMillis();
        System.out.println("Transaction started with ID: " + txnId);
    }

    @Override
    public void close() {
        if (isTxn) {
            rollback();
        }
        if (!isClosed) {
            System.out.println("Connection closed.");
            isClosed = true;
        } else {
            System.out.println("Connection is already closed.");
        }
    }

}
