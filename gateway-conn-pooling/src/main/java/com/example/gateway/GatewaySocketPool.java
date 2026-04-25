package com.example.gateway;

import java.net.Socket;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class GatewaySocketPool implements AutoCloseable {
    private final int poolSize;
    private final LinkedBlockingQueue<Socket> pool;
    private final Supplier<Socket> socketFactory;

    public GatewaySocketPool(String host, int port, int poolSize) {
        this.poolSize = poolSize;
        this.socketFactory = () -> {
            try {
                return new Socket(host, port);
            } catch (Exception e) {
                throw new RuntimeException(e.getCause());
            }
        };
        this.pool = new LinkedBlockingQueue<>(poolSize);
        List<Socket> sockets = IntStream.range(0, poolSize).boxed()
                .map(i -> socketFactory.get())
                .toList();
        pool.addAll(sockets);
    }

    public Socket borrowSocket() throws InterruptedException {
        Socket socket = pool.take(); // will block if no socket is available
        // if all connections are unavailable, this will stay stuck, causing timeout.
        if (isUnhealthy(socket)) {
            // let's create a new socket and return it
            try {
                Socket newSocket = socketFactory.get();
                return newSocket; // this new socket will get added to the pool once it is returned back
            } catch (Exception e) {
                throw new RuntimeException(e.getCause());
            }
        }
        return socket;
    }

    private boolean isUnhealthy(Socket socket) {
        return socket.isClosed() || !socket.isConnected() || socket.isInputShutdown() || socket.isOutputShutdown();
    }

    public void returnSocket(Socket socket) throws InterruptedException {
        if (isUnhealthy(socket)) {
            // we need to create new socket and add it
            // so that the pool size is maintained
            try {
                Socket newSocket = socketFactory.get();
                pool.put(newSocket);
            } catch (Exception e) {
                throw new RuntimeException(e.getCause());
            }
        } else {
            pool.put(socket); // will block if the pool is already full, which should not happen in normal
                              // flow
        }
    }

    @Override
    public void close() throws Exception {
        pool.forEach(socket -> {
            try {
                socket.close();
            } catch (Exception e) {

            }
        });
    }
}
