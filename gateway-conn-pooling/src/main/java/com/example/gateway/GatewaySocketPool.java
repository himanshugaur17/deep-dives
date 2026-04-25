package com.example.gateway;

import java.net.Socket;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

public class GatewaySocketPool implements AutoCloseable {
    private final int poolSize;
    private final LinkedBlockingQueue<Socket> pool;

    public GatewaySocketPool(String host, int port, int poolSize) {
        this.poolSize = poolSize;
        this.pool = new LinkedBlockingQueue<>(poolSize);
        List<Socket> sockets = IntStream.range(0, poolSize).boxed()
                .map(i -> {
                    try {
                        return new Socket(host, port);
                    } catch (Exception e) {
                        throw new RuntimeException(e.getCause());
                    }
                }).toList();
        pool.addAll(sockets);
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
