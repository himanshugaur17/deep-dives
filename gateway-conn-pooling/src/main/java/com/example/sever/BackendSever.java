package com.example.sever;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class BackendSever {
    private static final int PORT = 8080;
    private static AtomicInteger totalVirtualThreads = new AtomicInteger(0);

    public static void start() {
        // starting the server in a vitual thread
        // so that it does not block the main thread
        Thread.startVirtualThread(() -> {
            totalVirtualThreads.incrementAndGet();
            System.out.println("Starting the server on port " + PORT);
            try (ServerSocket serverSocket = new ServerSocket(PORT)) {
                // main backend server loop, accepting incoming connections
                while (true) {
                    Socket clientSocketOnBackendSide = serverSocket.accept(); // will stay blocked
                    System.out
                            .println("Accepted connection from " + clientSocketOnBackendSide.getRemoteSocketAddress());
                    Thread.startVirtualThread(() -> handleClient(clientSocketOnBackendSide));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void handleClient(Socket clientSocket) {
        // here we would handle the client connection, read/write data, etc.
        // for demonstration purposes, we will just print a message and close the
        // connection
        System.out.println("Handling client " + clientSocket.getRemoteSocketAddress());
        totalVirtualThreads.incrementAndGet();
        try {
            // Simulate some work with the client
            Thread.sleep(1000);
            System.out.println("Finished handling client " + clientSocket.getRemoteSocketAddress()
                    + " total virtual threads: " + totalVirtualThreads.get());
            totalVirtualThreads.decrementAndGet();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
