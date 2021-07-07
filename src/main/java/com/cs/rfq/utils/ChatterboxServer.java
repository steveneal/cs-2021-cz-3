package com.cs.rfq.utils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Simple chat server capable of sending and receiving String lines on separate in/out port numbers.
 */
public class ChatterboxServer {

    public static final int SERVER_PORT_OUT = 9000;
    public static final int SERVER_PORT_IN = 9001;

    //thread for sending keyboard input to SERVER_PORT_OUT
    private static Thread rfqSenderOutputThread;

    //monitors SERVER_PORT_OUT for socket closing
    private static Thread rfqSenderInputThread;

    public static void main(String[] args) throws Exception {
        runSender();
        runReceiver();
    }

    private static void runSender() throws Exception {
        ServerSocket rfqServerSocket = new ServerSocket(SERVER_PORT_OUT);
        Thread main = new Thread(() -> {
            while (true) {
                try {
                    Thread.yield();
                    log("waiting to connect");
                    Socket socket = rfqServerSocket.accept();
                    log("connected");

                    rfqSenderOutputThread = send(socket);
                    rfqSenderInputThread = receive(socket);

                    rfqSenderOutputThread.start();
                    rfqSenderInputThread.start();

                    synchronized (rfqSenderInputThread) {
                        //will die when there is no testdata to read or the caller disconnects
                        rfqSenderInputThread.wait();
                    }

                    rfqSenderOutputThread.interrupt();
                    log("disconnected");

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        main.setName("main");
        main.setDaemon(true);
        main.start();
    }

    private static void runReceiver() throws Exception {
        ServerSocket confServerSocket = new ServerSocket(SERVER_PORT_IN);
        new Thread(() -> {
            while (true) {
                try {
                    Socket socket = confServerSocket.accept();
                    receive(socket).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static Thread send(final Socket socket) {
        return new Thread(() -> {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
                do {
                    //naive polling of System.in to check for input and allow thread to be interrupted
                    if (System.in.available() > 0) {
                        String line = in.readLine();
                        out.println(line);
                        out.flush();
                        log("sent", line);
                    }  else {
                        Thread.sleep(500);
                    }
                } while (true);

            } catch (InterruptedException e) {
                log("connection closed by server");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static Thread receive(final Socket socket) {
        return new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                String line = in.readLine();
                while (line != null) {
                    log("got response", line);
                    line = in.readLine();
                }
                socket.close();
                //System.out.println("---- close socket -----");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static void log(String status) {
        log(status,"");
    }

    private static void log(String status, String message) {
        System.out.printf("%-10s> %-14s %s%n", Thread.currentThread().getName(), status, message);
    }
}
