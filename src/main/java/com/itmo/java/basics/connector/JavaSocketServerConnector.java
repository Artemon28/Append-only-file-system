package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {
    private final DatabaseServer databaseServer;
    private final ServerConfig config;

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();


    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        this.databaseServer = databaseServer;
        this.config = config;
        serverSocket = new ServerSocket(config.getPort());
    }
 
     /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            try {
                Socket clientSocket = serverSocket.accept();
                clientIOWorkers.submit(new ClientTask(clientSocket, databaseServer));
            } catch (IOException e) {
                throw new UncheckedIOException("exception in accepting new client socket", e);
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");
        try {
            clientIOWorkers.shutdown();
            connectionAcceptorExecutor.shutdown();
            serverSocket.close();
        } catch (IOException e) {
            throw new UncheckedIOException("exception in closing server socket", e);
        }
    }


    public static void main(String[] args) throws Exception {
        // можнно запускать прямо здесь
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {
        private final Socket client;
        private final DatabaseServer server;
        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.client = client;
            this.server = server;
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try {
                DatabaseCommand command = new CommandReader(new RespReader(client.getInputStream()), server.getEnv()).readCommand();
                new RespWriter(client.getOutputStream()).write(command.execute().serialize());
            } catch (IOException e) {
                throw new UncheckedIOException("exception in running command from client", e);
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                client.close();
            } catch (IOException e) {
                throw new UncheckedIOException("exception in closing client socket command", e);
            }
        }
    }
}
