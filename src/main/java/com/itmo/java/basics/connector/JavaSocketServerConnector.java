package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.client.connection.ConnectionConfig;
import com.itmo.java.client.connection.SocketKvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {
    private final DatabaseServer databaseServer;

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
        serverSocket = new ServerSocket(config.getPort());
    }
 
     /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            try {
                Socket clientSocket = serverSocket.accept();
                ClientTask clientTask = new ClientTask(clientSocket, databaseServer);
                clientIOWorkers.submit(clientTask);
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
        ServerConfig serverConfig = new ConfigLoader().readConfig().getServerConfig();
        DatabaseConfig databaseConfig = new ConfigLoader().readConfig().getDbConfig();
        ExecutionEnvironment env = new ExecutionEnvironmentImpl(databaseConfig);
        DatabaseServerInitializer initializer =
                new DatabaseServerInitializer(
                        new DatabaseInitializer(
                                new TableInitializer(
                                        new SegmentInitializer())));
        DatabaseServer databaseServer = DatabaseServer.initialize(env, initializer);
        JavaSocketServerConnector j = new JavaSocketServerConnector(databaseServer, serverConfig);
        j.start();
        SocketKvsConnection sss = new SocketKvsConnection(new ConnectionConfig(serverConfig.getHost(), serverConfig.getPort()));
        RespObject[] list = new RespObject[4];
        list[0] = (new RespCommandId(1));
        list[1] = (new RespBulkString("CREATE_TABLE".getBytes(StandardCharsets.UTF_8)));
        list[2] = (new RespBulkString("zzz".getBytes(StandardCharsets.UTF_8)));
        list[3] = (new RespBulkString("laba6tabletest".getBytes(StandardCharsets.UTF_8)));
        RespObject ans = sss.send(1, new RespArray(list));
        sss.close();
        try{
            RespObject ans1 = sss.send(2, new RespArray(list));
            System.out.println(ans1.asString());
        } catch (ConnectionException e){

        }

        System.out.println(ans.asString());

        j.close();
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
                if (client.isClosed()){
                    return;
                }
                DatabaseCommand command = new CommandReader(new RespReader(client.getInputStream()), server.getEnv()).readCommand();
                RespArray result = new RespArray(command.execute().serialize());
                new RespWriter(client.getOutputStream()).write(result);
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
