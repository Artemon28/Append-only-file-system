package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.client.client.SimpleKvsClient;
import com.itmo.java.client.command.CreateDatabaseKvsCommand;
import com.itmo.java.client.command.CreateTableKvsCommand;
import com.itmo.java.client.command.GetKvsCommand;
import com.itmo.java.client.command.SetKvsCommand;
import com.itmo.java.client.connection.ConnectionConfig;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.connection.SocketKvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
                clientIOWorkers.submit(() -> {
                    ClientTask clientTask = new ClientTask(clientSocket, databaseServer);
                    clientTask.run();
                });
            } catch (IOException e) {
                throw new UncheckedIOException("exception in accepting new client socket", e);
            } finally {
                close();
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
            clientIOWorkers.shutdownNow();
            connectionAcceptorExecutor.shutdownNow();
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
        RespObject q;
        try(SocketKvsConnection socketKvsConnection =
                    new SocketKvsConnection(new ConnectionConfig(serverConfig.getHost(), serverConfig.getPort()))) {
            socketKvsConnection.send(1, new CreateDatabaseKvsCommand("t1").serialize());
            socketKvsConnection.send(1, new CreateTableKvsCommand("t1", "da").serialize());
            socketKvsConnection.send(1, new SetKvsCommand("t1", "da", "key1", "value1").serialize());
            q = socketKvsConnection.send(1, new GetKvsCommand("t1", "da", "qwerty").serialize());
        }
        System.out.println(q.asString());
        j.close();
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {

        private final Socket client;
        private final DatabaseServer server;
        private RespReader reader;
        private RespWriter writer;
        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.client = client;
            this.server = server;
            try {
                reader = new RespReader(client.getInputStream());
                writer = new RespWriter(client.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
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
            try(CommandReader commandReader = new CommandReader(reader, server.getEnv())) {
                while (commandReader.hasNextCommand()) {
                    DatabaseCommand command = commandReader.readCommand();
                    DatabaseCommandResult databaseCommandResult = server.executeNextCommand(command).get();
                    writer.write(databaseCommandResult.serialize());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                client.close();
                reader.close();
                writer.close();
            } catch (IOException e) {
                throw new UncheckedIOException("exception in closing client socket command", e);
            }
        }
    }
}
