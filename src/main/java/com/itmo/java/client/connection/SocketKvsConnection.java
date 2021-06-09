package com.itmo.java.client.connection;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.impl.SuccessDatabaseCommandResult;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    Socket socket;
    private  final ConnectionConfig config;
    private RespReader reader;
    private RespWriter writer;

    public SocketKvsConnection(ConnectionConfig config) {
        this.config = config;
        try {
            socket = new Socket(config.getHost(), config.getPort());
            reader = new RespReader(socket.getInputStream());
            writer = new RespWriter(this.socket.getOutputStream());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            writer.write(command);
            return reader.readObject();
        } catch (IOException e) {
            throw new ConnectionException("yes", e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            writer.close();
            reader.close();
            socket.close();
        } catch (IOException e) {
            throw new UncheckedIOException("exception in closing connection socket", e);
        }
    }
}
