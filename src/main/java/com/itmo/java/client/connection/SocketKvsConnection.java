package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    Socket socket;
    private RespReader reader;
    private RespWriter writer;

    public SocketKvsConnection(ConnectionConfig config) {
        try {
            socket = new Socket(config.getHost(), config.getPort());
            reader = new RespReader(socket.getInputStream());
            writer = new RespWriter(this.socket.getOutputStream());
        } catch (IOException e) {
            throw new UncheckedIOException("exception in creating new connection socket", e);
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
            return reader.readArray();
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
