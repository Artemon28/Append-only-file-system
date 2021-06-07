package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    private ConnectionConfig config;
    Socket socket;

    public SocketKvsConnection(ConnectionConfig config) {
        this.config = config;
        try {
            socket = new Socket(config.getHost(), config.getPort());
        } catch (IOException e) {
            e.printStackTrace();
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
            RespWriter writeCommand = new RespWriter(socket.getOutputStream());
            RespObject answerFromServer = new RespReader(socket.getInputStream()).readArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
