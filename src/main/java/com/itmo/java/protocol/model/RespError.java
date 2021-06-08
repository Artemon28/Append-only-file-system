package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Сообщение об ошибке в RESP протоколе
 */
public class RespError implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '-';

    private final byte[] message;
    public RespError(byte[] message) {
        if (message == null){
            this.message = null;
            return;
        }
        this.message = message;
    }

    /**
     * Ошибка ли это? Ответ - да
     *
     * @return true
     */
    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public String asString() {
        if (message == null)
            return null;
        return new String(message);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        try{
            os.write(CODE);
            os.write(message);
            os.write(CRLF);
        } catch (IOException e){
            throw new IOException("IO exeption in writing error", e);
        }
    }
}
