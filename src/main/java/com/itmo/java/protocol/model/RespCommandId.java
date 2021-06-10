package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Id
 */
public class RespCommandId implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '!';
    private final int commandId;
    public RespCommandId(int commandId) {
        this.commandId = commandId;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public String asString() {
        return String.valueOf(commandId);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write((commandId >>> 24) & 0xFF);
        os.write((commandId >>> 16) & 0xFF);
        os.write((commandId >>>  8) & 0xFF);
        os.write((commandId >>>  0) & 0xFF);
        os.write(CRLF);

    }
}
