package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.io.OutputStream;

public class RespWriter implements AutoCloseable{
    private final OutputStream os;

    public RespWriter(OutputStream os) {
        this.os = os;
    }

    /**
     * Записывает в output stream объект
     */
    public void write(RespObject object) throws IOException {
        try{
            object.write(os);
        } catch (IOException e){
            os.close();
            throw new IOException("IO exception in writing object", e);
        }

    }

    @Override
    public void close() throws IOException {
        os.close();
    }
}
