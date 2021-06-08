package com.itmo.java.protocol.model;

import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    /**
     * Код объекта
     */
    public static final byte CODE = '$';

    public static final int NULL_STRING_SIZE = -1;

    public static final RespBulkString NULL_STRING = new RespBulkString(null);

    private final byte[] data;
    public RespBulkString(byte[] data) {
        if (data == null){
            this.data = null;
            return;
        }
        this.data = data;
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

    /**
     * Строковое представление
     *
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
        if (data == null)
            return null;
        return new String(data);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        if (data == null){
            os.write(Integer.toString(NULL_STRING_SIZE).getBytes(StandardCharsets.UTF_8));
            os.write(CRLF);
        } else {
            os.write(Integer.toString(data.length).getBytes(StandardCharsets.UTF_8));
            os.write(CRLF);
            os.write(data);
            os.write(CRLF);
        }
    }
}
