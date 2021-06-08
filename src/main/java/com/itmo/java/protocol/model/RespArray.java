package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    private final List<RespObject> listObjects;
    public RespArray(RespObject... objects) {
        listObjects = Arrays.asList(objects);
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
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        StringJoiner stringAnswer = new StringJoiner(" ");
        for (RespObject listObject : listObjects) {
            stringAnswer.add(listObject.asString());
        }
        return stringAnswer.toString();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        try(os) {
            os.write(CODE);
            os.write(Integer.toString(listObjects.size()).getBytes(StandardCharsets.UTF_8));
            os.write(CRLF);
        } catch (IOException e){
            throw new IOException("IO exeption in writing array", e);
        }
        for (RespObject object : listObjects) {
            object.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return listObjects;
    }
}
