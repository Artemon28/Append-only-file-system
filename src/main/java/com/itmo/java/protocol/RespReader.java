package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class RespReader implements AutoCloseable {
    private final InputStream is;
    private boolean isHasArray;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        this.is = is;
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return true;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        try {
            byte[] firstSymbol = is.readNBytes(1);
            if (new String(firstSymbol).equals("-")){
                return readError();
            } else if(new String(firstSymbol).equals("$")){
                return readBulkString();
            } else if(new String(firstSymbol).equals("*")){
                return readArray();
            } else if(new String(firstSymbol).equals("!")){
                return readCommandId();
            } else {
                throw new IOException("unknow byte" + new String(firstSymbol));
            }
        } catch (IOException e){
            throw new IOException("can't read next first symbol", e);
        }
    }


    private int readInt() throws IOException {
        try{
            StringBuilder size = new StringBuilder();
            byte[] sizeB = is.readNBytes(1);
            while (sizeB[0] != CR){
                size.append(new String(sizeB));
                sizeB = is.readNBytes(1);
            }
            return Integer.parseInt(size.toString());
        } catch (IOException e) {
            throw new IOException("IO exception in reading int", e);
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        StringBuilder errorMessage = new StringBuilder();
        byte[] currentSymbol = is.readNBytes(1);
        while (currentSymbol[0] != CR){
            errorMessage.append(new String(currentSymbol));
            currentSymbol = is.readNBytes(1);
        }
        is.readNBytes(1); //LF
        return new RespError(errorMessage.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        int bulkSize = readInt();
        if (bulkSize == -1){
            return RespBulkString.NULL_STRING;
        }
        is.readNBytes(1); //LF
        byte[] bulkString = is.readNBytes(bulkSize);
        is.readNBytes(2); //CRLF
        return new RespBulkString(bulkString);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        int arraySize = readInt();
        is.readNBytes(1); //LF
        RespObject[] listObjects = new RespObject[arraySize];
        for (int i = 0; i <= arraySize; i++){
            listObjects[i] = readObject();
        }
        return new RespArray(listObjects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        int commandId = readInt();
        is.readNBytes(1); //LF
        return new RespCommandId(commandId);
    }


    @Override
    public void close() throws IOException {
        is.close();
    }
}
