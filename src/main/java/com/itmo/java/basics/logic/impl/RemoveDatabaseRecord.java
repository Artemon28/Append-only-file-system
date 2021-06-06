package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

/**
 * Запись в БД, означающая удаление значения по ключу
 */
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {

    private final String recordKey;
    private final int recordKeySize;
    private static final int recordValueSize = -1;

    public RemoveDatabaseRecord(String _key) {
        recordKey = _key;
        recordKeySize = _key.length();
    }

    @Override
    public byte[] getKey() {
        return recordKey.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getValue() {
        return null;
    }

    @Override
    public long size() {
        return recordKeySize + 8;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return recordKeySize;
    }

    @Override
    public int getValueSize() {
        return recordValueSize;
    }
}
