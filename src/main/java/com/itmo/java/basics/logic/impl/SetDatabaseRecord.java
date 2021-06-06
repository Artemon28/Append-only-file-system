package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

/**
 * Запись в БД, означающая добавление значения по ключу
 */
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class SetDatabaseRecord implements WritableDatabaseRecord {

    private final String recordKey;
    private final byte[] recordValue;
    private final int recordKeySize;
    private final int recordValueSize;

    public SetDatabaseRecord(String _key, byte[] _value) {
        recordKey = _key;
        recordKeySize = _key.length();
        recordValue = _value;
        recordValueSize = _value.length;
    }

    @Override
    public byte[] getKey() {
        return recordKey.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getValue() {
        return recordValue;
    }

    @Override
    public long size() {
        return recordKeySize + recordValueSize + 8;
    }

    @Override
    public boolean isValuePresented() {
        return true;
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
