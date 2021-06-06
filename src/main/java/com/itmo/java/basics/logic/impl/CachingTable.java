package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

/**
 * Декоратор для таблицы. Кэширует данные
 */
public class CachingTable implements Table {

    Table table;
    DatabaseCacheImpl databaseCache = new DatabaseCacheImpl();

    public CachingTable(Table table){
        this.table = table;
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        try {
            table.write(objectKey, objectValue);
        } catch (DatabaseException dex){
            throw new DatabaseException("DatabaseException while write in table: " + table.getName());
        }
        databaseCache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (databaseCache.get(objectKey) != null){
            return Optional.of(databaseCache.get(objectKey));
        }
        try {
            return table.read(objectKey);
        } catch (DatabaseException dex) {
            throw new DatabaseException("error while reading table: " + table.getName(), dex);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        try {
            table.delete(objectKey);
        } catch (DatabaseException dex){
            throw new DatabaseException("DatabaseException while delete in table: " + table.getName());
        }
        databaseCache.delete(objectKey);
    }
}
