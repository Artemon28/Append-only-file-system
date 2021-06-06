package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    /**
     * @param databaseRoot путь к директории, которая может содержать несколько БД,
     *                     поэтому при создании БД необходимо создать директорию внутри databaseRoot.
     */

    private final String nameOfData;
    private final File dataBaseFile;
    private Map<String, Table> mapOfTables = new HashMap<>();


    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null) {
            throw new DatabaseException(" dbName is null in creating DataBase");
        }
        String pathtoDatabase = String.valueOf(databaseRoot.resolve(dbName));
        if (!(new File(pathtoDatabase).mkdir())) {
            throw new DatabaseException("can't create Database" + dbName);
        }
        return new DatabaseImpl(String.valueOf(databaseRoot.resolve(dbName)), dbName);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(String.valueOf(context.getDatabasePath()), context.getDbName(),
                context.getTables());
    }

    private DatabaseImpl(String pathName, String nameOfData, Map<String, Table> mapOfTables){
        dataBaseFile = new File(pathName);
        this.nameOfData = nameOfData;
        this.mapOfTables = mapOfTables;
    }

    private DatabaseImpl(String pathName, String dataBaseName){
        dataBaseFile = new File(pathName);
        nameOfData = dataBaseName;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return null;
    }

    @Override
    public String getName() {
        return nameOfData;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("tableName is null");
        }
        if (mapOfTables.containsKey(tableName)) {
            throw new DatabaseException("already have this table" + tableName);
        }
        Table table;
        TableIndex tableindex = new TableIndex();
        try {
            table = TableImpl.create(tableName, Paths.get(dataBaseFile.getAbsolutePath()), tableindex);
        } catch (DatabaseException dex) {
            throw new DatabaseException("can't create table", dex);
        }
        mapOfTables.put(tableName, table);
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (!mapOfTables.containsKey(tableName)) {
            throw new DatabaseException("table" + tableName + "is not exist");
        }
        if (objectKey == null || tableName == null)
            throw new DatabaseException("key or tableName is null");
        try {
            mapOfTables.get(tableName).write(objectKey, objectValue);
        } catch (DatabaseException dex) {
            throw new DatabaseException("DatabaseEx when writing of objectKey: " + objectKey, dex);
        }
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (!mapOfTables.containsKey(tableName)){
            throw new DatabaseException("table" + tableName + "is not exist");
        }
        if (objectKey == null || tableName == null){
            throw new DatabaseException("key or tableName is null");
        }
        try {
            Optional<byte[]> nullTest = mapOfTables.get(tableName).read(objectKey);
            if (nullTest.isEmpty())
                return Optional.empty();
            return nullTest;
        } catch (DatabaseException dex) {
            throw new DatabaseException("can't read Key:" + objectKey, dex);
        }
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (!mapOfTables.containsKey(tableName)){
            throw new DatabaseException("table" + tableName + "is not exist");
        }
        try {
            mapOfTables.get(tableName).delete(objectKey);
        } catch (DatabaseException dex) {
            throw new DatabaseException("DatabaseEx when deleting of objectKey: " + objectKey, dex);
        }
    }
}
