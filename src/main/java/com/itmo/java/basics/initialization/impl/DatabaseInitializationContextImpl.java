package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    private String dbName;
    private Path databaseRoot;
    private Map<String, Table> mapOfTablesContext = new HashMap<>();
    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot.resolve(dbName);
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public Path getDatabasePath() {
        return databaseRoot;
    }

    @Override
    public Map<String, Table> getTables() {
        return mapOfTablesContext;
    }

    @Override
    public void addTable(Table table){
        if (mapOfTablesContext.containsKey(table))
            throw new RuntimeException("already have table: " + table.getName() + "in database " + dbName);
        mapOfTablesContext.put(table.getName(), table);
    }
}
