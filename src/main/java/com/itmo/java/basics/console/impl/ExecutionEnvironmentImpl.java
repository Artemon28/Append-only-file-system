package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final String workingPath;
    private Map<String, Database> mapOfDatabases = new HashMap<>();

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        workingPath = config.getWorkingPath();
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        if (mapOfDatabases.containsKey(name)){
            return Optional.of(mapOfDatabases.get(name));
        }
        return Optional.empty();
    }

    @Override
    public void addDatabase(Database db) {
        mapOfDatabases.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Path.of(workingPath);
    }
}
