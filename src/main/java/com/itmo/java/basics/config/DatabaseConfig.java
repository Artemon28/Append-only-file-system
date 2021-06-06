package com.itmo.java.basics.config;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";
    private final String finalWorkingPath;

    public DatabaseConfig(String workingPath) {
        finalWorkingPath = workingPath;
    }

    public String getWorkingPath() {
        return finalWorkingPath;
    }
}
