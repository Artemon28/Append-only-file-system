package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    private final String configFileName;
    private String workingPath;
    private String host;
    private int port;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        configFileName = "server.properties";
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        configFileName = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        try {
            FileInputStream propFilePath = new FileInputStream(configFileName);
            Properties configFileProp = new Properties();
            configFileProp.load(propFilePath);
            workingPath = configFileProp.getProperty("kvs.workingPath");
            host = configFileProp.getProperty("kvs.host");
            String stringPort = configFileProp.getProperty("kvs.port");
            if (host == null || stringPort == null){
                host = ServerConfig.DEFAULT_HOST;
                port = ServerConfig.DEFAULT_PORT;
            } else {
                port = Integer.parseInt(stringPort);
            }
            if (workingPath == null){
                workingPath = DatabaseConfig.DEFAULT_WORKING_PATH;
            }
            ServerConfig serverConfig = new ServerConfig(host, port);
            DatabaseConfig databaseConfig = new DatabaseConfig(workingPath);
            DatabaseServerConfig databaseServerConfig = new DatabaseServerConfig(serverConfig, databaseConfig);
            return databaseServerConfig;
        } catch (IOException e) {
            return new DatabaseServerConfig(new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT),
                    new DatabaseConfig(DatabaseConfig.DEFAULT_WORKING_PATH));
        }
    }
}
