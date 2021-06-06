package com.itmo.java.client.client;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    private final String databaseName;
    private final Supplier<KvsConnection> connectionSupplier;
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.connectionSupplier = connectionSupplier;
    }

    private String createCommand(KvsCommand currentCommand) throws DatabaseExecutionException {
        try {
            RespObject respObject = connectionSupplier.get()
                    .send(currentCommand.getCommandId(), currentCommand.serialize());
            if (respObject.isError()) {
                throw new DatabaseExecutionException(respObject.asString());
            }
            return respObject.asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Exception with connection", e);
        }
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        CreateDatabaseKvsCommand createDbKvsCommand = new CreateDatabaseKvsCommand(databaseName);
        try {
            return createCommand(createDbKvsCommand);
        } catch (DatabaseExecutionException e) {
            throw new DatabaseExecutionException("Exception with connection in createDatabase", e);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        CreateTableKvsCommand createTableKvsCommand = new CreateTableKvsCommand(databaseName, tableName);
        try {
            return createCommand(createTableKvsCommand);
        } catch (DatabaseExecutionException e) {
            throw new DatabaseExecutionException("Exception with connection in createTable", e);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        GetKvsCommand getKvsCommand = new GetKvsCommand(databaseName, tableName, key);
        try {
            return createCommand(getKvsCommand);
        } catch (DatabaseExecutionException e) {
            throw new DatabaseExecutionException("Exception with connection in get", e);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        SetKvsCommand setKvsCommand = new SetKvsCommand(databaseName, tableName, key, value);
        try {
            return createCommand(setKvsCommand);
        } catch (DatabaseExecutionException e) {
            throw new DatabaseExecutionException("Exception with connection in set", e);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        DeleteKvsCommand deleteKvsCommand = new DeleteKvsCommand(databaseName, tableName, key);
        try {
            return createCommand(deleteKvsCommand);
        } catch (DatabaseExecutionException e) {
            throw new DatabaseExecutionException("Exception with connection in delete", e);
        }
    }
}
