package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

/**
 * Команда для создания таблицы
 */
public class CreateTableKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_TABLE";

    private final String databaseName;
    private final String tableName;
    private final int id;
    public CreateTableKvsCommand(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        id = idGen.incrementAndGet();
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        RespCommandId respCommandId = new RespCommandId(id);
        RespBulkString commandName = new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8));
        RespBulkString respBulkDbName = new RespBulkString(databaseName.getBytes(StandardCharsets.UTF_8));
        RespBulkString respBulkTableName = new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8));
        return new RespArray(respCommandId, commandName, respBulkDbName, respBulkTableName);
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
