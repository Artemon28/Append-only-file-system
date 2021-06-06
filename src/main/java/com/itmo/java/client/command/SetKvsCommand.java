package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

public class SetKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "SET_KEY";

    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;
    private final int id;
    public SetKvsCommand(String databaseName, String tableName, String key, String value) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
        this.value = value;
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
        RespBulkString respBulkKey = new RespBulkString(key.getBytes(StandardCharsets.UTF_8));
        RespBulkString respBulkValue = new RespBulkString(value.getBytes(StandardCharsets.UTF_8));
        return new RespArray(respCommandId, commandName, respBulkDbName, respBulkTableName, respBulkKey, respBulkValue);
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
