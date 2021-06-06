package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Команда для создания бд
 */
public class CreateDatabaseKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_DATABASE";

    /**
     * Создает объект
     *
     * @param databaseName имя базы данных
     */
    private final String databaseName;
    private final int id;
    public CreateDatabaseKvsCommand(String databaseName) {
        this.databaseName = databaseName;
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
        return new RespArray(respCommandId, commandName, respBulkDbName);
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
