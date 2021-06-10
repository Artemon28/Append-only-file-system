package com.itmo.java.basics.resp;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.util.List;

public class CommandReader implements AutoCloseable {
    private final RespReader reader;
    private final ExecutionEnvironment env;

    public CommandReader(RespReader reader, ExecutionEnvironment env) {
        this.reader = reader;
        this.env = env;
    }

    /**
     * Есть ли следующая команда в ридере?
     */
    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

    /**
     * Считывает комманду с помощью ридера и возвращает ее
     *
     * @throws IllegalArgumentException если нет имени команды и id
     */
    public DatabaseCommand readCommand() throws IOException {
        List<RespObject> messageObjects = reader.readArray().getObjects();
        if (messageObjects.size() <= DatabaseCommandArgPositions.valueOf("COMMAND_NAME").getPositionIndex()){
            throw new IllegalArgumentException("not enough arguments for command");
        }
        String currentCommand = messageObjects.get(DatabaseCommandArgPositions.valueOf("COMMAND_NAME")
                .getPositionIndex()).asString();
        return DatabaseCommands.valueOf(currentCommand).getCommand(env, messageObjects);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
