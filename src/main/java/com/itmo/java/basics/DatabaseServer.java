package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.protocol.model.RespArray;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    private static ExecutionEnvironment env;
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        try {
            initializer.perform(new InitializationContextImpl(env, null, null, null));
        } catch (DatabaseException dex) {
            throw new DatabaseException("Exception with initialization of env with path" + env.getWorkingPath(), dex);
        }
        return new DatabaseServer(env);
    }

    private DatabaseServer(ExecutionEnvironment env){
        this.env = env;
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        return CompletableFuture.supplyAsync(() -> {
            List<RespObject> messageObjects = message.getObjects();
            String currentCommand = messageObjects.get(DatabaseCommandArgPositions.valueOf("COMMAND_NAME")
                    .getPositionIndex()).asString();
            return DatabaseCommands.valueOf(currentCommand).getCommand(env, messageObjects).execute();
        }, executorService);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(() -> {
            return command.execute();
        }, executorService);
    }
    public ExecutionEnvironment getEnv() {
        //TODO implement
        return null;
    }
}