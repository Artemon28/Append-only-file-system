package com.itmo.java.basics.console;

import com.itmo.java.basics.console.impl.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import com.itmo.java.protocol.model.RespObject;

import java.nio.file.Path;

import java.util.List;

/**
 * Перечисление команд. Пример создания и использования:
 * DatabaseCommands.valueOf("GET_KEY").getCommand(env, commandArgs).execute()
 */
public enum DatabaseCommands {

    CREATE_DATABASE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            DatabaseFactory factory = (dbName, dbRoot) -> {
                try {
                    return DatabaseImpl.create(dbName, dbRoot);
                } catch (DatabaseException dex) {
                    throw new DatabaseException("Exception with creating Database: " + dbName, dex);
                }
            };
            return new CreateDatabaseCommand(env, factory, commandArgs);
        }
    },
    CREATE_TABLE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            return new CreateTableCommand(env, commandArgs);
        }
    },
    SET_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            return new SetKeyCommand(env, commandArgs);
        }
    },
    GET_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            return new GetKeyCommand(env, commandArgs);
        }
    },
    DELETE_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            return new DeleteKeyCommand(env, commandArgs);
        }
    };

    /**
     * Возвращает созданную команду. Каждый элемент перечисления создается со своей реализацией этого метода
     */
    public abstract DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs);
}
