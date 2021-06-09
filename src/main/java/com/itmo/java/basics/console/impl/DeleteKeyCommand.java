package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    private final ExecutionEnvironment env;
    private final String dbName;
    private final String tableName;
    private final String key;
    private final int numberOfArguments = 5;
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() != numberOfArguments)
            throw new IllegalArgumentException("not correct number of arguments, should be: "
                    + numberOfArguments + " but we have: " + commandArgs.size());
        this.env = env;
        dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        Optional<byte[]> previous;
        try {
            if (env.getDatabase(dbName).isEmpty())
                return DatabaseCommandResult.error("no this database " + dbName);
            previous = env.getDatabase(dbName).get().read(tableName, key);
            env.getDatabase(dbName).get().delete(tableName, key);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
        if (previous.isEmpty()){
            return DatabaseCommandResult.error("nothing to delete");
        }
        return DatabaseCommandResult.success(("previous value was " + new String(previous.get())).getBytes(StandardCharsets.UTF_8));
    }
}
