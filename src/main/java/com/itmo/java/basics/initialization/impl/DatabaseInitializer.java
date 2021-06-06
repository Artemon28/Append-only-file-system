package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;
    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        Path pathToDatabase = initialContext.currentDbContext().getDatabasePath();
        File f = new File(String.valueOf(pathToDatabase));
        if (!(f.exists() && f.isDirectory())) {
            throw new DatabaseException("No such database from context");
        }
        String[] listOfTablesNames = f.list();
        Arrays.sort(listOfTablesNames);
        for (String tableName : listOfTablesNames) {
            if (new File(String.valueOf(pathToDatabase.resolve(tableName))).isDirectory()) {
                TableIndex tableIndex = new TableIndex();
                TableInitializationContextImpl currentTableContext = new TableInitializationContextImpl(tableName, pathToDatabase,
                        tableIndex);
                InitializationContextImpl initialContext2 = new InitializationContextImpl(initialContext.executionEnvironment(),
                        initialContext.currentDbContext(), currentTableContext, null);
                try {
                    tableInitializer.perform(initialContext2);
                } catch (DatabaseException dex) {
                    throw new DatabaseException("DatabaseExeption in DatabaseInitializer perform ", dex);
                }
            }
        }
        initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
    }
}
