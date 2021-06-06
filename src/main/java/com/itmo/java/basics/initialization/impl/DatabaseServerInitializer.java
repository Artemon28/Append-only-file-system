package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;
import java.nio.file.Path;

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer dbInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        dbInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, начинает их инициализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path pathToEnvironment = context.executionEnvironment().getWorkingPath();
        File f = new File(String.valueOf(pathToEnvironment));
        if (!(f.exists() && f.isDirectory())) {
            f.mkdir();
            return;
        }
        String[] listOfDatabaseNames = f.list();
        for (String dbName : listOfDatabaseNames) {
            if (new File(String.valueOf(pathToEnvironment.resolve(dbName))).isDirectory()) {
                DatabaseInitializationContextImpl currentDbContext = new DatabaseInitializationContextImpl(dbName, pathToEnvironment);
                InitializationContextImpl context2 = new InitializationContextImpl(context.executionEnvironment(), currentDbContext, null, null);
                try {
                    dbInitializer.perform(context2);
                } catch (DatabaseException dex) {
                    throw new DatabaseException("Exeption in Databasetinit perform", dex);
                }
            }
        }
    }
}