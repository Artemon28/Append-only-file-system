package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;
    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path pathToTable = context.currentTableContext().getTablePath();
        File f = new File(String.valueOf(pathToTable));
        if (!(f.exists() && f.isDirectory())) {
            throw new DatabaseException("invalid context in TableInitializer");
        }
        String[] listOfSegmentsNames = f.list();
        Arrays.sort(listOfSegmentsNames);
        for (String segmentName : listOfSegmentsNames) {
            SegmentInitializationContextImpl currentSegmentContext = new SegmentInitializationContextImpl(segmentName,
                    pathToTable, 0);
            InitializationContextImpl context3 = new InitializationContextImpl(context.executionEnvironment(),
                    context.currentDbContext(), context.currentTableContext(), currentSegmentContext);
            try {
                segmentInitializer.perform(context3);
            } catch (DatabaseException dex) {
                throw new DatabaseException("DatabaseExeption in TableInitializer perform", dex);
            }
        }
        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}
