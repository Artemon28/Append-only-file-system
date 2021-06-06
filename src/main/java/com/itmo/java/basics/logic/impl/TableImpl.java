package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {

    private final String nameOfTable;
    private final File fileSegment;
    private final TableIndex tableIndex;
    private Segment activeSegment;

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("tableName is null in creating table");
        }
        String pathtoTable = String.valueOf(pathToDatabaseRoot.resolve(tableName));
        if (!(new File(pathtoTable).mkdir())){
            throw new DatabaseException("can't create table: " + tableName);
        }
        return new CachingTable(new TableImpl(String.valueOf(pathToDatabaseRoot.resolve(tableName)), tableName, tableIndex));
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new CachingTable(new TableImpl(String.valueOf(context.getTablePath()), context.getTableName(),
                context.getTableIndex(), context.getCurrentSegment()));
    }

    private TableImpl(String pathName, String tableName, TableIndex tableIndex, Segment activeSegment) {
        fileSegment = new File(pathName);
        nameOfTable = tableName;
        this.tableIndex = tableIndex;
        this.activeSegment = activeSegment;
    }

    private TableImpl(String pathName, String tableName, TableIndex tableIndex) {
        fileSegment = new File(pathName);
        nameOfTable = tableName;
        this.tableIndex = tableIndex;
    }

    @Override
    public String getName() {
        return nameOfTable;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (objectKey == null){
            throw new DatabaseException("key is null");
        }
        try{
            if (activeSegment == null || activeSegment.isReadOnly()) {
                activeSegment = SegmentImpl.create(SegmentImpl.createSegmentName(nameOfTable), fileSegment.toPath());
            }
            activeSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, activeSegment);
        } catch (IOException io) {
            throw new DatabaseException("error with write in table: " + nameOfTable, io);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("key is null in reading in table: " + nameOfTable);
        }
        if (tableIndex.searchForKey(objectKey).isEmpty()) {
            return Optional.empty();
        }
        try {
            Optional<Segment> nullTest = tableIndex.searchForKey(objectKey);
            if (nullTest.isEmpty()){
                return Optional.empty();
            }
            return nullTest.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("error with reading key: " + objectKey, e);
        }
    }

   @Override
   public void delete(String objectKey) throws DatabaseException {
       if (tableIndex.searchForKey(objectKey).isPresent()) {
           if (objectKey == null)
               throw new DatabaseException("null key in table: " + nameOfTable);
           try {
               if (activeSegment == null || activeSegment.isReadOnly()){
                   activeSegment = SegmentImpl.create(SegmentImpl.createSegmentName(nameOfTable), fileSegment.toPath());
               }
               activeSegment.delete(objectKey);
               tableIndex.onIndexedEntityUpdated(objectKey, activeSegment);
           } catch (IOException io) {
               throw new DatabaseException("error with write in table: " + nameOfTable, io);
           }
       }
   }
}
