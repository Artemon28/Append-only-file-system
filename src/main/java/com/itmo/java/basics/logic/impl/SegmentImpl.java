package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {

    private final File fileSeg;
    private final String nameSegment;
    private long offsetSegment;
    private SegmentIndex segIndex = new SegmentIndex();
    private final String pathSeg;


    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        if (segmentName == null || tableRootPath == null){
            throw new DatabaseException("one of param is null in create in class segment");
        }
        String pathtoSegment = String.valueOf(tableRootPath.resolve(segmentName));
        try {
            if (!(new File(pathtoSegment).createNewFile())) {
                throw new IOException("file " + segmentName + "not created");
            }
        } catch (IOException e) {
            throw new DatabaseException("IO exception during to creating file" + segmentName, e);
        }
        return new SegmentImpl(new File(String.valueOf(tableRootPath), segmentName), segmentName, pathtoSegment);
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return new SegmentImpl(new File(String.valueOf(context.getSegmentPath())), context.getSegmentName(),
                String.valueOf(context.getSegmentPath()), context.getCurrentSize(), context.getIndex());
    }

    private SegmentImpl(File fileSeg, String nameSegment, String path, long offsetSegment, SegmentIndex segIndex) {
        this.nameSegment = nameSegment;
        this.fileSeg = fileSeg;
        this.pathSeg = path;
        this.offsetSegment = offsetSegment;
        this.segIndex = segIndex;
    }

    private SegmentImpl(File _fileSeg, String _nameSegment, String _path) {
        nameSegment = _nameSegment;
        fileSeg = _fileSeg;
        pathSeg = _path;
        offsetSegment = 0;
    }

     static String createSegmentName(String tableName) {
         try {
             Thread.sleep(0);
         } catch (InterruptedException e) {
             e.printStackTrace();
         }
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return nameSegment;
    }


    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (objectKey == null) {
            throw new IOException("object key is null in write in class SegmentImpl");
        }
        if (isReadOnly()) {
            return false;
        }
        if (objectValue == null){
            return delete(objectKey);
        }
        try(FileOutputStream SegmentOutputStream = new FileOutputStream(pathSeg, true)){
            SetDatabaseRecord rec1 = new SetDatabaseRecord(objectKey, objectValue);
            SegmentOffsetInfoImpl segmentOffsetInformation = new SegmentOffsetInfoImpl(offsetSegment);
            segIndex.onIndexedEntityUpdated(objectKey, segmentOffsetInformation);
            DatabaseOutputStream DataOutputStream = new DatabaseOutputStream(SegmentOutputStream);
            DataOutputStream.write(rec1);
            offsetSegment += rec1.size();
        }
        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        if (segIndex.searchForKey(objectKey).isEmpty()){
            return Optional.empty();
        }
        try (FileInputStream segmentInputStream = new FileInputStream(pathSeg)) {
            DatabaseInputStream dataInputStream = new DatabaseInputStream(segmentInputStream);
            long nowOffSet = segIndex.searchForKey(objectKey).get().getOffset();
            if (dataInputStream.skip(nowOffSet) != nowOffSet){
                throw new IOException("can't skip enough to read key: " + objectKey);
            }
            Optional<DatabaseRecord> tempRec = dataInputStream.readDbUnit();
            if (tempRec.isPresent()){
                return Optional.of(tempRec.get().getValue());
            }
            return Optional.empty();
        }
    }

    @Override
    public boolean isReadOnly() {
        if (offsetSegment >= 100_000){
            return true;
        }
        return false;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        if (objectKey == null) {
            throw new IOException("objectKey is null in deleting in class SegmentImpl");
        }
        if (isReadOnly()) {
            return false;
        }
        try (FileOutputStream segmentOutputStream = new FileOutputStream(pathSeg, true)) {
            RemoveDatabaseRecord removeRecord = new RemoveDatabaseRecord(objectKey);
            segIndex.onIndexedEntityUpdated(objectKey, null);
            DatabaseOutputStream dataOutputStream = new DatabaseOutputStream(segmentOutputStream);
            if (dataOutputStream.write(removeRecord) != removeRecord.size()) {
                throw new IOException("can't write in segment " + nameSegment);
            }
            offsetSegment += removeRecord.size();
        }
        return true;
    }
}
