package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Vector;

public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path pathToSegment = context.currentSegmentContext().getSegmentPath();
        String pathSeg = String.valueOf(pathToSegment);
        SegmentIndex segIndex = new SegmentIndex();
        Vector<String> vectorOfKeys = new Vector<>();
        long currentOffset = 0;
        try (FileInputStream segmentInputStream = new FileInputStream(pathSeg)) {
            DatabaseInputStream dataInputStream = new DatabaseInputStream(segmentInputStream);
            Optional<DatabaseRecord> tempRec = dataInputStream.readDbUnit();
            while (!tempRec.isEmpty()) {
                segIndex.onIndexedEntityUpdated(new String(tempRec.get().getKey()),
                        new SegmentOffsetInfoImpl(currentOffset));
                vectorOfKeys.addElement(new String(tempRec.get().getKey()));
                currentOffset += tempRec.get().size();
                tempRec = dataInputStream.readDbUnit();
            }
            SegmentInitializationContextImpl currentSegmentCont = new SegmentInitializationContextImpl(
                    context.currentSegmentContext().getSegmentName(), pathToSegment,
                    currentOffset, segIndex);
            Segment segment = SegmentImpl.initializeFromContext(currentSegmentCont);
            context.currentTableContext().updateCurrentSegment(segment);
            for (String vectorOfKey : vectorOfKeys) {
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(vectorOfKey, segment);
            }
        } catch (FileNotFoundException e) {
            throw new DatabaseException("File " + context.currentSegmentContext().getSegmentName() + " not found", e);
        } catch (IOException e) {
            throw new DatabaseException("IOexception while reading file " + context.currentSegmentContext().getSegmentName(), e);
        }
    }
}
