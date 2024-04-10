package lsm.table;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import store.lsm.table.TableMetaData;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TableMetaDataTest {
    private static final String baseTestPath = "./src/test/resources/metadata/";
    private static final String fileName = String.valueOf(System.currentTimeMillis());

    @AfterEach
    public void removeTestTable() {
        Arrays.stream(Objects.requireNonNull(new File(baseTestPath).listFiles())).forEach(File::delete);
    }

    @Deprecated
    @Test
    public void testReadWriteDeprecated() throws IOException {
        // |      3      |     0     |    566  |     566    |    123   |    0    |
        RandomAccessFile file = new RandomAccessFile(baseTestPath + fileName, "rw");
        TableMetaData table = new TableMetaData();

        table.segmentSize = 3;
        table.dataStart = 0;
        table.dataLen = 566;
        table.indexStart = 566;
        table.indexLen = 123;
        table.version = 0;

        table.write(file);

        table = TableMetaData.read(file);
        assertEquals(3, table.segmentSize);
        assertEquals(0, table.dataStart);
        assertEquals(566, table.dataLen);
        assertEquals(566, table.indexStart);
        assertEquals(123, table.indexLen);
        assertEquals(0, table.version);
    }

    @Test
    public void testReadWrite() throws IOException {
        // |      3      |     0     |    566  |     566    |    123   |    0    |
        Path path = Path.of(baseTestPath + fileName);
        TableMetaData table = new TableMetaData(path);

        table.segmentSize = 3;
        table.dataStart = 0;
        table.dataLen = 566;
        table.indexStart = 566;
        table.indexLen = 123;
        table.version = 0;

        table.write();

        table = TableMetaData.read(path);
        assertEquals(3, table.segmentSize);
        assertEquals(0, table.dataStart);
        assertEquals(566, table.dataLen);
        assertEquals(566, table.indexStart);
        assertEquals(123, table.indexLen);
        assertEquals(0, table.version);
    }
}
