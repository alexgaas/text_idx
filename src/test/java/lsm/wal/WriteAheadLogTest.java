package lsm.wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import store.lsm.block.impl.BlockOperation;
import store.lsm.block.impl.StBlock;
import store.lsm.index.Index;
import store.lsm.wal.WriteAheadLog;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class WriteAheadLogTest {
    private static final String baseTestPath = "./src/test/resources/wal/";

    @AfterEach
    public void removeTestTable() {
        Arrays.stream(Objects.requireNonNull(new File(baseTestPath).listFiles())).forEach(File::delete);
    }

    @Test
    public void testRestoreFromLog() throws IOException {
        String key = "1";
        String value = "value1";

        String key2 = "2";
        String value2 = "value2";

        Index index = new Index();
        try(WriteAheadLog writeAheadLog = new WriteAheadLog(baseTestPath)) {
            // add first record
            StBlock block = new StBlock(key, value);
            byte[] bytes = BlockOperation.toByteArray(block);
            writeAheadLog.writeInt(bytes.length);
            writeAheadLog.write(bytes);
            // add second record
            block = new StBlock(key2, value2);
            bytes = BlockOperation.toByteArray(block);
            writeAheadLog.writeInt(bytes.length);
            writeAheadLog.write(bytes);

            writeAheadLog.restoreFromLog(index);
            assertEquals(2, index.size());
        }
    }

    @Test
    public void testRenameLogFileToCopy() throws IOException {
        try(WriteAheadLog writeAheadLog = new WriteAheadLog(baseTestPath)) {
            writeAheadLog.renameLogFileToCopy();
            String WRITE_AHEAD_LOG_TMP = "logTmp";
            assertTrue(Files.exists(Path.of(baseTestPath + WRITE_AHEAD_LOG_TMP)));
        }
    }

    @Test
    public void testRemoveLogCopy() throws IOException {
        try(WriteAheadLog writeAheadLog = new WriteAheadLog(baseTestPath)) {
            writeAheadLog.removeLogCopy();
            String WRITE_AHEAD_LOG_TMP = "logTmp";
            assertFalse(Files.exists(Path.of(baseTestPath + WRITE_AHEAD_LOG_TMP)));
        }
    }
}
