package lsm;
import org.junit.jupiter.api.*;
import store.lsm.Store;
import store.lsm.Lsm;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LsmTest {
    private static final String baseTestPath = "./src/test/resources/lsm/";

    @AfterEach
    public void removeTestTable() {
        Arrays.stream(Objects.requireNonNull(new File(baseTestPath).listFiles())).forEach(File::delete);
    }

    @Test
    public void basicLsmTest() throws IOException {
        try(Store lsm = new Lsm(baseTestPath, 4, 3)) {
            // put test data
            for (int i1 = 0; i1 < 10; i1++) {
                lsm.put(String.valueOf(i1), String.valueOf(i1));
            }
            // assert data in the LSM store
            for (int i1 = 0; i1 < 10; i1++) {
                assertEquals(String.valueOf(i1), lsm.get(String.valueOf(i1)));
            }
            // remove data from LSM store
            for (int i1 = 0; i1 < 10; i1++) {
                String s = String.valueOf(i1);
                lsm.remove(s);
            }
            // assert data have been removed
            for (int i = 0; i < 10; i++) {
                String s = lsm.get(String.valueOf(i));
                Assertions.assertNull(s);
            }
        }
    }

    @Test
    public void testPutOperations() throws IOException {
        try(Store lsm = new Lsm(baseTestPath, 2, 3)) {
            lsm.put("1", "value1");
            /*
            log:
            0000 0029 7b22 7479 7065 223a 2253 4554
            222c 226b 6579 223a 2231 222c 2276 616c
            7565 223a 2276 616c 7565 3122 7d
             */

            lsm.put("2", "value2");
            lsm.put("3", "value3");
            /*
            log - empty

            1713212049079.table:
            {
                "1":"{\"type\":\"SET\",\"key\":\"1\",\"value\":\"value1\"}",
                "2":"{\"type\":\"SET\",\"key\":\"2\",\"value\":\"value2\"}",
                "3":"{\"type\":\"SET\",\"key\":\"3\",\"value\":\"value3\"}"
            }
            {
                "1":{"start":0,"len":181}
            }
             */


            lsm.put("4", "value4");
            /*
            log:

            0000 0029 7b22 7479 7065 223a 2253 4554
            222c 226b 6579 223a 2234 222c 2276 616c
            7565 223a 2276 616c 7565 3422 7d

            1713212240256.table:
            {
                "1":"{\"type\":\"SET\",\"key\":\"1\",\"value\":\"value1\"}",
                "2":"{\"type\":\"SET\",\"key\":\"2\",\"value\":\"value2\"}",
                "3":"{\"type\":\"SET\",\"key\":\"3\",\"value\":\"value3\"}"
            }
            {
                "1":{"start":0,"len":181}
            }
             */

            lsm.put("5", "value5");
            lsm.put("6", "value6");
            /*
            log - empty

            1713212503730.table:
            {
                "1":"{\"type\":\"SET\",\"key\":\"1\",\"value\":\"value1\"}",
                "2":"{\"type\":\"SET\",\"key\":\"2\",\"value\":\"value2\"}",
                "3":"{\"type\":\"SET\",\"key\":\"3\",\"value\":\"value3\"}"
            }
            {
                "1":{"start":0,"len":181}
            }

            1713212503798.table:
            {
                "4":"{\"type\":\"SET\",\"key\":\"4\",\"value\":\"value4\"}",
                "5":"{\"type\":\"SET\",\"key\":\"5\",\"value\":\"value5\"}",
                "6":"{\"type\":\"SET\",\"key\":\"6\",\"value\":\"value6\"}"
            }
            {
                "4":{"start":0,"len":181}
            }
             */
        }
    }
}
