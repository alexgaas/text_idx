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
}
