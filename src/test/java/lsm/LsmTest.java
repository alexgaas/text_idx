package lsm;
import store.Store;
import store.lsm.Lsm;
import org.junit.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LsmTest {
    @Test
    public void set() throws IOException {
        Store lsm = new Lsm("src/test/resources/lsm/", 4, 3);
        for (int i = 0; i < 11; i++) {
            lsm.put(i + "", i + "");
        }

        for (int i = 0; i < 11; i++) {
            assertEquals(i + "", lsm.get(i + ""));
        }

        for (int i = 0; i < 11; i++) {
            lsm.remove(i + "");
        }

        for (int i = 0; i < 11; i++) {
            assertNull(lsm.get(i + ""));
        }

        lsm.close();
    }
}
