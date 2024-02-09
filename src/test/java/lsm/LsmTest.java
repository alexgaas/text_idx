package lsm;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import store.lsm.Store;
import store.lsm.Lsm;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LsmTest {
    @BeforeAll
    public void setup() throws IOException {
        Files.delete(Path.of("src/test/resources/"));
    }

    @Test
    public void basicLsmTest() throws IOException {
        Store lsm = new Lsm("src/test/resources/", 4, 3);

        IntStream.range(0, 11).forEachOrdered(i -> lsm.put(i + "", i + ""));
        IntStream.range(0, 11).forEachOrdered(i -> assertEquals(i + "", lsm.get(i + "")));
        IntStream.range(0, 11).mapToObj(i -> i + "").forEachOrdered(lsm::remove);
        IntStream.range(0, 11).mapToObj(i -> lsm.get(i + "")).forEachOrdered(Assertions::assertNull);

        lsm.close();
    }
}
