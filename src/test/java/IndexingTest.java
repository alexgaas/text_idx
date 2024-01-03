import app.indexing.IndexingStrategy;
import org.junit.jupiter.api.Test;

public class IndexingTest {
    @Test
    void test() throws Exception {
        IndexingStrategy.startIndexing("src/test/resources/144133901.txt");
    }
}
