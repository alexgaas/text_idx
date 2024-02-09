package store.lsm;
import java.io.Closeable;
public interface Store extends Closeable {
    void put(String key, String value);
    String get(String key);
    void remove(String key);
}
