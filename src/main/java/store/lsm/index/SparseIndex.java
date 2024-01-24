package store.lsm.index;

import java.util.AbstractMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class SparseIndex extends AbstractMap<String, IndexPosition> {
    private final TreeMap<String, IndexPosition> index;

    public SparseIndex(){
        super();
        this.index = new TreeMap<>();
    }

    @Override
    public Set<Entry<String, IndexPosition>> entrySet() {
        return index.entrySet();
    }

    @Override
    public IndexPosition getOrDefault(Object key, IndexPosition defaultValue) {
        return index.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super IndexPosition> action) {
        index.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super IndexPosition, ? extends IndexPosition> function) {
        index.replaceAll(function);
    }

    @Override
    public IndexPosition putIfAbsent(String key, IndexPosition value) {
        return index.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return index.remove(key, value);
    }

    @Override
    public boolean replace(String key, IndexPosition oldValue, IndexPosition newValue) {
        return index.replace(key, oldValue, newValue);
    }

    @Override
    public IndexPosition replace(String key, IndexPosition value) {
        return index.replace(key, value);
    }

    @Override
    public IndexPosition computeIfAbsent(String key, Function<? super String, ? extends IndexPosition> mappingFunction) {
        return index.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public IndexPosition computeIfPresent(String key, BiFunction<? super String, ? super IndexPosition, ? extends IndexPosition> remappingFunction) {
        return index.computeIfPresent(key, remappingFunction);
    }

    @Override
    public IndexPosition compute(String key, BiFunction<? super String, ? super IndexPosition, ? extends IndexPosition> remappingFunction) {
        return index.compute(key, remappingFunction);
    }

    @Override
    public IndexPosition merge(String key, IndexPosition value, BiFunction<? super IndexPosition, ? super IndexPosition, ? extends IndexPosition> remappingFunction) {
        return index.merge(key, value, remappingFunction);
    }

    @Override
    public IndexPosition put(String key, IndexPosition value){
        return index.put(key, value);
    }
}
