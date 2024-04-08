package lsm.index;

import org.junit.Test;
import store.lsm.index.IndexPosition;
import store.lsm.index.SparseIndex;
import store.lsm.index.SparseIndexQuery;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparseIndexTest {
    @Test
    public void testSetPositionList(){
        SparseIndex index = new SparseIndex();
        // 0, 3, 6
        index.put("0", new IndexPosition(0, 4));
        index.put("3", new IndexPosition(4, 4));
        index.put("6", new IndexPosition(8, 4));
        // 9, 12, 15
        index.put("9", new IndexPosition(12, 4));
        index.put("12", new IndexPosition(16, 4));
        index.put("15", new IndexPosition(20, 4));

        /*
            sparse index:
            0 - [], ..., 3  - [], ..., 6  - [], ...,
                                   4^            8^
            9 - [], ..., 12 - [], ..., 15 - []
         */
        SparseIndexQuery query = new SparseIndexQuery(index, null);
        query.setPositionListByKey("4"); // 0 - [], ..., 3  - []
        query.setPositionListByKey("8"); // 3  - [], ..., 6  - []

        assertNotNull(query.GetSparseKeyPositionList());
        assertEquals(query.GetSparseKeyPositionList().getFirst(), index.get("3"));
        assertEquals(query.GetSparseKeyPositionList().getLast(), index.get("9"));
    }
}
