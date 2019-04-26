package org.ditw.common;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.assertEquals;

/**
 * Created by dev on 2018-10-26.
 */
public class PrefixTreeDTest {

    private final static Set<String> t12 = new HashSet<>(Arrays.asList("t1", "t2"));
    private final static Set<String> t234 = new HashSet<>(Arrays.asList("t2", "t3", "t4"));
    private final static Set<String> t3 = new HashSet<>(Arrays.asList("t3"));
    private final static Set<String> t4 = new HashSet<>(Arrays.asList("t4"));
    private final static Set<String> t6 = new HashSet<>(Arrays.asList("t6"));
    private static Map<Integer[], Set<String>> createTestArray() {

        Map<Integer[], Set<String>> res = new HashMap<>();
        res.put(
                new Integer[] { 1, 2 }, t12
        );
        res.put(
                new Integer[] { 1, 2, 4, 5 }, t3
        );
        res.put(
                new Integer[] { 1, 3 }, t4
        );
        res.put(
                new Integer[] { 1, 3, 4 }, t3
        );
        res.put(
                new Integer[] { 1, 5, 4 }, t3
        );
        res.put(
                new Integer[] { 2, 3 }, t234
        );
        res.put(
                new Integer[] { 2, 4 }, t4
        );
        res.put(
                new Integer[] { 6 }, t6
        );

        return res;
    }

    private final static Map<Integer[], Set<String>> testArray1 = createTestArray();

    private final static PrefixTreeD<Integer, Set<String>> tr1 = PrefixTreeD.createPrefixTree(
            testArray1
    );
    @DataProvider(name = "prefixTreeTestData")
    public static Object[][] prefixTreeTestData() {
        return new Object[][]{
                {
                        tr1,
                        new Integer[] { 6 },
                        0,
                        new HashSet<>(Arrays.asList(t6))
                },
                {
                        tr1,
                        new Integer[] { 7, 1, 2, 4, 5, 8 },
                        1,
                        new HashSet<>(
                                Arrays.asList(t12, t3)
                        )
                },
                {
                        tr1,
                        new Integer[] { 7, 1, 2, 4, 5 },
                        1,
                        new HashSet<>(
                                Arrays.asList(t12, t3)
                        )
                },
                {
                        tr1,
                        new Integer[] { 1, 2 },
                        0,
                        new HashSet<>(
                                Arrays.asList(t12)
                        )
                },
                {
                        tr1,
                        new Integer[] { 1, 2, 4, 5 },
                        0,
                        new HashSet<>(
                                Arrays.asList(t12, t3)
                        )
                },
                {
                        tr1,
                        new Integer[] { 1, 2, 4 },
                        0,
                        new HashSet<>(Arrays.asList(t12))
                },
                {
                        tr1,
                        new Integer[] { 1, 5, 4 },
                        0,
                        new HashSet<>(
                                Arrays.asList(t3)
                        )
                },
                {
                        tr1,
                        new Integer[] { 1, 5 },
                        0,
                        new HashSet<>(0)
                },
        };
    }

    @Test(dataProvider = "prefixTreeTestData")
    public void prefixTreeTest(
            PrefixTreeD<Integer, Set<String>> tr,
            Integer[] toFind,
            int start,
            Set<Set<String>> foundResult
    ) {
        Set<ITrieItem<Integer, Set<String>>> found = tr.allPrefixes(toFind, start);
        Set<Set<String>> t = new HashSet<>(found.size());
        for (ITrieItem<Integer, Set<String>> i : found) {
            t.add(i.d());
        }
        assertEquals(t, foundResult);
    }
}
