package org.ditw.common;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * Created by dev on 2018-10-26.
 */
public class PrefixTreeTest {

    private final static Integer[][] testArray1 =
            new Integer[][] {
                    new Integer[] { 1, 2 },
                    new Integer[] { 1, 2, 4, 5 },
                    new Integer[] { 1, 3 },
                    new Integer[] { 1, 3, 4 },
                    new Integer[] { 1, 5, 4 },
                    new Integer[] { 2, 3 },
                    new Integer[] { 2, 4 },
            };
    private final static PrefixTree<Integer> tr1 = PrefixTree.createPrefixTree(
            Arrays.asList(testArray1)
    );
    @DataProvider(name = "prefixTreeTestData")
    public static Object[][] prefixTreeTestData() {
        return new Object[][]{
                {
                        tr1,
                        new Integer[] { 1, 2 },
                        true
                },
                {
                        tr1,
                        new Integer[] { 1, 2, 4, 5 },
                        true
                },
                {
                        tr1,
                        new Integer[] { 1, 2, 4 },
                        false
                },
                {
                        tr1,
                        new Integer[] { 1, 3, 4 },
                        true
                },
        };
    }

    @Test(dataProvider = "prefixTreeTestData")
    public void prefixTreeTest(PrefixTree<Integer> tr, Integer[] toFind, boolean foundResult) {
        boolean found = tr.find(toFind);
        assertEquals(found, foundResult);
    }

    @DataProvider(name = "findPrefixTestData")
    public static Object[][] findPrefixTestData() {
        return new Object[][]{
                {
                        tr1,
                        new Integer[] { 7, 1, 2, 4, 5, 8 },
                        0,
                        new HashSet<>(
                                Arrays.asList(
                                        new Integer[] { }
                                )
                        )
                },
                {
                        tr1,
                        new Integer[] { 7, 1, 2, 4, 5, 8 },
                        1,
                        new HashSet<>(
                                Arrays.asList(
                                        2, 4
                                )
                        )
                },
                {
                        tr1,
                        new Integer[] { 1, 2, 4, 5, 7, 8 },
                        0,
                        new HashSet<>(
                                Arrays.asList(
                                        2, 4
                                )
                        )
                },
                {
                        tr1,
                        new Integer[] { 1, 5 },
                        0,
                        new HashSet<>(
                                Arrays.asList(
                                        new Integer[] { }
                                )
                        )
                },
                {
                        tr1,
                        new Integer[] { 1, 2, 4, 5 },
                        0,
                        new HashSet<>(
                                Arrays.asList(
                                        2, 4
                                )
                        )
                },
                {
                        tr1,
                        new Integer[] { 1, 2 },
                        0,
                        new HashSet<>(
                                Arrays.asList(
                                        2
                                )
                        )
                },
                {
                        tr1,
                        new Integer[] { 1, 2, 4 },
                        0,
                        new HashSet<>(
                                Arrays.asList(
                                        2
                                )
                        )
                },
        };
    }

    @Test(dataProvider = "findPrefixTestData")
    public void findPrefixTest(PrefixTree<Integer> tr, Integer[] toFind, int start, Set<Integer> foundResult) {
        Set<Integer> found = tr.allPrefixes(toFind, start);
        assertEquals(found, foundResult);
    }

}
