package org.ditw.common;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by dev on 2018-10-26.
 */
public class Trie<T> implements Serializable {
    public final T v;
    public final boolean isWord;

    final Map<T, Trie<T>> childrenMap;

    boolean find(T[] q, int start) {
        if (start >= q.length-1) {
            if (q[start] == v)
                return isWord;
            else
                return false;
        }

        if (q[start] == v) {
            for (Trie<T> c : childrenMap.values()) {
                if (c.find(q, start+1))
                    return true;
            }
        }

        return false;
    }

    Set<Integer> allPrefixes(T[] q, int start) {
        Set<Integer> res = new HashSet<>();
        if (start >= q.length-1) {
            if (q[start].equals(v) && isWord)
                res.add(1);
            return res;
        }

        if (q[start].equals(v)) {
            if (isWord)
                res.add(1);
            for (Trie<T> c : childrenMap.values()) {
                Set<Integer> childRes = c.allPrefixes(q, start+1);
                for (Integer i : childRes) {
                    res.add(i+1);
                }
            }
        }

        return res;
    }

    protected Trie(T v, boolean isWord, Map<T, Trie<T>> childrenMap) {
        this.v = v;
        this.isWord = isWord;
        this.childrenMap = childrenMap;
    }

//    private static final Map<Integer, Trie<Integer>> EmptyChildMap_Integer = new HashMap<>(0);
//    private static <T> Trie<T> leaf(T v) {
//        return new Trie(v, true, new HashMap<>(0));
//    }

}
