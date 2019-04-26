package org.ditw.common;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by dev on 2018-10-26.
 */
public class TrieD<S, T> implements Serializable {

    public final ITrieItem<S, T> item;
    public final boolean isWord;

    final Map<S, TrieD<S, T>> childrenMap;

    protected TrieD(ITrieItem<S, T> item, Map<S, TrieD<S, T>> childrenMap) {
        this.item = item;
        isWord = item.d() != null;
        this.childrenMap = childrenMap;
    }

    boolean find(S[] q, int start) {
        if (start >= q.length-1) {
            return q[start].equals(item.k()) && isWord;
        }

        if (q[start].equals(item.k())) {
            for (TrieD<S, T> c : childrenMap.values()) {
                if (c.find(q, start+1))
                    return true;
            }
        }

        return false;

    }

    Set<ITrieItem<S, T>> allPrefixes(S[] q, int start) {
        Set<ITrieItem<S, T>> res = new HashSet<>();
        if (start >= q.length-1) {
            if (q[start].equals(item.k()) && isWord)
                res.add(item);
            return res;
        }

        if (q[start].equals(item.k())) {
            if (isWord)
                res.add(item);
            for (TrieD<S, T> c : childrenMap.values()) {
                Set<ITrieItem<S, T>> childRes = c.allPrefixes(q, start+1);
                res.addAll(childRes);
            }
        }

        return res;
    }
}
