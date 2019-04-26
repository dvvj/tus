package org.ditw.common;

import java.io.Serializable;
import java.util.*;

/**
 * Created by dev on 2018-10-26.
 */
public class PrefixTree<T> implements Serializable {
    private final Map<T, Trie<T>> children;
    public final int maxDepth;
    protected PrefixTree(int maxDepth, Map<T, Trie<T>> children) {
        this.children = children;
        this.maxDepth = maxDepth;
    }

    boolean find(T[] q) {
        if (q.length <= 0)
            throw new IllegalArgumentException("Empty input?");
        else {
            T q0 = q[0];
            if (children.containsKey(q0))
                return children.get(q0).find(q, 0);
            else
                return false;
        }
    }

    private final static Set<Integer> EmptyLengthSet = new HashSet<>(0);
    public Set<Integer> allPrefixes(T[] q, int start) {
        if (q.length <= start)
            throw new IllegalArgumentException("Empty input?");
        else {
            T q0 = q[start];
            if (children.containsKey(q0)) {
                T[] q1 = q.length > start + maxDepth ?
                        Arrays.copyOfRange(q, start, start + maxDepth) :
                        Arrays.copyOfRange(q, start, q.length);
                return children.get(q0).allPrefixes(q1, 0);
            }
            else
                return EmptyLengthSet;
        }
    }

    private static <T> Map<T, Trie<T>> createFrom(List<T[]> input) {
        Map<T, ArrayList<T[]>> head2TailMap = new HashMap<>();

        for (T[] i : input) {
            T head = i[0];
            if (!head2TailMap.containsKey(head)) {
                head2TailMap.put(head, new ArrayList<>());
            }

            if (i.length >= 1)
                head2TailMap.get(head).add(Arrays.copyOfRange(i, 1, i.length));
        }

        Map<T, Trie<T>> res = new HashMap<>(head2TailMap.size());
        for (T k : head2TailMap.keySet()) {
            List<T[]> tail = head2TailMap.get(k);

            List<T[]> nonEmptyTail = new ArrayList<>(tail.size());
            boolean hasEmpty = false;
            for (T[] t : tail) {
                if (t.length > 0)
                    nonEmptyTail.add(t);
                else {
                    hasEmpty = true;
                }
            }
            Map<T, Trie<T>> children = createFrom(nonEmptyTail);

            Trie<T> n = new Trie<>(k, hasEmpty, children);
            res.put(k, n);
        }

        return res;
    }


    public static <T> PrefixTree<T> createPrefixTree(List<T[]> input) {
        int maxDepth = Integer.MIN_VALUE;
        for (T[] i : input) {
            if (maxDepth < i.length)
                maxDepth = i.length;
        }
        Map<T, Trie<T>> children = createFrom(input);
        return new PrefixTree<>(maxDepth, children);
    }
}