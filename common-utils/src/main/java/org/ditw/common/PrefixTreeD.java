package org.ditw.common;

import java.io.Serializable;
import java.util.*;

/**
 * Created by dev on 2018-10-26.
 */
public class PrefixTreeD<S, T> implements Serializable {
    private final Map<S,  TrieD<S, T>> children;
    public final int maxDepth;
    protected PrefixTreeD(int maxDepth, Map<S, TrieD<S, T>> children) {
        this.children = children;
        this.maxDepth = maxDepth;
    }

    boolean find(S[] q) {
        if (q.length <= 0)
            throw new IllegalArgumentException("Empty input?");
        else {
            S q0 = q[0];
            if (children.containsKey(q0))
                return children.get(q0).find(q, 0);
            else
                return false;
        }
    }

    public Set<ITrieItem<S, T>> allPrefixes(S[] q, int start) {
        if (q.length <= start)
            throw new IllegalArgumentException("Empty input?");
        else {
            S q0 = q[start];
            if (children.containsKey(q0)) {
                S[] q1 = q.length > start + maxDepth ?
                        Arrays.copyOfRange(q, start, start + maxDepth) :
                        Arrays.copyOfRange(q, start, q.length);
                return children.get(q0).allPrefixes(q1, 0);
            }
            else
                return new HashSet<>(0);
        }
    }

    private static <S, T> ITrieItem<S, T> defTrieItem(final S s, final T t, int len) {
        return new ITrieItem<S, T>() {
            @Override
            public S k() {
                return s;
            }

            @Override
            public T d() {
                return t;
            }

            @Override
            public int length() {
                return len;
            }
        };
    }

    private static <S, T>
    Map<S, TrieD<S, T>> createFrom(Map<S[], T> input, int depth) {

        Map<S, Map<S[], T>> head2TailMap = new HashMap<>();

        for (S[] i : input.keySet()) {
            S head = i[0];
            T v = input.get(i);
            if (!head2TailMap.containsKey(head)) {
                head2TailMap.put(head, new HashMap<>());
            }

            if (i.length >= 1) {
                head2TailMap.get(head).put(
                        Arrays.copyOfRange(i, 1, i.length),
                        v
                );
            }
        }

        Map<S, TrieD<S, T>> res = new HashMap<>(head2TailMap.size());
        for (S k : head2TailMap.keySet()) {
            Map<S[], T> tail = head2TailMap.get(k);

            Map<S[], T> nonEmptyTail = new HashMap<>(tail.size());
            T v = null;
            for (S[] s : tail.keySet()) {
                if (s.length > 0)
                    nonEmptyTail.put(s, tail.get(s));
                else {
                    v = tail.get(s);
                }
            }
            Map<S, TrieD<S, T>> children = createFrom(nonEmptyTail, depth+1);

            ITrieItem<S, T> item = defTrieItem(k, v, depth);
            TrieD<S, T> n =
                    new TrieD<>(item, children);
            res.put(k, n);
        }

        return res;
    }


    public static <S, T> PrefixTreeD<S, T> createPrefixTree(Map<S[], T> input) {
        int maxDepth = Integer.MIN_VALUE;
        for (S[] i : input.keySet()) {
            if (maxDepth < i.length)
                maxDepth = i.length;
        }
        Map<S, TrieD<S, T>> children = createFrom(input, 1);
        return new PrefixTreeD<>(maxDepth, children);
    }

}
