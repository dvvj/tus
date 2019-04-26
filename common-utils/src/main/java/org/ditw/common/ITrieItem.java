package org.ditw.common;

import java.io.Serializable;

/**
 * Created by dev on 2018-10-26.
 */
public interface ITrieItem<K, U> extends Serializable {
    K k();
    U d();
    int length();
}
