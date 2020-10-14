package com.datastax.iterator;

import java.util.Iterator;
import java.util.function.BiFunction;

/**
 * Merge iterator: takes multiple _sorted_ row iterators and yields
 * items _preserving original order_.
 */
public class MergeIterator<T extends Comparable<T>> implements Iterator<T> {

    public static <T extends Comparable<T>> Iterator<T> create(BiFunction<T, T, T> merge,
                                                               Iterator<T>... iterators) {
        return new MergeIterator<>(merge, iterators);
    }

    private MergeIterator(BiFunction<T, T, T> merge,
                          Iterator<T>... iterators) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean hasNext() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public T next() {
        throw new RuntimeException("Not implemented");
    }

}
