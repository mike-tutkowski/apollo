package com.datastax.iterator;

import java.util.Iterator;

/**
 * Iterator that can be used to verify if the wrapped iterator preserves
 * order. Can be useful for testing.
 */
public class SortedInvariantCheckIterator<T extends Comparable<T>> implements Iterator<T> {

    private final Iterator<T> wrapped;
    private T lastItem;

    private SortedInvariantCheckIterator(Iterator<T> wrapped) {
        this.wrapped = wrapped;
    }

    public static <T extends Comparable> Iterator<T> wrap(Iterator<T> wrapped) {
        return new SortedInvariantCheckIterator<>(wrapped);
    }

    @Override
    public boolean hasNext() {
        return wrapped.hasNext();
    }

    @Override
    public T next() {
        T next = wrapped.next();
        assert next != null;
        assert lastItem == null || next.compareTo(lastItem) > 0 : "Sorted invariant is not preserved";
        lastItem = next;
        return next;
    }

}
