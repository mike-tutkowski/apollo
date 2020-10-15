package com.datastax.iterator;

import java.util.*;
import java.util.function.BiFunction;

/**
 * Merge iterator: takes multiple _sorted_ row iterators and yields
 * items _preserving original order_.
 */
public class MergeIterator<T extends Comparable<T>> implements Iterator<T> {
    private final BiFunction<T, T, T> _merge;
    private final List<PeekingIterator> _iterators = new ArrayList<>();

    @SafeVarargs
    public static <T extends Comparable<T>> Iterator<T> create(BiFunction<T, T, T> merge, Iterator<T>... iterators) {
        return new MergeIterator<>(merge, iterators);
    }

    @SafeVarargs
    private MergeIterator(BiFunction<T, T, T> merge, Iterator<T>... iterators) {
        _merge = merge;

        for (Iterator<T> itr : iterators) {
            _iterators.add(new PeekingIterator(itr));
        }
    }

    @Override
    public boolean hasNext() {
        for (PeekingIterator peekingIterator : _iterators) {
            if (peekingIterator.hasNext()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public T next() {
        List<T> nextItems = new ArrayList<>(_iterators.size());
        Map<T, PeekingIterator> mapNextItems = new HashMap<>(_iterators.size());

        for (PeekingIterator peekingIterator : _iterators) {
            if (peekingIterator.hasNext()) {
                nextItems.add(peekingIterator.peek());
                mapNextItems.put(peekingIterator.peek(), peekingIterator);
            }
        }

        if (nextItems.size() == 0) {
            throw new NoSuchElementException();
        }

        Collections.sort(nextItems);

        T sameNextItem = null;

        for (T nextItem : nextItems) {
            if (sameNextItem != null) {
                if (nextItem.compareTo(sameNextItem) == 0) {
                    PeekingIterator peekingIterator = mapNextItems.get(nextItem);

                    peekingIterator.next();

                    sameNextItem = _merge.apply(sameNextItem, nextItem);
                }
            } else {
                sameNextItem = nextItem;

                PeekingIterator peekingIterator = mapNextItems.get(nextItem);

                peekingIterator.next();
            }
        }

        return sameNextItem;
    }

    private class PeekingIterator implements Iterator<T> {
        private final Iterator<T> _itr;
        private T _next;

        public PeekingIterator(Iterator<T> iterator) {
            _itr = iterator;

            moveIterator();
        }

        public T peek() {
            return _next;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            T result = _next;

            moveIterator();

            return result;
        }

        @Override
        public boolean hasNext() {
            return _next != null;
        }

        public void moveIterator() {
            if (_itr.hasNext()) {
                _next = _itr.next();
            } else {
                _next = null;
            }
        }
    }
}
