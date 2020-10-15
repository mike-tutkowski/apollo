package com.datastax.iterator;

import java.util.*;

public class AsyncIterator<T> implements Iterator<T> {
    private static final int MAX_QUEUE_SIZE = 100;

    private final Queue<T> queue = new LinkedList<>();
    private final Iterator<T> itr;

    public AsyncIterator(Iterator<T> iterator) {
        itr = iterator;

        new Thread(() -> {
            while (true) {
                synchronized (queue) {
                    if (itr.hasNext()) {
                        T next = itr.next();

                        produce(next);
                    } else {
                        break;
                    }
                }
            }
        }).start();
    }

    @Override
    public boolean hasNext() {
        synchronized (queue) {
            return !queue.isEmpty() || itr.hasNext();
        }
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return consume();
    }

    private void produce(T item) {
        synchronized (queue) {
            while (queue.size() == MAX_QUEUE_SIZE) {
                try {
                    queue.wait();
                }
                catch (InterruptedException ex) {
                    System.out.println(ex.getMessage());
                }
            }

            queue.add(item);
            queue.notify();
        }
    }

    private T consume() {
        synchronized (queue) {
            while (queue.size() == 0) {
                try {
                    queue.wait();
                }
                catch (InterruptedException ex) {
                    System.out.println(ex.getMessage());
                }
            }

            T item = queue.remove();

            queue.notify();

            return item;
        }
    }
}
