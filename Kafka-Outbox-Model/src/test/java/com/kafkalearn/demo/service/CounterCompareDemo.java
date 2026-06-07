package com.kafkalearn.demo.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class CounterCompareDemo {
    static final int THREADS = 16;
    static final int TIMES = 1_000_000;

    public static void main(String[] args) throws Exception {
        testAtomicLong();
        testLongAdder();
    }

    static void testAtomicLong() throws Exception {
        AtomicLong counter = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(THREADS);
        long start = System.currentTimeMillis();

        for (int i = 0; i < THREADS; i++) {
            new Thread(() -> {
                for (int j = 0; j < TIMES; j++) {
                    counter.incrementAndGet();
                }
                latch.countDown();
            }).start();
        }

        latch.await();
        System.out.println("AtomicLong: " + (System.currentTimeMillis() - start));
    }

    static void testLongAdder() throws Exception {
        LongAdder counter = new LongAdder();
        CountDownLatch latch = new CountDownLatch(THREADS);
        long start = System.currentTimeMillis();

        for (int i = 0; i < THREADS; i++) {
            new Thread(() -> {
                for (int j = 0; j < TIMES; j++) {
                    counter.increment();
                }
                latch.countDown();
            }).start();
        }

        latch.await();
        System.out.println("LongAdder: " + (System.currentTimeMillis() - start));
    }
}