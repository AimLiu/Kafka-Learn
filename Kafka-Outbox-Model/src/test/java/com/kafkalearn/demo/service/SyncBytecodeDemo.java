package com.kafkalearn.demo.service;

public class SyncBytecodeDemo {
    private final Object lock = new Object();

    public void block() {
        synchronized (lock) {
            System.out.println("hello");
        }
    }

    public synchronized void method() {
        System.out.println("method");
    }
}