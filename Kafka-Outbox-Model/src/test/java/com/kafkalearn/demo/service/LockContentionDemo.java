package com.kafkalearn.demo.service;

public class LockContentionDemo {
    static final Object LOCK = new Object();

    public static void main(String[] args) {
        Runnable task = () -> {
            while (true) {
                synchronized (LOCK) {
                    try {
                        Thread.sleep(10_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };

        for (int i = 0; i < 5; i++) {
            new Thread(task, "worker-" + i).start();
        }
    }
}
