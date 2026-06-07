package com.kafkalearn.demo.service;

public class TLABDemo {
    static class SmallObject {
        int a;
        int b;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 5_000_000; i++) {
            new SmallObject();
        }
    }
}
