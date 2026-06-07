package com.kafkalearn.demo.service;

public class JitDemo {
    public static void main(String[] args) {
        long r = 0;
        for (int i = 0; i < 100_000_000; i++) {
            r += compute(i);
        }
        System.out.println(r);
    }

    static int compute(int x) {
        Object o1 = new Object();
        return x * 31 + 17;
    }
}
