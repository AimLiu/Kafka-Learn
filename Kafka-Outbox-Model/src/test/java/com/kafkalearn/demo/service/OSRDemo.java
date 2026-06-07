package com.kafkalearn.demo.service;

public class OSRDemo {
    public static void main(String[] args) {
        long sum = 0;
        for (int i = 0; i < 1_000_000_000; i++) {
            sum += i;
        }
        System.out.println(sum);
    }
}
