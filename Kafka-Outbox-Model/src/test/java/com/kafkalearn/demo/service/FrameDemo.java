package com.kafkalearn.demo.service;

public class FrameDemo {
    public static int add(int a, int b) {
        int c = a + b;
        return c;
    }

    public static void main(String[] args) {
        System.out.println(add(1, 2));
    }
}