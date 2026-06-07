package com.kafkalearn.demo.service;


public class StackOverflowDemo {
    static int depth = 0;

    public static void main(String[] args) {
        recurse();
    }

    static void recurse() {
        depth++;
        if (depth % 1000 == 0) {
            System.out.println("depth=" + depth);
        }
        recurse();
    }
}