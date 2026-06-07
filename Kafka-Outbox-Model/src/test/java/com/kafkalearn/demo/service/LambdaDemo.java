package com.kafkalearn.demo.service;

import java.util.function.Function;

public class LambdaDemo {
    public static void main(String[] args) {
        Function<String, Integer> f = s -> s.length();
        System.out.println(f.apply("hello"));
    }
}
