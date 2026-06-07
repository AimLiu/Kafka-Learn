package com.kafkalearn.demo.service;

import java.util.ArrayList;
import java.util.List;

public class HeapOomDemo {
    private static final List<byte[]> LIST = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        while (true) {
            LIST.add(new byte[1024 * 1024]);
            System.out.println("allocated " + LIST.size() + " MB");
            Thread.sleep(100);
        }
    }
}