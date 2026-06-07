package com.kafkalearn.demo.service;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DirectMemoryOomDemo {
    private static final List<ByteBuffer> BUFFERS = new ArrayList<>();

    public static void main(String[] args) {
        int count = 0;
        while (true) {
            BUFFERS.add(ByteBuffer.allocateDirect(1024 * 1024));
            count++;
            System.out.println("direct allocated " + count + " MB");
        }
    }
}