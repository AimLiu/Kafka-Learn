package com.kafkalearn.demo.service.gc;

import java.lang.ref.SoftReference;

public class SoftReferenceDemo {
    public static void main(String[] args) {
        SoftReference<byte[]> ref =
                new SoftReference<>(new byte[10 * 1024 * 1024]);

        System.out.println(ref.get() != null);

        // 尝试制造内存压力
        byte[] pressure = new byte[20 * 1024 * 1024];

        System.out.println(ref.get() != null);
    }
}