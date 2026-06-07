package com.kafkalearn.demo.service;

import org.openjdk.jol.info.ClassLayout;

public class JOLDemo {
    static class User {
        int id;
        String name;
        boolean active;
    }

    public static void main(String[] args) {
        System.out.println(ClassLayout.parseInstance(new User()).toPrintable());
    }
}