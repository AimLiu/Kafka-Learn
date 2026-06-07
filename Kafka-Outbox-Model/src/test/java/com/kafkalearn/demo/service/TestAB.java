package com.kafkalearn.demo.service;

class OrderDemo {
    static int a = print("a", 1);

    static int b = print("b", 2);

    static {
        b = 20;
        System.out.println("static block 1, a=" + a + ", b=" + b);
    }

    static {
        System.out.println("static block 2, a=" + a + ", b=" + b);
    }

    static int print(String name, int value) {
        System.out.println("assign " + name + "=" + value);
        return value;
    }
}

public class TestAB {
    public static void main(String[] args) {
        System.out.println(OrderDemo.a);
    }
}