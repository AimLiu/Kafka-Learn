package com.kafkalearn.demo.service.gc;

public class ReachabilityDemo {
    static class Node {
        private byte[] data = new byte[10 * 1024 * 1024];
        private Node other;
    }

    public static void main(String[] args) throws Exception {
        Node a = new Node();
        Node b = new Node();

        a.other = b;
        b.other = a;

        a = null;
        b = null;

        System.gc();

        Thread.sleep(3000);
        System.out.println("If no OOM happens, cyclic objects can be reclaimed.");
    }
}