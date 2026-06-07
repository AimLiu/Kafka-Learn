package com.kafkalearn.demo.service;

public class EscapeDemo {
    static class Point {
        int x;
        int y;
        Point(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    static int noEscape(int a, int b) {
        Point p = new Point(a, b);
        return p.x + p.y;
    }

    static Point escape(int a, int b) {
        return new Point(a, b);
    }

    public static void main(String[] args) {
        long start = System.nanoTime();
        int s = 0;
        for (int i = 0; i < 100_000_000; i++) {
            s += noEscape(i, i);
        }
        long end = System.nanoTime();
        System.out.println(s);
        System.out.println("cost ms = " + (end - start) / 1_000_000);
    }
}