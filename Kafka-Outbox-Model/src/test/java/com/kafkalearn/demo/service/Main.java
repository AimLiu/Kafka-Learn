package com.kafkalearn.demo.service;

public class Main {

    static long x = 10;

    static {
        x = 20L;
    }

    public static void main(String[] args) {
        System.out.println(x);
        Main curr = new Main();
        System.out.println(curr.x + "curr.testT :");
        curr.testT(200);
        System.out.println("----------------------------");

    }

    public int testT(int i){
        System.out.println(test(i));
        System.out.println(Main.class.getClassLoader());
        System.out.println(innerClass.class.getClassLoader());
        System.out.println("String loader: " + String.class.getClassLoader());
        System.out.println("DriverManager loader: " + java.sql.DriverManager.class.getClassLoader());
        System.out.println("This class loader: " + Main.class.getClassLoader());
        System.out.println("Platform loader: " + ClassLoader.getPlatformClassLoader());
        System.out.println("System loader: " + ClassLoader.getSystemClassLoader());
        return i;
    }

    private int test(int i){
        return i;
    }

    class innerClass{
        static String TEXT = "inner class";
    }
}
