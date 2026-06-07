package com.kafkalearn.demo.service;

public class ObjectCreateDemo {
    static class User {
        int id;
        String name;

        User(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static void main(String[] args) {
        User user = new User(1, "Liu");
        System.out.println(user.name);
    }
}