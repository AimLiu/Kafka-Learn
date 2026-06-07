package com.kafkalearn.demo.service;

/*

 */
class Test {
    static {
        // 这里的ConstClass init不会进行输出，因为 100 被编译进了 ConstClass 的常量池或字节码中，运行时没有真正 getstatic ConstClass.VALUE
        System.out.println("ConstClass init");
    }
    // 如果访问的是LONG_VAL, 那么就会进行类的初始化，因为这里的LONG_VAL是动态的
    public static final long LONG_VAL = System.currentTimeMillis();
    // 如果访问的是VALUE, 那么就不会进行类的初始化，因为这里的VALUE是固定值
    public static final int VALUE = 100;
    //  如果访问的是INTEGER_VAL, 那么就会进行类的初始化，因为这里的INTEGER_VAL是包装类型对象，不是编译期常量
    public static final Integer INTEGER_VAL = 1;
    //
    public static final String STR_VAL = "abc";
}

public class ConstClass {
    public static void main(String[] args) {
        System.out.println(Test.STR_VAL);
    }
}