package com.kafkalearn.demo.service;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class AppKeyGenerator {

    /**
     * 根据初始的 appKey 和网关所在的排序序号（偏移量），计算出新的 appKey
     *
     * @param baseAppKey 数据库中存储的原始 appKey（16进制字符串）
     * @param offset     偏移量（比如第1个网关偏移量为0，第2个为1，第3个为2...）
     * @return 计算后的新 appKey，大写，并且保证原有的长度
     */
    public static String generateAppKey(String baseAppKey, int offset) {
        // 1. 将 16 进制字符串转换为 BigInteger 对象
        // 参数 16 代表这是 16 进制的字符串
        BigInteger baseNumber = new BigInteger(baseAppKey, 16);

        // 2. 将偏移量（设备序号）转换为 BigInteger
        BigInteger offsetNumber = BigInteger.valueOf(offset);

        // 3. 执行大数加法操作
        BigInteger resultNumber = baseNumber.add(offsetNumber);

        // 4. 将计算结果转换回 16 进制字符串，并转为大写
        String resultStr = resultNumber.toString(16).toUpperCase();

        // 5. 补齐开头的 0。
        // 因为如果原 appKey 类似 "00A1..."，BigInteger 计算后转字符串会丢掉前导 0
        // 所以我们需要保证返回的字符串长度与原始 baseAppKey 长度一致
        int expectedLength = baseAppKey.length();
        if (resultStr.length() < expectedLength) {
            StringBuilder sb = new StringBuilder(expectedLength);
            for (int i = 0; i < expectedLength - resultStr.length(); i++) {
                sb.append('0');
            }
            sb.append(resultStr);
            return sb.toString();
        }

        return resultStr;
    }

    public static void main(String[] args) {
        // 1. 测试你的原始数据
        String baseKey = "77D350EBE977177292BC373ADB0CCE8E";

        System.out.println("--- 基础测试 ---");
        System.out.println("网关1 (原值+0): " + generateAppKey(baseKey, 0));
        System.out.println("网关2 (原值+1): " + generateAppKey(baseKey, 1));
        System.out.println("网关3 (原值+2): " + generateAppKey(baseKey, 2));

        // 2. 测试 16进制的进位情况 (逢16进1，即F+1=10)
        String carryOverKey = "77D350EBE977177292BC373ADB0CCE8F";
        System.out.println("\n--- 进位测试 ---");
        System.out.println("原值是以 F 结尾 : " + carryOverKey);
        System.out.println("+1 后产生进位   : " + generateAppKey(carryOverKey, 1));

        // ==========================================
        // 3. 结合你描述的业务场景（按创建时间排序分配）的伪代码演示
        // ==========================================
        System.out.println("\n--- 模拟业务场景分配 ---");
        List<GatewayDevice> gateways = new ArrayList<>();
        gateways.add(new GatewayDevice("GW-C", 1680000003000L)); // 创建时间最晚
        gateways.add(new GatewayDevice("GW-A", 1680000001000L)); // 创建时间最早
        gateways.add(new GatewayDevice("GW-B", 1680000002000L)); // 创建时间居中

        // 按创建时间升序排序
        gateways.sort(Comparator.comparingLong(g -> g.createTime));

        // 遍历分配 appKey
        for (int i = 0; i < gateways.size(); i++) {
            GatewayDevice device = gateways.get(i);
            String deviceAppKey = generateAppKey(baseKey, i); // 这里的 i 就是 0, 1, 2...
            System.out.println("为网关 [" + device.deviceId + "] 分配 AppKey: " + deviceAppKey);
        }
    }

    // 模拟网关设备实体类
    static class GatewayDevice {
        String deviceId;
        long createTime;

        public GatewayDevice(String deviceId, long createTime) {
            this.deviceId = deviceId;
            this.createTime = createTime;
        }
    }
}
