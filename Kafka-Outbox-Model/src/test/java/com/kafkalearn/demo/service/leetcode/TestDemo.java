package com.kafkalearn.demo.service.leetcode;

public class TestDemo {
    public static void main(String[] args) {
        TestDemo v1 = new TestDemo();
        v1.maxArea(new int[]{5,6,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,4});
    }
    public int maxArea(int[] height) {
        //从端点的两边开始遍历，当那边低，则缩进哪边，同时更新答案
        int ans = 0;
        int left = 0, right = height.length-1;
        while(left <= right){
            int width = right - left;
            int minHeight = Math.min(height[left], height[right]);
            ans = Math.max(width * minHeight, ans);
            if(height[left] > minHeight){
                right--;
            }else {
                left++;
            }
        }
        return ans;
    }
}
