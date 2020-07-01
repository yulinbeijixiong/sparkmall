package com.oujian;

import java.util.concurrent.TimeUnit;

public class JvmParam {
    public static void main(String[] args) {
        try {
            Object o = new Object();
            o.getClass();
            TimeUnit.SECONDS.sleep(50000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
