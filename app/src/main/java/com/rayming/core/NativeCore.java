package com.rayming.core;

/**
 * Created by Jay on 2017/3/30.
 */

public class NativeCore {
    static {
        System.loadLibrary("native_core");
    }
    public static native String stringFormJni();
}
