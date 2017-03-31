#include <jni.h>
#include <string>

extern "C"
JNIEXPORT jstring JNICALL
Java_com_rayming_core_NativeCore_stringFormJni(JNIEnv *env, jclass type) {
    // TODO
    std::string hello = "Hello World";
    return env->NewStringUTF(hello.c_str());
}