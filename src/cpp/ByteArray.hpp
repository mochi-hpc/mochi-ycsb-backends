#ifndef YCSB_MOCHI_BYTE_ARRAY_HPP
#define YCSB_MOCHI_BYTE_ARRAY_HPP

#include <jni.h>
#include <string>

struct ByteArray {

    static jbyteArray New(JNIEnv* env, const std::string& str) {
        jbyteArray result = env->NewByteArray(str.size());
        env->SetByteArrayRegion(result, 0, str.size(), (jbyte*)str.c_str());
        return result;
    }

};

#endif
