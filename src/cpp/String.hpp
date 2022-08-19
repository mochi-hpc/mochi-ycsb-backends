#ifndef YCSB_MOCHI_STRING_HPP
#define YCSB_MOCHI_STRING_HPP

#include <jni.h>
#include <string>

struct String {

    static jstring New(JNIEnv* env, const std::string& str) {
        // Note: this function assumes a NULL-terminated string,
        // so if str contains \0 characters, it will be truncated.
        return env->NewStringUTF(str.c_str());
    }

};

#endif
