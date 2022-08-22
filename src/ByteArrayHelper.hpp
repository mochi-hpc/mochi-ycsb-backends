/**
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef YCSB_CPP_BYTE_ARRAY_HELPER_HPP
#define YCSB_CPP_BYTE_ARRAY_HELPER_HPP

#include <jni.h>
#include <string>
#include "YCSBCppInterface.hpp"

namespace ycsb {

struct ByteArrayHelper {

    static jbyteArray New(JNIEnv* env, const Buffer& buffer) {
        auto size = buffer.size();
        jbyteArray result = env->NewByteArray(size);
        env->SetByteArrayRegion(result, 0, size, (jbyte*)buffer.data());
        return result;
    }

};

}

#endif
