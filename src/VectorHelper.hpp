/**
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef YCSB_CPP_VECTOR_HELPER_HPP
#define YCSB_CPP_VECTOR_HELPER_HPP

#include <jni.h>
#include <utility>
#include <iostream>

namespace ycsb {

struct VectorHelper {

    JNIEnv*   m_env;
    jobject   m_self;
    jclass    m_class_Vector;
    jmethodID m_id_Vector_add;

    VectorHelper(JNIEnv* env, jobject vector)
    : m_env(env)
    , m_self(vector)
    , m_class_Vector(env->GetObjectClass(m_self))
    , m_id_Vector_add(env->GetMethodID(m_class_Vector, "add", "(Ljava/lang/Object;)Z"))
    {}

    void add(jobject element) {
        m_env->CallObjectMethod(m_self, m_id_Vector_add, element);
    }
};

}

#endif
