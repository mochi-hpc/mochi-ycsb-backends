/**
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef YCSB_CPP_MAP_HELPER_HPP
#define YCSB_CPP_MAP_HELPER_HPP

#include "SetHelper.hpp"

#include <jni.h>
#include <utility>
#include <iostream>

namespace ycsb {

struct MapHelper {

    JNIEnv*   m_env;
    jobject   m_self;
    jclass    m_class_Map;
    jclass    m_class_Map_Entry;
    jmethodID m_id_Map_entrySet;
    jmethodID m_id_Map_put;
    jmethodID m_id_Map_Entry_getKey;
    jmethodID m_id_Map_Entry_getValue;

    MapHelper(JNIEnv* env, jobject map = nullptr)
    : m_env(env)
    , m_self(map)
    , m_class_Map(map ? env->GetObjectClass(map) : env->FindClass("java/util/Map"))
    , m_class_Map_Entry(env->FindClass("java/util/Map$Entry"))
    , m_id_Map_entrySet(env->GetMethodID(m_class_Map, "entrySet", "()Ljava/util/Set;"))
    , m_id_Map_put(env->GetMethodID(m_class_Map, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;"))
    , m_id_Map_Entry_getKey(env->GetMethodID(m_class_Map_Entry, "getKey", "()Ljava/lang/Object;"))
    , m_id_Map_Entry_getValue(env->GetMethodID(m_class_Map_Entry, "getValue", "()Ljava/lang/Object;"))
    {}

    template<typename F>
    void foreach(const F& fun) const {
        jobject entrySet = m_env->CallObjectMethod(m_self, m_id_Map_entrySet);
        auto set_helper = SetHelper(m_env, entrySet);
        set_helper.foreach([&fun, this](jobject entry) {
            jobject key   = m_env->CallObjectMethod(entry, m_id_Map_Entry_getKey);
            jobject val   = m_env->CallObjectMethod(entry, m_id_Map_Entry_getValue);
            fun(key, val);
        });
    }

    void put(jobject key, jobject value) {
        m_env->CallObjectMethod(m_self, m_id_Map_put, key, value);
    }
};

}

#endif
