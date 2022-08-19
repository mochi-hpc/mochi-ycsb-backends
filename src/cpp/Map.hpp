#ifndef YCSB_MOCHI_MAP_HPP
#define YCSB_MOCHI_MAP_HPP

#include "Set.hpp"

#include <jni.h>
#include <utility>
#include <iostream>

class Map {

    JNIEnv*   m_env;
    jobject   m_self;
    jclass    m_class_Map;
    jclass    m_class_Map_Entry;
    jmethodID m_id_Map_entrySet;
    jmethodID m_id_Map_put;
    jmethodID m_id_Map_Entry_getKey;
    jmethodID m_id_Map_Entry_getValue;

    public:

    Map(JNIEnv* env, jobject map)
    : m_env(env)
    , m_self(map)
    , m_class_Map(env->GetObjectClass(map))
    , m_class_Map_Entry(env->FindClass("java/util/Map$Entry"))
    , m_id_Map_entrySet(env->GetMethodID(m_class_Map, "entrySet", "()Ljava/util/Set;"))
    , m_id_Map_put(env->GetMethodID(m_class_Map, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;"))
    , m_id_Map_Entry_getKey(env->GetMethodID(m_class_Map_Entry, "getKey", "()Ljava/lang/Object;"))
    , m_id_Map_Entry_getValue(env->GetMethodID(m_class_Map_Entry, "getValue", "()Ljava/lang/Object;"))
    {}

    template<typename F>
    void foreach(const F& fun) const {
        jobject entrySet = m_env->CallObjectMethod(m_self, m_id_Map_entrySet);
        auto set_wrapper = Set(m_env, entrySet);
        set_wrapper.foreach([&fun, this](jobject entry) {
            jobject key   = m_env->CallObjectMethod(entry, m_id_Map_Entry_getKey);
            jobject val   = m_env->CallObjectMethod(entry, m_id_Map_Entry_getValue);
            fun(key, val);
        });
    }

    void put(jobject key, jobject value) {
        m_env->CallObjectMethod(m_self, m_id_Map_put, key, value);
    }

};

#endif
