#ifndef YCSB_MOCHI_MAP_HPP
#define YCSB_MOCHI_MAP_HPP

#include <jni.h>
#include <utility>
#include <iostream>

class Map {

    JNIEnv*   m_env;
    jobject   m_self;
    jclass    m_class_Map;
    jclass    m_class_Set;
    jclass    m_class_Iterator;
    jclass    m_class_Map_Entry;
    jmethodID m_id_Map_entrySet;
    jmethodID m_id_Set_iterator;
    jmethodID m_id_Iterator_hasNext;
    jmethodID m_id_Iterator_next;
    jmethodID m_id_Map_Entry_getKey;
    jmethodID m_id_Map_Entry_getValue;

    public:

    Map(JNIEnv* env, jobject map)
    : m_env(env)
    , m_self(map)
    , m_class_Map(env->GetObjectClass(map))
    , m_class_Set(env->FindClass("java/util/Set"))
    , m_class_Iterator(env->FindClass("java/util/Iterator"))
    , m_class_Map_Entry(env->FindClass("java/util/Map$Entry"))
    , m_id_Map_entrySet(env->GetMethodID(m_class_Map, "entrySet", "()Ljava/util/Set;"))
    , m_id_Set_iterator(env->GetMethodID(m_class_Set, "iterator", "()Ljava/util/Iterator;"))
    , m_id_Iterator_hasNext(env->GetMethodID(m_class_Iterator, "hasNext", "()Z"))
    , m_id_Iterator_next(env->GetMethodID(m_class_Iterator, "next", "()Ljava/lang/Object;"))
    , m_id_Map_Entry_getKey(env->GetMethodID(m_class_Map_Entry, "getKey", "()Ljava/lang/Object;"))
    , m_id_Map_Entry_getValue(env->GetMethodID(m_class_Map_Entry, "getValue", "()Ljava/lang/Object;"))
    {}

    template<typename F>
    void foreach(F&& fun) const {
        jobject entrySet = m_env->CallObjectMethod(m_self, m_id_Map_entrySet);
        jobject iterator = m_env->CallObjectMethod(entrySet, m_id_Set_iterator);
        while(m_env->CallBooleanMethod(iterator, m_id_Iterator_hasNext)) {
            jobject entry = m_env->CallObjectMethod(iterator, m_id_Iterator_next);
            jobject key   = m_env->CallObjectMethod(entry, m_id_Map_Entry_getKey);
            jobject val   = m_env->CallObjectMethod(entry, m_id_Map_Entry_getValue);
            (std::forward<F>(fun))(key, val);
        }
    }

};

#endif
