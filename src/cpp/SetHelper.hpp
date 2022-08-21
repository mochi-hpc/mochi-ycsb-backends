#ifndef YCSB_MOCHI_SET_HELPER_HPP
#define YCSB_MOCHI_SET_HELPER_HPP

#include <jni.h>
#include <utility>
#include <iostream>

namespace mochi {
namespace ycsb {

struct SetHelper {

    JNIEnv*   m_env;
    jobject   m_self;
    jclass    m_class_Set;
    jclass    m_class_Iterator;
    jmethodID m_id_Set_iterator;
    jmethodID m_id_Iterator_hasNext;
    jmethodID m_id_Iterator_next;

    SetHelper(JNIEnv* env, jobject set)
    : m_env(env)
    , m_self(set)
    , m_class_Set(env->GetObjectClass(set))
    , m_class_Iterator(env->FindClass("java/util/Iterator"))
    , m_id_Set_iterator(env->GetMethodID(m_class_Set, "iterator", "()Ljava/util/Iterator;"))
    , m_id_Iterator_hasNext(env->GetMethodID(m_class_Iterator, "hasNext", "()Z"))
    , m_id_Iterator_next(env->GetMethodID(m_class_Iterator, "next", "()Ljava/lang/Object;"))
    {}

    template<typename F>
    void foreach(const F& fun) const {
        jobject iterator = m_env->CallObjectMethod(m_self, m_id_Set_iterator);
        while(m_env->CallBooleanMethod(iterator, m_id_Iterator_hasNext)) {
            fun(m_env->CallObjectMethod(iterator, m_id_Iterator_next));
        }
    }

};

}
}

#endif
