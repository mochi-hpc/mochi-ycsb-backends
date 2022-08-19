#include <gov_anl_mochi_YokanDBClient.h>
#include "Status.hpp"
#include "Map.hpp"

#include <map>
#include <string>
#include <iostream>

extern "C" {

using mochi_key_t   = std::string;
using mochi_value_t = std::map<std::string, std::string>;
using mochi_db_t    = std::map<mochi_key_t, mochi_value_t>;

JNIEXPORT jlong JNICALL Java_gov_anl_mochi_YokanDBClient__1init
    (JNIEnv * env, jobject self) {
    auto impl = new mochi_db_t();
    return reinterpret_cast<jlong>(impl);
}

JNIEXPORT void JNICALL Java_gov_anl_mochi_YokanDBClient__1cleanup
    (JNIEnv * env, jobject self, jlong impl) {
    auto db = reinterpret_cast<mochi_db_t*>(impl);
    delete db;
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1read
    (JNIEnv * env, jobject self, jlong impl, jstring table,
     jstring key, jobject fields, jobject results) {
    auto db = reinterpret_cast<mochi_db_t*>(impl);
    // TODO
    return Status::OK(env);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1scan
    (JNIEnv * env, jobject self, jlong impl, jstring table,
     jstring startKey, jint recordCount, jobject fields, jobject results) {
    auto db = reinterpret_cast<mochi_db_t*>(impl);
    // TODO
    return Status::OK(env);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1update
    (JNIEnv * env, jobject self, jlong impl, jstring table,
     jstring key, jobject values) {
    auto db = reinterpret_cast<mochi_db_t*>(impl);

    const char* table_str = env->GetStringUTFChars(table, nullptr);
    const char* key_str   = env->GetStringUTFChars(key, nullptr);

    auto map_wrapper = Map(env, values);
    auto full_key = std::string(table_str) + "/" + key_str;
    auto& entry = (*db)[full_key];

    map_wrapper.foreach([&entry, table_str, key_str, env](jobject field, jobject value) {
            jstring     jstring_field = (jstring)field;
            jbyteArray  jbytes_value  = (jbyteArray)value;
            const char* str_field     = env->GetStringUTFChars(jstring_field, nullptr);
            jbyte*      str_value     = env->GetByteArrayElements(jbytes_value, nullptr);
            jsize       str_value_len = env->GetArrayLength(jbytes_value);

            entry[std::string(str_field)] = std::string((const char*)str_value, str_value_len);

            env->ReleaseByteArrayElements(jbytes_value, str_value, JNI_ABORT);
            env->ReleaseStringUTFChars(jstring_field, str_field);
    });

    env->ReleaseStringUTFChars(key, key_str);
    env->ReleaseStringUTFChars(table, table_str);
    return Status::OK(env);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1insert
    (JNIEnv * env, jobject self, jlong impl, jstring table,
     jstring key, jobject values) {
    auto db = reinterpret_cast<mochi_db_t*>(impl);

    const char* table_str = env->GetStringUTFChars(table, nullptr);
    const char* key_str   = env->GetStringUTFChars(key, nullptr);

    auto map_wrapper = Map(env, values);
    auto full_key = std::string(table_str) + "/" + key_str;
    auto& entry = (*db)[full_key] = mochi_value_t{};

    map_wrapper.foreach([&entry, table_str, key_str, env](jobject field, jobject value) {
            jstring     jstring_field = (jstring)field;
            jbyteArray  jbytes_value  = (jbyteArray)value;
            const char* str_field     = env->GetStringUTFChars(jstring_field, nullptr);
            jbyte*      str_value     = env->GetByteArrayElements(jbytes_value, nullptr);
            jsize       str_value_len = env->GetArrayLength(jbytes_value);

            entry[std::string(str_field)] = std::string((const char*)str_value, str_value_len);

            env->ReleaseByteArrayElements(jbytes_value, str_value, JNI_ABORT);
            env->ReleaseStringUTFChars(jstring_field, str_field);
    });

    env->ReleaseStringUTFChars(key, key_str);
    env->ReleaseStringUTFChars(table, table_str);
    return Status::OK(env);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1delete
    (JNIEnv * env, jobject self, jlong impl, jstring table, jstring key) {
    auto db = reinterpret_cast<mochi_db_t*>(impl);

    const char* table_str = env->GetStringUTFChars(table, nullptr);
    const char* key_str   = env->GetStringUTFChars(key, nullptr);

    auto full_key = std::string(table_str) + "/" + key_str;
    auto it       = db->find(full_key);
    bool found    = it != db->end();
    if(found)
        db->erase(it);

    env->ReleaseStringUTFChars(key, key_str);
    env->ReleaseStringUTFChars(table, table_str);

    if(found)
        return Status::OK(env);
    else
        return Status::NOT_FOUND(env);
}

}
