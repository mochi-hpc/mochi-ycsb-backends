#include <gov_anl_mochi_YokanDBClient.h>

#include "Status.hpp"
#include "Map.hpp"
#include "String.hpp"
#include "ByteArray.hpp"

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

    const char* table_str = env->GetStringUTFChars(table, nullptr);
    const char* key_str   = env->GetStringUTFChars(key, nullptr);

    auto full_key = std::string(table_str) + "/" + key_str;
    auto it       = db->find(full_key);
    auto found    = it != db->end();

    if(!found) {
        env->ReleaseStringUTFChars(key, key_str);
        env->ReleaseStringUTFChars(table, table_str);
        return Status::NOT_FOUND(env);
    }

    const auto& entry       = it->second;
    auto result_map_wrapper = Map(env, results);

    if(fields == nullptr) {

        for(const auto& p : entry) {
            jstring jstring_field   = String::New(env, p.first);
            jbyteArray jbytes_value = ByteArray::New(env, p.second);
            result_map_wrapper.put((jobject)jstring_field, (jobject)jbytes_value);
        }

    } else {
        auto fields_set_wrapper = Set(env, fields);

        fields_set_wrapper.foreach([&entry, &result_map_wrapper, env](jobject field) {

            jstring jstring_field = (jstring)field;
            const char* field_str = env->GetStringUTFChars(jstring_field, nullptr);
            auto it = entry.find(field_str);
            if(it != entry.end()) {
                jbyteArray jbytes_value = ByteArray::New(env, it->second);
                result_map_wrapper.put(field, (jobject)jbytes_value);
            }
            env->ReleaseStringUTFChars(jstring_field, field_str);
        });
    }

    env->ReleaseStringUTFChars(key, key_str);
    env->ReleaseStringUTFChars(table, table_str);

    return Status::OK(env);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1scan
    (JNIEnv * env, jobject self, jlong impl, jstring table,
     jstring startKey, jint recordCount, jobject fields, jobject results) {
    auto db = reinterpret_cast<mochi_db_t*>(impl);

    const char* table_str     = env->GetStringUTFChars(table, nullptr);
    const char* start_key_str = env->GetStringUTFChars(startKey, nullptr);

    auto full_start_key = std::string(table_str) + "/" + start_key_str;

    jclass hash_map_class   = env->FindClass("java/util/HashMap");
    jmethodID hash_map_init = env->GetMethodID(hash_map_class, "<init>", "()V");
    jclass vector_class     = env->FindClass("java/util/Vector");
    jmethodID vector_add    = env->GetMethodID(vector_class, "add", "(Ljava/lang/Object;)Z");

    if(fields == nullptr) {
        int i = 0;
        for(auto it = db->lower_bound(full_start_key); it != db->end() && i < recordCount; ++it, ++i) {
            jobject jhashmap = env->NewObject(hash_map_class, hash_map_init);
            auto hash_map_wrapper = Map(env, jhashmap);

            const auto& entry = it->second;
            for(const auto& p : entry) {
                jstring jstring_field   = String::New(env, p.first);
                jbyteArray jbytes_value = ByteArray::New(env, p.second);
                hash_map_wrapper.put((jobject)jstring_field, (jobject)jbytes_value);
            }

            env->CallObjectMethod(results, vector_add, jhashmap);
        }
    } else {
        int i = 0;

        auto field_set_wrapper = Set(env, fields);

        for(auto it = db->lower_bound(full_start_key); it != db->end() && i < recordCount; ++it, ++i) {
            jobject jhashmap  = env->NewObject(hash_map_class, hash_map_init);
            auto hash_map_wrapper = Map(env, jhashmap);
            const auto& entry = it->second;

            field_set_wrapper.foreach([db, env, &entry, &hash_map_wrapper](jobject field) {
                jstring jstring_field = (jstring)field;
                const char* field_str = env->GetStringUTFChars(jstring_field, nullptr);
                auto it = entry.find(field_str);
                if(it != entry.end()) {
                    jstring jstring_field   = String::New(env, it->first);
                    jbyteArray jbytes_value = ByteArray::New(env, it->second);
                    hash_map_wrapper.put((jobject)jstring_field, (jobject)jbytes_value);
                }
                env->ReleaseStringUTFChars(jstring_field, field_str);
            });

            env->CallObjectMethod(results, vector_add, jhashmap);
        }
    }

    env->ReleaseStringUTFChars(startKey, start_key_str);
    env->ReleaseStringUTFChars(table, table_str);

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
