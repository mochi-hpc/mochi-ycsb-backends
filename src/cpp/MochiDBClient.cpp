#include <gov_anl_mochi_MochiDBClient.h>

#include "StatusHelper.hpp"
#include "MapHelper.hpp"
#include "ByteArrayHelper.hpp"
#include "VectorHelper.hpp"

#include "MochiYCSB.hpp"

#include <map>
#include <string>
#include <iostream>

extern "C" {

namespace my = mochi::ycsb;

JNIEXPORT jlong JNICALL Java_gov_anl_mochi_MochiDBClient__1init
    (JNIEnv * env, jobject self) {
    // TODO pass parameters
    auto db = my::CreateDB("test");
    return reinterpret_cast<jlong>(db);
}

JNIEXPORT void JNICALL Java_gov_anl_mochi_MochiDBClient__1cleanup
    (JNIEnv * env, jobject self, jlong impl) {
    auto db = reinterpret_cast<my::DB*>(impl);
    delete db;
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_MochiDBClient__1read
    (JNIEnv * env, jobject self, jlong impl, jstring jtable,
     jstring jkey, jobject jfields, jobject jresults) {
    auto db = reinterpret_cast<my::DB*>(impl);

    const char* table = env->GetStringUTFChars(jtable, nullptr);
    const char* key   = env->GetStringUTFChars(jkey, nullptr);

    my::DB::FieldValueList results;
    my::Status status;

    if(jfields != nullptr) {
        std::vector<std::string> fields;
        my::SetHelper(env, jfields).foreach([env, &fields](jobject jfield) {
            const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
            fields.emplace_back(field);
            env->ReleaseStringUTFChars((jstring)jfield, field);
        });
        status = db->read(table, key, fields, results);
    } else {
        status = db->read(table, key, results);
    }

    auto result_map_helper = my::MapHelper(env, jresults);
    for(const auto& pair : results) {
        const auto& field  = pair.first;
        const auto& buffer = pair.second;
        auto jfield = env->NewStringUTF(field.c_str());
        auto jvalue = my::ByteArrayHelper::New(env, *buffer);
        result_map_helper.put(jfield, jvalue);
    }

    env->ReleaseStringUTFChars(jkey, key);
    env->ReleaseStringUTFChars(jtable, table);

    return my::StatusHelper::New(env, status);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_MochiDBClient__1scan
    (JNIEnv * env, jobject self, jlong impl, jstring jtable,
     jstring jstartKey, jint recordCount, jobject jfields, jobject jresults) {
    auto db = reinterpret_cast<my::DB*>(impl);

    const char* table    = env->GetStringUTFChars(jtable, nullptr);
    const char* startKey = env->GetStringUTFChars(jstartKey, nullptr);

    std::vector<my::DB::FieldValueList> results;
    my::Status status;

    if(jfields != nullptr) {
        std::vector<std::string> fields;
        my::SetHelper(env, jfields).foreach([env, &fields](jobject jfield) {
            const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
            fields.emplace_back(field);
            env->ReleaseStringUTFChars((jstring)jfield, field);
        });
        status = db->scan(table, startKey, recordCount, fields, results);
    } else {
        status = db->scan(table, startKey, recordCount, results);
    }

    auto result_vector_helper = my::VectorHelper(env, jresults);
    auto field_map_helper     = my::MapHelper(env);

    jclass    class_HashMap   = env->FindClass("java/util/HashMap");
    jmethodID id_HashMap_init = env->GetMethodID(class_HashMap, "<init>", "()V");

    for(const auto& record : results) {
        auto hash_map = env->NewObject(class_HashMap, id_HashMap_init);
        field_map_helper.m_self = hash_map;
        for(const auto& pair : record) {
            const auto& field  = pair.first;
            const auto& buffer = pair.second;
            auto jfield = env->NewStringUTF(field.c_str());
            auto jvalue = my::ByteArrayHelper::New(env, *buffer);
            field_map_helper.put(jfield, jvalue);
        }
        result_vector_helper.add(hash_map);
    }

    env->ReleaseStringUTFChars(jstartKey, startKey);
    env->ReleaseStringUTFChars(jtable, table);

    return my::StatusHelper::New(env, status);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_MochiDBClient__1update
    (JNIEnv * env, jobject self, jlong impl, jstring jtable,
     jstring jkey, jobject jvalues) {
    auto db = reinterpret_cast<my::DB*>(impl);

    const char* table = env->GetStringUTFChars(jtable, nullptr);
    const char* key   = env->GetStringUTFChars(jkey, nullptr);

    my::DB::FieldValueList values;
    auto values_map_helper = my::MapHelper(env, jvalues);
    values_map_helper.foreach([env, &values](jobject jfield, jobject jvalue) {
        const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
        jbyte*      value = env->GetByteArrayElements((jbyteArray)jvalue, nullptr);
        jsize       vsize = env->GetArrayLength((jbyteArray)jvalue);
        values.emplace_back(field,
                            std::make_unique<my::StringView>((const char*)value, vsize));
        env->ReleaseStringUTFChars((jstring)jfield, field);
    });

    auto status = db->update(table, key, values);

    unsigned i=0;
    values_map_helper.foreach([env, &values, &i](jobject jfield, jobject jvalue) {
        auto& buffer = values[i].second;
        env->ReleaseByteArrayElements((jbyteArray)jvalue, (jbyte*)buffer->data(), JNI_ABORT);
        i += 1;
    });

    env->ReleaseStringUTFChars(jkey, key);
    env->ReleaseStringUTFChars(jtable, table);

    return my::StatusHelper::New(env, status);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_MochiDBClient__1insert
    (JNIEnv * env, jobject self, jlong impl, jstring jtable,
     jstring jkey, jobject jvalues) {
    auto db = reinterpret_cast<my::DB*>(impl);

    const char* table = env->GetStringUTFChars(jtable, nullptr);
    const char* key   = env->GetStringUTFChars(jkey, nullptr);

    my::DB::FieldValueList values;
    auto values_map_helper = my::MapHelper(env, jvalues);
    values_map_helper.foreach([env, &values](jobject jfield, jobject jvalue) {
        const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
        jbyte*      value = env->GetByteArrayElements((jbyteArray)jvalue, nullptr);
        jsize       vsize = env->GetArrayLength((jbyteArray)jvalue);
        values.emplace_back(field,
                            std::make_unique<my::StringView>((const char*)value, vsize));
        env->ReleaseStringUTFChars((jstring)jfield, field);
    });

    auto status = db->insert(table, key, values);

    unsigned i=0;
    values_map_helper.foreach([env, &values, &i](jobject jfield, jobject jvalue) {
        auto& buffer = values[i].second;
        env->ReleaseByteArrayElements((jbyteArray)jvalue, (jbyte*)buffer->data(), JNI_ABORT);
        i += 1;
    });

    env->ReleaseStringUTFChars(jkey, key);
    env->ReleaseStringUTFChars(jtable, table);

    return my::StatusHelper::New(env, status);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_MochiDBClient__1delete
    (JNIEnv * env, jobject self, jlong impl, jstring jtable, jstring jkey) {
    auto db = reinterpret_cast<my::DB*>(impl);

    const char* table = env->GetStringUTFChars(jtable, nullptr);
    const char* key   = env->GetStringUTFChars(jkey, nullptr);

    auto status = db->erase(table, key);

    env->ReleaseStringUTFChars(jkey, key);
    env->ReleaseStringUTFChars(jtable, table);

    return my::StatusHelper::New(env, status);
}

}
