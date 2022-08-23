/**
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <cpp_ycsb_YcsbDBClient.h>

#include "StatusHelper.hpp"
#include "MapHelper.hpp"
#include "ByteArrayHelper.hpp"
#include "VectorHelper.hpp"

#include "YCSBCppInterface.hpp"

#include <map>
#include <string>
#include <unordered_map>
#include <iostream>
#include <dlfcn.h>

extern "C" {

JNIEXPORT jlong JNICALL Java_cpp_ycsb_YcsbDBClient__1init
    (JNIEnv * env, jobject self, jobject jproperty_map) {
    std::unordered_map<std::string, std::string> properties;
    ycsb::MapHelper(env, jproperty_map).foreach([env, &properties](jobject jkey, jobject jvalue) {
            const char* key   = env->GetStringUTFChars((jstring)jkey, nullptr);
            const char* value = env->GetStringUTFChars((jstring)jvalue, nullptr);
            properties.emplace(key, value);
            env->ReleaseStringUTFChars((jstring)jkey, key);
            env->ReleaseStringUTFChars((jstring)jvalue, value);
    });
    decltype(properties.begin()) it;
    if((it = properties.find("ycsb.cpp.library")) != properties.end()) {
        const auto& library = it->second;
        void* handle = dlopen(library.c_str(), RTLD_NOW|RTLD_GLOBAL);
        if(!handle) {
            char* error = dlerror();
            auto error_str = std::string(error);
            free(error);
            jclass exClass = env->FindClass("java/lang/RuntimeException");
            env->ThrowNew(exClass, error_str.c_str());
            return 0;
        }
        properties.erase(it);
    }
    std::string backend("test");
    if((it = properties.find("ycsb.cpp.backend")) != properties.end()) {
        backend = it->second;
        properties.erase(it);
    }
    auto db = ycsb::CreateDB(backend.c_str(), properties);
    if(!db) {
        jclass exClass = env->FindClass("java/lang/RuntimeException");
        auto error_str = std::string("Mochi YCSB backend \"") + backend + "\" not found";
        env->ThrowNew(exClass, error_str.c_str());
        return 0;
    }
    return reinterpret_cast<jlong>(db);
}

JNIEXPORT void JNICALL Java_cpp_ycsb_YcsbDBClient__1cleanup
    (JNIEnv * env, jobject self, jlong impl) {
    auto db = reinterpret_cast<ycsb::DB*>(impl);
    delete db;
}

JNIEXPORT jobject JNICALL Java_cpp_ycsb_YcsbDBClient__1read
    (JNIEnv * env, jobject self, jlong impl, jstring jtable,
     jstring jkey, jobject jfields, jobject jresults) {
    auto db = reinterpret_cast<ycsb::DB*>(impl);

    const char* table = env->GetStringUTFChars(jtable, nullptr);
    const char* key   = env->GetStringUTFChars(jkey, nullptr);

    ycsb::DB::Record results;
    ycsb::Status status;

    if(jfields != nullptr) {
        std::unordered_set<ycsb::StringView> fields;
        std::vector<ycsb::StringView> fields_strings;

        auto set_helper = ycsb::SetHelper(env, jfields);
        set_helper.foreach([env, &fields, &fields_strings](jobject jfield) {
            const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
            auto sw = ycsb::StringView(field);
            fields.emplace(sw);
            fields_strings.push_back(sw);
        });

        status = db->read(table, key, fields, results);

        unsigned i = 0;
        set_helper.foreach([env, &fields_strings, &i](jobject jfield) {
            auto& field = fields_strings[i];
            env->ReleaseStringUTFChars((jstring)jfield, field.data());
            i += 1;
        });
    } else {
        status = db->read(table, key, results);
    }

    auto result_map_helper = ycsb::MapHelper(env, jresults);
    for(const auto& pair : results) {
        const auto& field  = pair.first;
        const auto& buffer = pair.second;
        auto jfield = env->NewStringUTF(field.c_str());
        auto jvalue = ycsb::ByteArrayHelper::New(env, *buffer);
        result_map_helper.put(jfield, jvalue);
    }

    env->ReleaseStringUTFChars(jkey, key);
    env->ReleaseStringUTFChars(jtable, table);

    return ycsb::StatusHelper::New(env, status);
}

JNIEXPORT jobject JNICALL Java_cpp_ycsb_YcsbDBClient__1scan
    (JNIEnv * env, jobject self, jlong impl, jstring jtable,
     jstring jstartKey, jint recordCount, jobject jfields, jobject jresults) {
    auto db = reinterpret_cast<ycsb::DB*>(impl);

    const char* table    = env->GetStringUTFChars(jtable, nullptr);
    const char* startKey = env->GetStringUTFChars(jstartKey, nullptr);

    std::vector<ycsb::DB::Record> results;
    ycsb::Status status;

    if(jfields != nullptr) {
        std::unordered_set<ycsb::StringView> fields;
        std::vector<ycsb::StringView> fields_strings;

        auto set_helper = ycsb::SetHelper(env, jfields);
        set_helper.foreach([env, &fields, &fields_strings](jobject jfield) {
            const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
            auto sw = ycsb::StringView(field);
            fields.emplace(sw);
            fields_strings.emplace_back(sw);
        });

        status = db->scan(table, startKey, recordCount, fields, results);

        unsigned i = 0;
        set_helper.foreach([env, &fields_strings, &i](jobject jfield) {
            auto& field = fields_strings[i];
            env->ReleaseStringUTFChars((jstring)jfield, field.data());
            i += 1;
        });
    } else {
        status = db->scan(table, startKey, recordCount, results);
    }

    auto result_vector_helper = ycsb::VectorHelper(env, jresults);
    auto field_map_helper     = ycsb::MapHelper(env);

    jclass    class_HashMap   = env->FindClass("java/util/HashMap");
    jmethodID id_HashMap_init = env->GetMethodID(class_HashMap, "<init>", "()V");

    for(const auto& record : results) {
        auto hash_map = env->NewObject(class_HashMap, id_HashMap_init);
        field_map_helper.m_self = hash_map;
        for(const auto& pair : record) {
            const auto& field  = pair.first;
            const auto& buffer = pair.second;
            auto jfield = env->NewStringUTF(field.c_str());
            auto jvalue = ycsb::ByteArrayHelper::New(env, *buffer);
            field_map_helper.put(jfield, jvalue);
        }
        result_vector_helper.add(hash_map);
    }

    env->ReleaseStringUTFChars(jstartKey, startKey);
    env->ReleaseStringUTFChars(jtable, table);

    return ycsb::StatusHelper::New(env, status);
}

JNIEXPORT jobject JNICALL Java_cpp_ycsb_YcsbDBClient__1update
    (JNIEnv * env, jobject self, jlong impl, jstring jtable,
     jstring jkey, jobject jvalues) {
    auto db = reinterpret_cast<ycsb::DB*>(impl);

    const char* table = env->GetStringUTFChars(jtable, nullptr);
    const char* key   = env->GetStringUTFChars(jkey, nullptr);

    std::vector<ycsb::StringView> fields;
    std::vector<ycsb::StringView> values;
    ycsb::DB::RecordView record;

    auto values_map_helper = ycsb::MapHelper(env, jvalues);
    values_map_helper.foreach([env, &values, &fields, &record](jobject jfield, jobject jvalue) {
        const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
        jbyte*      value = env->GetByteArrayElements((jbyteArray)jvalue, nullptr);
        jsize       vsize = env->GetArrayLength((jbyteArray)jvalue);
        fields.emplace_back(field);
        values.emplace_back((const char*)value, vsize);
        record.emplace(fields.back(), values.back());
    });

    auto status = db->update(table, key, record);

    unsigned i=0;
    values_map_helper.foreach([env, &values, &fields, &i](jobject jfield, jobject jvalue) {
        auto& value = values[i];
        auto& field = fields[i];
        env->ReleaseByteArrayElements((jbyteArray)jvalue, (jbyte*)value.data(), JNI_ABORT);
        env->ReleaseStringUTFChars((jstring)jfield, field.data());
        i += 1;
    });

    env->ReleaseStringUTFChars(jkey, key);
    env->ReleaseStringUTFChars(jtable, table);

    return ycsb::StatusHelper::New(env, status);
}

JNIEXPORT jobject JNICALL Java_cpp_ycsb_YcsbDBClient__1insert
    (JNIEnv * env, jobject self, jlong impl, jstring jtable,
     jstring jkey, jobject jvalues) {
    auto db = reinterpret_cast<ycsb::DB*>(impl);

    const char* table = env->GetStringUTFChars(jtable, nullptr);
    const char* key   = env->GetStringUTFChars(jkey, nullptr);

    std::vector<ycsb::StringView> fields, values;
    ycsb::DB::RecordView record;

    auto values_map_helper = ycsb::MapHelper(env, jvalues);
    values_map_helper.foreach([env, &values, &fields, &record](jobject jfield, jobject jvalue) {
        const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
        jbyte*      value = env->GetByteArrayElements((jbyteArray)jvalue, nullptr);
        jsize       vsize = env->GetArrayLength((jbyteArray)jvalue);
        fields.emplace_back(field);
        values.emplace_back((const char*)value, vsize);
        record.emplace(fields.back(), values.back());
    });

    auto status = db->insert(table, key, record);

    unsigned i=0;
    values_map_helper.foreach([env, &values, &fields, &i](jobject jfield, jobject jvalue) {
        auto& value = values[i];
        auto& field = fields[i];
        env->ReleaseByteArrayElements((jbyteArray)jvalue, (jbyte*)value.data(), JNI_ABORT);
        env->ReleaseStringUTFChars((jstring)jfield, field.data());
        i += 1;
    });

    env->ReleaseStringUTFChars(jkey, key);
    env->ReleaseStringUTFChars(jtable, table);

    return ycsb::StatusHelper::New(env, status);
}

JNIEXPORT jobject JNICALL Java_cpp_ycsb_YcsbDBClient__1delete
    (JNIEnv * env, jobject self, jlong impl, jstring jtable, jstring jkey) {
    auto db = reinterpret_cast<ycsb::DB*>(impl);

    const char* table = env->GetStringUTFChars(jtable, nullptr);
    const char* key   = env->GetStringUTFChars(jkey, nullptr);

    auto status = db->erase(table, key);

    env->ReleaseStringUTFChars(jkey, key);
    env->ReleaseStringUTFChars(jtable, table);

    return ycsb::StatusHelper::New(env, status);
}

}
