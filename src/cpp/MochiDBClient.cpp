#include <gov_anl_mochi_MochiDBClient.h>

#include "StatusHelper.hpp"
#include "MapHelper.hpp"
#include "ByteArrayHelper.hpp"
#include "VectorHelper.hpp"

#include "MochiYCSB.hpp"

#include <map>
#include <string>
#include <unordered_map>
#include <iostream>
#include <dlfcn.h>

extern "C" {

namespace my = mochi::ycsb;

JNIEXPORT jlong JNICALL Java_gov_anl_mochi_MochiDBClient__1init
    (JNIEnv * env, jobject self, jobject jproperty_map) {
    std::unordered_map<std::string, std::string> properties;
    my::MapHelper(env, jproperty_map).foreach([env, &properties](jobject jkey, jobject jvalue) {
            const char* key   = env->GetStringUTFChars((jstring)jkey, nullptr);
            const char* value = env->GetStringUTFChars((jstring)jvalue, nullptr);
            properties.emplace(key, value);
            env->ReleaseStringUTFChars((jstring)jkey, key);
            env->ReleaseStringUTFChars((jstring)jvalue, value);
    });
    decltype(properties.begin()) it;
    if((it = properties.find("mochi.ycsb.library")) != properties.end()) {
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
    if((it = properties.find("mochi.ycsb.backend")) != properties.end()) {
        backend = it->second;
        properties.erase(it);
    }
    auto db = my::CreateDB(backend.c_str(), properties);
    if(!db) {
        jclass exClass = env->FindClass("java/lang/RuntimeException");
        auto error_str = std::string("Mochi YCSB backend \"") + backend + "\" not found";
        env->ThrowNew(exClass, error_str.c_str());
        return 0;
    }
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

    my::DB::Record results;
    my::Status status;

    if(jfields != nullptr) {
        std::vector<my::StringView> fields;

        auto set_helper = my::SetHelper(env, jfields);
        set_helper.foreach([env, &fields](jobject jfield) {
            const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
            fields.emplace_back(field);
        });

        status = db->read(table, key, fields, results);

        unsigned i = 0;
        set_helper.foreach([env, &fields, &i](jobject jfield) {
            auto& field = fields[i];
            env->ReleaseStringUTFChars((jstring)jfield, field.data());
            i += 1;
        });
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

    std::vector<my::DB::Record> results;
    my::Status status;

    if(jfields != nullptr) {
        std::vector<my::StringView> fields;

        auto set_helper = my::SetHelper(env, jfields);
        set_helper.foreach([env, &fields](jobject jfield) {
            const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
            fields.emplace_back(field);
        });

        status = db->scan(table, startKey, recordCount, fields, results);

        unsigned i = 0;
        set_helper.foreach([env, &fields, &i](jobject jfield) {
            auto& field = fields[i];
            env->ReleaseStringUTFChars((jstring)jfield, field.data());
            i += 1;
        });
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

    std::vector<my::StringView> fields;
    std::vector<my::StringView> values;

    auto values_map_helper = my::MapHelper(env, jvalues);
    values_map_helper.foreach([env, &values, &fields](jobject jfield, jobject jvalue) {
        const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
        jbyte*      value = env->GetByteArrayElements((jbyteArray)jvalue, nullptr);
        jsize       vsize = env->GetArrayLength((jbyteArray)jvalue);
        fields.emplace_back(field);
        values.emplace_back((const char*)value, vsize);
    });

    auto status = db->update(table, key, fields, values);

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

    return my::StatusHelper::New(env, status);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_MochiDBClient__1insert
    (JNIEnv * env, jobject self, jlong impl, jstring jtable,
     jstring jkey, jobject jvalues) {
    auto db = reinterpret_cast<my::DB*>(impl);

    const char* table = env->GetStringUTFChars(jtable, nullptr);
    const char* key   = env->GetStringUTFChars(jkey, nullptr);

    std::vector<my::StringView> fields, values;

    auto values_map_helper = my::MapHelper(env, jvalues);
    values_map_helper.foreach([env, &values, &fields](jobject jfield, jobject jvalue) {
        const char* field = env->GetStringUTFChars((jstring)jfield, nullptr);
        jbyte*      value = env->GetByteArrayElements((jbyteArray)jvalue, nullptr);
        jsize       vsize = env->GetArrayLength((jbyteArray)jvalue);
        fields.emplace_back(field);
        values.emplace_back((const char*)value, vsize);
    });

    auto status = db->insert(table, key, fields, values);

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
