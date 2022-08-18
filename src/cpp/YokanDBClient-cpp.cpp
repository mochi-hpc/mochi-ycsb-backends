#include <gov_anl_mochi_YokanDBClient.h>
#include "Status.hpp"

#include <map>
#include <string>

extern "C" {

JNIEXPORT jlong JNICALL Java_gov_anl_mochi_YokanDBClient__1init
    (JNIEnv * env, jobject self) {
    auto impl = new std::map<std::string, std::string>();
    return reinterpret_cast<jlong>(impl);
}

JNIEXPORT void JNICALL Java_gov_anl_mochi_YokanDBClient__1cleanup
    (JNIEnv * env, jobject self, jlong impl) {
    auto db = reinterpret_cast<std::map<std::string, std::string>*>(impl);
    delete db;
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1read
    (JNIEnv * env, jobject self, jlong impl, jstring table,
     jstring key, jobject fields, jobject results) {
    // TODO
    return Status::OK(env);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1scan
    (JNIEnv * env, jobject self, jlong impl, jstring table,
     jstring startKey, jint recordCount, jobject fields, jobject results) {
    // TODO
    return Status::OK(env);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1update
    (JNIEnv * env, jobject self, jlong impl, jstring table,
     jstring key, jobject values) {
    // TODO
    return Status::OK(env);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1insert
    (JNIEnv * env, jobject self, jlong impl, jstring table,
     jstring key, jobject values) {
    // TODO
    return Status::OK(env);
}

JNIEXPORT jobject JNICALL Java_gov_anl_mochi_YokanDBClient__1delete
    (JNIEnv * env, jobject self, jlong impl, jstring table, jstring key) {
    // TODO
    return Status::OK(env);
}

}
