#ifndef YCSB_MOCHI_STATUS_HPP
#define YCSB_MOCHI_STATUS_HPP

#include <jni.h>

struct Status {

#define MOCHI_STATUS_DECLARE(__name__) \
    static jobject __name__(JNIEnv * env) { \
        jclass statusClass = env->FindClass("site/ycsb/Status"); \
        jfieldID field     = env->GetStaticFieldID(statusClass, #__name__, "Lsite/ycsb/Status;"); \
        jobject status     = env->GetStaticObjectField(statusClass, field); \
        return status; \
    }

    MOCHI_STATUS_DECLARE(OK)
    MOCHI_STATUS_DECLARE(ERROR)
    MOCHI_STATUS_DECLARE(NOT_FOUND)
    MOCHI_STATUS_DECLARE(NOT_IMPLEMENTED)
    MOCHI_STATUS_DECLARE(UNEXPECTED_STATE)
    MOCHI_STATUS_DECLARE(BAD_REQUEST)
    MOCHI_STATUS_DECLARE(FORBIDDEN)
    MOCHI_STATUS_DECLARE(SERVICE_UNAVAILABLE)
    MOCHI_STATUS_DECLARE(BATCHED_OK)

#undef MOCHI_STATUS_DECLARE

    static jobject New(JNIEnv * env, const char* name, const char* description) {
        jclass statusClass    = env->FindClass("site/ycsb/Status"); \
        jstring jname         = env->NewStringUTF(name);
        jstring jdescription  = env->NewStringUTF(description);
        jmethodID constructor = env->GetMethodID(statusClass, "<init>", "(Ljava/lang/String;Ljava/lang/String;)V");
        jobject status        = env->NewObject(statusClass, constructor, jname, jdescription);
        return status;
    }

};

#endif
