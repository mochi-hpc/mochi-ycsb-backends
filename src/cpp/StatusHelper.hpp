#ifndef YCSB_MOCHI_STATUS_HELPER_HPP
#define YCSB_MOCHI_STATUS_HELPER_HPP

#include <jni.h>
#include "MochiYCSB.hpp"

namespace mochi {
namespace ycsb {

struct StatusHelper {

    static jobject New(JNIEnv* env, const Status& status) {
        jclass statusClass    = env->FindClass("site/ycsb/Status"); \
        jstring jname         = env->NewStringUTF(status.name.c_str());
        jstring jdescription  = env->NewStringUTF(status.description.c_str());
        jmethodID constructor = env->GetMethodID(statusClass, "<init>", "(Ljava/lang/String;Ljava/lang/String;)V");
        jobject jstatus       = env->NewObject(statusClass, constructor, jname, jdescription);
        return jstatus;
    }
};

}
}

#endif
