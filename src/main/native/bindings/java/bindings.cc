#include "me_haohui_libhdfspp_NativeIoService.h"
#include "me_haohui_libhdfspp_NativeTcpConnection.h"
#include "me_haohui_libhdfspp_NativeRemoteBlockReader.h"
#include "me_haohui_libhdfspp_NativeRpcEngine.h"

#include "libhdfs++/hdfs.h"

#include "reader/block_reader.h"

#include "common/libhdfs++-internal.h"
#include "common/util.h"
#include "rpc/rpc_engine.h"

#include <google/protobuf/message_lite.h>
#include <asio/ip/tcp.hpp>

#include <cassert>

using namespace ::hdfs;
using ::asio::ip::tcp;
namespace pb = ::google::protobuf;

static jbyteArray ToJavaStatusRep(JNIEnv *env, const Status &stat) {
  if (stat.ok()) {
    return nullptr;
  }

  auto rep = StatusHelper::Rep(stat);
  jbyteArray arr = env->NewByteArray(rep.second);
  void *b = env->GetPrimitiveArrayCritical(arr, nullptr);
  memcpy(b, rep.first, rep.second);
  env->ReleasePrimitiveArrayCritical(arr, b, 0);
  return arr;
}

static void SetStatusArray(JNIEnv *env, jobjectArray jstatus, const Status &stat) {
  jbyteArray arr = ToJavaStatusRep(env, stat);
  env->SetObjectArrayElement(jstatus, 0, arr);
}

static void ReadPBMessage(JNIEnv *env, jbyteArray jbytes, pb::MessageLite *msg) {
  void *b = env->GetPrimitiveArrayCritical(jbytes, nullptr);
  msg->ParseFromArray(b, env->GetArrayLength(jbytes));
  env->ReleasePrimitiveArrayCritical(jbytes, b, JNI_ABORT);
}

static std::string ReadByteString(JNIEnv *env, jbyteArray jbytes) {
  char *data = reinterpret_cast<char*>(env->GetPrimitiveArrayCritical(jbytes, nullptr));
  std::string res(data, env->GetArrayLength(jbytes));
  env->ReleasePrimitiveArrayCritical(jbytes, data, JNI_ABORT);
  return res;
}

JNIEXPORT jlong JNICALL Java_me_haohui_libhdfspp_NativeIoService_create(JNIEnv *, jclass) {
  return reinterpret_cast<uintptr_t>(IoService::New());
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeIoService_nativeRun(JNIEnv *, jclass, jlong handle) {
  IoService *self = reinterpret_cast<IoService*>(handle);
  self->Run();
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeIoService_stop(JNIEnv *, jclass, jlong handle) {
  IoService *self = reinterpret_cast<IoService*>(handle);
  self->Stop();
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeIoService_destroy(JNIEnv *, jclass, jlong handle) {
  IoService *self = reinterpret_cast<IoService*>(handle);
  delete self;
}

JNIEXPORT jlong JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_create(JNIEnv *, jclass, jlong io_service_handle) {
  IoServiceImpl *io_service = reinterpret_cast<IoServiceImpl*>(io_service_handle);
  tcp::socket *sock = new tcp::socket(io_service->io_service());
  return reinterpret_cast<uintptr_t>(sock);
}

JNIEXPORT jbyteArray JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_connect(JNIEnv *env, jclass, jlong handle, jstring jhost, jint port) {
  tcp::socket *self = reinterpret_cast<tcp::socket*>(handle);
  const char *host = env->GetStringUTFChars(jhost, nullptr);
  tcp::endpoint ep(asio::ip::address::from_string(host), port);
  asio::error_code ec;
  self->connect(ep, ec);
  env->ReleaseStringUTFChars(jhost, host);
  if (!ec) {
    return nullptr;
  }
  assert (false && "Unimplemented");
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_disconnect(JNIEnv *, jclass, jlong handle) {
  tcp::socket *self = reinterpret_cast<tcp::socket*>(handle);
  ::asio::error_code ec;
  self->close(ec);
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_destroy(JNIEnv *, jclass, jlong handle) {
  tcp::socket *self = reinterpret_cast<tcp::socket*>(handle);
  delete self;
}

JNIEXPORT jlong JNICALL Java_me_haohui_libhdfspp_NativeRemoteBlockReader_create(JNIEnv *, jclass, jlong jconn) {
  tcp::socket *conn = reinterpret_cast<tcp::socket*>(jconn);
  auto self = new RemoteBlockReader<tcp::socket>(BlockReaderOptions(), conn);
  return reinterpret_cast<uintptr_t>(self);
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeRemoteBlockReader_destroy(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<RemoteBlockReader<tcp::socket>*>(handle);
}

JNIEXPORT jbyteArray JNICALL
Java_me_haohui_libhdfspp_NativeRemoteBlockReader_connect(JNIEnv *env, jclass, jlong handle,
                                                         jbyteArray jclient_name, jbyteArray jtoken,
                                                         jbyteArray jblock, jlong length, jlong offset) {
  auto self = reinterpret_cast<RemoteBlockReader<tcp::socket>*>(handle);
  std::string client_name(ReadByteString(env, jclient_name));
  hadoop::common::TokenProto token;
  if (jtoken) {
    ReadPBMessage(env, jtoken, &token);
  }
  hadoop::hdfs::ExtendedBlockProto block;
  ReadPBMessage(env, jblock, &block);
  Status stat = self->connect(client_name, jtoken ? &token : nullptr, &block, length, offset);
  return ToJavaStatusRep(env, stat);
}

JNIEXPORT jint JNICALL
Java_me_haohui_libhdfspp_NativeRemoteBlockReader_readSome(JNIEnv *env, jclass, jlong handle, jobject jdst,
                                                          jint position, jint limit, jobjectArray jstatus) {
  auto self = reinterpret_cast<RemoteBlockReader<tcp::socket>*>(handle);
  char *start = reinterpret_cast<char*>(env->GetDirectBufferAddress(jdst));
  Status stat;
  if (!start || position > limit) {
    stat = Status::InvalidArgument("Invalid buffer");
    SetStatusArray(env, jstatus, stat);
    return 0;
  }
  size_t transferred = self->read_some(asio::buffer(start + position, limit - position), &stat);
  if (!stat.ok()) {
    SetStatusArray(env, jstatus, stat);
  }
  return transferred;
}

JNIEXPORT jlong JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_create(JNIEnv *env, jclass, jlong io_service_handle,
                                                jbyteArray jclient_name, jbyteArray jprotocol,
                                                jint version) {
  IoServiceImpl *io_service = reinterpret_cast<IoServiceImpl*>(io_service_handle);

  RpcEngine *engine = new RpcEngine(&io_service->io_service(), ReadByteString(env, jclient_name),
                                    ReadByteString(env, jprotocol).c_str(), version);
  return reinterpret_cast<uintptr_t>(engine);
}

JNIEXPORT jbyteArray JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_connect(JNIEnv *env, jclass, jlong handle,
                                                 jstring jhost, jint port) {
  RpcEngine *self = reinterpret_cast<RpcEngine*>(handle);
  const char *host = env->GetStringUTFChars(jhost, nullptr);
  tcp::endpoint ep(asio::ip::address::from_string(host), port);
  Status status = self->Connect(ep);
  env->ReleaseStringUTFChars(jhost, host);
  return ToJavaStatusRep(env, status);
}

JNIEXPORT void JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_startReadLoop(JNIEnv *, jclass, jlong handle) {
  RpcEngine *self = reinterpret_cast<RpcEngine*>(handle);
  self->StartReadLoop();
}

JNIEXPORT jbyteArray JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_rpc(JNIEnv *env, jclass, jlong handle, jbyteArray jmethod,
                                             jbyteArray request, jobjectArray jstatus) {
  RpcEngine *self = reinterpret_cast<RpcEngine*>(handle);
  auto response = std::make_shared<std::string>();
  Status stat = self->RawRpc(ReadByteString(env, jmethod),
                             std::move(ReadByteString(env, request)), response);
  if (!stat.ok()) {
    SetStatusArray(env, jstatus, stat);
    return nullptr;
  }
  jbyteArray jresp = env->NewByteArray(response->size());
  void *b = env->GetPrimitiveArrayCritical(jresp, nullptr);
  memcpy(b, response->c_str(), response->size());
  env->ReleasePrimitiveArrayCritical(jresp, b, 0);
  return jresp;
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeRpcEngine_destroy(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<RpcEngine*>(handle);
}
