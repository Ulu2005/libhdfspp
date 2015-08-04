/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIBHDFSPP_LOGGER_H_
#define LIBHDFSPP_LOGGER_H_ 

#include <cstdarg>
#include <mutex>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "log.pb.h"

namespace hdfs
{

class Logger
{
 public:
  typedef enum {        //type of different file operation
    OPEN,
    OPEN_RET,
    CLOSE,
    CLOSE_RET,
    READ,
    READ_RET
  } FuncType;

  Logger ();
  virtual ~Logger ();

  bool startLog(const char* logFile);
  bool logMessage(FuncType type, va_list &va);
  bool writeDelimitedLog(::hadoop::hdfs::log &msg);

 private:
  long getTime();       //get time in nanosecond and refresh current day

  std::mutex mutex_;
  int current_day_;
  ::google::protobuf::io::FileOutputStream* logFile_;
};

} /* iotools */ 

#endif

