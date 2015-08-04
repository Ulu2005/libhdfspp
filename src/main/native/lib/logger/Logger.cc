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

#include <iostream>
#include <thread>
#include <ctime>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <google/protobuf/io/coded_stream.h>

#include "Logger.h"

using namespace hdfs; 
namespace pbio = google::protobuf::io; 

Logger ioLogger;

Logger::Logger()
  : mutex_()
  , current_day_(-1)
  , logFile_(nullptr)
{
}

Logger::~Logger()
{
  if (logFile_ != nullptr) {
    logFile_->Close();
    delete logFile_;
    logFile_ = nullptr;
  }
}

bool Logger::startLog(const char* logFile)
{
  if (!logFile) return false;

  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  int logFileFd = open(logFile, O_CREAT | O_WRONLY | O_TRUNC, mode);
  
  if (logFileFd == -1) return false;
  logFile_ = new pbio::FileOutputStream(logFileFd);
  
  return true;
}

bool Logger::logMessage(FuncType type, va_list &va)
{
  hadoop::hdfs::log msg; 
  msg.set_time(getTime());
  msg.set_date(current_day_);     //should always set after time
  msg.set_threadid((long)pthread_self()); 

  switch (type) {
    case OPEN:
      msg.set_type(hadoop::hdfs::log_FuncType_OPEN);
      msg.add_argument(va_arg(va, long));
      msg.set_path(va_arg(va, char*)); 
      msg.add_argument(va_arg(va, long));
      msg.add_argument(va_arg(va, long));
      msg.add_argument(va_arg(va, long));
      msg.add_argument(va_arg(va, long));
      break;
    case OPEN_RET:
      msg.set_type(hadoop::hdfs::log_FuncType_OPEN_RET);
      msg.add_argument(va_arg(va, long));
      break;
    case CLOSE:
      msg.set_type(hadoop::hdfs::log_FuncType_CLOSE);
      msg.add_argument(va_arg(va, long));
      msg.add_argument(va_arg(va, long));
      break;
    case CLOSE_RET:
      msg.set_type(hadoop::hdfs::log_FuncType_CLOSE_RET);
      msg.add_argument(va_arg(va, long));
      break;
    case READ:
      msg.set_type(hadoop::hdfs::log_FuncType_READ);
      msg.add_argument(va_arg(va, long));
      msg.add_argument(va_arg(va, long));
      msg.add_argument(va_arg(va, long));
      msg.add_argument(va_arg(va, long));
      msg.add_argument(va_arg(va, long));
      break;
    case READ_RET:
      msg.set_type(hadoop::hdfs::log_FuncType_READ_RET);
      msg.add_argument(va_arg(va, long));
      break;
  }
  va_end(va);

  std::lock_guard<std::mutex> lock(mutex_);

  if (!writeDelimitedLog(msg)) return false;
  logFile_->Flush();
  
  return true;
}

bool Logger::writeDelimitedLog(::hadoop::hdfs::log &msg)
{  
  const int size = msg.ByteSize();
  pbio::CodedOutputStream output(logFile_);

  output.WriteVarint32(size);
  uint8_t* buffer = output.GetDirectBufferForNBytesAndAdvance(size);

  if (buffer != nullptr) {
    msg.SerializeWithCachedSizesToArray(buffer); 
  } else {
    msg.SerializeWithCachedSizes(&output);
    if (output.HadError()) return false;
  }
  
  // The CodedOutputStream's destructor will set the underlying stream's
  // position to where last byte is wrote.
  return true;
}

long Logger::getTime()
{
  struct timespec now; 
  struct tm tm; 

  clock_gettime(CLOCK_REALTIME, &now);
  localtime_r(&now.tv_sec, &tm);

  if (current_day_ != tm.tm_yday) {   
    current_day_ = tm.tm_yday;
  }   

  long nano_second = 0;
  nano_second += tm.tm_hour * 3600;
  nano_second += tm.tm_min * 60;
  nano_second += tm.tm_sec;
  nano_second *= 1000000000;
  nano_second += now.tv_nsec;

  return nano_second; 
}

