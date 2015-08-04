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

#include <fcntl.h>
#include <unistd.h>
#include <google/protobuf/io/coded_stream.h>

#include "LogReader.h"

#define BUFSIZE 32

using namespace hdfs;
namespace pbio = ::google::protobuf::io;

LogReader::LogReader()
  : isOK_(false)
  , isEOF_(false)
  , logFile_(nullptr)
{
}

LogReader::LogReader (const char* logPath)
  : LogReader()
{
  isOK_ = this->setPath(logPath); 
}

LogReader::~LogReader()
{
  if (logFile_ != nullptr) {
    logFile_ = nullptr;
  }
}

void LogReader::close()
{
  if (logFile_ != nullptr) {
    logFile_->Close();
    delete logFile_;
  }
}

bool LogReader::isEOF()
{
  return isEOF_;
}

bool LogReader::setPath(const char* logPath)
{
  if (!logPath) {
    return false;
  }

  int logFd = open(logPath, O_RDONLY);
  if (logFd == -1) {
    return false;
  }
  logFile_ = new pbio::FileInputStream(logFd);

  return true;
}

std::unique_ptr<hadoop::hdfs::log> LogReader::next()
{
  if (isEOF_ || (!isOK_)) {
    return nullptr;
  }
  
  pbio::CodedInputStream input(logFile_);
  uint32_t size;

  if (!input.ReadVarint32(&size)) {
    isEOF_ = true;
    return nullptr;
  }
  
  pbio::CodedInputStream::Limit limit = input.PushLimit(size);
  std::unique_ptr<hadoop::hdfs::log> msg(new hadoop::hdfs::log());
  
  if (!msg->MergeFromCodedStream(&input)) return nullptr;
  if (!input.ConsumedEntireMessage()) return nullptr; 

  input.PopLimit(limit);
   
  return msg;
}

