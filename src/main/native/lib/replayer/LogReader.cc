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

#include "LogReader.h"

#define BUFSIZE 32

using namespace hdfs;
namespace pbio = ::google::protobuf::io;

LogReader::LogReader()
  : isOK_(false)
  , isEOF_(false)
  , indexFile_(nullptr)
  , logFile_(nullptr)
{
}

LogReader::LogReader (const char* logPath, const char* indexPath)
  : LogReader()
{
  isOK_ = this->setPath(logPath, indexPath); 
}

LogReader::~LogReader()
{
  if (indexFile_ != nullptr) {
    indexFile_ = nullptr;
  }

  if (logFile_ != nullptr) {
    logFile_ = nullptr;
  }
}

void LogReader::close()
{
  if (indexFile_ != nullptr) {
    fclose(indexFile_);
  }

  if (logFile_ != nullptr) {
    logFile_->Close();
    delete logFile_;
  }
}

bool LogReader::isEOF()
{
  return isEOF_;
}

bool LogReader::setPath(const char* logPath, const char* indexPath)
{
  if (!logPath || !indexPath) {
    return false;
  }

  int logFd = open(logPath, O_RDONLY);
  int indexFd = open(indexPath, O_RDONLY);

  if ((logFd == -1) || (indexFd == -1)) {
    return false;
  }

  logFile_ = new pbio::FileInputStream(logFd);
  indexFile_ = fdopen(indexFd, "r"); 

  return true;
}

std::unique_ptr<hadoop::hdfs::log> LogReader::next()
{
  if (isEOF_ || (!isOK_)) {
    return nullptr;
  }

  char buf[BUFSIZE];
  if (fgets(buf, sizeof(buf), indexFile_) == NULL) {
    if (feof(indexFile_)) {
      isEOF_ = true;  
    } else {
      isOK_ = false;
    }

    return nullptr;
  }

  int size = std::atoi(buf);
  std::unique_ptr<hadoop::hdfs::log> msg(new hadoop::hdfs::log());
  if (!msg->ParseFromBoundedZeroCopyStream(logFile_, size)) {
    isOK_ = false;
    return nullptr; 
  }

  return msg;
}

