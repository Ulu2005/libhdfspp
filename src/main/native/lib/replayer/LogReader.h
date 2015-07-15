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

//This class works as a reader to log, which includes a log file and 
//a index file 

#ifndef LIBHDFSPP_READER_H_
#define LIBHDFSPP_READER_H_ 

#include <memory>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "log.pb.h"

namespace hdfs
{

class LogReader
{
 public:
  LogReader();
  LogReader(const char* logPath, const char* indexPath);
  virtual ~LogReader();

  void close(); 
  bool isEOF();
  bool setPath(const char* logPath, const char* indexPath); 
  std::unique_ptr<hadoop::hdfs::log> next();

 private:
  bool isOK_;
  bool isEOF_;
  FILE* indexFile_; 
  ::google::protobuf::io::FileInputStream* logFile_;
};

} /* hdfs */ 

#endif

