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

// Static wrapper class for logger.

#ifndef LIBHDFSPP_LOGGING_H
#define LIBHDFSPP_LOGGING_H 

#include "Logger.h"

namespace hdfs
{

class Logging
{
 public:
  Logging ();
  virtual ~Logging ();

  static void startLog(const char* logFile, const char* indexFile);
  static void logMessage(Logger::FuncType type, ...);

 private:
  static void appendPid(std::string &str);

  static std::string indexFilePath;
  static std::string logFilePath;
  static bool failed;
};

} /* iotools */ 

#endif

