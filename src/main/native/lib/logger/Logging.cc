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

#include <cstdarg>
#include <iostream>
#include <unistd.h>

#include "Logging.h"

using namespace hdfs;

// static initialization
extern Logger ioLogger;

std::string Logging::logFilePath("");
bool Logging::failed = false;

void Logging::startLog(const char* logFile)
{
  logFilePath = logFile;
  appendPid(logFilePath);

  if (!ioLogger.startLog(logFilePath.c_str())) {
    std::cerr << "Failed to start IO logger." << std::endl;
    failed = true;
  }
}

void Logging::logMessage(Logger::FuncType type, ...)
{
  if (failed) {
    return;
  }

  va_list va;
  va_start(va, type);
  ioLogger.logMessage(type, va);
  va_end(va);
}

void Logging::appendPid(std::string &str)
{
  int pid = (int) getpid();

  std::size_t found = str.find(".log");
  if (found != std::string::npos) {
    str = str.substr(0, found);
  } 

  str.append("_");
  str.append(std::to_string(pid));
  str.append(".log");
}

