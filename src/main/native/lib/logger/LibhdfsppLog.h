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

#ifndef LIBHDFSPP_LOG_H_
#define LIBHDFSPP_LOG_H_

#include "Logging.h"

namespace hdfs {

#define LOG_ENABLE true
#define LOG_PATH "PUT_LOG_PATH_HERE"
#define INDEX_PATH "PUT_INDEX_PATH_HERE"

#define LOG_START() do {\
  if (LOG_ENABLE)\
  Logging::startLog(LOG_PATH, INDEX_PATH);\
} while(0)

#define LOG_OPEN() do {\
  if (LOG_ENABLE)\
  Logging::logMessage(\
      Logger::OPEN, fs, path, flags, bufferSize, replication, blockSize);\
} while(0)

#define LOG_OPEN_RET(ret) do {\
  if (LOG_ENABLE)\
  Logging::logMessage(Logger::OPEN_RET, ret);\
} while(0)

#define LOG_CLOSE() do {\
  if (LOG_ENABLE)\
  Logging::logMessage(Logger::CLOSE, fs, file);\
} while(0)

#define LOG_CLOSE_RET(ret) do {\
  if (LOG_ENABLE)\
  Logging::logMessage(Logger::CLOSE_RET, ret);\
} while(0)

#define LOG_READ() do {\
  if (LOG_ENABLE)\
  Logging::logMessage(Logger::READ, fs, file, position, buf, length);\
} while(0)

#define LOG_READ_RET(ret) do {\
  if (LOG_ENABLE)\
  Logging::logMessage(Logger::READ_RET, ret);\
} while(0)

}

#endif

