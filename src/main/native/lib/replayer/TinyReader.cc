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

// A basic reader for log file. 

#include <iostream>
#include <string>

#include "LogReader.h"
#include "CmlParser.h"

static int open_count = 0;
static int open_ret_count = 0;
static int close_count = 0;
static int close_ret_count = 0;
static int read_count = 0;
static int read_ret_count = 0;
static int start_date = 0;
static int end_date = 0;
static long start_time = 0;
static long end_time = 0;

void printLogInfo(const hadoop::hdfs::log &msg);
std::string getLogType(const hadoop::hdfs::log &msg);
void countOp(const hadoop::hdfs::log &msg);

int main(int argc, const char* argv[]) {
  hdfs::CmlParser cml(argc, argv);
  if (cml.getArgSize() != 1) {
    std::cout << "Usage: " << cml.getProgName() << " "
      << "[-v|--verbose] <log file>" << std::endl;
    return 0;
  }

  bool verbose = cml.getFlag("v") || cml.getFlag("verbose");

  hdfs::LogReader reader(cml.getArg(0).c_str());
  int index(0);
  std::unique_ptr<hadoop::hdfs::log> msg;

  while((msg = reader.next()) != nullptr) {
    index++;
    countOp(*msg);

    if (verbose) {
      std::cout << "#" << index << std::endl;
      printLogInfo(*msg);
    } 
  }

  if (!reader.isEOF()) {
    std::cerr << "Failed to parse log #" << (++index) << std::endl;
  }

  std::cout << "open: " << open_count; 
  std::cout << " open_ret: " << open_ret_count << std::endl;
  std::cout << "close: " << close_count;
  std::cout << " close_ret: " << close_ret_count << std::endl;
  std::cout << "read: " << read_count;
  std::cout << " read_ret: " << read_ret_count  << std::endl;

  int total = open_count + read_count + close_count;
  int date = end_date - start_date;
  long time = (end_time - start_time)/1000000 + 
              (date < 0 ? date + 365 : date) * 24 * 3600 * 1000;
  std::cout << "\nTotal: " << total << "\t";
  std::cout << "Time: " << time << "ms" << "\t";
  std::cout << "Thoroughput: " << (double)total/time << "/ms" << std::endl;

  reader.close();
  return 0;
}

/* Print log message */
void printLogInfo(const hadoop::hdfs::log &msg)
{
  std::cout << "date: " << msg.date() << std::endl; 
  std::cout << "time: " << msg.time() << std::endl; 
  std::cout << "thread id: " <<  msg.threadid() << std::endl; 
  std::cout << "type: " << getLogType(msg) << std::endl; 
  if (msg.type() == hadoop::hdfs::log_FuncType_OPEN) {
    std::cout << "path: " << msg.path() << std::endl; 
  }

  std::cout << "argu size: " << msg.argument_size() << std::endl; 
  for (int i = 0; i < msg.argument_size(); ++i) {
    if (msg.argument(i) > (long)(1 << 31)) {
      std::cout << std::hex << "\t" << msg.argument(i) << std::endl; 
    } else {
      std::cout << std::dec << "\t" << msg.argument(i) << std::endl; 
    }
  }

  std::cout << std::dec << " " << std::endl; 
}

/* Get string of log FuncType */
std::string getLogType(const hadoop::hdfs::log &msg)
{
  switch (msg.type()) {
    case hadoop::hdfs::log_FuncType_OPEN:
      return "OPEN";   
    case hadoop::hdfs::log_FuncType_OPEN_RET:
      return "OPEN_RET";
    case hadoop::hdfs::log_FuncType_CLOSE:
      return "CLOSE";
    case hadoop::hdfs::log_FuncType_CLOSE_RET:
      return "CLOSE_RET";
    case hadoop::hdfs::log_FuncType_READ:
      return "READ";
    case hadoop::hdfs::log_FuncType_READ_RET:
      return "READ_RET";
    default:
      return "unknown"; 
  }
}

void countOp(const hadoop::hdfs::log &msg)
{
  if (start_time == 0) {
    start_date = msg.date();
    start_time = msg.time();
  } else {
    end_date = msg.date();
    end_time = msg.time();
  }

  switch (msg.type()) {
    case hadoop::hdfs::log_FuncType_OPEN:
      open_count++;
      break;
    case hadoop::hdfs::log_FuncType_OPEN_RET:
      open_ret_count++;
      break;
    case hadoop::hdfs::log_FuncType_CLOSE:
      close_count++;
      break;
    case hadoop::hdfs::log_FuncType_CLOSE_RET:
      close_ret_count++;
      break;
    case hadoop::hdfs::log_FuncType_READ:
      read_count++;
      break;
    case hadoop::hdfs::log_FuncType_READ_RET:
      read_ret_count++;
      break;
    default:
      break;
  }
}

