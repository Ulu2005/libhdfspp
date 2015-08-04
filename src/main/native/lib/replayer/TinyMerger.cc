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

#include <vector>
#include <set>
#include <string>
#include <cstring>
#include <iostream>
#include <chrono>
#include <sys/types.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include "LogReader.h"
#include "logger/Logger.h"

#define LOG_NAME "libhdfspp_merged.log"

using namespace hdfs;

static Logger logger;

std::vector<LogReader> getReaders(DIR* dir, std::string parent);
void mergeLog(std::vector<LogReader> &readers);
int findOldestMsg(const std::vector<std::unique_ptr<hadoop::hdfs::log>> &msgs);

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cout << "Usage: " << argv[0] << " <input log file directory> ";
    std::cout << "<merged log file directory>" << std::endl;
    return 0;
  } 

  //initialize readers and output file stream
  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    std::cout << "Failed to open log file directory." << std::endl;
    return 0; 
  }

  std::string inDir(argv[1]), outDir(argv[2]), output; 
  auto append_slash = [](std::string &str) {
    if (str.at(str.size() - 1) != '/') str.append("/");
  };
  append_slash(inDir);
  append_slash(outDir);

  std::vector<LogReader> readers = getReaders(dir, inDir);

  output = outDir + LOG_NAME;
  if (!logger.startLog(output.c_str())) {
    std::cout << "Failed to create merged log file." << std::endl;
    return 0; 
  }

  //merge log files 
  std::chrono::time_point<std::chrono::system_clock> start, end;

  std::cout << "Start to merge log files." << std::endl;
  start = std::chrono::system_clock::now();

  mergeLog(readers);

  //close files 
  for (auto r : readers) {
    r.close();
  }
  closedir(dir);

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> time = end - start;
  std::cout << "Finish merging log files in " << time.count();
  std::cout << "seconds." << std::endl;

  return 0;
}

/* Get readers for all log file in directory */
std::vector<LogReader> getReaders(DIR* dir, std::string parent)
{
  std::vector<std::string> files;
  std::vector<LogReader> readers;
  struct dirent* entry;

  while ((entry = readdir(dir)) != NULL) {
    std::string name(entry->d_name);

    if (name.find(".log") != std::string::npos) {
      files.push_back(name);
      std::cout << "Reading " << name << std::endl; 
    }
  }
  
  for (auto filename : files) {
    readers.push_back(LogReader((parent + filename).c_str()));
  }

  return readers;
}

/* Read from multiple log files and merge them into a new one */
void mergeLog(std::vector<LogReader> &readers)
{
  std::vector<std::unique_ptr<hadoop::hdfs::log>> msgs;
  for (auto r : readers) {
    msgs.push_back(r.next());
  }

  while(true) {
    // find min msg
    int index = findOldestMsg(msgs);
    if (index == -1) {
      break;
    }

  logger.writeDelimitedLog(*msgs.at(index));
  msgs.at(index) = readers.at(index).next();
  }
}

/* Find index of messages with min time, retur -1 if all messages are nullptr */
int findOldestMsg(const std::vector<std::unique_ptr<hadoop::hdfs::log>> &msgs)
{
  int min = -1;
  int min_date = -1;
  long min_time = -1;

  for (int i = 0; i < (int)msgs.size(); ++i) {
    if (msgs[i] == nullptr) {
      continue;
    }

    if ((min == -1) || 
        ((msgs[i]->date() <= min_date) && (msgs[i]->time() < min_time))) {
      min = i;
      min_date = msgs[i]->date();
      min_time = msgs[i]->time(); 
    }
  }

  return min;
}

