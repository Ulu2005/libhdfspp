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

#define LOGNAME "log_merged.log"
#define INDEXNAME "index_mergerd.log"

using namespace hdfs;

static FILE* newIndexFile = nullptr;
static ::google::protobuf::io::FileOutputStream* newLogFile = nullptr;

std::vector<std::string> getFiles(DIR* dir);
std::string getPid(const std::string str);
std::vector<LogReader> getReaders(const char* parent, 
    const std::vector<std::string> files);
void mergeLog(std::vector<LogReader> &readers);
int findMinMsg(const std::vector<std::unique_ptr<hadoop::hdfs::log>> &msgs);

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cout << "Usage: " << argv[0] << " "
      << "<log file directory> " 
      << "<output file directory>" << std::endl;
    return 0;
  } 

  //initialize readers and output file stream
  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    std::cout << "Failed to open log file directory." << std::endl;
    return 0; 
  }

  std::string outDir(argv[2]), outIndex, outLog; 
  if (outDir.at(outDir.size() - 1) != '/') {
    outDir.append("/");
  }

  outIndex = outDir + INDEXNAME;
  outLog = outDir + LOGNAME;

  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  int logFileFd = open(outLog.c_str(), O_CREAT | O_WRONLY | O_TRUNC, mode);
  newIndexFile = fopen(outIndex.c_str(), "w+");
  if ((logFileFd == -1) || (newIndexFile == NULL)) {
    std::cout << "Failed to create merged log file." << std::endl;
    return 0; 
  }

  newLogFile = new ::google::protobuf::io::FileOutputStream(logFileFd);

  //merge log files 
  std::chrono::time_point<std::chrono::system_clock> start, end;
  std::vector<LogReader> readers = getReaders(argv[1], getFiles(dir));

  std::cout << "Start to merge log files." << std::endl;
  start = std::chrono::system_clock::now();

  mergeLog(readers);

  //close files 
  for (auto r : readers) {
    r.close();
  }

  fclose(newIndexFile);
  newLogFile->Close();
  delete newLogFile;
  closedir(dir);

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> time = end - start;
  std::cout << "Finish merging log files in " << time.count();
  std::cout << "seconds." << std::endl;

  return 0;
}

/* Put all file names in a folder into a vector */
std::vector<std::string> getFiles(DIR* dir)
{
  std::vector<std::string> files;
  struct dirent* entry;

  while ((entry = readdir(dir)) != NULL) {
    std::string name(entry->d_name);

    if (name.find("log_") != std::string::npos) {
      files.push_back(name); 
    } else if (name.find("index_") != std::string::npos) {
      files.push_back(name); 
    }
  }

  return files; 
}

/* Extract pid from string with format "log_xxxx.log" and "index_xxxx.log" */
std::string getPid(const std::string str)
{
  std::size_t head;
  if ((head = str.find("log_")) == std::string::npos) {
    head = str.find("index_");
    head += std::strlen("index_"); 
  } else {
    head += std::strlen("log_");
  }

  return str.substr(head, (str.size() - std::strlen(".log") - head));
}

/* Generate readers by pairing index file and log file in file name vector */
std::vector<LogReader> getReaders(const char* parent, 
    const std::vector<std::string> files)
{
  std::string path(parent);
  if (path.at(path.length() - 1) != '/') { //last character in path
    path.append("/"); 
  }

  std::string index, log;
  std::vector<LogReader> readers;
  std::set<std::string> pids;

  for (auto filename : files) { //collect pid
    pids.insert(getPid(filename));   
  }

  for (auto pid : pids) { //build complete path and create LogReader
    index = path + "index_" + pid + ".log"; 
    log = path + "log_" + pid + ".log";
    readers.push_back(LogReader(log.c_str(), index.c_str()));
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
    int index = findMinMsg(msgs);
    if (index == -1) {
      break;
    }

    // output min msg
    int size = msgs.at(index)->ByteSize();
    if (fprintf(newIndexFile, "%d\n", size) <= 0) {
      std::cerr << "Failed to write index file." << std::endl;
      break;
    }

    if (!msgs.at(index)->SerializeToZeroCopyStream(newLogFile)) {
      std::cerr << "Failed to write log file." << std::endl;
      break;
    }

    // get a new msg from same reader
    msgs.at(index) = readers.at(index).next();
  }
}

/* Find index of message with min time, retur -1 if all messages are nullptr */
int findMinMsg(const std::vector<std::unique_ptr<hadoop::hdfs::log>> &msgs)
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

