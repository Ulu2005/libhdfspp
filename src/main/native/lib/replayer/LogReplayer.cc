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

#include <map>
#include <mutex>
#include <chrono>
#include <vector>
#include <thread>
#include <iostream>

#include "libhdfs++/chdfs.h"
#include "LogReader.h"
#include "CmlParser.h"

//constants
static const unsigned MAX_THREADS = std::thread::hardware_concurrency() * 2;
static const int MB = 1024 * 1024;

//program options
static bool wait_before_new_thread = false;
static std::string parent_folder = "";

//global variables
static std::mutex mtx;
static bool need_count = true;
static int read_bytes = 0;
static double run_time = 0;
static hdfsFS fs = nullptr;
static std::map<long, hdfsFile> files;
static std::vector<std::unique_ptr<hadoop::hdfs::log>> jobs;

void getReadInfo(int bytes, double seconds);
void printBandwidth();
void handleJobs();
void handleOpen(const hadoop::hdfs::log &msg);
void handleOpenRet(const hadoop::hdfs::log &msg);
void handleRead(const hadoop::hdfs::log &msg);
void handleClose(const hadoop::hdfs::log &msg);

int main(int argc, const char* argv[]) {
  hdfs::CmlParser cml(argc, argv);

  if (cml.getArgSize() != 4) {
    std::cout << "Usage: " << cml.getProgName();
    std::cout << " [-w|--wait] [--parent-folder=<path>] ";
    std::cout << " <log file> <index file> " << "<host> <port>" << std::endl;
    return 0;
  }
  wait_before_new_thread = cml.getFlag("w") || cml.getFlag("wait");
  cml.getOption("parent-folder", parent_folder);

  hdfs::LogReader reader(cml.getArg(0).c_str(), cml.getArg(1).c_str());
  fs = hdfsConnect(cml.getArg(2).c_str(), std::atoi(cml.getArg(3).c_str())); 

  int index(0);
  std::unique_ptr<hadoop::hdfs::log> msg;
  std::chrono::time_point<std::chrono::system_clock> start, end;

  std::cout << "Start replaying file operations." << std::endl;
  start = std::chrono::system_clock::now();
  std::thread count_thread(printBandwidth);

  while((msg = reader.next()) != nullptr) {
    switch (msg->type()) {
      case hadoop::hdfs::log_FuncType_OPEN:
        handleOpen(*msg);
        break;
      case hadoop::hdfs::log_FuncType_OPEN_RET:
        handleOpenRet(*msg);
        break;
      case hadoop::hdfs::log_FuncType_CLOSE:
        handleJobs(); //handle all jobs in vector and join the work threads here
        handleClose(*msg);
        break;
      case hadoop::hdfs::log_FuncType_READ:
        jobs.push_back(std::move(msg));
        break;
      default:
        ;
    } 

    index++;
  }
  end = std::chrono::system_clock::now();
  std::chrono::duration<double> time = end - start;
  need_count = false;
  count_thread.join();

  if (!reader.isEOF()) {
    std::cerr << "Failed to parse log #" << (++index) << std::endl;
  } else {
    std::cout << "Total time: " << time.count() << " seconds. ";
    std::cout << "Total file operations: " << index / 2 << "." << std::endl; 
  }

  reader.close();
  hdfsDisconnect(fs);

  return 0;
}

/* Pass read info to the thread */
void getReadInfo(int bytes, double seconds)
{
  mtx.lock();
  read_bytes += bytes;
  run_time += seconds;
  mtx.unlock();
}

/* Calculate and print bandwidth info */
void printBandwidth()
{
  while(need_count) {
    mtx.lock(); 
    if (run_time > 0) {
      std::cout << "Current bandwidth: ";
      std::cout << read_bytes / run_time / MB << " MB/s" << std::endl; 
      read_bytes = 0;
      run_time = 0; 
    }
    mtx.unlock();

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

/* spawn threads to handle file operation and wait them to finish */
void handleJobs()
{
  if (jobs.empty()) {
    return;
  }

  std::vector<std::thread> threads;
  long last_time = jobs[0]->time();

  for (int i = 0; i < (int)jobs.size(); ++i) {
    if (wait_before_new_thread) {
      long time = jobs[i]->time();
      std::this_thread::sleep_for(std::chrono::nanoseconds(time - last_time));
      last_time = time;
    }

    switch (jobs[i]->type()) {
      case hadoop::hdfs::log_FuncType_READ:
        threads.push_back(std::thread(handleRead, *jobs[i])); 
        break;
      default:
        ;
    }

    if (threads.size() == MAX_THREADS) { //avoid too many requests to hdfs
      for (int i = 0; i < (int)threads.size(); ++i) {
        threads[i].join(); 
      }

      threads.clear(); 
    }
  }

  for (int i = 0; i < (int)threads.size(); ++i) {
    threads[i].join(); 
  }

  jobs.clear();
}

void handleOpen(const hadoop::hdfs::log &msg)
{
  std::string path = msg.path();
  if (parent_folder != "") {
    if (path.at(0) == '/') {
      path = "/" +  parent_folder + path;
    } else {
      path = parent_folder + "/" + path;
    }
  }

  hdfsFile file = hdfsOpenFile(fs, path.c_str(), 
      (int)msg.argument(1), 
      (int)msg.argument(2), 
      (short)msg.argument(3), 
      (int)msg.argument(4));

  files[msg.threadid()] = file;
}

void handleOpenRet(const hadoop::hdfs::log &msg)
{
  files[msg.argument(0)] = files[msg.threadid()];
  files.erase(msg.threadid());
}

void handleRead(const hadoop::hdfs::log &msg)
{
  auto file = files.find(msg.argument(1));
  if (file != files.end()) {
    size_t buf_size = msg.argument(4);
    char* buffer = new char[buf_size];
    auto start = std::chrono::system_clock::now();

    auto ret = hdfsPread(fs, file->second, 
        (off_t)msg.argument(2), 
        reinterpret_cast<void*>(buffer), 
        buf_size);

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    getReadInfo(ret, elapsed.count());
    
    delete[] buffer;
  } else {
    std::cerr << "Read: file " 
      << msg.argument(1) 
      << "not found." << std::endl;
  }
}

void handleClose(const hadoop::hdfs::log &msg)
{
  auto file = files.find(msg.argument(1));
  if (file != files.end()) {
    auto ret = hdfsCloseFile(fs, file->second);
    (void)ret;//make gcc happy
  } else {
    std::cerr << "Close: file " 
      << msg.argument(1) 
      << "not found." << std::endl;
  }
}

