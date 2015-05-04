#include "libhdfs++/chdfs.h"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <chrono>
#include <random>
#include <thread>


/*
    For now everything is hard coded to start scan_thread_count threads that each do a sequential scan over a file in hdfs.
    Default is to read 32MB whole packets at a time.  This should reproduce the deadlock issue.
*/

const int scan_thread_count = 8;


//default to reading 1MB blocks for linear scans
static const size_t KB = 1024;
static const size_t MB = 1024 * 1024;


struct seek_info {
  seek_info() : seek_count(0), fail_count(0), runtime(0) {};
  std::string str() {
    std::stringstream ss;
    ss << "seeks: " << seek_count << " failed reads:" << fail_count << " runtime:" << runtime << " seeks/sec:" << (seek_count+fail_count)/runtime;
    return ss.str();
  }

  int seek_count;
  int fail_count;
  double runtime;
};

struct scan_info {
  scan_info() : read_bytes(0), runtime(0.0) {};
  std::string str() {
    std::stringstream ss;
    ss << "read " << read_bytes << "bytes in " << runtime << " seconds, bandwidth " << read_bytes / runtime / MB << "MB/s";
    return ss.str();
  }

  uint64_t read_bytes;
  double runtime;
};


seek_info single_threaded_random_seek(hdfsFS fs, hdfsFile file, 
                                      unsigned int count = 1000,   //how many seeks to try
                                      off_t window_min = 0,        //minimum offset into file
                                      off_t window_max = 64 * MB); //max offset into file

scan_info single_threaded_linear_scan(hdfsFS fs, hdfsFile file,
                                      size_t read_size = 128 * KB,
                                      off_t start = 0,      //where in file to start reading
                                      off_t end = 64 * MB); //byte offset in file to stop reading


struct scanner {
  hdfsFS fs;
  hdfsFile file;
  size_t read_size;
  off_t start;
  off_t end;

  //out
  scan_info info;

  scanner(hdfsFS fs, hdfsFile file, size_t read_size, off_t start, off_t end) : fs(fs), file(file), read_size(read_size), 
                                                                                start(start), end(end) {};
  void operator()(){
    info = single_threaded_linear_scan(fs, file, read_size, start, end);
    std::cout << info.str() << std::endl;
  }

  scan_info getResult(){
    return info;
  }
};

void n_threaded_linear_scan(hdfsFS fs, std::string path, int threadcount, 
                                size_t read_size = 128 * KB,
                                off_t start = 0,
                                off_t end = 64 * MB) 
{
  std::cout << "concurrency max is " << std::thread::hardware_concurrency() << std::endl;

  std::vector<scanner> scanners;
  std::vector<std::thread> threads;
  std::vector<hdfsFile> hdfsFiles;

  //spawn
  for(int i=0; i< threadcount; i++) {
    std::cout << "starting thread " << i << std::endl;
    hdfsFile file = hdfsOpenFile(fs, path.c_str(), 0, 0, 0, 0);
    hdfsFiles.push_back(file);
    scanner s(fs, file, read_size, start, end);
    scanners.push_back(s);
    threads.push_back(std::thread(s));
  }

  //join
  for(int i=0; i<threadcount; i++) {
    std::cout << "joining thread " << i << std::endl;
    threads[i].join();
    hdfsCloseFile(fs, hdfsFiles[i]);
  }

}


int main(int argc, char **argv) {
  if(argc != 4) {
    std::cout << "usage: ./perf_tests <host> <port> <file> [...]" << std::endl;
    return 1;
  }


  hdfsFS fs = hdfsConnect(argv[1], std::atoi(argv[2]));   
  n_threaded_linear_scan(fs, argv[3], scan_thread_count, 128*KB, 0, 32*MB);


  return 0;
}


seek_info single_threaded_random_seek(hdfsFS fs, hdfsFile file, unsigned int count, off_t window_min, off_t window_max){
  seek_info info;

  //rng setup, bound by window size
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(window_min, window_max);

  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  for(unsigned int i=0;i<count;i++){
    std::uint64_t idx = dis(gen);
    char buf[1];
    std::int64_t cnt = hdfsPread(fs, file, idx, buf, 1);
    if(cnt != 1) {
      info.fail_count++;
    } else {
      info.seek_count++;
    }
  }

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed = end - start;
  info.runtime = elapsed.count();

  return info;
}



scan_info single_threaded_linear_scan(hdfsFS fs, hdfsFile file, size_t buffsize, off_t start_offset, off_t end_offset) {
  scan_info info;

  std::vector<char> buffer;
  buffer.reserve(buffsize);

  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();  

  std::int64_t count = start_offset;
  while(count <= end_offset) {
    std::int64_t read_bytes = hdfsPread(fs, file, count, &buffer[0], buffsize);
    if(read_bytes <= 0) {
      //todo: add some retry logic to step over block boundary
      break;
    } else {
      count += read_bytes;
    }
  }

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> dt = end - start;

  info.read_bytes = count - start_offset;
  info.runtime = dt.count();
 
  return info;  
}


