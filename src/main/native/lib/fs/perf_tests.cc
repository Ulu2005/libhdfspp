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
    Basic perfomance/stress tests for libhdfs++.  
*/


//useful constants
static const size_t KB = 1024;
static const size_t MB = 1024 * 1024;


struct seek_info {
  seek_info() : seek_count(0), fail_count(0), runtime(0) {};
  std::string str() {
    std::stringstream ss;
    ss << "seeks: " << seek_count << ", failed seeks:" << fail_count << " runtime:" << runtime << " seeks/sec:" << (seek_count+fail_count)/runtime;
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
                                      off_t window_max = 32 * MB); //max offset into file

scan_info single_threaded_linear_scan(hdfsFS fs, hdfsFile file,
                                      size_t read_size = 128 * KB,
                                      off_t start = 0,      //where in file to start reading
                                      off_t end = 64 * MB); //byte offset in file to stop reading

void n_threaded_linear_scan(hdfsFS fs, std::string path, int threadcount, 
                                size_t read_size = 128 * KB,
                                off_t start = 0,
                                off_t end = 64 * MB); 

void n_threaded_random_seek(hdfsFS fs, std::string path, int threadcount,
                                unsigned int count = 1000,
                                off_t window_min = 0,
                                off_t window_max = 32 * MB);


void open_read_close_test(hdfsFS fs, std::string path, 
                                size_t read_size = 1,          //number of bytes to read per cycle
                                unsigned int cycle_count = 1000,     //number of open-read-close cycles
                                off_t window_min = 0,         //min offset into file
                                off_t window_max = 32 * MB);  //max offset into file


void n_threaded_open_read_close(hdfsFS fs, std::string path, int threadcount, 
                                size_t read_size = 128 * KB,
                                off_t start = 0,
                                off_t end = 64 * MB); 




int main(int argc, char **argv) {
  if(argc < 5) {
    std::cout << "usage: ./perf_tests <host> <port> <file> [-threaded_read, -threaded_seek, -open_read_close]" << std::endl;
    std::cout << "\t-threaded_read <threadcount> <read size> <max offset>" << std::endl;
    std::cout << "\t-threaded_seek <threadcount> <number of seeks> <max offset>" << std::endl;
    std::cout << "\t-open_read_close <read size> <number of cycles> <max offset>" << std::endl;
    std::cout << "\t-threaded_open_read_close <threadcount> <read size> <max offset>" << std::endl;

    return 1;
  }
  
  hdfsFS fs = hdfsConnect(argv[1], std::atoi(argv[2])); 
  
  std::string cmd(argv[4]);
  if(cmd == "-threaded_read") {
    //Start a number of threads and have them scan through a file.  Helpful for reproducing threading issues.
    if(argc != 8) {
      std::cerr << "usage ./perf_tests <host> <port> <file> -threaded_read <thread count> <read size> <max offset>" << std::endl;
      return 1;
    }

    int threadcount = std::atoi( argv[5] );
    int readsize    = std::atoi( argv[6] );
    int maxoffset   = std::atoi( argv[7] );

    n_threaded_linear_scan(fs, argv[3], threadcount, readsize, 0/*min offset*/, maxoffset);
  } else if (cmd == "-threaded_seek") {
    //Start a number of threads and have them do 1 byte reads at random offsets.
    if(argc != 8) {
      std::cerr << "usage ./perf_tests <host> <port> <file> -threaded_seek <thread count> <seek count> <max offset>" << std::endl;
      return 1;
    }

    int threadcount = std::atoi( argv[5] );
    int seekcount   = std::atoi( argv[6] );
    int maxoffset   = std::atoi( argv[7] );

    n_threaded_random_seek(fs, argv[3], threadcount, seekcount, 0/*min offset*/, maxoffset);
  } else if (cmd == "-open_read_close") {
    //Similar to random seek, but close fd between each seek operation.  Helpful for getting memory leaks to show up.
    if(argc != 8) {
      std::cerr << "usage ./perf_tests <host> <port> <file> -open_read_close <read size> <number of cycles> <max offset>" << std::endl;
      return 1;
    }

    int readsize  = std::atoi( argv[5] );
    int numcycles = std::atoi( argv[6] );
    int maxoffset = std::atoi( argv[7] );     

    open_read_close_test(fs, argv[3], readsize, numcycles, 0/*min offset*/, maxoffset);
  } else if (cmd == "-threaded_open_read_close") {
    //Start a number of threads and have them scan through a file.  Helpful for reproducing threading issues.
    if(argc != 8) {
      std::cerr << "usage ./perf_tests <host> <port> <file> -threaded_open_read_close <thread count> <read size> <max offset>" << std::endl;
      return 1;
    }

    int threadcount = std::atoi( argv[5] );
    int readsize    = std::atoi( argv[6] );
    int maxoffset   = std::atoi( argv[7] );

    n_threaded_open_read_close(fs, argv[3], threadcount, readsize, 0/*min offset*/, maxoffset);
  } else {
    std::cerr << "command " << cmd << " not recognized" << std::endl;
  }

  hdfsDisconnect(fs);

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

  std::vector<char> empty_tmp;
  buffer = empty_tmp; 
  return info;  
}

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
                                size_t read_size,
                                off_t start,
                                off_t end) 
{
  //note: Not going to attempt to aggragate the read bandwidths because there are no 
  //  guarentees that the threads will be operating in parallel for the majority of
  //  the test.  

  std::cout << "hardware concurrency max is " << std::thread::hardware_concurrency() << std::endl;

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

struct seeker {
  hdfsFS fs;
  hdfsFile file;
  int threadcount;
  unsigned int count;
  off_t window_min;
  off_t window_max;

  //out
  seek_info info;

  seeker(hdfsFS fs, hdfsFile file, int threadcount, unsigned int count,  off_t window_min, off_t window_max) : fs(fs), file(file), 
                    threadcount(threadcount), count(count), window_min(window_min), window_max(window_max) {};

  void operator()(){
    info = single_threaded_random_seek(fs, file, count, window_min, window_max);
    std::cout << info.str() << std::endl;
  }

  seek_info getResult(){
    return info;
  }
};



void n_threaded_random_seek(hdfsFS fs, std::string path, int threadcount,
                                unsigned int count,
                                off_t window_min,
                                off_t window_max)
{
  std::cout << "hardware concurrency max is " << std::thread::hardware_concurrency() << std::endl;

  std::vector<seeker> seekers;
  std::vector<std::thread> threads;
  std::vector<hdfsFile> hdfsFiles;


  //spawn
  for(int i=0; i< threadcount; i++) {
    std::cout << "starting thread " << i << std::endl;
    hdfsFile file = hdfsOpenFile(fs, path.c_str(), 0, 0, 0, 0);
    hdfsFiles.push_back(file);
    seeker s(fs, file, threadcount, count, window_min, window_max);
    seekers.push_back(s);
    threads.push_back(std::thread(s));
  }
  
  //join
  for(int i=0; i<threadcount; i++) {
    std::cout << "joining thread " << i << std::endl;
    threads[i].join();
    hdfsCloseFile(fs, hdfsFiles[i]);
  }

}


void open_read_close_test(hdfsFS fs, std::string path, size_t read_size, unsigned int cycle_count, off_t window_min, off_t window_max) {

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(window_min, window_max - read_size);      //assume > 32MB file, size should be passed by param
 
  std::vector<char> buf;
  buf.resize(read_size);  

  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  for(unsigned int i=0;i<cycle_count;i++) {
    //open
    hdfsFile file = hdfsOpenFile(fs, path.c_str(), 0, 0, 0, 0);

    //read from random offset onto buffer
    off_t offset = dis(gen);
    std::int64_t read_bytes = hdfsPread(fs, file, offset, &buf[0], read_size);

    if( (size_t)read_bytes != read_size) {
      std::cerr << "Read failed.  Wanted " << read_size << " bytes, read " << read_bytes << " bytes at offset " << offset << std::endl;
    }

    //close
    int res = hdfsCloseFile(fs, file);
    if(0 != res) {
      std::cerr << "failed to close file on iteration " << i << std::endl; 
    }
  }

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed = end - start;
  double dt = elapsed.count();

  std::cout << "completed " << cycle_count << " cycles in " << dt << " seconds. " << cycle_count/dt << " cycles/second" << std::endl;

}


void n_threaded_open_read_close(hdfsFS fs, std::string path, int threadcount, 
                                size_t read_size,
                                off_t start,
                                off_t end) 
{
  std::cout << "hardware concurrency max is " << std::thread::hardware_concurrency() << std::endl;

  std::vector<std::thread> threads;
  auto open_read_close = [fs, path, read_size, start, end] {
    hdfsFile file = hdfsOpenFile(fs, path.c_str(), 0, 0, 0, 0);
    
    scan_info info = single_threaded_linear_scan(fs, file, read_size, start, end);
    std::cout << info.str() << std::endl;
  
    hdfsCloseFile(fs, file);
  };

  //spawn
  for(int i=0; i< threadcount; i++) {
    std::cout << "starting thread " << i << std::endl;
    threads.push_back(std::thread(open_read_close));
  }

  //join
  for(int i=0; i<threadcount; i++) {
    std::cout << "joining thread " << i << std::endl;
    threads[i].join();
  }
}
