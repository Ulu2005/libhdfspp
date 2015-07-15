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

//Expose a pure C interface that doesn't need any C++/C++11 header files
//Avoid use of C++11 in this file as well because it's going to be backported c++98 eventually anyway. eg raw pthreads instead of C++11 threads

//Intended to be compatible with libhdfs(3).  Currently only a subset of operations are supported.
//  hdfsConnect
//  hdfsDisconnect
//  hdfsOpenFile
//  hdfsCloseFile
//  hdfsPread


//todo: 
//  Need to be able to pass in parameters to hint at resource allocation like how many threads call run on io_service
//  Implement real streams that keep track of position on top of inputstream

#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>

#include "libhdfs++/chdfs.h"
#include "libhdfs++/hdfs.h"
#include "logger/LibhdfsppLog.h"

#include <iostream>
#include <string>
#include <thread>


//---------------------------------------------------------------------------------------
//  Wrap C++ calls in C
//---------------------------------------------------------------------------------------

using namespace hdfs;

void *call_run(void *servicePtr) {
  IoService *wrappedService = reinterpret_cast<IoService*>(servicePtr);
  wrappedService->Run();
  return NULL;
}

//Copied almost directly from inputstream_test.
//Eventually add a way for the user to specify how many threads call run on io_service
class Executor {
public:
  Executor() {
    //Create a new IoService object. This wraps the boost io_service object.
    io_service_ = std::unique_ptr<IoService>(IoService::New());

    //Call run on IoService object in a background thread, the run call should never return.
    int ret = pthread_create(&processing_thread, NULL, call_run, reinterpret_cast<void*>(io_service_.get()));
    
    if(ret != 0) {
      //reset io_service ptr to null so caller can check.  Don't want to throw.
      io_service_ = NULL;
    }

    LOG_START();
  }

  ~Executor() {
    //Stop IoService event loop and background thread.
	if(io_service_) {
    	io_service_->Stop();
	}
  }
 
  IoService *io_service() {
    return io_service_.get();
  }

private:
  std::unique_ptr<IoService> io_service_;
  pthread_t processing_thread; 
};


struct hdfsFile_struct {
  hdfsFile_struct() : inputStream(NULL) {};
  hdfsFile_struct(InputStream *is) : inputStream(is) {};

  virtual ~hdfsFile_struct() {
    delete inputStream;
	inputStream = NULL;
  }

  InputStream *inputStream;
};


struct hdfsFS_struct {
  hdfsFS_struct() : fileSystem(NULL), backgroundIoService(NULL) {};
  hdfsFS_struct(FileSystem *fs, Executor *ex) : fileSystem(fs), backgroundIoService(ex) {};
  virtual ~hdfsFS_struct() {
    //todo:  Can't correctly free these structs at the moment due to broken ownership model.
    //       When reimplementing the C API be really careful about who is responsible for destroying what.


    //delete fileSystem;
	//fileSystem = NULL;
    //delete backgroundIoService;
	//backgroundIoService = NULL;
  };
 
  FileSystem *fileSystem;
  Executor   *backgroundIoService;
};


/*  FS initialization routine
 *    -need to start background thread(s) to run asio::io_service
 *    -connect to specified namenode host:port
 *    -this will need to be extended to allow application to pass a few things:
 *      -malloc/delete pair for using specialized pools
 *      -thread count for background threads
 */
hdfsFS hdfsConnect(const char *nnhost, unsigned short nnport) {
  std::unique_ptr<Executor> background_io_service = std::unique_ptr<Executor>(new Executor());

  if(NULL == background_io_service) 
    return NULL;

  //connect to NN, fileSystem will be set on success
  FileSystem *fileSystem = NULL;
  Status stat = FileSystem::New(background_io_service->io_service(), nnhost, nnport, &fileSystem);
  if(!stat.ok())
    return NULL;

  //make a hdfsFS handle
  return new hdfsFS_struct(fileSystem, background_io_service.release());
}


int hdfsDisconnect(hdfsFS fs) {
  //delete fs if it exists, fs dtor shall clean up everything it owns
  if(NULL != fs)
    delete fs;  
  else
    return -1;
  return 0;
}


hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize, short replication, int blockSize) {
  LOG_OPEN();
  
  //The following four params are just placeholders until write path is finished, add void so compiler doesn't complain.
  (void)flags;
  (void)bufferSize;
  (void)replication;
  (void)blockSize;

  //assuming we want to do a read with default settings, sufficient for now
  InputStream *isPtr = NULL;
  Status stat = fs->fileSystem->Open(path, &isPtr);
  if(!stat.ok()) {
    LOG_OPEN_RET(NULL); 
    return NULL;
  }

  //may need to switch to scoped lock if anything in here can throw.
  hdfsFile ret =  new hdfsFile_struct(isPtr);
  LOG_OPEN_RET(ret);
  return ret;
}


int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
  LOG_CLOSE();
  (void)fs;
  
  if(NULL != file) {
    delete file;
  } else {
    LOG_CLOSE_RET(-1);
    return -1;
  }
    
  LOG_CLOSE_RET(0);
  return 0;
}


size_t hdfsPread(hdfsFS fs, hdfsFile file, off_t position, void *buf, size_t length) {
  LOG_READ();
  
  if(NULL == fs || NULL == file) {
    //possibly set errno here
    LOG_READ_RET(0);
    return 0;   
  }

  size_t readBytes = 0;
  Status stat = file->inputStream->PositionRead(buf, length, position, &readBytes);
  if(!stat.ok()) {
    //possibly set errno here
    LOG_READ_RET(0);
    return 0;
  }

  LOG_READ_RET(readBytes);
  return readBytes;
}


