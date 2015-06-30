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

#include "filesystem.h"

#include "common/util.h"

#include <asio/ip/tcp.hpp>

#include <limits>

#include <sys/stat.h>
#include <sys/types.h>

namespace hdfs {

static const char kNamenodeProtocol[] = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
static const int kNamenodeProtocolVersion = 1;

using ::asio::ip::tcp;

FileSystem::~FileSystem()
{}

Status FileSystem::New(IoService *io_service, const char *server,
                       unsigned short port, FileSystem **fsptr) {
  std::unique_ptr<FileSystemImpl> impl(new FileSystemImpl(io_service));
  Status stat = impl->Connect(server, port);
  if (stat.ok()) {
    *fsptr = impl.release();
  }
  return stat;
}

FileSystemImpl::FileSystemImpl(IoService *io_service)
    : io_service_(static_cast<IoServiceImpl*>(io_service))
    , engine_(&io_service_->io_service(), RpcEngine::GetRandomClientName(),
              kNamenodeProtocol, kNamenodeProtocolVersion)
    , namenode_(&engine_)
{}

Status FileSystemImpl::Connect(const char *server, unsigned short port) {
  asio::error_code ec;
  tcp::resolver resolver(io_service_->io_service());
  tcp::resolver::query query(tcp::v4(), server, std::to_string(port));
  tcp::resolver::iterator iterator = resolver.resolve(query, ec);

  if (ec) {
    return ToStatus(ec);
  }

  Status stat = engine_.Connect(*iterator);
  if (!stat.ok()) {
    return stat;
  }
  engine_.StartReadLoop();
  return stat;
}

Status FileSystemImpl::Open(const char *path, InputStream **isptr) {
  using ::hadoop::hdfs::GetBlockLocationsRequestProto;
  using ::hadoop::hdfs::GetBlockLocationsResponseProto;

  GetBlockLocationsRequestProto req;
  auto resp = std::make_shared<GetBlockLocationsResponseProto>();
  req.set_src(path);
  req.set_offset(0);
  req.set_length(std::numeric_limits<long long>::max());
  Status stat = namenode_.GetBlockLocations(&req, resp);
  if (!stat.ok()) {
    return stat;
  }

  *isptr = new InputStreamImpl(this, &resp->locations());
  return Status::OK();
}

//fs util functions
/*
message HdfsFileStatusProto {
  enum FileType {
    IS_DIR = 1;
    IS_FILE = 2;
    IS_SYMLINK = 3;
  }
  required FileType fileType = 1;
  required bytes path = 2;          // local name of inode encoded java UTF8
  required uint64 length = 3;
  required FsPermissionProto permission = 4;
  required string owner = 5;
  required string group = 6;
  required uint64 modification_time = 7;
  required uint64 access_time = 8;

  // Optional fields for symlink
  optional bytes symlink = 9;             // if symlink, target encoded java UTF8

  // Optional fields for file
  optional uint32 block_replication = 10 [default = 0]; // only 16bits used
  optional uint64 blocksize = 11 [default = 0];
  optional LocatedBlocksProto locations = 12;  // suppled only if asked by client

  // Optional field for fileId
  optional uint64 fileId = 13 [default = 0]; // default as an invalid id
  optional int32 childrenNum = 14 [default = -1];
  // Optional field for file encryption
  optional FileEncryptionInfoProto fileEncryptionInfo = 15;

  optional uint32 storagePolicy = 16 [default = 0]; // block storage policy id
}

*/



int FileSystemImpl::stat(const std::string &path, struct stat *buf){
  using ::hadoop::hdfs::GetFileInfoRequestProto;
  using ::hadoop::hdfs::GetFileInfoResponseProto;
  using ::hadoop::hdfs::FsPermissionProto;

  GetFileInfoRequestProto req;
  auto resp = std::make_shared<GetFileInfoResponseProto>();

  Status s = namenode_.GetFileInfo(&req, resp);
  if (!s.ok()) {
    //todo: set errno
    return -1;
  }

  memset(buf, 0, sizeof(struct stat));

  if(resp->has_fs()) {
    //have HdfsFileStatusProto
    auto file_status = resp->fs();
    uint64_t file_length = file_status.length();
    std::string owner = file_status.owner();
    std::string group = file_status.group();

    auto perms = file_status.permission();


  }

  //const FsPermissionProto& hdfs_permissions = resp->fs_permission_proto();
  
  //buf->dev_t     st_dev;      /* ID of device containing file */
  //buf->ino_t     = 1;         /* inode number */
  
  //buf->mode_t    st_mode;     /* protection */
  //buf->nlink_t   = 1;         /* number of hard links */

  //std::string owner = resp->owner();
  //buf->uid_t     st_uid;         /* user ID of owner */

  //std::string group = resp->group();
  //buf->gid_t     st_gid;         /* group ID of owner */

  //buf->dev_t     = 0;        /* device ID (if special file) */
  //buf->off_t     = resp->length()  //st_size;        /* total size, in bytes */
  //buf->blksize_t = 1;                  //st_blksize;     /* blocksize for filesystem I/O */
  //buf->blkcnt_t  = 1;                  //st_blocks;      /* number of 512B blocks allocated */
  return 1;
}



}
