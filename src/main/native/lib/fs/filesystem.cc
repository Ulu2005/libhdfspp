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
#include <pwd.h>
#include <grp.h>

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

  if (buf == nullptr) {
    //TODO: set errno
    return -1;
  }

  GetFileInfoRequestProto req;
  auto resp = std::make_shared<GetFileInfoResponseProto>();
  req.set_src(path);

  Status stat = namenode_.GetFileInfo(&req, resp);
  if (!stat.ok() || !resp->has_fs()) {
    //TODO: set errno
    return -1;
  }

  memset(buf, 0, sizeof(struct stat));
  auto file_status = resp->fs();

  //dev_t     st_dev;       /* ID of device containing file */
  //ino_t     st_ino;       /* inode number */
  
  buf->st_mode = file_status.permission().perm();

  switch (file_status.filetype()) {
    case ::hadoop::hdfs::HdfsFileStatusProto_FileType_IS_DIR:
      buf->st_mode |= S_IFDIR;
      break;
    case ::hadoop::hdfs::HdfsFileStatusProto_FileType_IS_FILE:
      buf->st_mode |= S_IFREG;
      break;
    case ::hadoop::hdfs::HdfsFileStatusProto_FileType_IS_SYMLINK:
      buf->st_mode |= S_IFLNK;
      break;
    default:
      break; 
  }

  buf->st_nlink = 1;         /* number of hard links */
 
  //HDFS records owner's name only. But that name comes from specified
  //user when file is created. So set the uid from Linux if name literal
  //matches, set to 0 otherwise.
  struct passwd *linuxUserInfo = ::getpwnam(file_status.owner().c_str());
  if (linuxUserInfo) {
    buf->st_uid = linuxUserInfo->pw_uid;
  }

  //Similar to the uid
  struct group *group = ::getgrgid(::getgid());
  if (group) {
    if (std::string(group->gr_name) == file_status.group()) {
      buf->st_gid = group->gr_gid; 
    }
  } 
  
  //dev_t     st_rdev;    /* device ID (if special file) */
  buf->st_size = file_status.length();
  buf->st_blksize = file_status.blocksize();
  //blkcnt_t  st_blocks;  /* number of 512B blocks allocated */
  
  //HDFS records time in milliseconds
  buf->st_atime = (time_t)(file_status.access_time() / 1000);
  buf->st_mtime = (time_t)(file_status.modification_time() / 1000);
  //time_t    st_ctime;   /* time of last status change */

  return 0;
}


}
