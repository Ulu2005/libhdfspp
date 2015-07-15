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

#include <string>
#include <map>
#include <vector>

namespace hdfs
{

class CmlParser
{
 public:
  CmlParser (const int argc, const char *argv[])
    : flags_()
    , opts_()
    , args_()
  {
    prog_ = argv[0];
    prog_ = prog_.substr(prog_.find_last_of("/") + 1);

    for (int i = 1; i < argc; ++i) {
      std::string arg(argv[i]);
      auto begin = arg.find("--");

      if (begin == 0) {//start with "--"
        auto isOpt = arg.find("=");

        if (isOpt == std::string::npos) {//is flag
          arg = arg.substr(begin + 2);
          flags_[arg] = true;
          return;
        }

        //is option
        std::string option = arg.substr(begin + 2, (isOpt - begin - 2));
        std::string value = arg.substr(isOpt + 1);
        if (value.at(0) == '/') {
          value.erase(value.begin());//remove heading '/'
        }

        if (value.at(value.length() - 1) == '/') {
          value.erase(value.length() - 1);//remove trailing '/'
        }

        opts_[option] = value;
      } else if ((begin = arg.find("-")) == 0) {//start with "-"
        arg = arg.substr(begin + 1);
        flags_[arg] = true;
      } else {
        args_.push_back(arg);  
      }
    } 
  }

  virtual ~CmlParser (){}

  std::string getProgName()
  {
    return prog_;
  }

  bool getFlag(std::string flag)
  {
    if (flags_.find(flag) == flags_.end()) {
      return false;
    }

    return true;
  }

  bool getOption(std::string option, std::string& value)
  {
    if (opts_.find(option) == opts_.end()) {
      return false;
    }

    value = opts_.find(option)->second;
    return true;
  }

  std::string getArg(unsigned index)
  {
    return args_.at(index); 
  }

  unsigned getArgSize()
  {
    return args_.size();
  }

 private:
  std::string prog_;
  std::map<std::string, bool> flags_;
  std::map<std::string, std::string> opts_;
  std::vector<std::string> args_;
}; 

} /* hdfs */ 

