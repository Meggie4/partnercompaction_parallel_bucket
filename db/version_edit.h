// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"
//////////meggie
#include "util/murmurhash.h"
#include <memory>
#include "util/hyperloglog.h"
#include "db/partner_meta.h"
//////////meggie

namespace leveldb {

class VersionSet;

///////////meggie
class PartnerMeta;
struct Partner {
    uint64_t partner_number;
	  uint64_t partner_size;
    InternalKey partner_smallest;
    InternalKey partner_largest;
    uint64_t meta_number;
    uint64_t meta_size;
    uint64_t meta_usage;
    std::shared_ptr<PartnerMeta> pm;
	  // std::shared_ptr<HyperLogLog> hll;
    // int hll_add_count;
    // Partner() : hll(std::make_shared<HyperLogLog>(12)), 
    //             hll_add_count(0) {}
};
///////////meggie

struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  //////////////meggie 
  //partner compaction
  InternalKey origin_smallest;
  InternalKey origin_largest;
  std::vector<Partner> partners;
  
  //level0 bucket
  char prefix;
  uint64_t meta_number;
  uint64_t meta_size;
  uint64_t meta_usage;
  std::shared_ptr<PartnerMeta> pm;
  //////////////meggie
  FileMetaData() : 
    refs(0), 
    allowed_seeks(1 << 30), 
    file_size(0),
    /////////meggie
    prefix(0)
    /////////meggie
  {}
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
	  /////////////meggie
	  f.origin_smallest = smallest;
    f.origin_largest = largest;
	  /////////////meggie
    new_files_.push_back(std::make_pair(level, f));
  }
  
  ///////////////meggie
  // void AddFile(int level, uint64_t file,
  //              uint64_t file_size,
  //              const InternalKey& smallest,
  //              const InternalKey& largest
	// 		         //const std::shared_ptr<HyperLogLog>& hll, 
  //              //int hll_add_count
  //              ) {
	//   FileMetaData f;
	//   f.number = file;
	//   f.file_size = file_size;
	//   f.smallest = smallest;
	//   f.largest = largest;
	//   f.origin_smallest = smallest;
	//   f.origin_largest = largest;
	//   // f.hll = hll;
  //   // f.hll_add_count = hll_add_count;
	//   new_files_.push_back(std::make_pair(level, f));
  // }
  
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest,
               const InternalKey& origin_smallest,
               const InternalKey& origin_largest, 
               std::vector<Partner>& partners) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
	  f.origin_smallest = origin_smallest;
    f.origin_largest = origin_largest;
    //f.partners.assign(partners.begin(), partners.end());
    f.partners.push_back(partners[0]);
    new_files_.push_back(std::make_pair(level, f));
  }
  
  // void AddFile(int level, uint64_t file,
  //              uint64_t file_size,
  //              const InternalKey& smallest,
  //              const InternalKey& largest,
  //              const InternalKey& origin_smallest,
  //              const InternalKey& origin_largest, 
  //              std::vector<Partner>& partners
	// 		         //const std::shared_ptr<HyperLogLog>& hll, 
  //              //int hll_add_count
  //              ) {
	//   FileMetaData f;
	//   f.number = file;
	//   f.file_size = file_size;
	//   f.smallest = smallest;
	//   f.largest = largest;
	//   f.origin_smallest = origin_smallest;
	//   f.origin_largest = origin_largest;
	//   f.partners.assign(partners.begin(), partners.end());
	//   // f.hll = hll;
  //   // f.hll_add_count = hll_add_count;
	//   new_files_.push_back(std::make_pair(level, f));
  // }
  ///////////////meggie

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  ///////////meggie
  void AddPartner(int level, uint64_t file, Partner& partner) {
    updated_files_.push_back(std::make_pair(level, std::make_pair(file, partner))); 
  }
  void AddFile(int level, uint64_t number, uint64_t file_size, const InternalKey& smallest, 
                  const InternalKey& largest, uint64_t meta_number, uint64_t meta_size, 
                  uint64_t meta_usage, std::shared_ptr<PartnerMeta> pm, char prefix) {
    DEBUG_T("add level 0 file, prefix:%c, file_number:%llu, file_size:%llu, meta_usage:%llu\n", 
                                prefix, number, file_size, meta_usage);
    FileMetaData f;
    f.number = number;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
	  f.meta_number = meta_number;
    f.meta_size = meta_size;
    f.meta_usage = meta_usage;
    f.pm = pm;
    f.prefix = prefix;
    new_files_.push_back(std::make_pair(level, f));
  }
  void UpdateFile(int level, uint64_t number, uint64_t file_size, 
                  const InternalKey& smallest, const InternalKey& largest, uint64_t meta_usage) {
    DEBUG_T("update level 0 file, file_number:%llu\n", number);
    FileMetaData f;
    f.number = number;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    f.meta_usage = meta_usage;
    update_level0_files_.push_back(f);               
  }
  ///////////meggie

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;
  ///////////////meggie
  typedef std::vector< std::pair<int, std::pair< uint64_t, Partner>> > UpdatedFileSet;
  ///////////////meggie
  
  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector< std::pair<int, FileMetaData> > new_files_;
  /////////meggie
  UpdatedFileSet updated_files_;
  std::vector<FileMetaData> update_level0_files_;
  /////////meggie
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
