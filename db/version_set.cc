// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

#include "util/debug.h"
#include "db/partner_meta.h"

////////////meggie
#include "util/timer.h"
#include "util/debug.h"
#ifdef TIMER_LOG
	#define vvstart_timer(s) vset_->timer->StartTimer(s)
	#define vvrecord_timer(s) vset_->timer->Record(s)
#else
	#define vvstart_timer(s)
	#define vvrecord_timer(s)
#endif

int searching_partner = 0;
int found_searching_partner = 0;
int not_found_searching_partner = 0;
int searching_not_found_partner = 0;
int predict_not_in_partner = 0;
int searching_found_partner = 0;
int searching_found_sstable = 0;
////////////meggie
namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        DEBUG_T("in ~version, to delete filemetadata:%p, file_number:%llu\n", f, f->number);
        delete f;
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        flist_(flist),
        index_(flist->size()) {        // Marks as invalid
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value, 
                                 void* handle = nullptr) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

////////////////////////meggie
int FindFileWithPartner(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key, int start_index, int end_index) {
  uint32_t left = start_index;
  uint32_t right = end_index + 1;
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

class Version::LevelFileNumIteratorWithPartner : public Iterator {
public:
	LevelFileNumIteratorWithPartner(const InternalKeyComparator& icmp,
						 const std::vector<FileMetaData*>* flist, int start_index, int end_index)
		: icmp_(icmp),
		  flist_(flist),
      index_(end_index),
		  start_index_(start_index),
      end_index_(end_index){        // Marks as invalid
	}
	virtual bool Valid() const {
		return (index_ >= start_index_ &&  
                index_ <= end_index_);
	}
	virtual void Seek(const Slice& target) {
		index_ = FindFileWithPartner(icmp_, *flist_, 
                target, start_index_, end_index_);
        //DEBUG_T("LevelFileNumIteratorWithPartner, seek, index:%d\n",
          //      index_);
	}
	virtual void SeekToFirst() { index_ = start_index_; }
	virtual void SeekToLast() {
		index_ = end_index_;
	}
	virtual void Next() {
		assert(Valid());
		index_++;
	}
	virtual void Prev() {
		assert(Valid());
		if (index_ == start_index_) {
			index_ = end_index_ + 1;  // Marks as invalid
		} else {
			index_--;
		}
	}
	Slice key() const {
		assert(Valid());
		return (*flist_)[index_]->largest.Encode();
	}
	Slice value() const {
		assert(Valid());
		/////////////meggie
		uintptr_t flist_ptr = reinterpret_cast<uintptr_t>((void*)flist_); 
    //DEBUG_T("flist_ptr:%llu\n", flist_ptr);
    EncodeFixed64(value_buf_, flist_ptr);
		EncodeFixed32(value_buf_+ 8, index_);
		//uintptr_t flist_arg = DecodeFixed64(value_buf_);
		//DEBUG_T("flist_arg:%llu\n", flist_arg);
		/////////////meggie
		return Slice(value_buf_, sizeof(value_buf_));
	}
	virtual Status status() const { return Status::OK(); }
private:
	const InternalKeyComparator icmp_;
	const std::vector<FileMetaData*>* const flist_;
  /////////meggie
	uint32_t index_;
  uint32_t start_index_;
  uint32_t end_index_;
  /////////meggie

	// Backing store for value().  Holds the file number and size.
	/////////////meggie
	mutable char value_buf_[12];
	/////////////meggie
};

static InternalKeyComparator global_icmp(BytewiseComparator());

static Iterator* GetFileIteratorWithPartner(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value, 
                                 void* handle = nullptr) {
	TableCache* cache = reinterpret_cast<TableCache*>(arg);
	//delete args;
    if (file_value.size() != 12) {
		return NewErrorIterator(
				   Status::Corruption("FileReader invoked with unexpected value"));
	} else {
		uintptr_t flist_arg = DecodeFixed64(file_value.data());
		const std::vector<FileMetaData*>* flist = (const std::vector<FileMetaData*>*)(reinterpret_cast<void*>(flist_arg));
		uint32_t index = DecodeFixed32(file_value.data() + 8);
    //DEBUG_T("GetFileIteratorWithPartner, index:%d, flist_arg:%llu\n", 
    //      index, flist_arg);
		FileMetaData* file = (*flist)[index];
    //DEBUG_T("file->number:%lld\n", file->number);
		int sz = file->partners.size() + 1;
		Iterator** list = new Iterator*[sz];
    //level0 bucket
    if(file->prefix != 0){
      DEBUG_T("level0 file get partner iterator\n");
      list[0] = cache->NewPartnerIterator(options, file->number, file->pm.get());
    }
    else 
		  list[0] = cache->NewIterator(options, file->number, file->file_size);
		for(int i = 0; i < sz - 1; i++) {
			// list[i + 1] = cache->NewIterator(options,
			// 					  file->partners[i].partner_number,
			// 					  file->partners[i].partner_size);
      DEBUG_T("in GetFileIterator, partner number is:%llu\n", file->partners[i].partner_number);
      list[i + 1] = cache->NewPartnerIterator(options,
                file->partners[i].partner_number,
                file->partners[i].pm.get()
                // file->partners[i].meta_number,
                // file->partners[i].meta_size
                );
		}
		return NewMergingIterator(&global_icmp, list, sz);
	}
}

Iterator* VersionSet::NewIteratorWithPartner(TableCache* cache, 
        const FileMetaData* file) {
    //DEBUG_T("NewIteratorWithPartner, partners size is %d\n", file->partners.size());
    int sz = file->partners.size() + 1;
    Iterator** list = new Iterator*[sz];
    list[0] = cache->NewIterator(ReadOptions(), file->number, file->file_size);
    for(int i = 0; i < sz - 1; i++) {
        // list[i + 1] = cache->NewIterator(ReadOptions(),
        //                       file->partners[i].partner_number,
        //                       file->partners[i].partner_size);
        DEBUG_T("NewIteratorWithPartner, partner number is:%llu\n", file->partners[i].partner_number);
        // if(file->partners[i].pm != nullptr) {
        //   DEBUG_T("partners is not nullptr:%p\n", file->partners[i].pm);
        // } else {
        //   DEBUG_T("partners is nullptr\n");
        // }
        list[i + 1] = cache->NewPartnerIterator(ReadOptions(),
                              file->partners[i].partner_number,
                              file->partners[i].pm.get()
                              // file->partners[i].meta_number,
                              // file->partners[i].meta_size
                              );
    }
    return NewMergingIterator(&icmp_, list, sz);
}
////////////////meggie

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]),
      &GetFileIterator, vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  // TODO(sanjay): Change Version::Get() to use this function.
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;

  stats->seek_file = nullptr;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = nullptr;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  std::vector<FileMetaData*> tmp;
  FileMetaData* tmp2;
  //逐层搜索
  for (int level = 0; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Get the list of files to search in this level
    FileMetaData* const* files = &files_[level][0];
    //针对level 0
    if (level == 0) {
      // Level-0 files may overlap each other.  Find all files that
      // overlap user_key and process them in order from newest to oldest.
      tmp.reserve(num_files);
      //获取满足条件的重叠的SSTable
      for (uint32_t i = 0; i < num_files; i++) {
        FileMetaData* f = files[i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
          tmp.push_back(f);
        }
      }
      if (tmp.empty()) continue;
      
      //根据新旧值进行排列
      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      //从最新的开始搜索，对于含有partner的数据，那肯定是从partner先开始搜索
      files = &tmp[0];
      num_files = tmp.size();
    } else {
      //针对其他level 
      // Binary search to find earliest index whose largest key >= ikey.
      // 根据指定的key, 获取满足条件的SSTable:小于当前SSTable的largest, 大于左边的SSTable的largest
      uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
      if (index >= num_files) {
        files = nullptr;
        num_files = 0;
      } else {
        ////获取成功
        tmp2 = files[index];
        //判断是否大于当前的SSTable的smallest, 如果小于，那就无效
        if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
          // All of "tmp2" is past any data for user_key
          files = nullptr;
          num_files = 0;
        } else {
          files = &tmp2;
          num_files = 1;
        }
      }
    }
    
    //找到了可能含有相应key的SSTable集合
    for (uint32_t i = 0; i < num_files; ++i) {
      if (last_file_read != nullptr && stats->seek_file == nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        stats->seek_file = last_file_read;
        stats->seek_file_level = last_file_read_level;
      }

      FileMetaData* f = files[i];
      last_file_read = f;
      last_file_read_level = level;

      Saver saver;
      saver.state = kNotFound;
      saver.ucmp = ucmp;
      saver.user_key = user_key;
      saver.value = value;
     
      ///////////meggie
      //std::set<std::string> mykeys = {
      //    "user2731456362417878207",
      //};
      //if(mykeys.find(user_key.ToString()) != mykeys.end()){
      //    DEBUG_T("search in level%d, number:%d, address:%p, smallest:%s, largest:%s, partners.size:%d\n", 
      //            level, f->number, 
      //            f,
      //            f->smallest.user_key().ToString().c_str(),
      //            f->largest.user_key().ToString().c_str(),
      //            f->partners.size());
      //}
      if(f->partners.size() != 0){
          bool search_partner = false;
          for(int j = f->partners.size() - 1; j >= 0; j--) {
              Partner ptner = f->partners[j];
              if((ucmp->Compare(user_key, ptner.partner_smallest.user_key()) >= 0) 
                      && (ucmp->Compare(user_key, ptner.partner_largest.user_key()) <= 0)){  
                  search_partner = true;
                  //首先看partner中是否存在要找的key
                  uint64_t block_offset, block_size;
                  searching_partner++;
                  // char *keystr = (char*)user_key.data();
                  // keystr[user_key.size()]=0;
                  // bool predict = f->partners[0].pm->CheckPredictIndex(&f->partners[0].pm->predict_set_,
                  //                   (const uint8_t*)keystr);
                  // if (!predict) {
                  //     predict_not_in_partner++;
                  //     goto search_sstable;
                  // }
                  const uint64_t partner_read_micros_start = vset_->env_->NowMicros();
                  bool find = f->partners[0].pm->Get(k, &block_offset, &block_size, &s);
                  const uint64_t partnermeta_read_micros_need = vset_->env_->NowMicros() - partner_read_micros_start;
                  DEBUG_T("partner meta read need time:%llu\n", partnermeta_read_micros_need);
                  if(!find) {
                      not_found_searching_partner++;
                      goto search_sstable;
                  }
                  found_searching_partner++;
                  // DEBUG_T("find, search partner, number:%d, smallest:%s, largest:%s\n",
                  //     ptner.partner_number,
                  //     ptner.partner_smallest.user_key().ToString().c_str(),
                  //     ptner.partner_largest.user_key().ToString().c_str());
                  // DEBUG_T("table cache get, sstable numner:%llu, partner number:%llu, meta number:%llu, block offset:%llu, block size:%llu\n", 
                  //     f->number, f->partners[0].partner_number, f->partners[0].meta_number, block_offset, block_size);
                  s = vset_->table_cache_->Get(options, ptner.partner_number, 
                                        ikey, &saver, SaveValue, block_offset, block_size);
                  if(!s.ok()){
                      DEBUG_T("partner table_cache get failed\n");
                      return s;
                  }
                  const uint64_t partner_read_micros_need = vset_->env_->NowMicros() - partner_read_micros_start;
                  DEBUG_T("partner read need time:%llu, partnerdata read need time:%llu\n", partner_read_micros_need, 
                                                  partner_read_micros_need - partnermeta_read_micros_need);
                  switch (saver.state) {
                    case kNotFound:
                        DEBUG_T("NotFound in partner\n");
                        searching_not_found_partner++;
                        break;// Keep searching in origin sstable
                    case kFound:
                        searching_found_partner++;
                      return s;
                    case kDeleted:
                      s = Status::NotFound(Slice());  // Use empty error message for speed
                      return s;
                    case kCorrupt:
                      s = Status::Corruption("corrupted key for ", user_key);
                      return s;
                  }
              }
        }
      }
search_sstable:
      const uint64_t sstable_read_micros_start = vset_->env_->NowMicros();
      //////meggie
      //level0 bucket 
      if(level == 0) {
        uint64_t block_offset, block_size;
        bool find = f->pm->Get(k, &block_offset, &block_size, &s);
        if(!find) {
            break;
        }
        s = vset_->table_cache_->Get(options, f->number, 
                    ikey, &saver, SaveValue, block_offset, block_size);
      } else 
      //////meggie
        s = vset_->table_cache_->Get(options, f->number, f->file_size,
                                    ikey, &saver, SaveValue);
      const uint64_t sstable_read_micros_need = vset_->env_->NowMicros() - sstable_read_micros_start;
      //DEBUG_T("sstable read need time:%llu\n", sstable_read_micros_need);
      ///////////meggie

      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          break;      // Keep searching in other files
        case kFound:
          searching_found_sstable++;
          return s;
        case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
    }
  }

  return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size(); ) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr && user_cmp->Compare(file_limit,
                                                       user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
    ///////////meggie
    std::map<uint64_t, Partner> updated_files;
    std::map<uint64_t, FileMetaData*> update_level0_files;
    ///////////meggie
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          DEBUG_T("in ~builder,to delete filemetadata:%p, number:%llu\n", f, f->number);
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }

    /////////////meggie
	  const VersionEdit::UpdatedFileSet upd = edit->updated_files_;
    for (VersionEdit::UpdatedFileSet::const_iterator iter = upd.begin(); iter != upd.end(); ++iter) {
      const int level = iter->first;
      DEBUG_T("update number:%d\n", iter->second.first);
      levels_[level].updated_files.insert(std::make_pair(iter->second.first, iter->second.second));
    }

    //level0 bucket
    for (size_t i = 0; i < edit->update_level0_files_.size(); i++) {
      FileMetaData* f = new FileMetaData(edit->update_level0_files_[i]);
      levels_[0].update_level0_files.insert(std::make_pair(edit->update_level0_files_[i].number, f));
      f->refs = 1;
    }
    /////////////meggie
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }


#ifndef NDEBUG
    // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(),
                    this_begin.DebugString().c_str());
            abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
	  //////////meggie
    } else if(levels_[level].updated_files.find(f->number) != 
			                      levels_[level].updated_files.end()) {
        std::vector<FileMetaData*>* files = &v->files_[level];
        FileMetaData* fm = new FileMetaData(*f);
        fm->refs = 1;
        Partner& ptner = levels_[level].updated_files[f->number];
        //fm->partners.push_back(ptner);
        if(fm->partners.empty()) {
          fm->partners.push_back(ptner);
        } else{
          fm->partners[0] = ptner;	   
        }
	   
        //更新FileMetadata的最小值和最大值
        DEBUG_T("add partner, origin SSTable:%d, smallest:%s, largest:%s\n", 
                fm->number, 
                fm->smallest.user_key().ToString().c_str(), 
                fm->largest.user_key().ToString().c_str());
        DEBUG_T("add partner, partner number:%d, partner_smallest:%s, partner_largest:%s\n",
                ptner.partner_number,
                ptner.partner_smallest.user_key().ToString().c_str(),
                ptner.partner_largest.user_key().ToString().c_str());
        DEBUG_T("now partners number is %d\n", fm->partners.size());
        if(vset_->icmp_.Compare(ptner.partner_largest, fm->largest) > 0)
            fm->largest = ptner.partner_largest;
        if(vset_->icmp_.Compare(ptner.partner_smallest, fm->smallest) < 0)
            fm->smallest = ptner.partner_smallest;
        DEBUG_T("UpdateFile, smallest:%s, largest:%s\n",
                fm->smallest.user_key().ToString().c_str(),
                fm->largest.user_key().ToString().c_str());

        //fm->allowed_seeks = (fm->file_size / 16384);
        fm->allowed_seeks = ((fm->file_size + fm->partners[0].partner_size) / 16384);
        if (fm->allowed_seeks < 100) fm->allowed_seeks = 100;
        
        if (level > 0 && !files->empty()) {
          // Must not overlap
          assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                        fm->smallest) < 0);
        }
        files->push_back(fm);
    } else if(level == 0 && levels_[level].update_level0_files.find(f->number) != 
			                      levels_[level].update_level0_files.end()) { 
        std::vector<FileMetaData*>* files = &v->files_[level];
        FileMetaData* update_f = levels_[level].update_level0_files[f->number];
        update_f->meta_number = f->meta_number;
        update_f->meta_size = f->meta_size;
        update_f->pm = f->pm;
        update_f->prefix = f->prefix;
        update_f->allowed_seeks = (update_f->file_size / 16384);
        if (update_f->allowed_seeks < 100) 
          update_f->allowed_seeks = 100;
        files->push_back(update_f);
    //////////meggie
	  } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        // if(level == 1)
        //   fprintf(stderr, "MaybeAddFile, level:%d, number:%llu, smallest:%s, largest:%s\n",
        //       level,
        //       f->number,
        //       f->smallest.user_key().ToString().c_str(),
        //       f->largest.user_key().ToString().c_str());
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest, f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp, 
                       ////////////meggie
                       Timer* timer
                       ////////////meggie
                       )
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      ///////////meggie
      timer(timer),
      ///////////meggie
      current_(nullptr) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    DEBUG_T("after apply\n");
    builder.SaveTo(v);
    DEBUG_T("after save to\n");
  }
  Finalize(v);
  DEBUG_T("after finalize v\n");
  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == nullptr);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    DEBUG_T("to delete version\n");
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->DeleteFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover(bool *save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption(
            "CURRENT points to a non-existent file", s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  /////////meggie
  //level0 bucket
  //获取level0中nvm索引所占空间最大的文件
  uint64_t max_nvm_bytes;
  FileMetaData* max_nvm_bytes_fm = nullptr;
  for(size_t i = 0; i < v->files_[0].size(); i++) {
    if(i == 0) {
      max_nvm_bytes = v->files_[0][i]->meta_usage;
      max_nvm_bytes_fm = v->files_[0][i];
    } else if (v->files_[0][i]->meta_usage > max_nvm_bytes) {
      max_nvm_bytes = v->files_[0][i]->meta_usage;
      max_nvm_bytes_fm = v->files_[0][i];
    }
  }
  v->level0_max_nvm_file_ = max_nvm_bytes_fm;
  /////////meggie

  for (int level = 0; level < config::kNumLevels-1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      //////////meggie
      // score = v->files_[level].size() /
      //     static_cast<double>(config::kL0_CompactionTrigger);
      //需要重新计算score，按照什么方式呢？按照元文件的大小
      //总共最多有9个文件，现在不能按照文件数量来确定是否触发compaction
      //如果有一个文件对应的NVM索引大小超过了阈值40MB？还是20MB？
      if(v->level0_max_nvm_file_ != nullptr)
        score = v->level0_max_nvm_file_->meta_usage / 
            static_cast<double>(config::kL0_CompactionTrigger);
      else 
        score = 0;
      DEBUG_T("level %d score is %f\n", level, score);
      /////////meggie
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
      DEBUG_T("level %d score is %f\n", level, score);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer),
           "files[ %d %d %d %d %d %d %d ]",
           int(current_->files_[0].size()),
           int(current_->files_[1].size()),
           int(current_->files_[2].size()),
           int(current_->files_[3].size()),
           int(current_->files_[4].size()),
           int(current_->files_[5].size()),
           int(current_->files_[6].size()));
  return scratch->buffer;
}

////////////////////meggie
////level0 bucket 
void VersionSet::GetLevel0FileMeta(std::map<char, FileMetaData*>& fmmp) {
  const std::vector<FileMetaData*>& files = current_->files_[0];
  for(size_t i = 0; i < files.size(); i++) {
    fmmp.insert(std::make_pair(files[i]->prefix, files[i]));
  }
}

uint64_t VersionSet::BytesLevel0NVM() {
  if(current_->level0_max_nvm_file_ != nullptr) {
    return current_->level0_max_nvm_file_->meta_usage;
  } else {
    return 0;
  }
}
////针对的是victim inputs没有partner的情况, 获取traditional compaction 
void VersionSet::AddInputDeletions(VersionEdit* edit, Compaction* c, std::vector<int> tcompaction_index) {
    const std::vector<FileMetaData*>& files0 = c->inputs_[0];
    const std::vector<FileMetaData*>& files1 = c->inputs_[1];
    int level = c->level();
    DEBUG_T("after partner compaction, delete file:\n");
    for(int i = 0; i < files0.size(); i++) {
        DEBUG_T("%d, ", files0[i]->number);
        edit->DeleteFile(level, files0[i]->number);
    } 

    for(int i = 0; i < tcompaction_index.size(); i++) {
        int number  = files1[tcompaction_index[i]]->number;
        DEBUG_T("%d, ", number);
        edit->DeleteFile(level + 1, number);
    } 
    DEBUG_T("\n");
}

Iterator* VersionSet::GetIteratorWithPartner(const std::vector<FileMetaData*>& files, int start_index, int end_index) {
	  ReadOptions options;
    options.verify_checksums = options_->paranoid_checks;
    options.fill_cache = false;
    //options.comparator = &icmp_;
   
    return NewTwoLevelIterator(new Version::LevelFileNumIteratorWithPartner(icmp_, &files, 
							start_index, end_index), &GetFileIteratorWithPartner, table_cache_, options);
}

Iterator* VersionSet::GetSingleCompactionIterator(Compaction* c) {
      const std::vector<FileMetaData*>& files0 = c->inputs_[0];
      int sz = files0.size();
      return GetIteratorWithPartner(files0, 0, sz - 1);
}

void VersionSet::TestIterator(Iterator* iter, bool range, InternalKey start, InternalKey end, bool containsend) {
//void VersionSet::TestIterator(Iterator* iter){ 
    DEBUG_T("test iterator\n");
    ParsedInternalKey ikey;
    if(!range) {
        DEBUG_T("before TestIterator seek to first\n");
        if(iter == nullptr)
          DEBUG_T("iter is nullptr\n");
        iter->SeekToFirst();
        DEBUG_T("after TestIterator seek to first\n");
        for(; iter->Valid(); iter->Next()) {
            DEBUG_T("before get key\n");
            Slice key = iter->key();
            DEBUG_T("to get value\n");
            Slice value = iter->value();
            DEBUG_T("after get value\n");
            ParseInternalKey(key, &ikey);
            Slice user_key = ikey.user_key;
            DEBUG_T("user_key:%s\n", user_key.ToString().c_str());
        }
        DEBUG_T("\n");
    } else {
        iter->Seek(start.Encode());
        DEBUG_T("before get key\n");
        for(; iter->Valid(); iter->Next()) {
            DEBUG_T("before get key\n");
            Slice key = iter->key();
            DEBUG_T("to get value\n");
            Slice value = iter->value();
            DEBUG_T("after get value\n");
            ParseInternalKey(key, &ikey);
            InternalKey mykey;
            mykey.SetFrom(ikey);
            if((containsend &&  icmp_.Compare(mykey, end) > 0) || (!containsend && icmp_.Compare(mykey, end) >= 0))
                break;
            Slice user_key = ikey.user_key;
            DEBUG_T("user_key:%s\n", user_key.ToString().c_str());
        }
        DEBUG_T("\n");
    }
}


void VersionSet::MergeTSplitCompaction(Compaction* c, 
					std::vector<SplitCompaction*>& t_sptcompactions,
						std::vector<TSplitCompaction*>& result) {
	
    const std::vector<FileMetaData*>& files0 = c->inputs_[0];
    const std::vector<FileMetaData*>& files1 = c->inputs_[1];
	  int sz = t_sptcompactions.size();
    bool containsend;

    if(sz == 1) {
        TSplitCompaction* t_sptcompaction = new TSplitCompaction;
		    int inputs1_index = t_sptcompactions[0]->inputs1_index;
        t_sptcompaction->victim_iter = t_sptcompactions[0]->victim_iter;
		    t_sptcompactions[0]->victim_iter = nullptr;
        t_sptcompaction->victim_start = t_sptcompactions[0]->victim_start;
        t_sptcompaction->victim_end = t_sptcompactions[0]->victim_end;
        t_sptcompaction->containsend = t_sptcompactions[0]->containsend;
        t_sptcompaction->inputs1_iter = NewIteratorWithPartner(
											table_cache_, files1[inputs1_index]);

        // if(c->inputs_[1][0]->number == 5){
        //   DEBUG_T("test single inputs1_iter, number is 5\n");
        //   TestIterator(t_sptcompaction->inputs1_iter, false, InternalKey(), InternalKey(), false);
        // }
        result.push_back(t_sptcompaction);
        return;
    }
    
    std::vector<SplitCompaction*> tmpt_sptcompactions;
    tmpt_sptcompactions.push_back(t_sptcompactions[0]);
    SplitCompaction* pre = t_sptcompactions[0];
    int count = 1;
	
    TSplitCompaction* t_sptcompaction = new TSplitCompaction;
	  //DEBUG_T("traditional compaction:\n");
	
    for(int i = 1; i <= sz; i++) {
        if(((i == sz) || t_sptcompactions[i]->inputs1_index - pre->inputs1_index > 1)) {
          Iterator* victim_iter, *inputs1_iter;
          t_sptcompaction->victim_start = tmpt_sptcompactions[0]->victim_start;
          t_sptcompaction->victim_end = tmpt_sptcompactions[count - 1]->victim_end;
          t_sptcompaction->containsend = tmpt_sptcompactions[count - 1]->containsend;
          
          int victimstart_index = tmpt_sptcompactions[0]->victims[0];
          int victimsz = tmpt_sptcompactions[count - 1]->victims.size();
          int victimend_index = tmpt_sptcompactions[count - 1]->victims[victimsz - 1];
          
          int inputs1start_index = tmpt_sptcompactions[0]->inputs1_index;
          int inputs1end_index = tmpt_sptcompactions[count - 1]->inputs1_index;		
          
          DEBUG_T("inputs1 index from %d~%d\n", inputs1start_index, inputs1end_index);
          inputs1_iter = GetIteratorWithPartner(files1, inputs1start_index, inputs1end_index);
                
          //DEBUG_T("in MergeTSplitCompaction, test inputs1_iter\n");
          //TestIterator(inputs1_iter, false, InternalKey(), 
          //        InternalKey(), false);
        
          DEBUG_T("victims index from %d~%d\n", victimstart_index, victimend_index);
          victim_iter = GetIteratorWithPartner(files0, victimstart_index, victimend_index);
                
          DEBUG_T("in MergeTSplitCompaction, test victim_iter\n");
            //TestIterator(victim_iter, true, 
            //       t_sptcompaction->victim_start, 
            //       t_sptcompaction->victim_end, 
            //       t_sptcompaction->containsend);
		      //TestIterator(victim_iter);

            /*
            Iterator* list[2];
            list[0] = victim_iter;
            list[1] = inputs1_iter;
            Iterator* merge_iter = NewMergingIterator(&icmp_,
                    list, 2);
            merge_iter->SetChildRange(0, 
                    t_sptcompaction->victim_start.Encode(),
                    t_sptcompaction->victim_end.Encode(), 
                    t_sptcompaction->containsend);

            DEBUG_T("print merge iter:\n");
            merge_iter->SeekToFirst();
            for(; merge_iter->Valid(); merge_iter->Next()) {
                Slice key = merge_iter->key();
                ParsedInternalKey ikey;
                ParseInternalKey(key, &ikey);
                Slice user_key = ikey.user_key;
                DEBUG_T("user_key:%s\n", user_key.ToString().c_str());
            }
            DEBUG_T("print merge iter end\n");
            */
            //delete merge_iter;
            //DEBUG_T("after delete merge_iter\n");

          t_sptcompaction->victim_iter = victim_iter;
          t_sptcompaction->inputs1_iter = inputs1_iter;
          result.push_back(t_sptcompaction);

          if(i < sz) {
                    tmpt_sptcompactions.clear();
                    t_sptcompaction = new TSplitCompaction;
            tmpt_sptcompactions.push_back(t_sptcompactions[i]);
            pre = t_sptcompactions[i];
              count = 1;
          }
        } else {
          tmpt_sptcompactions.push_back(t_sptcompactions[i]);
          pre = t_sptcompactions[i];
          count++;
        }
    }
}

void VersionSet::PrintSplitCompaction(SplitCompaction* sptcompaction) {
    DEBUG_T("victim contains:");
    for(auto iter = sptcompaction->victims.begin(); 
            iter != sptcompaction->victims.end(); iter++) {
        DEBUG_T("%d ", *iter);
    }
    DEBUG_T("\n");
    DEBUG_T("victim_start:%s, victim_end:%s, containsend:%d\n", 
            sptcompaction->victim_start.user_key().ToString().c_str(),
            sptcompaction->victim_end.user_key().ToString().c_str(),
            sptcompaction->containsend? 1: 0);
}

void VersionSet::PrintLevel01() {
  DEBUG_T("level0 like this:\n");
  for(int i = 0; i < current_->files_[0].size(); i++) {
    FileMetaData* fm = current_->files_[0][i];
    DEBUG_T("number:%llu, smallest:%s, largest:%s\n", fm->number, 
            fm->smallest.user_key().ToString().c_str(),
            fm->largest.user_key().ToString().c_str());
  }
  DEBUG_T("level1 like this:\n");
  for(int i = 0; i < current_->files_[1].size(); i++) {
    FileMetaData* fm = current_->files_[1][i];
    DEBUG_T("number:%llu, smallest:%s, largest:%s\n", fm->number, 
            fm->smallest.user_key().ToString().c_str(),
            fm->largest.user_key().ToString().c_str());
  }
}
 
bool VersionSet::HasPartnerInVictim(Compaction* c) {
	std::vector<FileMetaData*>& inputs0 = c->inputs_[0];
	int sz0 = inputs0.size();
	for(int i = 0; i < sz0; i++) {
		if(!inputs0[i]->partners.empty())
			return true;
	}
	return false;
}

double VersionSet::GetOverlappingRatio(Compaction* c, 
								SplitCompaction* sptcompaction) {
	std::vector<FileMetaData*>& inputs0 = c->inputs_[0];
    std::vector<FileMetaData*>& inputs1 = c->inputs_[1];
    int sz0 = inputs0.size();
    int sz1 = inputs1.size();
	
	std::vector<int>& victims = sptcompaction->victims;
	int sz = victims.size();
	
	Table* tableptr = nullptr;
    Iterator* iter;
	
	int inputs1_index = sptcompaction->inputs1_index;
	
	InternalKey smallest, largest;
	smallest = inputs1[inputs1_index]->smallest;
	largest = inputs1[inputs1_index]->largest;
	
	uint64_t overlapsz;
			  
	if(sz > 1) {
		int victimstart_index = victims[0];
		int victimend_index = victims[1];
		uint64_t midoverlapsz = 0, offset_start, offset_end;
		iter = table_cache_->NewIterator(ReadOptions(), 
										 inputs0[victimstart_index]->number,
										 inputs0[victimstart_index]->file_size,
										 &tableptr); 
		//DEBUG_T("to get origin_smallest, while smallest:%s\n", smallest.user_key().ToString().c_str());
		if(icmp_.Compare(smallest, 
						 inputs0[victimstart_index]->origin_smallest) < 0)
			offset_start = 0;
		else {
			offset_start = tableptr->ApproximateOffsetOf(smallest.Encode());
		}
		delete iter;
		//DEBUG_T("after get origin_smallest:\n");
		int i = victimstart_index + 1;
		while(i < victimend_index) {
			midoverlapsz += inputs0[i]->file_size;
			i++;
		}
		
		iter = table_cache_->NewIterator(ReadOptions(), 
										 inputs0[victimend_index]->number,
										 inputs0[victimend_index]->file_size,
										 &tableptr); 
		if(icmp_.Compare(largest, 
						 inputs0[victimend_index]->origin_largest) > 0)
			offset_end = inputs0[victimend_index]->file_size;
		else {
			offset_end = tableptr->ApproximateOffsetOf(largest.Encode());
		}
		delete iter;
		
		overlapsz = inputs0[victimstart_index]->file_size - offset_start + 
					midoverlapsz + offset_end;
	} else {
		int victim_index = victims[0];
		iter = table_cache_->NewIterator(ReadOptions(), 
									inputs0[victim_index]->number,
									inputs0[victim_index]->file_size,
									&tableptr); 
		uint64_t offset_start, offset_end;
		if(icmp_.Compare(smallest, 
						 inputs0[victim_index]->origin_smallest) < 0)
			offset_start = 0;
		else {
			offset_start = tableptr->ApproximateOffsetOf(smallest.Encode());
		}
		
		if(icmp_.Compare(largest, 
						 inputs0[victim_index]->origin_largest) > 0)
			offset_end = inputs0[victim_index]->file_size;
		else {
			offset_end = tableptr->ApproximateOffsetOf(largest.Encode());
		}
		overlapsz = offset_end - offset_start;
		
		delete iter;
	}
	
	return (overlapsz  + 1.0) / inputs1[inputs1_index]->file_size;
}

// double VersionSet::GetOverlappingRatio_1(Compaction* c, 
// 								SplitCompaction* sptcompaction) {
// 	std::vector<FileMetaData*>& inputs0 = c->inputs_[0];
//     std::vector<FileMetaData*>& inputs1 = c->inputs_[1];
// 	std::vector<int>& victims = sptcompaction->victims;
//     std::vector<HyperLogLog*> v;
//     uint64_t total_keys = 0; 
//     uint64_t selected_total_keys = 0;
//     for(int i = 0; i < inputs0.size(); i++) {
//         FileMetaData* f = inputs0[i];
//         v.push_back(f->hll.get());
//         total_keys += f->hll_add_count;
//         for(auto partner : f->partners) {
//             v.push_back(partner.hll.get());
//             total_keys += partner.hll_add_count;
//         }
//     }
 
//     FileMetaData* selected_file = inputs1[sptcompaction->inputs1_index];
//     v.push_back(selected_file->hll.get());
//     total_keys += selected_file->hll_add_count;
//     selected_total_keys += selected_file->hll_add_count;
//     for(auto partner : selected_file->partners) {
//         v.push_back(partner.hll.get());
//         total_keys += partner.hll_add_count;
//         selected_total_keys += partner.hll_add_count;
//     }

//     uint64_t distinct_nums = HyperLogLog::MergedEstimate(v);
//     uint64_t overlapped_nums = total_keys - distinct_nums;
//     DEBUG_T("distinct_nums:%lld, total_keys:%lld\n", 
//             distinct_nums, selected_total_keys);

//     double ratio = (overlapped_nums * 1.0) / selected_total_keys;
//     return ratio;
// }

// double VersionSet::GetOverlappingRatio_2(Compaction* c, 
// 								SplitCompaction* sptcompaction) {
//     std::vector<FileMetaData*>& inputs1 = c->inputs_[1];
//     std::vector<HyperLogLog*> v;
//     uint64_t selected_total_keys = 0;
    
//     FileMetaData* selected_file = inputs1[sptcompaction->inputs1_index];
//     v.push_back(selected_file->hll.get());
//     selected_total_keys += selected_file->hll_add_count;
//     for(auto partner : selected_file->partners) {
//         v.push_back(partner.hll.get());
//         selected_total_keys += partner.hll_add_count;
//     }

//     uint64_t distinct_nums = HyperLogLog::MergedEstimate(v);

//     DEBUG_T("distinct_nums:%lld, total_keys:%lld\n", 
//             distinct_nums, selected_total_keys);

//     uint64_t overlapped_nums = selected_total_keys - distinct_nums;

//     double ratio = (overlapped_nums * 1.0) / selected_total_keys;
//     return ratio;
// }

// double VersionSet::GetGlobalOverlappingRatio(Compaction* c) {
// 	  std::vector<FileMetaData*>& inputs0 = c->inputs_[0];
//     std::vector<FileMetaData*>& inputs1 = c->inputs_[1];

//     uint64_t total_keys = 0; 
//     std::vector<HyperLogLog*> v;

//     for(int i = 0; i < inputs0.size(); i++) {
//         FileMetaData* f = inputs0[i];
//         v.push_back(f->hll.get());
//         total_keys += f->hll_add_count;
//         for(auto partner : f->partners) {
//             v.push_back(partner.hll.get());
//             total_keys += partner.hll_add_count;
//         }
//     }
    
//     for(int j = 0; j < inputs1.size(); j++) {
//         FileMetaData* f = inputs1[j];
//         v.push_back(f->hll.get());
//         total_keys += f->hll_add_count;
//         for(auto partner : f->partners) {
//             v.push_back(partner.hll.get());
//             total_keys += partner.hll_add_count;
//         }
//     }

//     uint64_t distinct_nums = HyperLogLog::MergedEstimate(v);
//     DEBUG_T("global distinct_nums:%lld, global total_keys:%lld\n", 
//             distinct_nums, total_keys);
//     uint64_t overlapped_nums = total_keys - distinct_nums;
//     double ratio = (overlapped_nums * 1.0) / total_keys;

//     return ratio;
// }
/*
1.inputs[0]可能含有多个不重叠的文件; 遍历inputs1， 针对每个inputs1中的sstable，都拥有一个SplitCompaction对象；对SplitCompaction赋值：
  * InternalKey victim_start， victim_end; 记录了该sstable与inputs[0]重叠的键范围
  * 以及其在inputs1中的索引inputs1_index;
  * 与inputs[0]重叠的victims，
  * 并获取重叠的victims的迭代器，包含了partner
2.根据两种情况选择进行traditional compaction:
 * 该sstable中的partner数量过多
 * 该sstable与partner的重叠率过高
*/
void VersionSet::GetSplitCompactions(Compaction* c, 
						std::vector<SplitCompaction*>& t_sptcompactions,
						std::vector<SplitCompaction*>& p_sptcompactions) {
    //assert(c->level() > 0);
    std::vector<FileMetaData*>& inputs0 = c->inputs_[0];
    std::vector<FileMetaData*>& inputs1 = c->inputs_[1];
    int sz0 = inputs0.size();
    int sz1 = inputs1.size();
	
    DEBUG_T("sz0:%d, inputs0 info:\n", sz0);
    for(int i = 0; i < sz0; i++) {
        DEBUG_T("filenumber:%lld, smallest%s, largest:%s\n",
                inputs0[i]->number, 
                inputs0[i]->smallest.user_key().ToString().c_str(),
                inputs0[i]->largest.user_key().ToString().c_str());
        if(!inputs0[i]->partners.empty()) {
          DEBUG_T("partnernumber:%lld, smallest%s, largest:%s\n",
                inputs0[i]->partners[0].partner_number,
                inputs0[i]->partners[0].partner_smallest.user_key().ToString().c_str(),
                inputs0[i]->partners[0].partner_largest.user_key().ToString().c_str());
        }
    }
    
    DEBUG_T("sz1:%d, inputs1 info:\n", sz1);
    for(int j = 0; j < sz1; j++) {
        DEBUG_T("filenumber:%lld, smallest:%s, largest:%s\n",
                inputs1[j]->number, 
                inputs1[j]->smallest.user_key().ToString().c_str(),
                inputs1[j]->largest.user_key().ToString().c_str());
        if(!inputs1[j]->partners.empty()) {
          DEBUG_T("partnernumber:%lld, smallest%s, largest:%s\n",
                inputs1[j]->partners[0].partner_number,
                inputs1[j]->partners[0].partner_smallest.user_key().ToString().c_str(),
                inputs1[j]->partners[0].partner_largest.user_key().ToString().c_str());
        }
    }


    //double global_ratio = GetGlobalOverlappingRatio(c);

    int i = 0, j = 0;
    for(; i < sz1; i++) {
        SplitCompaction* sptcompaction =  new SplitCompaction;
		    std::set<int> victims;
        //inputs1的sstable的最小值是否小于inputs0当前的sstable的最大值， 
        //inputs1的sstable的最大值是否大于inputs0下一个的sstable的最小值，如果是的，那结束的位置就是当前的sstable,
        //并且compaction的范围需要选定，
        while((j + 1) < sz0 && icmp_.Compare(inputs1[i]->smallest, 
                        inputs0[j]->largest) > 0) {
            j++;
        }
        victims.insert(j);
        sptcompaction->victim_start = (i == 0 || 
                icmp_.Compare(inputs0[j]->smallest, 
                        inputs1[i]->smallest) > 0)? inputs0[j]->smallest: 
                            inputs1[i]->smallest;
        while(j + 1 < sz0 && icmp_.Compare(inputs1[i]->largest,
            inputs0[j + 1]->smallest) >= 0) {
            j++;
        }
        victims.insert(j);
        //选小的
        sptcompaction->victim_end = ((i + 1 < sz1) && 
            icmp_.Compare(inputs0[j]->largest, 
            inputs1[i+1]->smallest) > 0)? inputs1[i+1]->smallest: 
            inputs0[j]->largest;
        if((i + 1 < sz1) && icmp_.Compare(sptcompaction->victim_end, 
           inputs1[i+1]->smallest) == 0)
            sptcompaction->containsend = false;
        else 
            sptcompaction->containsend = true;
		
		    for(auto iter = victims.begin(); iter != victims.end(); iter++) 
			      sptcompaction->victims.push_back(*iter);
        
        int victimsz = sptcompaction->victims.size();
        int victimstart_index = sptcompaction->victims[0];
        int victimend_index = sptcompaction->victims[victimsz - 1];
        DEBUG_T("in GetSplitCompactions\n");
		    sptcompaction->victim_iter = GetIteratorWithPartner(inputs0, victimstart_index, victimend_index); 
        //DEBUG_T("in GetSplitCompactions, test victim_iter\n");
        //TestIterator(sptcompaction->victim_iter, true, 
        //        sptcompaction->victim_start, 
        //        sptcompaction->victim_end, 
        //        sptcompaction->containsend);
       
        sptcompaction->inputs1_index = i;

        DEBUG_T("inputs1[%d], sptcompaction:", i);
        PrintSplitCompaction(sptcompaction);
       
        if((victimend_index - victimstart_index + 1) > 2) {
            t_sptcompactions.push_back(sptcompaction);
            DEBUG_T("victim count more than 2\n");
        } else if (inputs1[i]->partners.size() > 0 && 
                    //inputs1[i]->partners[0].meta_usage > (inputs1[i]->partners[0].meta_size >> 1)){
                    inputs1[i]->partners[0].meta_usage > (16 << 10 << 10)) {
            t_sptcompactions.push_back(sptcompaction);
            DEBUG_T("partner meta usage is too large, meta_usage:%llu, meta_size:%llu, partner_size:%llu\n", inputs1[i]->partners[0].meta_usage, 
                        inputs1[i]->partners[0].meta_size, inputs1[i]->partners[0].partner_size);
        }  else {
            p_sptcompactions.push_back(sptcompaction);
        }
        
        // else {
        //     double ratio = GetOverlappingRatio_2(c, sptcompaction);
			  //     DEBUG_T("in GetSplitCompactions, ratio:%lf\n", ratio);
        //     if(inputs1[i]->partners.size() > 1){
        //         if(global_ratio > 0.2 && ratio > 0.15) {
        //             DEBUG_T("global_ratio greater than 0.2, ratio greater than 0.15, partner count is:%d\n", 
        //                 inputs1[i]->partners.size());
        //             t_sptcompactions.push_back(sptcompaction);
        //         } else if(ratio > 2.0){
        //             DEBUG_T("global_ratio less than 0.2, ratio greater than 0.20, partner count is:%d\n", 
        //                 inputs1[i]->partners.size());
        //             t_sptcompactions.push_back(sptcompaction);
        //         } else {
        //             p_sptcompactions.push_back(sptcompaction);
        //         }
        //     } else {
        //         p_sptcompactions.push_back(sptcompaction);
        //     }
        // } 
    }
}
////////////////////meggie


uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  ///////////////meggie
  int index = 0;
  ///////////////meggie
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    //uint64_t sstable_size = 0, partner_size = 0;
    //DEBUG_T("start addlivefiles, version%d\n", index);
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
        //sstable_size += files[i]->file_size;
        /////////meggie
        if(files[i]->prefix != 0)
          live->insert(files[i]->meta_number);
        for(int j = 0; j < files[i]->partners.size(); j++) {
            //DEBUG_T("AddLiveFiles, partner number%d\n", 
            //        files[i]->partners[j].partner_number);
            live->insert(files[i]->partners[j].partner_number);
            live->insert(files[i]->partners[j].meta_number); 
            //partner_size += files[i]->partners[j].partner_size;
        }
        /////////meggie
      }
    }
    // DEBUG_T("in this version, SSTable size is %llu, partner_size is %llu\n", sstable_size, partner_size);
    // DEBUG_T("end addlivefiles, version%d\n", index++);
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          ///////////////meggie
          if(c->level() == 0) {
            list[num++] = table_cache_->NewPartnerIterator(
              options, files[i]->number, files[i]->pm.get());
          } else
          /////////////meggie
            list[num++] = table_cache_->NewIterator(
                options, files[i]->number, files[i]->file_size);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  ///////////meggie
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);
  //const bool seek_compaction = false;
  ///////////meggie
  
  //因文件大小导致的compaction
  //这里主要是配置inputs[0]
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    c = new Compaction(options_, level);
    // Pick the first file that comes after compact_pointer_[level]
    //获取到compact_pointer_指向后一个sstable
    ////////////meggie
    //level0 bucket
    if(level == 0) {
      FileMetaData* fm = current_->level0_max_nvm_file_;
      DEBUG_T("get inputs[0], number:%llu, smallest:%s, largest:%s\n", 
                fm->number, fm->smallest.user_key().ToString().c_str(), 
                fm->largest.user_key().ToString().c_str());
      c->inputs_[0].push_back(current_->level0_max_nvm_file_);
      DEBUG_T("level1, files like these:\n");
      for(int m = 0; m < current_->files_[1].size(); m++) {
        DEBUG_T("file[%d], number:%llu, smallest:%s, largest:%s\n", m,
                current_->files_[1][m]->number, current_->files_[1][m]->smallest.user_key().ToString().c_str(), 
                current_->files_[1][m]->largest.user_key().ToString().c_str());
      }
    } else {
    ////////////meggie
      for (size_t i = 0; i < current_->files_[level].size(); i++) {
        FileMetaData* f = current_->files_[level][i];
        if (compact_pointer_[level].empty() ||
            icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
          c->inputs_[0].push_back(f);
          break;
        }
      }
      //从头开始循环
      if (c->inputs_[0].empty()) {
        // Wrap-around to the beginning of the key space
        c->inputs_[0].push_back(current_->files_[level][0]);
      }
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  DEBUG_T("before set input_version\n");
  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  ////meggie
  // if (level == 0) {
  //   InternalKey smallest, largest;
  //   GetRange(c->inputs_[0], &smallest, &largest);
  //   // Note that the next call will discard the file we placed in
  //   // c->inputs_[0] earlier and replace it with an overlapping set
  //   // which will include the picked file.
  //   current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
  //   assert(!c->inputs_[0].empty());
  // }
  //////////meggie

  DEBUG_T("to get inputs[1]\n");

  //这里主要配置inputs[1]
  SetupOtherInputs(c);

  return c;
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  GetRange(c->inputs_[0], &smallest, &largest);

  //获取与inputs[0]重叠的inputs[1]
  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  // 获取当前整个compaction对应的最大范围
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  //用于在不改变level + 1的文件数目的情况下，扩展level的文件数目
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    //根据all_start和all_limit, 从level中再次获取重叠的SSTable
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    //获取level中涉及的sstable数目
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    //获取level + 1中涉及的sstable数目
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    //获取扩展后level中涉及的sstable数目
    const int64_t expanded0_size = TotalFileSize(expanded0);
    //如果扩展后level中涉及的sstable数目更多了
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      //保证最后参与的文件数目小于25
      InternalKey new_start, new_limit;
      //获取扩展后level文件的范围
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      //根据level扩展后的重叠范围，在level + 1中获取重叠的文件
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      //如果level + 1的sstable数目没有改变，那就可以扩展，要不然，扩展失败
      if (expanded1.size() == c->inputs_[1].size()) {
        ////////////meggie
        if(level >= 1)
            DEBUG_T("expanded in inputs0, maybe wrong!\n");
        ////////////meggie
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size),
            int(expanded0.size()),
            int(expanded1.size()),
            long(expanded0_size), long(inputs1_size));
        //重新获取并设置level中的smallest和largest
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        //再次获取整个compaction的范围，赋值为all_start, all_limit
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  //获取level + 2和该compaction重叠的文件集合
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  DEBUG_T("inputs1 size is:%d\n", c->inputs_[1].size());

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  //设置level的compaction_pointer_,用于选择下次该level的victim 
  compact_pointer_[level] = largest.Encode().ToString();
  //将compact_pointer_加入到edit中
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
