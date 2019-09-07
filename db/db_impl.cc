// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

////////////meggie
#include "util/debug.h"
#include "util/timer.h"
#include "util/threadpool_new.h"
#include "db/single_partner_table.h"

#ifdef TIMER_LOG
	#define start_timer(s) timer->StartTimer(s)
	#define record_timer(s) timer->Record(s)
#else
	#define start_timer(s)
	#define record_timer(s)
#endif
//uint64_t partner_number;
int use_origin_victim_num = 0;
int partner_compaction_num = 0;
std::vector<uint64_t> partner_size;
////////////meggie
namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  ////////////meggie
  MultiWriteBatch* mbatch;
  ////////////meggie
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
    /////////////meggie
    // std::shared_ptr<HyperLogLog> hll;
    // int hll_add_count;
    // Output() : hll(std::make_shared<HyperLogLog>(12)), 
    //            hll_add_count(0){}
    /////////////meggie
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {
  }
};

/////////////////meggie
struct DBImpl::PartnerCompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  uint64_t number;
  uint64_t curr_file_size;
  InternalKey curr_smallest, curr_largest;
  uint64_t meta_number, meta_size;
  std::shared_ptr<PartnerMeta> pm;
  bool init;
  
  // std::shared_ptr<HyperLogLog> hll;
  // int hll_add_count;

  // State kept for output being generated
  WritableFile* outfile;
  SinglePartnerTable* partner_table;

  uint64_t meta_usage;

  explicit PartnerCompactionState(Compaction* c)
      : compaction(c),
        outfile(nullptr),
        partner_table(nullptr),
        init(false)
        // hll(std::make_shared<HyperLogLog>(12)), 
        // hll_add_count(0)
        {
  }
};

struct DBImpl::MinorCompactionState {
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
    uint64_t meta_usage;
    uint64_t meta_number, meta_size;
    std::shared_ptr<PartnerMeta> pm;

    WritableFile* outfile;
    SinglePartnerTable* partner_table;
    bool new_create;

    Output():outfile(nullptr),
        partner_table(nullptr),
        new_create(true){
    } 
  };
  std::map<char, Output> outputs; 
};

struct DBImpl::MultiCompactionState {
  uint64_t number;
  uint64_t file_size;
  InternalKey smallest, largest;
  uint64_t meta_usage;
  uint64_t meta_number, meta_size;
  std::shared_ptr<PartnerMeta> pm;

  WritableFile* outfile;
  SinglePartnerTable* partner_table;
  bool new_create;

  MultiCompactionState():outfile(nullptr),
        partner_table(nullptr),
        new_create(true){
  } 
};
/////////////////meggie

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src,
                        const std::string& dbname_nvm) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    //////////meggie
    src.env->CreateDir(dbname_nvm);
    //////////meggie
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

/////////////////meggie
void DBImpl::PrintTimerAudit(){
    printf("--------timer information--------\n");
    //timer->DebugString();
    printf("%s\n", timer->DebugString().c_str());
    printf("-----end timer information-------\n");
}

static void AddKeyToHyperLogLog(std::shared_ptr<HyperLogLog>& hll, const Slice& key) {
	  const Slice& user_key = ExtractUserKey(key);
    int64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
    hll->AddHash(hash);
}
////////////meggie

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname,const std::string& dbname_nvm)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options,dbname_nvm)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      ///////////meggie
      dbname_nvm_(dbname_nvm),
      ///////////meggie
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_), dbname_nvm_)),
      db_lock_(nullptr),
      shutting_down_(nullptr),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      ///////////meggie
      memtable_size_(9),
      tmp_mbatch_(new MultiWriteBatch(memtable_size_)), 
      timer(new Timer()),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_, timer)) {
      // versions_(new VersionSet(dbname_, &options_, table_cache_,
      //                          &internal_comparator_)) {
      ///////////meggie
  has_imm_.Release_Store(nullptr);
  ///////////meggie
  timer = new Timer();
  thpool_ = new ThreadPool(10, "thpool_");
  mem_group_.resize(memtable_size_);
  imm_group_.resize(memtable_size_);
  has_imm_group_.resize(memtable_size_);
  for(int i = 0; i < memtable_size_; i++) {
    has_imm_group_[i].Release_Store(nullptr);
  }
  mem_allocated_ = false;
  compact_thpool_ = new ThreadPool(memtable_size_, "compact_thpool");
  batch_thpool_ = new ThreadPool(memtable_size_, "batch_thpool");
  remaining_mems_ = memtable_size_;
  ///////////meggie
}

DBImpl::~DBImpl() {
  //在db_bench中benchmark的析构函数中，会调用dbimpl的析构函数
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-null value is ok
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  DEBUG_T("in ~dbimpl, to delete table_cache_\n");
  delete table_cache_;
  DEBUG_T("in ~dbimpl, after delete table_cache_\n");
  ////////////meggie
  delete timer;
  delete thpool_;
  delete tmp_mbatch_;
  delete compact_thpool_;
  delete batch_thpool_;
  ////////////meggie

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    DEBUG_T("in ~dbimpl, to delete block_cache_\n");
    delete options_.block_cache;
    DEBUG_T("in ~dbimpl, after delete block_cache_\n");
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);
  //////////meggie
  std::vector<std::string> filenames_nvm;
  //////////meggie
  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  //////////meggie
  if(dbname_ != dbname_nvm_){
      env_->GetChildren(dbname_nvm_, &filenames_nvm);
      filenames.insert(filenames.end(), 
              filenames_nvm.begin(), filenames_nvm.end());
  }
  //////////meggie
 uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        ////////////////meggie
        case kMapFile:
          keep = (live.find(number) != live.end());
          break;
        ////////////////meggie
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        ////////////meggie
        //if(number == partner_number) 
          //  DEBUG_T("now Delete file%d\n", partner_number);
        ////////////meggie
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            static_cast<int>(type),
            static_cast<unsigned long long>(number));
        /////////////////meggie
        if(find(filenames_nvm.begin(), filenames_nvm.end(), filenames[i]) 
                    != filenames_nvm.end()){
            //DEBUG_T("delete mapfile:%lld\n", filenames[i]);
            env_->DeleteFile(dbname_nvm_ + "/" + filenames[i]);
        }
        else  
        /////////////////meggie
          env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  //////////////meggie
  env_->CreateDir(dbname_nvm_);
  //////////////meggie
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      ////////////meggie
      mem->isNVMMemtable = false;
      ////////////meggie
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        ////////////meggie
        mem_->isNVMMemtable = false;
        ////////////meggie
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
	  ///////////////meggie
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
	  ///////////////meggie
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  ////////////meggie
  //Status s = WriteLevel0Table(imm_, &edit, base);
  DEBUG_T("to flush immutable memtable to level0\n");
  Status s = WriteLevel0TableWithBucket(imm_, &edit, base);
  ///////////meggie
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (
             //////////////meggie
             //imm_ == nullptr &&
             !NeedsCompaction() && 
             //////////////meggie
             manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    DEBUG_T("in MaybeScheduleCompaction\n");
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  ///////////meggie
  // if (imm_ != nullptr) {
  //   DEBUG_T("to deal with minor compaction...\n");
  //   uint64_t cm_start = env_->NowMicros();
  //   CompactMemTable();
  //   uint64_t cm_end = env_->NowMicros();
  //   DEBUG_T("compact memtable need %llu\n", cm_end - cm_start);
  //   return;
  // }

  if(NeedsCompaction()) {
      CompactMultiMemTable();
      return;
  }
  //////////meggie

  DEBUG_T("to deal with major compaction....\n");

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    start_timer(PICK_COMPACTION);
    c = versions_->PickCompaction();
    record_timer(PICK_COMPACTION);
  }

  DEBUG_T("after pick compaction\n");

  Status status;
  if (c == nullptr) {
    // Nothing to do
  ////////meggie
  //针对level0, 不能直接移到level1
  //} else if (!is_manual && c->IsTrivialMove()) {
  } else if (c->level() != 0 && !is_manual && c->IsTrivialMove()) {
  ////////meggie
    DEBUG_T("major compaction, move to next level\n");
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    ////////////meggie
    c->edit()->DeleteFile(c->level(), f->number);
    if(f->partners.size() != 0){
        DEBUG_T("file%d trivial move to level+1\n", f->number);
        c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest, f->origin_smallest, 
                       f->origin_largest, f->partners);
    } else 
        c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    ////////////meggie
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    ////////////meggie
    if(c->level() == 0) {
        CompactionState* compact = new CompactionState(c);
        status = DoCompactionWork(compact);
        if (!status.ok()) {
            RecordBackgroundError(status);
        }
        CleanupCompaction(compact);
    } else {
        status = DoSplitCompactionWork(c);
    }
    ////////////meggie
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;
  DEBUG_T("major compaction, after CompactionWork\n");
  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }

  if(compact->outfile != nullptr)
     delete compact->outfile;

  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    //////////////meggie
    compact->builder = new TableBuilder(options_, compact->outfile, file_number);
    //////////////meggie
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  /////////////meggie
  //level0 bucket, 删除bucket在map<prefix, FileMetaData*>中的条目
  if(compact->compaction->level() == 0) {
    for (size_t i = 0; i < compact->compaction->num_input_files(0); i++) {
      FileMetaData* fm = compact->compaction->input(0, i);
      DEBUG_T("to delete bucket prefix:%c, number:%llu\n", fm->prefix, fm->number);
      bucket_prefixes_.erase(fm->prefix);
    }
  }
  ////////////meggie

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    //////////////meggie
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, 
        out.largest);
    DEBUG_T("InstallCompactionResults, AddFile, file_number:%lld, smallest:%s, largest:%s\n", 
            out.number, out.smallest.user_key().ToString().c_str(), 
            out.largest.user_key().ToString().c_str());
    //////////////meggie
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

////////////////meggie
Status DBImpl::WriteLevel0TableWithBucket(MemTable* mem, VersionEdit* edit, Version* base) {
  mutex_.AssertHeld();
  //const uint64_t start_micros = env_->NowMicros();
  Iterator* iter = mem->NewIterator();
  Status s;
  MinorCompactionState* mcompact = new MinorCompactionState();
  {
    mutex_.Unlock();
    //获取前缀对应的FileMetaData;
    std::map<char, FileMetaData*> fmmp;
    versions_->GetLevel0FileMeta(fmmp);
    DEBUG_T("WriteLevel0TableWithBucket, after get fmmp, size:%d\n", fmmp.size());
    s = MinorCompaction(iter, mcompact, fmmp);
    mutex_.Lock();
  }

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  //update file
  UpdateLevel0File(edit, mcompact); 
  //cleanup
  CleanupCompaction(mcompact);
  return s;
}

void DBImpl::UpdateLevel0File(VersionEdit* edit, MinorCompactionState* mcompact) {
  DEBUG_T("update level0 file\n");
  for(auto iter = mcompact->outputs.begin(); iter != mcompact->outputs.end(); iter++) {
    if(iter->second.new_create){
      DEBUG_T("add level0 file, number:%llu, smallest:%s, largest:%s\n", 
              iter->second.number, 
              iter->second.smallest.user_key().ToString().c_str(),
              iter->second.largest.user_key().ToString().c_str());
      edit->AddFile(0, iter->second.number, iter->second.file_size, iter->second.smallest, 
                          iter->second.largest, iter->second.meta_number, iter->second.meta_size, 
                          iter->second.meta_usage, iter->second.pm, iter->first);
    }
    else {
      DEBUG_T("update level0 file, number:%llu, smallest:%s, largest:%s\n", 
              iter->second.number, 
              iter->second.smallest.user_key().ToString().c_str(),
              iter->second.largest.user_key().ToString().c_str());
      edit->UpdateFile(0, iter->second.number, iter->second.file_size, iter->second.smallest, 
                          iter->second.largest, iter->second.meta_usage);
    }
  }
}
/*
保证每个partnerTable对应一个线程，用于对元数据进行下刷add, 
需要为每个线程配备2个队列，传入两个队列
queue1, 用于插入key，和取出key，
queue2, 包含3个属性：block_offset和block_size, 以及这个block所含key的个数 
当一个块构造完了，就唤醒这个线程，进行处理
*/
struct DBImpl::SavePartnerMetaArgs {
    DBImpl* db;
    SinglePartnerTable* spt;
}; 
void DBImpl::SavePartnerMeta(void* args) {
    SavePartnerMetaArgs* spm = reinterpret_cast<SavePartnerMetaArgs*>(args);
    DBImpl* db = spm->db;
    db->DealWithPartnerMeta(spm->spt);
    delete spm;
}
void DBImpl::DealWithPartnerMeta(SinglePartnerTable* spt) {
    DEBUG_T("-----------in thread save partner meta---------\n");
    while(!spt->bailout || !spt->meta_queue.empty()) {
      std::unique_lock<std::mutex> job_lock(spt->queue_mutex);
      while(!spt->bailout && spt->meta_queue.empty()){
        DEBUG_T("spt meta:%p, waiting queue or bail out....\n", spt->meta_);
        spt->meta_available_var.wait(job_lock, [spt]() ->bool { return spt->meta_queue.size() || spt->bailout;});
        DEBUG_T("spt meta:%p, finished waiting queue or bail out....\n", spt->meta_);
      }

      if(spt->bailout && spt->meta_queue.empty()){
        break;
      }

      std::pair<uint64_t, std::pair<uint64_t, uint64_t>> meta = spt->meta_queue.front();
      spt->meta_queue.pop_front();
      job_lock.unlock();
      uint64_t key_size = meta.first;
      DEBUG_T("get key count from queue:%d\n", key_size);
      uint64_t block_offset = meta.second.first;
      uint64_t block_size = meta.second.second;
      int i = 0;
      while(i < key_size && !spt->key_queue.empty()) {
        std::string key = spt->key_queue.front();
        spt->key_queue.pop_front();
        spt->meta_->Add(Slice(key), block_offset, block_size);
        i++;
      }
    }
out:
    DEBUG_T("-----------spt meta:%p, out of thread save partner meta---------\n", spt->meta_);
    //std::lock_guard<std::mutex> lck(spt->wait_mutex);
    spt->finished = true;
    spt->wait_var.notify_one();//唤醒正在等待任务完成的线程，表示有一个任务已经完成了
}

Status DBImpl::OpenPartnerTableWithLevel0(MinorCompactionState* mcompact, char prefix, FileMetaData* fm) {
  //需要在write level0时获取当前前缀对应的FileMetaData*,如果不存在，为nullptr
  uint64_t file_number;
  MinorCompactionState::Output out;
  DEBUG_T("open prefix:%c, partner table\n", prefix);
  if(fm != nullptr) {
    out.number = fm->number;
    out.smallest = fm->smallest;
    out.largest = fm->largest;
    out.file_size = fm->file_size;
    out.meta_number = fm->meta_number;
    out.meta_size = fm->meta_size;
    out.pm = fm->pm;
    out.new_create = false;
  } else {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    out.file_size = 0;
    out.meta_number = versions_->NewFileNumber();
    out.meta_size = 40 << 10 << 10;
    mutex_.Unlock();
    std::string metaFile = MapFileName(dbname_nvm_, out.meta_number);
    ArenaNVM* arena = new ArenaNVM(out.meta_size, &metaFile, false);
    arena->nvmarena_ = true;
    //compact->pm = new PartnerMeta(internal_comparator_, arena, false);
    out.pm = std::make_shared<PartnerMeta>(internal_comparator_, arena, false);
  }
  std::string fname = TableFileName(dbname_, out.number);
  Status s = env_->NewWritableFile(fname, &out.outfile, true);
  if (s.ok()) {
      TableBuilder* builder = new TableBuilder(options_, out.outfile, out.number, out.file_size);
      SinglePartnerTable* spt = new SinglePartnerTable(builder, out.pm.get());
      out.partner_table = spt;
      assert(out.partner_table != nullptr);
      DEBUG_T("------open partner table, number:%llu-------\n", out.number);
      //开启meta线程
      SavePartnerMetaArgs* spm = new SavePartnerMetaArgs;
      spm->db = this;
      spm->spt = spt;
      env_->StartThread(SavePartnerMeta, spm);
      // DEBUG_T("----------start thread----------\n");
  }
  mcompact->outputs.insert(std::make_pair(prefix, out));
  return s;
}

Status DBImpl::FinishPartnerTableWithLevel0(MinorCompactionState* mcompact, Iterator* input) {
  assert(mcompact != nullptr);
  Status s;
  for(auto iter = mcompact->outputs.begin(); iter != mcompact->outputs.end(); iter++) {
      assert(iter->second.outfile != nullptr);
      assert(iter->second.partner_table != nullptr);

      // Check for iterator errors
      s = input->status();
      //TODO
      if (s.ok()) {
        s = iter->second.partner_table->Finish();
      } else {
        iter->second.partner_table->Abandon();
      }
      iter->second.file_size = iter->second.partner_table->FileSize();
      iter->second.meta_usage = iter->second.partner_table->NVMSize();
      DEBUG_T("to print memory usage\n");
      DEBUG_T("after finish level0 partner table, file number:%llu, file size: %llu, meta usage:%zu\n",
            iter->second.number, iter->second.file_size, iter->second.meta_usage);    
      delete iter->second.partner_table;
      iter->second.partner_table = nullptr;

      // Finish and check for file errors
      if (s.ok()) {
        s = iter->second.outfile->Sync();
      }
      if (s.ok()) {
        s = iter->second.outfile->Close();
      }
      delete iter->second.outfile;
      iter->second.outfile = nullptr;
  }
  return s;
}

void DBImpl::CleanupCompaction(MinorCompactionState* mcompact) {
  mutex_.AssertHeld();
  assert(mcompact != nullptr);
  for(auto iter = mcompact->outputs.begin(); iter != mcompact->outputs.end(); iter++) {
    // if (iter->second.partner_table != nullptr) {
    //   // May happen if we get a shutdown call in the middle of compaction
    //   iter->second.partner_table->Abandon();
    //   delete iter->second.partner_table;
    // } else {
    //   assert(iter->second.outfile == nullptr);
    // }
    // delete iter->second.outfile;
    if(iter->second.new_create)
      pending_outputs_.erase(iter->second.number);
  }
  delete mcompact;
}

void DBImpl::TestPMIter(Iterator* iter) {
  iter->SeekToFirst();
  DEBUG_T("test pm iter!\n");
  for(; iter->Valid(); iter->Next()) {
    DEBUG_T("key:%s, ",iter->key().ToString().c_str());
  }
  DEBUG_T("\n");
}

Status DBImpl::MinorCompaction(Iterator* iter, MinorCompactionState* mcompact, std::map<char, FileMetaData*>& fmmp) {
  std::map<char, InternalKey> largest;
  std::map<char, InternalKey> smallest;
  Status s;
  DEBUG_T("start minor compaction\n");
  iter->SeekToFirst();
  for (; iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    //DEBUG_T("iter, key:%s\n", key.ToString().c_str());
    InternalKey ikey;
    ikey.DecodeFrom(key);
    char prefix = ikey.user_key()[4];
    auto miter = mcompact->outputs.find(prefix);
    if(miter == mcompact->outputs.end()) {
      //(TODO)设置FileMetaData*
      if(fmmp.find(prefix) != fmmp.end()) 
        s = OpenPartnerTableWithLevel0(mcompact, prefix, fmmp[prefix]);
      else 
        s = OpenPartnerTableWithLevel0(mcompact, prefix, nullptr);
      //DEBUG_T("after open partner table, prefix %c, number:%llu\n", prefix, mcompact->outputs[prefix].number);
      if(!s.ok()) {
          break;
      }    
      smallest[prefix] = ikey;
    } 
    largest[prefix] = ikey;
    //uint64_t add_start = env_->NowMicros();
    (mcompact->outputs[prefix].partner_table)->Add(key, iter->value());
    //uint64_t add_end = env_->NowMicros();
    //DEBUG_T("level0 add need time:%d\n", add_end - add_start);
  }
  //设置数据全部添加完毕

  for(auto miter = mcompact->outputs.begin(); miter != mcompact->outputs.end(); miter++) {
      assert(miter->second.outfile != nullptr);
      assert(miter->second.partner_table != nullptr);

      // Check for iterator errors
      s = iter->status();
      //TODO
      if (s.ok()) {
        s = miter->second.partner_table->Finish();
      } else {
        miter->second.partner_table->Abandon();
      }

      /////线程同步
      miter->second.partner_table->bailout = true;
      //fprintf(stderr, "-------spt meta:%p, notify bail out----------\n");
      miter->second.partner_table->meta_available_var.notify_one();
      std::unique_lock<std::mutex> job_lock(miter->second.partner_table->wait_mutex);
      //fprintf(stderr, "-------spt meta:%p, waiting meta thread finished----------\n", miter->second.partner_table->meta_);
      miter->second.partner_table->wait_var.wait(job_lock, [miter]()->bool{return static_cast<bool>(miter->second.partner_table->finished);});
      //fprintf(stderr, "-------spt meta:%p, meta thread finished----------\n", miter->second.partner_table->meta_);
      job_lock.unlock();
      //end 线程同步

      miter->second.file_size = miter->second.partner_table->FileSize();
      miter->second.meta_usage = miter->second.partner_table->NVMSize();
      DEBUG_T("after finish partner table, prefix:%c file number:%llu, file size: %llu, meta usage:%zu\n",
            miter->first, miter->second.number, miter->second.file_size, miter->second.meta_usage); 

      // if(miter->second.number == 9) {
      //   DEBUG_T("after finish partner table, to iter pm\n ");
      //   TestPMIter(miter->second.pm->NewIterator());
      // }

      if(miter->second.new_create || internal_comparator_.Compare(smallest[miter->first], miter->second.smallest) < 0) {
            miter->second.smallest = smallest[miter->first];
      }
      if(miter->second.new_create || internal_comparator_.Compare(largest[miter->first], miter->second.largest) > 0) {
            miter->second.largest = largest[miter->first];
      }

      delete miter->second.partner_table;
      miter->second.partner_table = nullptr;

      // Finish and check for file errors
      if (s.ok()) {
        s = miter->second.outfile->Sync();
      }
      if (s.ok()) {
        s = miter->second.outfile->Close();
      }
      delete miter->second.outfile;
      miter->second.outfile = nullptr;
  }

  DEBUG_T("end minor compaction\n");

  delete iter;
  return s;
}

Status DBImpl::OpenPartnerTable(PartnerCompactionState* compact, int input1_index) {
    FileMetaData* fm = compact->compaction->input(1, input1_index);
    ArenaNVM* arena;
    DEBUG_T("to open partner table\n");
    if (!fm->partners.empty()) {
      compact->number = fm->partners[0].partner_number;
      compact->curr_smallest = fm->partners[0].partner_smallest;
      compact->curr_largest = fm->partners[0].partner_largest;
      compact->curr_file_size = fm->partners[0].partner_size;
      compact->init = true;
      compact->meta_number = fm->partners[0].meta_number;
      compact->meta_size = fm->partners[0].meta_size;
      compact->pm = fm->partners[0].pm;
      DEBUG_T("number:%llu, get old pm:%p\n", compact->number, compact->pm);
    } else {
      mutex_.Lock();
      uint64_t file_number = versions_->NewFileNumber();
      pending_outputs_.insert(file_number);
      compact->number = file_number;
      compact->curr_smallest.Clear();
      compact->curr_largest.Clear();
      compact->curr_file_size = 0;
      compact->meta_number = versions_->NewFileNumber();
      compact->meta_size = 16 << 10 << 10;
      std::string metaFile = MapFileName(dbname_nvm_, compact->meta_number);
      arena = new ArenaNVM(compact->meta_size, &metaFile, false);
      arena->nvmarena_ = true;
      //compact->pm = new PartnerMeta(internal_comparator_, arena, false);
      compact->pm = std::make_shared<PartnerMeta>(internal_comparator_, arena, false);
      DEBUG_T("open new partner number: %llu, meta nvm number:%llu, pm:%p after get arena nvm\n", file_number, compact->meta_number, compact->pm);
      //之后更新meta_number
      mutex_.Unlock(); 
    }
    // std::string metaFile = MapFileName(dbname_nvm_, compact->meta_number);
    // arena = new ArenaNVM(compact->meta_size, &metaFile, false);
    // arena->nvmarena_ = true;
    // compact->pm = new PartnerMeta(internal_comparator_, arena, false);
    // DEBUG_T("open new partner number: %llu, meta nvm number:%llu, pm:%p after get arena nvm\n", compact->meta_number, compact->meta_number, compact->pm);
    // compact->pm->Ref();
    // Make the output file
    std::string fname = TableFileName(dbname_, compact->number);
    Status s = env_->NewWritableFile(fname, &compact->outfile, true);
    if (s.ok()) {
      TableBuilder* builder = new TableBuilder(options_, compact->outfile, compact->number, compact->curr_file_size);
      SinglePartnerTable* spt = new SinglePartnerTable(builder, compact->pm.get());
      compact->partner_table = spt;
      assert(compact->partner_table != nullptr);
      //开启meta线程
      SavePartnerMetaArgs* spm = new SavePartnerMetaArgs;
      spm->db = this;
      spm->spt = spt;
      env_->StartThread(SavePartnerMeta, spm);
      DEBUG_T("----------start thread----------\n");
    }
  return s;
} 

Status DBImpl::FinishPartnerTable(PartnerCompactionState* compact, Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->partner_table != nullptr);

  const uint64_t output_number = compact->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  //TODO
  if (s.ok()) {
    s = compact->partner_table->Finish();
  } else {
    compact->partner_table->Abandon();
  }

  /////线程同步
  compact->partner_table->bailout = true;
  compact->partner_table->meta_available_var.notify_one();
  std::unique_lock<std::mutex> job_lock(compact->partner_table->wait_mutex);
  compact->partner_table->wait_var.wait(job_lock, [compact]()->bool{return static_cast<bool>(compact->partner_table->finished);});
  job_lock.unlock();
  ////end 线程同步

  const uint64_t current_bytes = compact->partner_table->FileSize();
  compact->curr_file_size = current_bytes;
  
  compact->meta_usage = compact->partner_table->NVMSize();
  DEBUG_T("to print memory usage\n");
  DEBUG_T("after finish partner table number:%llu, partner size: %llu, arena nvm need room:%zu\n",
        compact->number, compact->curr_file_size, compact->meta_usage);
  delete compact->partner_table;
  compact->partner_table = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;
  return s;
}

void DBImpl::CleanupCompaction(PartnerCompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->partner_table != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->partner_table->Abandon();
    delete compact->partner_table;
    delete compact->outfile;
  } else {
    assert(compact->outfile == nullptr);
  }
  //(TODO)如果真的出错了，怎么办，这样子全部丢弃partner肯定是不行的，需要再想想
  pending_outputs_.erase(compact->number);
  delete compact;
}


struct DBImpl::TraditionalCompactionArgs {
    DBImpl* db;
    CompactionState* compact;
    TSplitCompaction* t_sptcompaction;
    Status s;
};

struct DBImpl::PartnerCompactionArgs {
    DBImpl* db;
    PartnerCompactionState* compact;
    SplitCompaction* p_sptcompaction;
    Status s;
};

void DBImpl::DoTraditionCompactionWork(void* args) {
    TraditionalCompactionArgs* tcargs = 
            reinterpret_cast<TraditionalCompactionArgs*>(args);
    DBImpl* db = tcargs->db;
    db->DealWithTraditionCompaction(tcargs->compact, 
                        tcargs->t_sptcompaction, &tcargs->s);
    //delete tcargs;
}

void DBImpl::DoPartnerCompactionWork(void* args) {
    PartnerCompactionArgs* pcargs = 
            reinterpret_cast<PartnerCompactionArgs*>(args);
    DBImpl* db = pcargs->db;
    db->DealWithPartnerCompaction(pcargs->compact, 
                        pcargs->p_sptcompaction, &pcargs->s);
    //delete pcargs;
}

void DBImpl::AddFileWithTraditionalCompaction(VersionEdit* edit, 
        std::vector<CompactionState*>& t_compactionstate_list) {
    DEBUG_T("after partner compaction, add file:\n");
    for(int i = 0; i < t_compactionstate_list.size(); i++) {
       CompactionState* compact = t_compactionstate_list[i];
       int level = compact->compaction->level();
       for(size_t j = 0; j < compact->outputs.size(); j++) {
           const CompactionState::Output& out = compact->outputs[j];
           DEBUG_T("%d, ", out.number);
           edit->AddFile(level + 1, out.number, out.file_size
                   , out.smallest, out.largest);
       }
    }
    DEBUG_T("\n");
}

// void DBImpl::UpdateFileWithPartnerCompaction(VersionEdit* edit,
//         std::vector<uint64_t>& pcompaction_files, 
//         std::vector<CompactionState*>& p_compactionstate_list) {
//    int sz1 = pcompaction_files.size();
//    int sz2 = p_compactionstate_list.size();
//    assert(sz1 == sz2);
//    DEBUG_T("UpdateFileWithPartnerCompaction, size:%d\n", sz1);
//    for(int i = 0; i < sz1; i++) {
//        CompactionState* compact = p_compactionstate_list[i];
//        if(compact->outputs.size() == 0)
//            continue;
//        std::vector<Partner> partners;
// 	   int level = compact->compaction->level();
//        assert(compact->outputs.size() == 1);
//        const CompactionState::Output& out = compact->outputs[0];
//        Partner ptner; 
//        ptner.partner_number = out.number;
//        ptner.partner_size = out.file_size;
//        ptner.partner_smallest = out.smallest;
//        ptner.partner_largest = out.largest;
//        ptner.hll = out.hll;
//        ptner.hll_add_count = out.hll_add_count;
//        partners.push_back(ptner);
//        edit->AddPartner(level + 1, pcompaction_files[i], ptner);
//    }
// }

void DBImpl::UpdateFileWithPartnerCompaction(VersionEdit* edit, std::vector<uint64_t>& pcompaction_files, 
                                            std::vector<PartnerCompactionState*>& p_compactionstate_list) {
   int sz1 = pcompaction_files.size();
   int sz2 = p_compactionstate_list.size();
   assert(sz1 == sz2);
   DEBUG_T("UpdateFileWithPartnerCompaction, size:%d\n", sz1);
   for(int i = 0; i < sz1; i++) {
       PartnerCompactionState* compact = p_compactionstate_list[i];
       std::vector<Partner> partners;
	     int level = compact->compaction->level();
       Partner ptner; 
       ptner.partner_number = compact->number;
       ptner.partner_size = compact->curr_file_size;
       ptner.partner_smallest = compact->curr_smallest;
       ptner.partner_largest = compact->curr_largest;
       ptner.meta_number = compact->meta_number;
       ptner.meta_size = compact->meta_size;
       ptner.meta_usage = compact->meta_usage;
       ptner.pm = compact->pm;
       partners.push_back(ptner);
       edit->AddPartner(level + 1, pcompaction_files[i], ptner);
   }
}

bool DBImpl::ValidAndInRange(Iterator* iter, InternalKey end, 
                            bool containsend, ParsedInternalKey* ikey_victim) {
    if(!iter->Valid()){
        DEBUG_T("victim_iter is not Valid\n");
        return false;
    }
    //DEBUG_T("ValidAndInRange is Valid\n");
    Slice key = iter->key();
    if(!ParseInternalKey(key, ikey_victim)){
        //DEBUG_T("ParsedInternalKey failed\n");
        return false;
    }
    //DEBUG_T("ikey_victim user_key :%s\n",
      //      ikey_victim->user_key.ToString().c_str());
    //DEBUG_T("end user_key:%s\n", end.user_key().ToString().c_str());
    InternalKey mykey;
    mykey.SetFrom(*ikey_victim);
    //if((containsend &&  internal_comparator_.Compare(mykey, 
    //            end) > 0) || (!containsend && 
    //        internal_comparator_.Compare(mykey, end) >= 0)){
    if((containsend &&  user_comparator()->Compare(ikey_victim->user_key, 
                end.user_key()) > 0) || (!containsend && 
            user_comparator()->Compare(ikey_victim->user_key, 
                end.user_key()) >= 0)){
        DEBUG_T("ikey_victim user_key :%s\n",
            ikey_victim->user_key.ToString().c_str());
        DEBUG_T("end user_key:%s\n", end.user_key().ToString().c_str());
        DEBUG_T("victim_iter is not in range\n");
        return false;
    }
    return true;
}

void DBImpl::DealWithTraditionCompaction(CompactionState* compact, 
                        TSplitCompaction* t_sptcompaction, Status* status) {
    DEBUG_T("in DealWithTraditionCompaction\n");
    DEBUG_T("victim_start:%s, victim_end:%s\n", 
            t_sptcompaction->victim_start.user_key().ToString().c_str(),
            t_sptcompaction->victim_end.user_key().ToString().c_str());
    
    Iterator* list[2];
    //(TODO)这两个迭代器要重新获取
    list[0] = t_sptcompaction->victim_iter;
    list[1] = t_sptcompaction->inputs1_iter;
    DEBUG_T("before get merge iter\n");
    Iterator* merge_iter = NewMergingIterator(&internal_comparator_,
            list, 2);
    DEBUG_T("after get merge iter\n");
    merge_iter->SetChildRange(0, 
            t_sptcompaction->victim_start.Encode(),
            t_sptcompaction->victim_end.Encode(), 
            t_sptcompaction->containsend);
    DEBUG_T("after set child range\n");
    merge_iter->SeekToFirst();
    //Status status;
    ParsedInternalKey ikey;
    Iterator* current_iter;
    std::string current_user_key;
    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

    DEBUG_T("in traditional compaction, after seek iter\n");
    for(; merge_iter->Valid() && !shutting_down_.Acquire_Load(); ) {
        Slice key = merge_iter->key();
        bool drop = false;
        if(!ParseInternalKey(key, &ikey)){
             current_user_key.clear();
             has_current_user_key = false;
             last_sequence_for_key = kMaxSequenceNumber;
        } else {
             if (!has_current_user_key ||
                user_comparator()->Compare(ikey.user_key,
                                         Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
              }
              if (last_sequence_for_key <= compact->smallest_snapshot) {
                drop = true;    
              } else if (ikey.type == kTypeDeletion &&
                     ikey.sequence <= compact->smallest_snapshot &&
                     compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
                drop = true;
              }
              last_sequence_for_key = ikey.sequence;
        }
        if(!drop) {
            if(compact->builder == nullptr) {
                *status = OpenCompactionOutputFile(compact);
                if(!(*status).ok()) {
                    break;
                }
            }    
            if(compact->builder->NumEntries() == 0) {
                //DEBUG_T("first entry of a sstable\n");
                compact->current_output()->smallest.DecodeFrom(key);
                //DEBUG_T("after first entry of a sstable\n");
            }

            compact->current_output()->largest.DecodeFrom(key);
            compact->builder->Add(key, merge_iter->value());
            // AddKeyToHyperLogLog(compact->current_output()->hll, key);
            // compact->current_output()->hll_add_count++;

            if(compact->builder->FileSize() >= 
                    compact->compaction->MaxOutputFileSize()) {
                //DEBUG_T("to finish generated a sstable\n");
                *status = FinishCompactionOutputFile(compact, merge_iter);
                if(!(*status).ok()) {
                    break;
                }
                //DEBUG_T("have generated a sstable\n");
            }
        }
        merge_iter->Next();
    }
   
    DEBUG_T("file largest is %s\n", compact->current_output()->largest.user_key().ToString().c_str());
    if((*status).ok() && shutting_down_.Acquire_Load()) {
        *status = Status::IOError("Deleting DB during compaction");
    }
   
    if((*status).ok() && compact->builder != nullptr) {
        *status = FinishCompactionOutputFile(compact, merge_iter);
    }


    if(!(*status).ok()) {
        RecordBackgroundError(*status);
    }
    delete merge_iter;
}

// void DBImpl::DealWithPartnerCompaction(CompactionState* compact, 
//                             SplitCompaction* p_sptcompaction) {
//     DEBUG_T("in DealWithPartnerCompaction\n");
//     DEBUG_T("victim_start:%s, victim_end:%s, containsend:%d\n", 
//         p_sptcompaction->victim_start.user_key().ToString().c_str(),
//         p_sptcompaction->victim_end.user_key().ToString().c_str(),
//               p_sptcompaction->containsend? 1: 0);
//     assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
//     assert(compact->builder == nullptr);
//     assert(compact->outfile == nullptr);

//     Compaction* c = compact->compaction;
//     std::vector<int>& victims = p_sptcompaction->victims;
    
//     Iterator* input = p_sptcompaction->victim_iter;
//     InternalKey victim_end = p_sptcompaction->victim_end;
//     bool containsend = p_sptcompaction->containsend;

//     input->Seek(p_sptcompaction->victim_start.Encode());
    
//     Status status;
//     ParsedInternalKey ikey;
//     std::string current_user_key;
//     bool has_current_user_key = false;
//     SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

//     for(; ValidAndInRange(input, victim_end, containsend, &ikey) 
//                 && !shutting_down_.Acquire_Load(); ) {
//         //DEBUG_T("partner compaction, user_key:%s\n",
//           //      ikey.user_key.ToString().c_str());
//         Slice key = input->key();
//         //DEBUG_T("Get key\n");
//         // Prioritize immutable compaction work
//         /*if (has_imm_.NoBarrier_Load() != nullptr) {
//           mutex_.Lock();
//           if (imm_ != nullptr) {
//             CompactMemTable();
//             // Wake up MakeRoomForWrite() if necessary.
//             background_work_finished_signal_.SignalAll();
//           }
//           mutex_.Unlock();
//         }*/
//         bool drop = false;
        
//         if (!has_current_user_key ||
//           user_comparator()->Compare(ikey.user_key,
//                                      Slice(current_user_key)) != 0) {
//             // First occurrence of this user key
//             current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
//             has_current_user_key = true;
//             last_sequence_for_key = kMaxSequenceNumber;
//         }
//         if (last_sequence_for_key <= compact->smallest_snapshot) {
//             drop = true;    
//         } else if (ikey.type == kTypeDeletion &&
//                  ikey.sequence <= compact->smallest_snapshot &&
//                  compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
//             drop = true;
//         }
//         last_sequence_for_key = ikey.sequence;
        
//         if(!drop) {
//             if(compact->builder == nullptr) {
//                 DEBUG_T("has set builder\n");
//                 status = OpenCompactionOutputFile(compact);
//                 if(!status.ok()) {
//                     break;
//                 }
//             }    
//             if(compact->builder->NumEntries() == 0) {
//                 compact->current_output()->smallest.DecodeFrom(key);
//             }

//             compact->current_output()->largest.DecodeFrom(key);
//             compact->builder->Add(key, input->value());
//             AddKeyToHyperLogLog(compact->current_output()->hll, key);
//             compact->current_output()->hll_add_count++;

//             //if(compact->builder->FileSize() >= 
//             //        compact->compaction->MaxOutputFileSize()) {
//             //    status = FinishCompactionOutputFile(compact, input);
//             //    if(!status.ok()) {
//             //        break;
//             //    }
//             //}
//         }

//         input->Next();
//     }

//     if(status.ok() && shutting_down_.Acquire_Load()) {
//         status = Status::IOError("Deleting DB during compaction");
//     }
   
//     if(status.ok() && compact->builder != nullptr) {
//         status = FinishCompactionOutputFile(compact, input);
//         DEBUG_T("push partner_size:%llu\n", 
//                 compact->current_output()->file_size);
//         partner_size.push_back(compact->current_output()->file_size);
//         DEBUG_T("after DealWithPartnerCompaction, compact->outputs size:%d\n", 
//             compact->outputs.size());
//     }
    
//     if(!status.ok()) {
//         RecordBackgroundError(status);
//     }
// }
void DBImpl::DealWithPartnerCompaction(PartnerCompactionState* compact, 
                            SplitCompaction* p_sptcompaction, Status* status) {
    DEBUG_T("in DealWithPartnerCompaction\n");
    DEBUG_T("victim_start:%s, victim_end:%s, containsend:%d\n", 
        p_sptcompaction->victim_start.user_key().ToString().c_str(),
        p_sptcompaction->victim_end.user_key().ToString().c_str(),
              p_sptcompaction->containsend? 1: 0);
    assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
    assert(compact->partner_table == nullptr);
    assert(compact->outfile == nullptr);

    Compaction* c = compact->compaction;
    std::vector<int>& victims = p_sptcompaction->victims;
    
    //（TODO）victim_iter的获取， victim_iter需要重新获取
    Iterator* input = p_sptcompaction->victim_iter;
    InternalKey victim_end = p_sptcompaction->victim_end;
    bool containsend = p_sptcompaction->containsend;

    //定位到开始位置
    input->Seek(p_sptcompaction->victim_start.Encode());
    
    //Status status;
    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    InternalKey this_smallest, this_largest;
    uint64_t entries = 0;

    DEBUG_T("in partner compaction, after seek iter\n");
    for(; ValidAndInRange(input, victim_end, containsend, &ikey) 
                && !shutting_down_.Acquire_Load(); ) {
        //DEBUG_T("partner compaction, user_key:%s\n",
          //      ikey.user_key.ToString().c_str());
        Slice key = input->key();
        //DEBUG_T("Get key\n");
        // Prioritize immutable compaction work
        /*if (has_imm_.NoBarrier_Load() != nullptr) {
          mutex_.Lock();
          if (imm_ != nullptr) {
            CompactMemTable();
            // Wake up MakeRoomForWrite() if necessary.
            background_work_finished_signal_.SignalAll();
          }
          mutex_.Unlock();
        }*/
        bool drop = false;
        
        if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
            // First occurrence of this user key
            current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
            has_current_user_key = true;
            last_sequence_for_key = kMaxSequenceNumber;
        }
        if (last_sequence_for_key <= compact->smallest_snapshot) {
            drop = true;    
        } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
            drop = true;
        }
        last_sequence_for_key = ikey.sequence;
        
        if(!drop) {            
            if(compact->partner_table == nullptr) {
                //DEBUG_T("to set partner table\n");
                *status = OpenPartnerTable(compact, p_sptcompaction->inputs1_index);
                DEBUG_T("after open partner table, partner number:%llu\n", compact->number);
                if(!(*status).ok()) {
                    break;
                }              
                this_smallest.DecodeFrom(key);
            }    
            this_largest.DecodeFrom(key);
            //DEBUG_T("to add key %s into partner table\n", key.ToString().c_str());
            compact->partner_table->Add(key, input->value());
            //DEBUG_T("after add key into partner table\n");
            // AddKeyToHyperLogLog(compact->hll, key);
            // compact->hll_add_count++;

            //if(compact->builder->FileSize() >= 
            //        compact->compaction->MaxOutputFileSize()) {
            //    status = FinishCompactionOutputFile(compact, input);
            //    if(!status.ok()) {
            //        break;
            //    }
            //}
        }

        input->Next();
        entries++;
    }

    if((*status).ok() && shutting_down_.Acquire_Load()) {
        *status = Status::IOError("Deleting DB during compaction");
    }
   
    DEBUG_T("after finish iter input, add entries:%llu\n", entries);

    if((*status).ok() && compact->partner_table != nullptr) {
       DEBUG_T("before finish partner table, this_smallest user key is %s, this_largest user key is:%s\n", 
              this_smallest.user_key().ToString().c_str(), 
              this_largest.user_key().ToString().c_str());
        if(!compact->init || internal_comparator_.Compare(this_smallest, compact->curr_smallest) < 0) {
          compact->curr_smallest = this_smallest;
        }
        if(!compact->init || internal_comparator_.Compare(this_largest, compact->curr_largest) > 0) {
          compact->curr_largest = this_largest;
        }
        DEBUG_T("before finish partner table, smallest user key is %s, largest user key is:%s\n", 
              compact->curr_smallest.user_key().ToString().c_str(), 
              compact->curr_largest.user_key().ToString().c_str());
        *status = FinishPartnerTable(compact, input);
    }

    // delete input;
    // input = nullptr;
    if(!(*status).ok()) {
        RecordBackgroundError(*status);
    }
}

Status DBImpl::DealWithSingleCompaction(CompactionState* compact) {
    DEBUG_T("in DealWithSingleCompaction\n");
    //（TODO）这个迭代器要重新获取
    Iterator* input =  versions_->GetSingleCompactionIterator(
            compact->compaction);
    if(input)
        input->SeekToFirst();
    else {
        return Status::IOError("input is nullptr");
    }
    
    Status status;
    ParsedInternalKey ikey;
    Iterator* current_iter;
    std::string current_user_key;
    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    
    for(; input->Valid() && !shutting_down_.Acquire_Load(); ) {
        Slice key = input->key();
        bool drop = false;
        if(!ParseInternalKey(key, &ikey)){
             current_user_key.clear();
             has_current_user_key = false;
             last_sequence_for_key = kMaxSequenceNumber;
        } else {
             if (!has_current_user_key ||
                user_comparator()->Compare(ikey.user_key,
                                         Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
              }
              if (last_sequence_for_key <= compact->smallest_snapshot) {
                drop = true;    
              } else if (ikey.type == kTypeDeletion &&
                     ikey.sequence <= compact->smallest_snapshot &&
                     compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
                drop = true;
              }
              last_sequence_for_key = ikey.sequence;
        }
        if(!drop) {
            if(compact->builder == nullptr) {
                status = OpenCompactionOutputFile(compact);
                if(!status.ok()) {
                    break;
                }
            }    
            if(compact->builder->NumEntries() == 0) {
                //DEBUG_T("first entry of a sstable\n");
                compact->current_output()->smallest.DecodeFrom(key);
                //DEBUG_T("after first entry of a sstable\n");
            }

            compact->current_output()->largest.DecodeFrom(key);
            compact->builder->Add(key, input->value());
            // AddKeyToHyperLogLog(compact->current_output()->hll, key);
            // compact->current_output()->hll_add_count++;

            if(compact->builder->FileSize() >= 
                    compact->compaction->MaxOutputFileSize()) {
                //DEBUG_T("to finish generated a sstable\n");
                status = FinishCompactionOutputFile(compact, input);
                if(!status.ok()) {
                    break;
                }
                //DEBUG_T("have generated a sstable\n");
            }
        }
        input->Next();
    }
    if(status.ok() && shutting_down_.Acquire_Load()) {
        status = Status::IOError("Deleting DB during compaction");
    }
   
    if(status.ok() && compact->builder != nullptr) {
        status = FinishCompactionOutputFile(compact, input);
    }

    if(!status.ok()) {
        RecordBackgroundError(status);
    }
    delete input;
    return status;
}

Status DBImpl::DoSplitCompactionWork(Compaction* c) {
  std::vector<SplitCompaction*> t_sptcompactions;
  std::vector<SplitCompaction*> p_sptcompactions;
  Status status;
  start_timer(DO_SPLITCOMPACTION_WORK); 
  DEBUG_T("---------------start in SplitCompaction----------------\n");
  versions_->GetSplitCompactions(c, t_sptcompactions,
                  p_sptcompactions);

  DEBUG_T("t_sptcompactions size:%d, p_sptcompaction size:%d\n",
          t_sptcompactions.size(), p_sptcompactions.size());

  std::vector<TSplitCompaction*> tcompactionlist; 
  std::vector<CompactionState*> t_compactionstate_list;
  std::vector<PartnerCompactionState*> p_compactionstate_list;
  std::vector<int> tcompaction_index;
  std::vector<uint64_t> pcompaction_files;

  std::vector<TraditionalCompactionArgs*> tcargs_set;
  std::vector<PartnerCompactionArgs*> pcargs_set;
  
  SequenceNumber smallest_snapshot;
  if (snapshots_.empty()) {
    smallest_snapshot = versions_->LastSequence();
  } else {
    smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  mutex_.Unlock();
  
  if(t_sptcompactions.size() == 0 &&
          p_sptcompactions.size() == 0 ) {
      DEBUG_T("DealWithSingleCompaction, inputs1 size is 0\n");
      CompactionState* compact = new CompactionState(c);
      compact->smallest_snapshot = smallest_snapshot;
      status = DealWithSingleCompaction(compact);
      DEBUG_T("after DealWithSingleCompaction\n");
      
      mutex_.Lock();
      
      if(status.ok()) {
          status = InstallCompactionResults(compact);
      }
      CleanupCompaction(compact);
      DEBUG_T("after CleanupCompaction\n");
      return status;
  }

  DEBUG_T("----------traditional compaction, has----------\n");
  if(t_sptcompactions.size() > 0) {
    versions_->MergeTSplitCompaction(c, t_sptcompactions, tcompactionlist);
    for(int i = 0; i < t_sptcompactions.size(); i++){ 
            tcompaction_index.push_back(t_sptcompactions[i]->inputs1_index);
        delete t_sptcompactions[i];
    }
    DEBUG_T("add traditional task size %d to threadpool\n", tcompactionlist.size());
    for(int i = 0; i < tcompactionlist.size(); i++) {
        TraditionalCompactionArgs* tcargs = new TraditionalCompactionArgs;
        tcargs->db = this;
        tcargs->compact = new CompactionState(c);
        tcargs->compact->smallest_snapshot = smallest_snapshot;
        tcargs->t_sptcompaction = tcompactionlist[i];
        thpool_->AddJob(DoTraditionCompactionWork, tcargs); 
        t_compactionstate_list.push_back(tcargs->compact);
        tcargs_set.push_back(tcargs);
    }
  }

	DEBUG_T("----------partner compaction, has:----------\n");
	if(p_sptcompactions.size() > 0) {
      for(int i = 0; i < p_sptcompactions.size(); i++) {
        FileMetaData* fm = c->input(1, p_sptcompactions[i]->inputs1_index);
        int number = fm->number;
        pcompaction_files.push_back(number);
        DEBUG_T("%d, ", p_sptcompactions[i]->inputs1_index);
        PartnerCompactionArgs* pcargs = new PartnerCompactionArgs;
        pcargs->db = this;
        pcargs->compact = new PartnerCompactionState(c);
        pcargs->compact->smallest_snapshot = smallest_snapshot;
        pcargs->p_sptcompaction = p_sptcompactions[i];
        thpool_->AddJob(DoPartnerCompactionWork,	pcargs);
        p_compactionstate_list.push_back(pcargs->compact);
        pcargs_set.push_back(pcargs);
      }
  }
  thpool_->WaitAll();

	DEBUG_T("---------------finish in SplitCompaction----------------\n\n");

  mutex_.Lock();
  
  Status tstatus, pstatus;
  for(int i = 0; i < tcargs_set.size(); i++) {
    if(tstatus.ok()){
     tstatus = tcargs_set[i]->s; 
    }
    delete tcargs_set[i];
  }

  for(int i = 0; i < pcargs_set.size(); i++) {
    if(pstatus.ok()){
     pstatus = pcargs_set[i]->s; 
    }
     delete pcargs_set[i];
  }

  VersionEdit edit;
  if(tstatus.ok() && pstatus.ok()) {
    versions_->AddInputDeletions(&edit, c, tcompaction_index);
  } 
  
  if(tstatus.ok()) {
     AddFileWithTraditionalCompaction(&edit, t_compactionstate_list);
  }else {
    DEBUG_T("traditional shutting down....\n");
  }

  if(pstatus.ok()){
    UpdateFileWithPartnerCompaction(&edit, pcompaction_files, p_compactionstate_list);
  } else {
     DEBUG_T("partner shutting down....\n");
  }
  DEBUG_T("-------------finish delete and add-----------\n\n");

  if((tcompactionlist.size() > 0 && tstatus.ok()) || 
      (p_sptcompactions.size() > 0 && pstatus.ok())) {
    versions_->LogAndApply(&edit, &mutex_);
  }

  DEBUG_T("-------------finish LogAndApply-----------\n\n");
  
  for(int i = 0; i < tcompactionlist.size(); i++) {
      delete tcompactionlist[i];
      CleanupCompaction(t_compactionstate_list[i]);
  }


	for(int i = 0; i < p_sptcompactions.size(); i++) {
		delete p_sptcompactions[i];
    CleanupCompaction(p_compactionstate_list[i]);
  }

  DEBUG_T("-------------finish DoSplitCompactionWork-----------\n\n");

  record_timer(DO_SPLITCOMPACTION_WORK); 
  if(!tstatus.ok())
    return tstatus;
  if(!pstatus.ok())
    return pstatus;
	return Status::OK();
}

// Status DBImpl::DoSplitCompactionWork(Compaction* c) {
//   std::vector<SplitCompaction*> t_sptcompactions;
//   std::vector<SplitCompaction*> p_sptcompactions;
//   Status status;
//   start_timer(DO_SPLITCOMPACTION_WORK); 
//   DEBUG_T("---------------start in SplitCompaction----------------\n");
//   versions_->GetSplitCompactions(c, t_sptcompactions,
//                   p_sptcompactions);

//   DEBUG_T("t_sptcompactions size:%d, p_sptcompaction size:%d\n",
//           t_sptcompactions.size(), p_sptcompactions.size());

//   std::vector<TSplitCompaction*> tcompactionlist; 
//   std::vector<CompactionState*> t_compactionstate_list;
//   std::vector<PartnerCompactionState*> p_compactionstate_list;
//   std::vector<int> tcompaction_index;
//   std::vector<uint64_t> pcompaction_files;
  
//   SequenceNumber smallest_snapshot;
//   if (snapshots_.empty()) {
//     smallest_snapshot = versions_->LastSequence();
//   } else {
//     smallest_snapshot = snapshots_.oldest()->sequence_number();
//   }

//   mutex_.Unlock();
  
//   if(t_sptcompactions.size() == 0 &&
//           p_sptcompactions.size() == 0 ) {
//       DEBUG_T("DealWithSingleCompaction, inputs1 size is 0\n");
//       return status;
//   }

//   DEBUG_T("----------traditional compaction, has----------\n");
//   if(t_sptcompactions.size() > 0) {
//     versions_->MergeTSplitCompaction(c, t_sptcompactions, tcompactionlist);
//     for(int i = 0; i < t_sptcompactions.size(); i++){ 
//       tcompaction_index.push_back(t_sptcompactions[i]->inputs1_index);
//       delete t_sptcompactions[i];
//     }
//     DEBUG_T("add traditional task size %d to threadpool\n",
//                 tcompactionlist.size());
//     for(int i = 0; i < tcompactionlist.size(); i++) {
//         TraditionalCompactionArgs* tcargs = new TraditionalCompactionArgs;
//         tcargs->db = this;
//         tcargs->compact = new CompactionState(c);
//         tcargs->compact->smallest_snapshot = smallest_snapshot;
//         tcargs->t_sptcompaction = tcompactionlist[i];
//         t_compactionstate_list.push_back(tcargs->compact);
//         DealWithTraditionCompaction(tcargs->compact, tcargs->t_sptcompaction);
//     }
//   }

// 	DEBUG_T("----------partner compaction, has:----------\n");
// 	if(p_sptcompactions.size() > 0) {
//       for(int i = 0; i < p_sptcompactions.size(); i++) {
//         FileMetaData* fm = c->input(1, p_sptcompactions[i]->inputs1_index);
//         int number = fm->number;
//         pcompaction_files.push_back(number);
//         DEBUG_T("%d,\n", p_sptcompactions[i]->inputs1_index);
//         PartnerCompactionArgs* pcargs = new PartnerCompactionArgs;
//         pcargs->db = this;
//         pcargs->compact = new PartnerCompactionState(c);
//         pcargs->compact->smallest_snapshot = smallest_snapshot;
//         p_compactionstate_list.push_back(pcargs->compact);
//         pcargs->p_sptcompaction = p_sptcompactions[i];
//         DealWithPartnerCompaction(pcargs->compact, pcargs->p_sptcompaction);
//       }
//   }

// 	DEBUG_T("---------------finish in SplitCompaction----------------\n\n");

//   mutex_.Lock();
  
//   for(int i = 0; i < tcompactionlist.size(); i++) {
//       delete tcompactionlist[i];
//   }

// 	for(int i = 0; i < p_sptcompactions.size(); i++) {
// 		delete p_sptcompactions[i];
//   }

//   DEBUG_T("-------------finish DoSplitCompactionWork-----------\n\n");

//   record_timer(DO_SPLITCOMPACTION_WORK); 
// 	return Status::OK();
// }
////////////////meggie


Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  ///////////////meggie
  // if(compact->compaction->level() >= 1) {
  //   DoSplitCompactionWork(compact->compaction);
  // }
  ///////////////meggie

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  start_timer(DO_COMPACTION_WORK);
  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  ///////////meggie
  // if(compact->compaction->level() == 0) {
  //   DEBUG_T("before compaction:\n");
  //   versions_->PrintLevel01();
  //   DEBUG_T("compaction files:\n");
  //   DEBUG_T("input0:\n");
  //   for(int i = 0; i < compact->compaction->num_input_files(0); i++){
  //     FileMetaData* fm = compact->compaction->input(0, i);
  //     DEBUG_T("number:%llu, smallest:%s, largest:%s\n", fm->number, 
  //           fm->smallest.user_key().ToString().c_str(),
  //           fm->largest.user_key().ToString().c_str());
  //   }
  //   DEBUG_T("input1:\n");
  //   for(int i = 0; i < compact->compaction->num_input_files(1); i++){
  //     FileMetaData* fm = compact->compaction->input(1, i);
  //     DEBUG_T("input1, number:%llu, smallest:%s, largest:%s\n", fm->number, 
  //           fm->smallest.user_key().ToString().c_str(),
  //           fm->largest.user_key().ToString().c_str());
  //   }
  // }
  //////////meggie

  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    ////////////meggie
    //level0 bucket
    //if (has_imm_.NoBarrier_Load() != nullptr) {
    // if (compact->compaction->level() != 0 && has_imm_.NoBarrier_Load() != nullptr) {
    // ///////////////////meggie
    //   const uint64_t imm_start = env_->NowMicros();
    //   mutex_.Lock();
    //   if (imm_ != nullptr) {
    //     CompactMemTable();
    //     // Wake up MakeRoomForWrite() if necessary.
    //     background_work_finished_signal_.SignalAll();
    //   }
    //   mutex_.Unlock();
    //   imm_micros += (env_->NowMicros() - imm_start);
    // }
    if(compact->compaction->level() != 0 && NeedsCompaction()) {
        const uint64_t imm_start = env_->NowMicros();
        mutex_.Lock();
        CompactMultiMemTable();
        background_work_finished_signal_.SignalAll();
        mutex_.Unlock();
        imm_micros += (env_->NowMicros() - imm_start);
    }
    ////////////meggie

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());
	    ////////////meggie
      // AddKeyToHyperLogLog(compact->current_output()->hll, key);
      // compact->current_output()->hll_add_count++;
	    ////////////meggie

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }

  ///////////meggie
  // if(compact->compaction->level() == 0) {
  //   DEBUG_T("after compaction:\n");
  //   versions_->PrintLevel01();
  // }
  //////////meggie

  record_timer(DO_COMPACTION_WORK);
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) { }
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  ///////////////meggie
  // MemTable* mem = mem_;
  // MemTable* imm = imm_;
  // mem->Ref();
  // if (imm != nullptr) imm->Ref();
  ///////////////meggie
  Version* current = versions_->current();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    //////////////meggie
    //if (mem->Get(lkey, value, &s)) {
      // Done
    //} else if (imm != nullptr && imm->Get(lkey, value, &s)) {
    //} else
    if (FindInMemTable(lkey, value, &s)) {
      // Done
    //////////////meggie
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
      MaybeScheduleCompaction();
  }
  ///////////////meggie
  // mem->Unref();
  // if (imm != nullptr) imm->Unref();
  ////////////meggie
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != nullptr
       ? static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number()
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        ////////////meggie
        //status = WriteBatchInternal::InsertInto(updates, mem_);
        status = WriteBatchInternal::InsertInto(updates, mem_group_);
        ////////////meggie
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}


/////////////////meggie
Status DBImpl::Write(const WriteOptions& options, bool multi, 
                    MultiWriteBatch* my_batch) { 
  DEBUG_T("in multi writer\n");
  Writer w(&mutex_);
  w.mbatch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);//将writer对象插入到writers队列中
  while (!w.done && &w != writers_.front()) {//处于等待状态，直至当前的writer对象处于队列头部，或者因为合并处理完
    w.cv.Wait();
  }
  if (w.done) {//如果因为合并处理完了，那就返回
    return w.status;
  }
  //DEBUG_T("start deal with writer\n");
  //当前的writer对象处于队列头部
  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == nullptr);//为新的键值对腾出空间
  uint64_t last_sequence = versions_->LastSequence();//获取最后一个序列号
  Writer* last_writer = &w;//将当前的writer对象设置为last_writer，表示上一个处理的writer对象
  //DEBUG_T("to build batch group\n");
  if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
    MultiWriteBatch* updates = BuildMultiBatchGroup(&last_writer);
    //传入last_writer, 最后返回last_writer中writer对象，表示上一个处理的writer对象
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    uint64_t updates_num = WriteBatchInternal::Count(updates);
    DEBUG_T("to insert into mulit memtable, kv num:%lld\n", updates_num);
    last_sequence += updates_num;

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      //status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      //bool sync_error = false;
      //if (status.ok() && options.sync) {
      //  status = logfile_->Sync();
      //  if (!status.ok()) {
      //    sync_error = true;
      //  }
      //}
      if (status.ok()) {
		    DEBUG_T("to insert into mulit memtable, kv num:%lld\n", updates_num);
        status = WriteBatchInternal::InsertInto(updates, mem_group_, batch_thpool_);
		    DEBUG_T("after inserted into mulit memtable\n");
      }
      mutex_.Lock();
      //if (sync_error) {
      //  // The state of the log file is indeterminate: the log record we
      //  // just added may or may not show up when the DB is re-opened.
      //  // So we force the DB into a mode where all future writes fail.
      //  RecordBackgroundError(status);
      //}
    }
    if (updates == tmp_mbatch_) tmp_mbatch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

MultiWriteBatch* DBImpl::BuildMultiBatchGroup(Writer** last_writer) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    Writer* first = writers_.front();
    MultiWriteBatch* result = first->mbatch;
    assert(result != nullptr);
    
    size_t size = WriteBatchInternal::ByteSize(first->mbatch);
    
    size_t max_size = 1 << 20;//获取允许的最大字节数，1MB
    if (size <= (128<<10)) {//如果小于128KB
      max_size = size + (128<<10);//那令最大字节数为所含字节数+128KB 
    }

    *last_writer = first;//设置
    std::deque<Writer*>::iterator iter = writers_.begin();//从writers_队列头部进行遍历
    ++iter;  // Advance past "first"，从第二个writer对象开始遍历，跳过了first
    for (; iter != writers_.end(); ++iter) {//将当前的writers_队列中的所有writer对象进行合并
      Writer* w = *iter;
      if (w->sync && !first->sync) {//如果两个writer对象要求不一样的sync状态，那就不进行合并
        // Do not include a sync write into a batch handled by a non-sync write.
        break;
      }
      if (w->batch != nullptr) {//如果当前的Writer对象的batch不为nullptr
        size += WriteBatchInternal::ByteSize(w->mbatch);//记录合并后所有writer对象字节数目之和
        if (size > max_size) {//如果大于最大值，那就不合并
          // Do not make batch too big
          break;
        }

        // Append to *result
        if (result == first->mbatch) {//如果是第一个batch
          // Switch to temporary batch instead of disturbing caller's batch
          result = tmp_mbatch_;//利用一个tmp_batch_来存放合并后的batch
          assert(WriteBatchInternal::Count(result) == 0);
          WriteBatchInternal::Append(result, first->mbatch);//将第一个和后面的第二个合并在一起  
        }
        WriteBatchInternal::Append(result, w->mbatch);
      }
      *last_writer = w;
    }
    return result;
}

bool DBImpl::NeedsFlush(std::vector<int>& flush_index) {
    for(int i = 0; i < memtable_size_; i++) {
        if(mem_group_[i]->ApproximateMemoryUsage() > 
                options_.write_buffer_size)
            flush_index.push_back(i);
    }
    int flush_size = flush_index.size();
    if(flush_size == 0)
        return false;
    else 
        return true;
}

inline bool DBImpl::NeedsWait(std::vector<int>& flush_index) {
   for(int i = 0; i < flush_index.size(); i++) {
     if(has_imm_group_[flush_index[i]].NoBarrier_Load() != nullptr)
      return true;
   }
   return false;
}

void DBImpl::CreateNewMemtable(std::vector<int>& flush_index) {
    for(int i = 0; i < flush_index.size(); i++) {
        int index = flush_index[i];
        imm_group_[index] = mem_group_[index];
        has_imm_group_[index].Release_Store(imm_group_[index]);
        mem_group_[index] = new MemTable(internal_comparator_);
        mem_group_[index]->isNVMMemtable = false;
        mem_group_[index]->Ref();
        remaining_mems_--;
    } 
}

bool DBImpl::NeedsCompaction() {
    for(int i = 0; i < memtable_size_; i++) {
        if(imm_group_[i] != nullptr)
            return true;
    }
    return false;
}

//multi mem
struct compaction_struct {
    DB* db;
    MemTable* mem;
    DBImpl::MultiCompactionState* compact;
};

void DBImpl::ParallelCompactMemTableWrapper(void* args) {
    compaction_struct* cs = reinterpret_cast<compaction_struct*>(args);
    DBImpl* db = reinterpret_cast<DBImpl*>(cs->db);
    db->ParallelCompactMemTable(cs->mem, cs->compact);
}

void DBImpl::ParallelCompactMemTable(MemTable* mem, MultiCompactionState* compact) {
    start_timer(PARALLEL_COMPACTION_MEMTABLE);
    Iterator* iter = mem->NewIterator();
    iter->SeekToFirst();
    InternalKey this_smallest, this_largest;
    bool first = true;

    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      //DEBUG_T("iter, key:%s\n", key.ToString().c_str());
      InternalKey ikey;
      ikey.DecodeFrom(key);
      if(first) {
        this_smallest = ikey;
        first = false;
      } 
      this_largest = ikey;
      //uint64_t add_start = env_->NowMicros();
      compact->partner_table->Add(key, iter->value());
      //uint64_t add_end = env_->NowMicros();
      //DEBUG_T("level0 add need time:%d\n", add_end - add_start);
    }

    // Check for iterator errors
    Status s = iter->status();
    //TODO
    if (s.ok()) {
      s = compact->partner_table->Finish();
    } else {
      compact->partner_table->Abandon();
    }

    /////线程同步
    compact->partner_table->bailout = true;
    DEBUG_T("-------spt meta:%p, notify bail out----------\n");
    compact->partner_table->meta_available_var.notify_one();
    std::unique_lock<std::mutex> job_lock(compact->partner_table->wait_mutex);
    DEBUG_T("-------spt meta:%p, waiting meta thread finished----------\n", compact->partner_table->meta_);
    compact->partner_table->wait_var.wait(job_lock, [compact]()->bool{return static_cast<bool>(compact->partner_table->finished);});
    DEBUG_T("-------spt meta:%p, meta thread finished----------\n", compact->partner_table->meta_);
    job_lock.unlock();
    //end 线程同步

    compact->file_size = compact->partner_table->FileSize();
    compact->meta_usage = compact->partner_table->NVMSize();
    DEBUG_T("after finish partner table, file number:%llu, file size: %llu, meta usage:%zu\n",
            compact->number, compact->file_size, compact->meta_usage); 

    
    if(compact->new_create || internal_comparator_.Compare(this_smallest, compact->smallest) < 0) {
          compact->smallest = this_smallest;
    }
    if(compact->new_create || internal_comparator_.Compare(this_largest, compact->largest) > 0) {
          compact->largest = this_largest;
    }

    delete compact->partner_table;
    compact->partner_table = nullptr;

    // Finish and check for file errors
    if (s.ok()) {
      s = compact->outfile->Sync();
    }
    if (s.ok()) {
      s = compact->outfile->Close();
    }
    delete compact->outfile;
    compact->outfile = nullptr;

    delete iter;
    record_timer(PARALLEL_COMPACTION_MEMTABLE);
}

//partner table
Status DBImpl::OpenPartnerTableWithLevel0(MultiCompactionState* mcompact, char prefix, FileMetaData* fm) {
  //需要在write level0时获取当前前缀对应的FileMetaData*,如果不存在，为nullptr
  uint64_t file_number;
  DEBUG_T("open prefix:%c, partner table\n", prefix);
  if(fm != nullptr) {
    mcompact->number = fm->number;
    mcompact->smallest = fm->smallest;
    mcompact->largest = fm->largest;
    mcompact->file_size = fm->file_size;
    mcompact->meta_number = fm->meta_number;
    mcompact->meta_size = fm->meta_size;
    mcompact->pm = fm->pm;
    mcompact->new_create = false;
  } else {
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    mcompact->number = file_number;
    mcompact->smallest.Clear();
    mcompact->largest.Clear();
    mcompact->file_size = 0;
    mcompact->meta_number = versions_->NewFileNumber();
    pending_outputs_.insert(mcompact->meta_number);
    mcompact->meta_size = 80 << 10 << 10;
    std::string metaFile = MapFileName(dbname_nvm_, mcompact->meta_number);
    ArenaNVM* arena = new ArenaNVM(mcompact->meta_size, &metaFile, false);
    arena->nvmarena_ = true;
    //compact->pm = new PartnerMeta(internal_comparator_, arena, false);
    mcompact->pm = std::make_shared<PartnerMeta>(internal_comparator_, arena, false);
  }
  std::string fname = TableFileName(dbname_, mcompact->number);
  Status s = env_->NewWritableFile(fname, &mcompact->outfile, true);
  DEBUG_T("after new writable file\n");
  if (s.ok()) {
      DEBUG_T("to create single partner table\n");
      TableBuilder* builder = new TableBuilder(options_, mcompact->outfile, mcompact->number, mcompact->file_size);
      SinglePartnerTable* spt = new SinglePartnerTable(builder, mcompact->pm.get());
      mcompact->partner_table = spt;
      assert(mcompact->partner_table != nullptr);
      DEBUG_T("------open partner table, number:%llu-------\n", mcompact->number);
      //开启meta线程
      SavePartnerMetaArgs* spm = new SavePartnerMetaArgs;
      spm->db = this;
      spm->spt = spt;
      env_->StartThread(SavePartnerMeta, spm);
      DEBUG_T("----------start thread----------\n");
  }
  return s;
}

void DBImpl::CompactMultiMemTable() {
    mutex_.AssertHeld();
   
    start_timer(COMPACT_MULTI_MEMTABLE);

    VersionEdit edit;
    Status s;
    Version* base = versions_->current();
    base->Ref();

    //获取各个前缀对应的FileMetaData;
    std::map<char, FileMetaData*> fmmp;
    versions_->GetLevel0FileMeta(fmmp);

    int count = 0; 
    compaction_struct* cs[memtable_size_];
    for(int i = 0; i < memtable_size_; i++) {
        if(imm_group_[i] == nullptr){
            cs[i] = nullptr;
            continue;
        } else {
            cs[i] = new compaction_struct;
            ///////需要针对每个memtable都绑定一个partner table, 即open一个partner table
            cs[i]->mem = imm_group_[i];
            cs[i]->db = this;
            MultiCompactionState* mcompact = new MultiCompactionState();
            char prefix = '0' + i + 1;
            Status s = OpenPartnerTableWithLevel0(mcompact, prefix, fmmp[prefix]);
            if(!s.ok()) {
              break;
            }
            cs[i]->compact = mcompact;
            count++;
        }
    }
    
    mutex_.Unlock();
    for(int i = 0; i < memtable_size_; i++) {
        if(cs[i] != nullptr)
            compact_thpool_->AddJob(ParallelCompactMemTableWrapper, cs[i]);
    }
    DEBUG_T("wait memtables compaction (numbers:%d) finished...\n", count);
    compact_thpool_->WaitAll();
    DEBUG_T("memtables compaction has finished\n");
    mutex_.Lock();
    
    int level = 0;
    for(int i = 0; i < memtable_size_; i++) {
        if(cs[i] != nullptr) {
            char prefix = '0' + i + 1;
            MultiCompactionState* compact = cs[i]->compact;
            const Slice min_user_key = compact->smallest.user_key();
            const Slice max_user_key = compact->largest.user_key();
            if(compact->new_create) {
              pending_outputs_.erase(compact->number);
              pending_outputs_.erase(compact->meta_number);
              DEBUG_T("add level0 file, number:%llu, smallest:%s, largest:%s\n", 
                    compact->number, 
                    compact->smallest.user_key().ToString().c_str(),
                    compact->largest.user_key().ToString().c_str());
              edit.AddFile(0, compact->number, compact->file_size, compact->smallest, 
                          compact->largest, compact->meta_number, compact->meta_size, 
                          compact->meta_usage, compact->pm, prefix);
            }
            else {
              DEBUG_T("update level0 file, number:%llu, smallest:%s, largest:%s\n", 
                      compact->number, 
                      compact->smallest.user_key().ToString().c_str(),
                      compact->largest.user_key().ToString().c_str());
              edit.UpdateFile(0, compact->number, compact->file_size, compact->smallest, 
                                  compact->largest, compact->meta_usage);
            }
            delete compact;
        } 
    }
    
    base->Unref();
    if(s.ok() && shutting_down_.Acquire_Load()) {
        s = Status::IOError("Deleting DB during memtable compaction");
    }

    if(s.ok()) {
        edit.SetPrevLogNumber(0);
        edit.SetLogNumber(logfile_number_);
        s = versions_->LogAndApply(&edit, &mutex_);
    }
    
    if(s.ok()) {
        for(int i = 0; i < memtable_size_; i++) {
            if(cs[i] != nullptr) {
                imm_group_[i]->Unref();
                imm_group_[i] = nullptr;
                has_imm_group_[i].Release_Store(nullptr);
                remaining_mems_++;
                delete cs[i];
            }
            DeleteObsoleteFiles();
        }
    } else {
        RecordBackgroundError(s);
    }

    record_timer(COMPACT_MULTI_MEMTABLE);
}

bool DBImpl::FindInMemTable(LookupKey& lkey, std::string* value, Status* s) {
   Slice user_key = lkey.user_key();
   int index = user_key.data()[4] - '0' - 1;
   //DEBUG_T("user_key:%s, index:%d\n", 
   //        user_key.ToString().c_str(), index);
   assert(mem_group_[index] != nullptr);
   MemTable* mem = mem_group_[index];
   if(mem->Get(lkey, value, s))
       return true;
   else {
       MemTable* imm;
       if(imm_group_[index] != nullptr){
           imm = imm_group_[index];
           if(imm->Get(lkey, value, s))
               return true;
       }
   }
   return false;
}
/////////////////meggie


// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  /////////////meggie
  std::vector<int> flush_index;
  /////////////meggie
  while (true) {
    /////////////meggie
    flush_index.clear();
    /////////////meggie
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        ////////////meggie
        //versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
        versions_->BytesLevel0NVM() >= config::kL0_SlowdownWritesTrigger) {
        ////////////meggie
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               //(mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) 
               //////////////meggie
               !NeedsFlush(flush_index)) {
               //////////////meggie
      // There is room in current memtable
      break;
    //////////////meggie
    //} else if (imm_ != nullptr) {
    } else if (NeedsWait(flush_index)) {
    //////////////meggie
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (
      ////////////meggie
      //versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      versions_->BytesLevel0NVM() >= config::kL0_StopWritesTrigger) {
      ////////////meggie
      // There are too many level-0 files.
      //Log(options_.info_log, "Too many L0 files; waiting...\n");
      Log(options_.info_log, "L0 nvm bytes is too large; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);

      //////////////meggie
      // imm_ = mem_;
      // has_imm_.Release_Store(imm_);
      // mem_ = new MemTable(internal_comparator_);
      // ////////////meggie
      // mem_->isNVMMemtable = false;
      // ////////////meggie
      // mem_->Ref();
      DEBUG_T("to CreateNewMemtable\n");
      CreateNewMemtable(flush_index);
      ////////////meggie

      force = false;   // Do not force another compaction if have room
      DEBUG_T("immutable memtable need to flush...\n");
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr, const std::string& dbname_nvm) {
  *dbptr = nullptr;

  /////////////meggie
  DBImpl* impl = new DBImpl(options, dbname, dbname_nvm);
  /////////////meggie
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);

  ///////////////meggie
  //if (s.ok() && impl->mem_ == nullptr) {
  //  // Create new log and a corresponding memtable.
  //  uint64_t new_log_number = impl->versions_->NewFileNumber();
  //  WritableFile* lfile;
  //  s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
  //                                   &lfile);
  //  if (s.ok()) {
  //    edit.SetLogNumber(new_log_number);
  //    impl->logfile_ = lfile;
  //    impl->logfile_number_ = new_log_number;
  //    impl->log_ = new log::Writer(lfile);
  //    impl->mem_ = new MemTable(impl->internal_comparator_);
  //    impl->mem_->isNVMMemtable = false;
  //    impl->mem_->Ref();
  //  }
  //}

  if(s.ok() && impl->mem_allocated_ == false) {
      uint64_t new_log_number = impl->versions_->NewFileNumber();
      WritableFile* lfile;
      s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      
      for(int i = 0; i < impl->memtable_size_; i++) {
          impl->mem_group_[i] = new MemTable(impl->internal_comparator_);
          impl->mem_group_[i]->isNVMMemtable = false;
          impl->mem_group_[i]->Ref();
      }
      impl->mem_allocated_ = true;
  }
  ///////////////meggie

  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    //////////////meggie
    //assert(impl->mem_ != nullptr);
    assert(impl->mem_allocated_);
    //////////////meggie
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options,const std::string& dbname_nvm) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  //////////////meggie
  std::vector<std::string> filenames_nvm;
  DEBUG_T("to delete file in nvm\n");
  //////////////meggie
  //获取dbname目录下的所有文件
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  //////////////meggie、
  //获取dbname_nvm目录下的1所有文件
  result = env->GetChildren(dbname_nvm, &filenames_nvm);
  if(!result.ok())
        return Status::OK();
  //两个目录下的文件合在一起
  filenames.insert(filenames.end(), filenames_nvm.begin(), 
          filenames_nvm.end());
  //////////////meggie

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      //遍历两个目录下的文件，如果满足后缀（.map, .ldb等）， 那就在相应目录下删除
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del;
        /////////////////meggie
        if(find(filenames_nvm.begin(), filenames_nvm.end(), filenames[i]) != filenames_nvm.end()){
            //fprintf(stderr, "nvm filenames:%s, delete\n", filenames[i].c_str());
            del = env->DeleteFile(dbname_nvm + "/" + filenames[i]);
        }
        else 
        /////////////////meggie
            del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
    ///////////////meggie
    env->DeleteDir(dbname_nvm);
    ///////////////meggie
  }
  return result;
}

}  // namespace leveldb
