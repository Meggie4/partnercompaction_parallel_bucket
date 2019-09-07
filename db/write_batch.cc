// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"
////////////meggie
#include "util/threadpool_new.h"
////////////meggie

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const {
  return rep_.size();
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  virtual void Put(const Slice& key, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }

  ////////////////meggie
  virtual void Put(SequenceNumber sequence, const Slice& key, const Slice& value) {  
    mem_->Add(sequence, kTypeValue, key, value);
  }
  virtual void Delete(SequenceNumber sequence, const Slice& key) {
    mem_->Add(sequence, kTypeDeletion, key, Slice());
  }
  ////////////////meggie
};
}  // namespace

///////////////meggie
static int GetKeyPrefixIndex(const Slice& key) {
   char start = key.data()[4];
   int index = start - '0' - 1;
   return index;
}

Status WriteBatch::Iterate(std::vector<Handler*>& handler_group) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  
  assert(handler_group.size() > 0);
  SequenceNumber sequence = reinterpret_cast<MemTableInserter*>(
          handler_group[0])->sequence_;

  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          int index = GetKeyPrefixIndex(key);
          handler_group[index]->Put(sequence, key, value);
          //DEBUG_T("put, MemTable %d, key:%s\n", index, key.ToString().c_str());
          sequence++;
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          int index = GetKeyPrefixIndex(key);
          handler_group[index]->Delete(sequence, key);
          //DEBUG_T("delete, MemTable %d, key:%s\n", index, key.ToString().c_str());
          sequence++;
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}
///////////////meggie

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::Append(const WriteBatch &source) {
  WriteBatchInternal::Append(this, &source);
}

Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

///////////////////meggie
void MultiWriteBatch::Put(const Slice& key, const Slice& value) {
   int index = GetKeyPrefixIndex(key);
   batches_[index].Put(key, value);
}

void MultiWriteBatch::Delete(const Slice& key) {
    int index = GetKeyPrefixIndex(key);
    batches_[index].Delete(key);
}

void MultiWriteBatch::Clear() {
    for(int i = 0; i < batch_size_; i++) {
        batches_[i].Clear();
    }
}

size_t MultiWriteBatch::ApproximateSize() const {
    size_t res = 0;
    for(int i = 0; i < batch_size_; i++) {
        res += batches_[i].ApproximateSize();
    }
    return res;
}

void MultiWriteBatch::Append(const MultiWriteBatch& source) {
    WriteBatchInternal::Append(this, &source);
}

struct WriteBatchInternal::batch_struct {
    const WriteBatch* batch;
    WriteBatch::Handler* handler;
    Status s;
};

void WriteBatchInternal::DealWithBatchIterator(void* args) {
    batch_struct* bs = reinterpret_cast<batch_struct*>(args);
    const WriteBatch* batch = bs->batch;
    WriteBatch::Handler* handler = bs->handler;
    bs->s = batch->Iterate(handler);
}

Status WriteBatchInternal::InsertInto(const MultiWriteBatch* mbatch, 
        std::vector<MemTable*>& memtable_group, ThreadPool* thpool) {
    assert(memtable_group.size() == mbatch->batch_size_);
    int sz = memtable_group.size();
    batch_struct bs[sz];
    
    for(int i = 0; i < sz; i++) {
        MemTableInserter* inserter = new MemTableInserter;
        inserter->sequence_ = WriteBatchInternal::Sequence(&(mbatch->batches_[i]));
        inserter->mem_ = memtable_group[i];
        
        bs[i].batch = &(mbatch->batches_[i]);
        bs[i].handler = inserter;
        
        thpool->AddJob(DealWithBatchIterator, &bs[i]);
    }
    DEBUG_T("wait batch thpool finished.....\n");
    thpool->WaitAll();
    DEBUG_T("batch thpool has finished!\n");
    
    for(int i = 0; i < sz; i++) {    
        delete bs[i].handler;
    }
    
    return Status::OK();
}

Status WriteBatchInternal::InsertInto(const WriteBatch* b, 
        std::vector<MemTable*>& memtable_group) {
    std::vector<WriteBatch::Handler*> inserter_group;
    int sz = memtable_group.size();
    for(int i = 0; i < sz; i++) {
        MemTableInserter* inserter = new MemTableInserter;
        inserter->sequence_ = WriteBatchInternal::Sequence(b);
        inserter->mem_ = memtable_group[i];
        inserter_group.push_back(inserter);
    }
    
    Status s = b->Iterate(inserter_group);
    for(int i = 0; i < sz; i++) {    
        delete inserter_group[i];
    }
    return s;
}

void WriteBatchInternal::Append(MultiWriteBatch* dst, const MultiWriteBatch* src) {
  assert(dst->batch_size_ == src->batch_size_);
  int sz = dst->batch_size_;
  for(int i = 0; i < sz; i++) {
      Append(&dst->batches_[i], &src->batches_[i]);
  }
}

void WriteBatchInternal::SetSequence(MultiWriteBatch* b, SequenceNumber seq) {
  int sz = b->batch_size_;
  for(int i = 0; i < sz; i++) {
    SetSequence(&b->batches_[i], seq);
  }
}
///////////////////meggie

}  // namespace leveldb
