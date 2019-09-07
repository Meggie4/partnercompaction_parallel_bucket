// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/debug.h"
#include "db/dbformat.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  if (s.ok()) {
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
}

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value, 
                             void* input_handle) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  /////////////meggie
  Status s;
  if(input_handle != nullptr) {
    handle = *(reinterpret_cast<BlockHandle*>(input_handle));
    s = Status::OK();
  } else {
    Slice input = index_value;
    s = handle.DecodeFrom(&input);
  }
  /////////////meggie
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  if (s.ok()) {
    //DEBUG_T("handle decode success\n");
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        //从底层读取block
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        } else {
          DEBUG_T("condition1, read block failed, status: %s\n", s.ToString().c_str());
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      } else {
        DEBUG_T("condition2, read block failed\n");
      }
    }
  } else {
    DEBUG_T("handle decode failed\n");
  }


  Iterator* iter;
  if (block != nullptr) {
    //DEBUG_T("block is not nullptr\n");
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    DEBUG_T("block is nullptr\n");
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      //获取data block的迭代器
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*saver)(arg, block_iter->key(), block_iter->value());
      } 
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}


////////////////meggie
/*
  原始的sstable的block获取步骤如下：
  (1)读取footer
  (2)读取index block
  (3)根据index block，获取data block, 如果block cache中存在，那就直接获取，否则从file中获取
  (4)根据data block的迭代器seek得到
  针对partner table, 步骤如下：
  （1）由于nvm index中存在了data block编号，以及data block的偏移量
  （2）可以获取需要的data block
  （3）得到data block后，根据在data block中的偏移量， 获取相应的数据
*/
Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&), 
                          uint64_t block_offset, 
                          uint64_t block_size) {
      //获取data block的迭代器
      std::string dst;
      PutVarint64(&dst, block_offset);
      PutVarint64(&dst, block_size);
      //DEBUG_T("internal get, block offset is:%llu, block_size is %llu\n", block_offset, block_size);
      Iterator* block_iter = BlockReader(this, options, Slice(dst));
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        //DEBUG_T("block iter is valid\n");
        (*saver)(arg, block_iter->key(), block_iter->value());
      } 
      // else {
      //   DEBUG_T("block iter is not valid\n");
      // }
      Status s = block_iter->status();
      delete block_iter;
      return s;
}

Status Table::OpenPartnerTable(const Options& options,
                   RandomAccessFile* file,
                   Table** table) {
  *table = nullptr;

  Rep* rep = new Table::Rep;
  rep->options = options;
  rep->file = file;
  rep->index_block = nullptr;
  rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
  rep->filter_data = nullptr;
  rep->filter = nullptr;
  *table = new Table(rep);

  return Status::OK();
}

class Table::PartnerTableIterator : public Iterator {
public:
	PartnerTableIterator(Iterator* meta_iter, Table* table, const ReadOptions options)
		: meta_iter_(meta_iter),
		  table_(table),
      options_(options)
      {        // Marks as invalid
	}

  ~PartnerTableIterator() {
    if(meta_iter_ != nullptr) {
      delete meta_iter_;
    }
  }

	virtual bool Valid() const {
		return meta_iter_->Valid();
	}
	virtual void Seek(const Slice& target) {
		meta_iter_->Seek(target);
	}
	virtual void SeekToFirst() { 
    // if(meta_iter_ == nullptr) {
    //   DEBUG_T("meta iter is nullptr\n");
    // } else {
    //   //DEBUG_T("meta iter is not nullptr\n");
    // }
    meta_iter_->SeekToFirst(); 
    //DEBUG_T("after meta_iter_ seek to first, %p\n", this);
    //DEBUG_T("after again\n");
  }
	virtual void SeekToLast() { meta_iter_->SeekToLast(); }
	virtual void Next() {meta_iter_->Next();}
	virtual void Prev() { meta_iter_->Prev();}
	Slice key() const {
    //DEBUG_T("before partner table  iter get key\n");
	  Slice res = meta_iter_->key();
    return res;
	}
	Slice value() const {
    Slice block_info = meta_iter_->value();
    //DEBUG_T("block info is %s， c_str:%s\n", block_info.data(), block_info.ToString().c_str());
    const char* info = block_info.data();
    BlockHandle handle;
    handle.set_offset(DecodeFixed64(info));
    handle.set_size(DecodeFixed64(info + 8));
  
    //DEBUG_T("iter value, block offset is:%llu, block_size is %llu\n", handle.offset(), handle.size());
    Iterator* block_iter = BlockReader(table_, options_, Slice(), &handle);
    block_iter->Seek(meta_iter_->key());
    // if(!block_iter->Valid())
    //   DEBUG_T("block iter is unvalid\n");
    // DEBUG_T("value is %s\n", block_iter->value().ToString().c_str());
    // DEBUG_T("after get value\n");
    Slice val = block_iter->value();
    delete block_iter;
    return val;
	}
	virtual Status status() const { return Status::OK(); }
private:
	Iterator* meta_iter_;
  Table* table_;
  const ReadOptions options_;
};


Iterator* Table::NewPartnerIterator(const ReadOptions& options, Iterator* meta_iter) {
  return new PartnerTableIterator(meta_iter, this, options);
}
///////////////meggie

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
