// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "util/debug.h"
////////////meggie
#include "util/hyperloglog.h"
////////////meggie

namespace leveldb {
////////////meggie
static void AddKeyToHyperLogLog(std::shared_ptr<HyperLogLog>& hll, const Slice& key) {
	const Slice& user_key = ExtractUserKey(key);
    int64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
    hll->AddHash(hash);
}
////////////meggie

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    ////////////meggie
    TableBuilder* builder = new TableBuilder(options, file, meta->number);
    ////////////meggie
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      //DEBUG_T("add key to level sstable\n", key.ToString().c_str());
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());
	    ////////meggie
	    // AddKeyToHyperLogLog(meta->hll, key);
	    // meta->hll_add_count++;
      ////////meggie
    }
	
    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      //DEBUG_T("file_size:%zu\n", meta->file_size);
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
