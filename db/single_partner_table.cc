#include "db/single_partner_table.h"
#include "db/partner_meta.h"
#include "leveldb/env.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/table_builder.h"

namespace leveldb {
    //针对读取partner的情况，通过meta获取到data block以及offset， 通过table cache获取相应的数据， 传入block offset以及block size
    SinglePartnerTable::SinglePartnerTable(TableBuilder* builder, PartnerMeta* meta)
    : builder_(builder), 
      num_in_block(0),
      bailout(false),
      finished(false),
      meta_(meta){
    }

    SinglePartnerTable::~SinglePartnerTable() {
        delete builder_;
        //meta_->Unref();
    }

    void SinglePartnerTable::Add(const Slice& key, const Slice& value) {
        //（1）首先根据key, 获取其应该存放的partner number,同时记录相应的block offset, 以及block size
        //重构Add函数，能够返回block offset， 同时当一个data block写完时，得到block size，将meta写入到nvm skiplist中，并且不需要编写元数据块
        uint64_t block_size = 0;
        uint64_t block_offset;
        bool res = builder_->PartnerAdd(key, value, &block_offset, &block_size);
        if (!res) return;
        if(block_size != 0) {
            //（3）当一个block构造完毕，那就调用insertMeta将该block中的所有键值对的元信息插入到PartnerMeta中
            curr_blockoffset_ = block_offset;
            curr_blocksize_ = block_size;
            //DEBUG_T("add key queue, key:%s\n", key.ToString().c_str());
            
            key_queue.push_back(key.ToString());
            ++num_in_block;
            std::unique_lock<std::mutex> lck(queue_mutex);
            meta_queue.push_back(std::make_pair(static_cast<int>(num_in_block), std::make_pair(curr_blockoffset_, curr_blocksize_)));
            lck.unlock();
            meta_available_var.notify_one();
            //DEBUG_T("-------spt meta:%p, notify queue not empty------\n", meta_);
            num_in_block = 0;

            //DEBUG_T("flush block, offset:%llu, size:%llu\n", curr_blockoffset_, curr_blocksize_);
            // queue_.push_back(key.ToString());
            // insertMeta();
        } else {
            //DEBUG_T("data block offset is:%llu\n", block_offset);
            //block还没有构造完毕，加入到
            curr_blockoffset_ = block_offset;
            //queue_.push_back(key.ToString());
            
            key_queue.push_back(key.ToString());
            ++num_in_block;
        }
    }

    void SinglePartnerTable::insertMeta() {
        if(queue_.empty())
            return;
        for(int i = 0; i < queue_.size(); i++) {
            //一个个插入
            //DEBUG_T("insertmeta, key is %s\n", queue_[i].c_str());
            meta_->Add(Slice(queue_[i]), curr_blockoffset_, curr_blocksize_);
        }
        queue_.clear();
    }

    Status SinglePartnerTable::Finish() {
        uint64_t block_size = 0;
        Status s = builder_->PartnerFinish(&block_size);
        if(block_size == 0) {
            return s;
        }
        curr_blocksize_ = block_size;
        
        std::unique_lock<std::mutex> lck(queue_mutex);
        meta_queue.push_back(std::make_pair(static_cast<int>(num_in_block), std::make_pair(curr_blockoffset_, curr_blocksize_)));
        lck.unlock();
        meta_available_var.notify_one();
        DEBUG_T("insert to queue,key count:%d and notify thread\n", static_cast<int>(num_in_block));
       
        //insertMeta();
        return s;
    }

    void SinglePartnerTable::Abandon() {
        builder_->Abandon();
    }

    uint64_t SinglePartnerTable::FileSize() {
        return builder_->FileSize();
    }

    size_t SinglePartnerTable::NVMSize() {
        return meta_->ApproximateMemoryUsage();
    }
}