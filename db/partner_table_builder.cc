#include "db/partner_table_builder.h"
#include "db/partner_meta.h"
#include "leveldb/env.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/table_builder.h"

/*
    partner table中包含多个partner number, vector<uint64> partnerNumbers
    get时，首先得到partner在partnerNumbers中的编号，从而获得文件编号，然后根据table cache获取所对应的data block以及在data block中的偏移
    add时，也是根据key的前面一个前缀，来获取相应的编号，首先写到相应的文件中，并获取相应的偏移，然后再写到index中
*/

namespace leveldb {
    PartnerTableBuilder::PartnerTableBuilder(const std::string& common_prefix, PartnerMeta* meta)
    : meta_(meta),
      common_prefix_(common_prefix){
    }

    void PartnerTableBuilder::Add(TableBuilder* builder, const Slice& key, const Slice& value) {
        //（1）首先根据key, 获取其应该存放的partner number,同时记录相应的block offset, 以及block size
        //重构Add函数，能够返回block offset， 同时当一个data block写完时，得到block size，将meta写入到nvm skiplist中，并且不需要编写元数据块
        uint64_t block_size = 0;
        uint64_t block_offset;
        bool res = builder->PartnerAdd(key, value, &block_offset, &block_size);
        if (!res) return;
        if(block_size != 0) {
            //（3）当一个block构造完毕，那就调用insertMeta将该block中的所有键值对的元信息插入到PartnerMeta中
            curr_info_[builder].curr_blockoffset_ = block_offset;
            curr_info_[builder].curr_blocksize_ = block_size;
            curr_info_[builder].queue_.push_back(key);
            insertMeta(builder);

        } else {
            DEBUG_T("data block offset is:%llu\n", block_offset);
            //block还没有构造完毕，加入到
            curr_info_[builder].curr_blockoffset_ = block_offset;
            curr_info_[builder].queue_.push_back(key);
        }
    }

    void PartnerTableBuilder::insertMeta(TableBuilder* builder) {
        for(auto key : curr_info_[builder].queue_) {
            //一个个插入
            meta_->Add(key, curr_info_[builder].curr_blockoffset_, curr_info_[builder].curr_blocksize_);
        }
        curr_info_[builder].queue_.clear();
    }

    //针对没有办法获得公共前缀的，那就直接只有一个partner，针对这个partner拥有一个nvm skiplist索引
    //先观察一下sstable的范围
    std::string PartnerTableBuilder::GetPrefix(const Slice& key) {
        std::string skey = key.ToString();
        size_t len = common_prefix_.size();
        if(len < skey.size()) {
            return skey.substr(0, len + 1);
        } else {
            return skey;
        }
    }

    //需要
    TableBuilder* PartnerTableBuilder::GetTableBuilder(const std::string& prefix) {
        DEBUG_T("prefix is %s\n", prefix.c_str());
        auto iter = builders_.find(prefix);
        if(iter != builders_.end()) {
            return iter->second;
        } else {
            //不存在的话，在外部申请文件，然后生成builder，传入进去
            return NULL;
        }
    }

    void PartnerTableBuilder::AddTableBuilder(std::string& prefix, TableBuilder* builder) {
        builders_.insert(std::make_pair(prefix, builder));
    }

    void PartnerTableBuilder::Finish() {
        //调用各个builder的finish操作，结束data block的构造
        //需要重构finish函数，不需要写入元数据
        for(auto iter = builders_.begin(); iter != builders_.end(); iter++) {
            //针对partner finish, 此时可以获取相应的data block size 
            DEBUG_T("to finish table builder\n");
            uint64_t block_size = 0;
            iter->second->PartnerFinish(&block_size);
            curr_info_[iter->second].curr_blocksize_ = block_size;
            insertMeta(iter->second);
        }
    }
}
