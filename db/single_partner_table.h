#ifndef STORAGE_DB_SINGLE_PARTNER_TABLE_H_
#define STORAGE_DB_SINGLE_PARTNER_TABLE_H_ 
#include <cstdio>
#include <map>
#include <list>
#include <string>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "leveldb/slice.h"
#include "db/partner_meta.h"
#include "port/port.h"
namespace leveldb {
    class PartnerMeta;
    class TableBuilder;
    class SinglePartnerTable{
        public:
            SinglePartnerTable(TableBuilder* builder, PartnerMeta* meta);
            SinglePartnerTable(const SinglePartnerTable&) = delete;
            void operator=(const SinglePartnerTable&) = delete;

            void Add(const Slice& key, const Slice& value);
            //bool Get(const LookupKey& lkey,  std::string* value, Status* s);
            Status Finish();
            void Abandon();
            uint64_t FileSize();
            size_t NVMSize();
            ~SinglePartnerTable();
            
            //for meta thread
            std::list<std::string> key_queue;
            std::list<std::pair<uint64_t, std::pair<uint64_t, uint64_t>>> meta_queue;
            std::atomic_int  num_in_block;
            //所有的key的加入完毕了
            std::atomic_bool  bailout;
            //等待，直到所有的任务都完成了
            std::atomic_bool  finished;
            std::mutex   queue_mutex;
            std::mutex   wait_mutex;
            std::condition_variable meta_available_var;
            std::condition_variable wait_var;

            PartnerMeta* meta_;

        private:
            void insertMeta();
            TableBuilder* builder_;
            uint64_t curr_blockoffset_;
            uint64_t curr_blocksize_;
            std::vector<std::string> queue_;
    };
}

#endif 
