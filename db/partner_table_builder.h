/**
 *used for kv in ssd, and index in nvm
 *
 *
 */
#ifndef STORAGE_DB_PARTNER_TABLE_H_
#define STORAGE_DB_PARTNER_TABLE_H_ 
#include <cstdio>
#include "leveldb/slice.h"
#include <map>
#include <string>
#include "db/partner_meta.h"
namespace leveldb {
    class PartnerMeta;
    class TableBuilder;
    class PartnerTableBuilder{
        public:
            PartnerTableBuilder(const std::string& common_prefix, PartnerMeta* meta);
            PartnerTableBuilder(const PartnerTableBuilder&) = delete;
            void operator=(const PartnerTableBuilder&) = delete;
            //~PartnerTableBuilder();
            
            void Add(TableBuilder* builder, const Slice& key, const Slice& value);
            //bool Get(const LookupKey& lkey,  std::string* value, Status* s);
            void Finish();
            void AddTableBuilder(std::string& prefix, TableBuilder* builder);
            TableBuilder* GetTableBuilder(const std::string& prefix);
            std::string GetPrefix(const Slice& key);
            
        private:
            struct info {
                uint64_t curr_blockoffset_;
                uint64_t curr_blocksize_;
                std::vector<Slice> queue_;
            };
            void insertMeta(TableBuilder* builder);
            PartnerMeta* meta_;
            const std::string common_prefix_;
            //prefix对应的table builder
            std::map<std::string, TableBuilder*> builders_;
            //key对应的block offset
            std::map<TableBuilder*, info> curr_info_;
    };
}

#endif 
