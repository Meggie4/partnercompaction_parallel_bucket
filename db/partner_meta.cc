#include "db/partner_meta.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "db/skiplist.h"
#include "port/cache_flush.h"
#include <cstdio>
#include <gnuwrapper.h>
#include <string>
#include "util/debug.h"

namespace leveldb {
    static Slice GetLengthPrefixedSlice(const char* data) {
        uint32_t len;
        const char* p = data;
        p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
        return Slice(p, len);
    }
    
    void* PartnerMeta::operator new(std::size_t sz) {
        return malloc(sz);
    } 

    void* PartnerMeta::operator new[](std::size_t sz) {
        return malloc(sz);
    }

    void PartnerMeta::operator delete(void* ptr) {
        free(ptr);
    }

    PartnerMeta::PartnerMeta(const InternalKeyComparator& cmp, 
        ArenaNVM* arena, bool recovery) 
        : comparator_(cmp), 
          arena_(arena), 
          //bloom_(BLOOMSIZE, BLOOMHASH),
          meta_(comparator_, arena, recovery){          
    }

    PartnerMeta::~PartnerMeta() {
        if(arena_){
            DEBUG_T("before delete arena\n");
            delete arena_;
            DEBUG_T("after delete arena\n");
        }
    }

    // void PartnerMeta::AddPredictIndex(std::unordered_set<std::string> *set, const uint8_t* data) {
    //     this->bloom_.add(data, strlen((const char*)data));
    // }

    // int PartnerMeta::CheckPredictIndex(std::unordered_set<std::string> *set, const uint8_t* data) {
    //     return this->bloom_.possiblyContains(data, (size_t)strlen((const char*)data));
    // }

    // //TODO: Implement prediction clear
    // void PartnerMeta::ClearPredictIndex(std::unordered_set<std::string> *set) {
    // }

    size_t PartnerMeta::ApproximateMemoryUsage() {
        return arena_->MemoryUsage();
    }

    int PartnerMeta::KeyComparator::operator()(const char* aptr, 
            const char* bptr) const {
        Slice a = GetLengthPrefixedSlice(aptr);
        Slice b = GetLengthPrefixedSlice(bptr);
        return comparator.Compare(a, b);
    }

    static const char* EncodeKey(std::string* scratch, const Slice& target) {
        scratch->clear();
        PutVarint32(scratch, target.size());
        scratch->append(target.data(), target.size());
        return scratch->data();
    }

    class PartnerMetaIterator: public Iterator {
    public:
        explicit PartnerMetaIterator(PartnerMeta::Meta* meta) : iter_(meta) { }
        virtual bool Valid() const { return iter_.Valid(); }
        virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
        virtual void SeekToFirst() { 
            //DEBUG_T("before PartnerMetaIterator seek to first\n");
            iter_.SeekToFirst(); 
            //DEBUG_T("after PartnerMetaIterator seek to first\n");
        }
        virtual void SeekToLast() { iter_.SeekToLast(); }
        virtual void Next() { iter_.Next(); }
        virtual void Prev() { iter_.Prev(); }

#ifdef USE_OFFSETS
        virtual char *GetNodeKey(){return reinterpret_cast<char *>((intptr_t)iter_.node_ - (intptr_t)iter_.key_offset()); }
#else
        virtual char *GetNodeKey(){return iter_.key(); }
#endif

#if defined(USE_OFFSETS)
        virtual Slice key() const { 
            //DEBUG_T("partner meta before get inode\n");
            intptr_t inode = (intptr_t)iter_.node_;
            //DEBUG_T("partner meta after get inode\n");
            intptr_t ioffset = (intptr_t)iter_.key_offset();
            //DEBUG_T("partner meta after get key offset\n");
            intptr_t skey =  inode - ioffset;
            //DEBUG_T("partner meta after get skey\n");
            return GetLengthPrefixedSlice(reinterpret_cast<const char *>(skey)); 
        }
#else
        virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
#endif
        virtual Slice value() const {
// #if defined(USE_OFFSETS)
//             Slice key_slice = GetLengthPrefixedSlice(reinterpret_cast<const char *>((intptr_t)iter_.node_ - (intptr_t)iter_.key_offset()));
// #else
//             Slice key_slice = GetLengthPrefixedSlice(iter_.key());
// #endif
//             return Slice(key_slice.data() + key_slice.size(), 16);
            uint32_t key_length;
#if defined(USE_OFFSETS)
            const char* entry = reinterpret_cast<const char*>((intptr_t)iter_.node_ - 
                                                (intptr_t)iter_.key_offset());
                                                #else 
            const char* entry = iter_.key();
#endif
            const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);

            uint64_t offset, size;
            offset = DecodeFixed64(key_ptr + key_length);
            size = DecodeFixed64(key_ptr + key_length + 8); 

            // Slice encode = Slice(key_ptr + key_length, 16);
            // offset = DecodeFixed64(key_ptr + key_length);
            // size = DecodeFixed64(key_ptr + key_length + 8); 

            //DEBUG_T("meta iter, offset is %llu, size is %llu\n", offset, size);
            return Slice(key_ptr + key_length, 16);
        }
        //NoveLSM
        //virtual void SetHead(void *ptr) { iter_.SetHead(ptr); }
        void* operator new(std::size_t sz) {
            return malloc(sz);
        }

        void* operator new[](std::size_t sz) {
            return malloc(sz);
        }
        void operator delete(void* ptr)
        {
            free(ptr);
        }
        virtual Status status() const { return Status::OK(); }

    private:
        PartnerMeta::Meta::Iterator iter_;
        std::string tmp_;       // For passing to EncodeKey

        // No copying allowed
        PartnerMetaIterator(const PartnerMetaIterator&);
        void operator=(const PartnerMetaIterator&);
    };

    Iterator* PartnerMeta::NewIterator() {
        return new PartnerMetaIterator(&meta_);
    }

    //这里的partner number指的是0~9中， 以及data的offset以及data block的大小
    void PartnerMeta::Add(const Slice& key, uint64_t block_offset, uint64_t block_size) {
       ///存储的形式是：key_size + key + block_offset(64) + block_size(64)
       size_t key_size = key.size();
       const size_t encoded_len = VarintLength(key_size) + key_size + 8 + 8;
       char* buf = nullptr;
       
       buf = ((ArenaNVM*)arena_)->AllocateAlignedNVM(encoded_len);
       
       if(!buf) {
           perror("Memory allocation failed");
           exit(-1);
       }
    
       //add prediction
    //    char *keystr = (char*)user_key.data();
    //    keystr[user_key.size()]=0;
    //    AddPredictIndex(&predict_set_, (const uint8_t *)keystr);

       char* p = EncodeVarint32(buf, key_size);
       
       //DEBUG_T("add key is %s\n", key.ToString().c_str());
       memcpy_persist(p, key.data(), key_size);
        
       p += key_size;
       
       //DEBUG_T("add offset is %llu, add block size is:%llu\n", block_offset, block_size);
       EncodeFixed64(p, block_offset);
       p += 8;
       EncodeFixed64(p, block_size);
       p += 8;
       assert(p - buf == encoded_len);

       meta_.Insert(buf);
    }

    bool PartnerMeta::Get(const LookupKey& key, uint64_t* block_offset, uint64_t* block_size, Status* s) {
        Slice memkey = key.memtable_key();
        Meta::Iterator iter(&meta_);
        //DEBUG_T("memtable key is %s\n", memkey.ToString().c_str());
        Env* env = Env::Default();
        const uint64_t seek_start = env->NowMicros();
        iter.Seek(memkey.data());
        
        if(iter.Valid()) {
            //DEBUG_T("find!\n");
#if defined(USE_OFFSETS)
            const char* entry = reinterpret_cast<const char*>((intptr_t)iter.node_ - 
                                                (intptr_t)iter.key_offset());
#else 
            const char* entry = iter.key();
#endif
            uint32_t key_length;
            const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
            if(comparator_.comparator.user_comparator()->Compare(Slice(key_ptr, key_length - 8), 
                        key.user_key()) == 0) {
                uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
                switch (static_cast<ValueType>(tag & 0xff)) {
                case kTypeValue: {
                    *block_offset = DecodeFixed64(key_ptr + key_length);
                    *block_size = DecodeFixed64(key_ptr + key_length + 8); 
                    //DEBUG_T("offset, is %d, size:%llu\n", *block_offset, *block_size);
                    //DEBUG_T("partner meta seek need time:%llu\n", env->NowMicros() - seek_start);
                    return true;
                }
                case kTypeDeletion:
                    *s = Status::NotFound(Slice());
                    return true;
                }
            }
        }
        //DEBUG_T("not find!\n");
        return false;
    }
                
}
