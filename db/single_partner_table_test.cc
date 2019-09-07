#include "db/single_partner_table.h"
#include "util/testharness.h"
#include "db/partner_meta.h"
#include "db/filename.h"
#include "util/arena.h"
#include "util/debug.h"
#include "leveldb/env.h"
#include <stdint.h>
#include "leveldb/db.h"
#include "db/db_impl.h"
#include "db/table_cache.h"
#include "leveldb/slice.h"
#include <memory>

namespace leveldb {
    class SinglePartnerTableTest {};
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

    struct SavePartnerMetaArgs {
        //DBImpl* db;
        SinglePartnerTable* spt;
    }; 

    void DealWithPartnerMeta(SinglePartnerTable* spt) {
        DEBUG_T("-----------in thread save partner meta---------\n");
        while(!spt->bailout || !spt->meta_queue.empty()) {
        std::unique_lock<std::mutex> job_lock(spt->queue_mutex);
        while(!spt->bailout && spt->meta_queue.empty()){
            spt->meta_available_var.wait(job_lock, [spt]() ->bool { return spt->meta_queue.size() || spt->bailout;});
        }

        if(spt->bailout && spt->meta_queue.empty()){
            break;
        }

        std::pair<uint64_t, std::pair<uint64_t, uint64_t>> meta = spt->meta_queue.front();
        spt->meta_queue.pop_front();
        uint64_t key_size = meta.first;
        uint64_t block_offset = meta.second.first;
        uint64_t block_size = meta.second.second;
        int i = 0;
        while(i < key_size && !spt->key_queue.empty()) {
            std::string key = spt->key_queue.front();
            spt->key_queue.pop_front();
            //DEBUG_T("add spt:%p, key:%s, block_offset:%llu, block_size:%llu\n", spt, key.c_str(), block_offset, block_size);
            spt->meta_->Add(Slice(key), block_offset, block_size);
            i++;
        }
        }
    out:
        DEBUG_T("-----------out of thread save partner meta---------\n");
        //std::lock_guard<std::mutex> lck(spt->wait_mutex);
        spt->finished = true;
        spt->wait_var.notify_one();//唤醒正在等待任务完成的线程，表示有一个任务已经完成了
    }

    static void SavePartnerMeta(void* args) {
        SavePartnerMetaArgs* spm = reinterpret_cast<SavePartnerMetaArgs*>(args);
        //DBImpl* db = spm->db;
        DealWithPartnerMeta(spm->spt);
        delete spm;
    }

    TEST(SinglePartnerTableTest, AddSimple) {
        InternalKeyComparator cmp(BytewiseComparator());
        Env* env = Env::Default();
        std::string nvm_path;
        env->GetMEMDirectory(&nvm_path);
        uint64_t meta_number= 1, meta_size = (4 << 10) << 10;
        std::string indexFile = MapFileName(nvm_path, meta_number);
        DEBUG_T("after get mapfilename:%s\n", indexFile.c_str());
        ArenaNVM* arena = new ArenaNVM(meta_size, &indexFile, false);
        arena->nvmarena_ = true;
        DEBUG_T("after get arena nvm\n");
        std::shared_ptr<PartnerMeta> pm = std::make_shared<PartnerMeta>(cmp, arena, false);
        std::string dbname;
        env->GetTestDirectory(&dbname);
        std::string fname = TableFileName(dbname, 1);
        WritableFile* file;
        Status s = env->NewWritableFile(fname, &file);
        if (!s.ok()) {
            DEBUG_T("new writable file failed\n");
        }
        Options option = leveldb::Options();
        uint64_t file_number = 1;
        TableBuilder* builder = new TableBuilder(option, file, file_number);
        SinglePartnerTable* spt = new SinglePartnerTable(builder, pm.get());
        
        SavePartnerMetaArgs* spm = new SavePartnerMetaArgs;
        spm->spt = spt;
        env->StartThread(SavePartnerMeta, spm);
        DEBUG_T("----------start thread----------\n");
        
        Slice value1("this is my key1");
        Slice value2("this is my key1 again");
        Slice value3("this is my key3");
        Slice value4("this is my key4");
        Slice value5("this is my key5");
        LookupKey lkey1(Slice("abcdmykey1"), 0);
        LookupKey lkey2(Slice("abcdmykey1"), 1);
        LookupKey lkey3(Slice("abcdmykey3"), 0);
        LookupKey lkey4(Slice("abcdmykey4"), 0);
        LookupKey lkey5(Slice("abcdmykey5"), 0);
        spt->Add(lkey1.internal_key(), value1);
        //更新
        spt->Add(lkey2.internal_key(), value2);
        spt->Add(lkey3.internal_key(), value3);
        spt->Add(lkey4.internal_key(), value4);
        spt->Add(lkey5.internal_key(), value5);
        spt->Finish();

        spt->bailout = true;
        spt->meta_available_var.notify_one();
        std::unique_lock<std::mutex> job_lock(spt->wait_mutex);
        DEBUG_T("-------waiting meta thread finished----------\n");
        spt->wait_var.wait(job_lock, [spt]()->bool{return static_cast<bool>(spt->finished);});
        DEBUG_T("thread finished...\n");

        uint64_t file_size = spt->FileSize();
        delete spt;
        spt = nullptr;
        DEBUG_T("after first finish, file size is %llu........\n", file_size);
        
        // builder = new TableBuilder(option, file, file_number, file_size);
        // spt = new SinglePartnerTable(builder, pm.get());
        // Slice value6("this is my key6");
        // Slice value7("this is my key7");
        // Slice value8("this is my key8");
        LookupKey lkey6(Slice("abcdmykey6"), 0);
        // LookupKey lkey7(Slice("abcdmykey7"), 0);
        // LookupKey lkey8(Slice("abcdmykey8"), 0);
        // spt->Add(lkey6.internal_key(), value6);
        // spt->Add(lkey7.internal_key(), value7);
        // spt->Add(lkey8.internal_key(), value8);
        // spt->Finish();
        // delete spt;
        // spt = nullptr;
        // DEBUG_T("after second finish........\n");

        //读取数据
        //(TODO)可以把pm保存在filemetadata中，避免频繁地创建释放
        uint64_t block_offset, block_size;
        bool find = pm->Get(lkey3, &block_offset, &block_size, &s);
        TableCache* table_cache = new TableCache(dbname, option, option.max_open_files, nvm_path);
        ReadOptions roptions;
        std::string resValue;
        Saver saver;
        saver.state = kNotFound;
        saver.ucmp = cmp.user_comparator();
        saver.user_key = lkey3.user_key();
        saver.value = &resValue;
        if(find) {
            DEBUG_T("offset is %llu, block size:%llu\n", block_offset, block_size);
            s = table_cache->Get(roptions, file_number, lkey3.internal_key(), &saver, SaveValue, block_offset, block_size);
            //pm->Unref();
            DEBUG_T("get value3 %s\n", (*saver.value).c_str());
        } else {
            DEBUG_T("cannot find key from nvm skiplist\n");
        }

        DEBUG_T("after get key3......\n");

        //迭代器
        // Iterator* iter = table_cache->NewPartnerIterator(ReadOptions(), file_number, pm);
        // iter->SeekToFirst();
        // while(iter->Valid()) {
        //     DEBUG_T("key is %s, value is %s\n", iter->key().ToString().c_str(), iter->value().ToString().c_str());
        //     iter->Next();
        // }
        // pm->Unref();
        // delete iter;
    }
}

int main() {
    leveldb::test::RunAllTests();
}