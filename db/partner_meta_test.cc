//test partner Meta
#include "db/partner_meta.h"
#include "db/filename.h"
#include "util/testharness.h"
#include "util/arena.h"
#include "util/debug.h"
#include "leveldb/env.h"
#include <stdint.h>
#include <memory>

namespace leveldb {
    class PartnerMetaTest{};
    TEST(PartnerMetaTest, AddAndGet) {
        //获取比较器
        //fprintf(stderr, "before get mapfilename\n");
        DEBUG_T("before get mapfilename\n");
        InternalKeyComparator cmp(BytewiseComparator());
        Env* env = Env::Default();
        std::string nvm_path;
        env->GetMEMDirectory(&nvm_path);
        std::string metaFile = MapFileName(nvm_path, 1);
        size_t metaFileSize = (4 << 10) << 10;
        DEBUG_T("after get mapfilename:%s\n", metaFile.c_str());
        ArenaNVM* arena = new ArenaNVM(metaFileSize, &metaFile, false);
        arena->nvmarena_ = true;
        DEBUG_T("after get arena nvm\n");
        std::shared_ptr<PartnerMeta> pi = std::make_shared<PartnerMeta>(cmp, arena, false);
        DEBUG_T("after get partner Meta\n");
        //pi->Ref();
        LookupKey lkey(Slice("00000000mykey"), 0);
        pi->Add(lkey.internal_key(), 1, 0);
        DEBUG_T("after add key\n");
        uint64_t block_offset = 0;
        uint64_t block_size;
        Status s;
        LookupKey lkey2(Slice("00000000mykey"), 1);
        bool find = pi->Get(lkey, &block_offset, &block_size, &s);
        if(find) {
            DEBUG_T("offset is %llu, block size:%llu\n", block_offset, block_size);
        }
        //pi->Unref();
    }
}

int main() {
    leveldb::test::RunAllTests();
}