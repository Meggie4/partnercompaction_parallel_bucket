//test for partner table

#include "db/partner_table_builder.h"
#include "util/testharness.h"
#include "db/partner_meta.h"
#include "db/filename.h"
#include "util/arena.h"
#include "util/debug.h"
#include "leveldb/env.h"
#include <stdint.h>
#include "leveldb/db.h"
#include "db/db_impl.h"

namespace leveldb {
    class PartnerTableTest {};
    TEST(PartnerTableTest, AddSimple) {
        InternalKeyComparator cmp(BytewiseComparator());
        Env* env = Env::Default();
        std::string nvm_path;
        env->GetMEMDirectory(&nvm_path);
        std::string indexFile = MapFileName(nvm_path, 1);
        size_t indexFileSize = (4 << 10) << 10;
        DEBUG_T("after get mapfilename:%s\n", indexFile.c_str());
        ArenaNVM* arena = new ArenaNVM(indexFileSize, &indexFile, false);
        arena->nvmarena_ = true;
        DEBUG_T("after get arena nvm\n");
        PartnerMeta* pm = new PartnerMeta(cmp, arena, false);
        pm->Ref();
        std::string dbname;
        env->GetTestDirectory(&dbname);
        //假设partners的公共前缀为abc， 当添加abcd时，选择一个新的partner, 添加abcf选择另外一个新的partner
        //也就是大于这个partners的为公共前缀
        const std::string& common_prefix = "abc";
        PartnerTableBuilder* pbuilder = new PartnerTableBuilder(common_prefix, pm);
        //然后传入新的builder, 比如文件0001, 对应前缀为abcd, 那为这个前缀的partner，都加入到这个partner
        std::string prefix = "abcd";
        std::string fname = TableFileName(dbname, 1);
        WritableFile* file;
        Status s = env->NewWritableFile(fname, &file);
        if (!s.ok()) {
            DEBUG_T("new writable file failed\n");
        }
        Options option = leveldb::Options();
        TableBuilder* builder = new TableBuilder(option, file, 1);
        //add builder和前缀
        pbuilder->AddTableBuilder(prefix, builder);
        LookupKey lkey(Slice("abcdmykey"), 0);
        //根据前缀获取builder
        Slice ikey = lkey.user_key();
        std::string myprefix = pbuilder->GetPrefix(ikey);
        TableBuilder* mybuilder = pbuilder->GetTableBuilder(myprefix);
        Slice value("this is my key");
        if(mybuilder == NULL) {
            DEBUG_T("need to get new builder\n");
        }
        DEBUG_T("there is old builder\n");
        pbuilder->Add(mybuilder, lkey.internal_key(), value);
        pbuilder->Finish();
        delete pbuilder;
    }
}

int main() {
    leveldb::test::RunAllTests();
}