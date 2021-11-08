#ifndef INDEX_HANDLER_H
#define INDEX_HANDLER_H

#include "index_scan.h"
#include "../buf_manager/buf_manager.h"
#include "./btree/btree.h"

class IndexHandler {
private:
    BufManager* bm;
    int id;
    Btree<int>* btree;

public:
    IndexHandler();                    
    ~IndexHandler();
    int createIndex(const char *fileName);
    int destroyIndex(const char *fileName);
    int openIndex(const char *fileName); 
    int closeIndex();
    int search(int lowerBound, int upperBound, IndexScan &indexScan);            
                                             // 查找某个范围内的记录，结果通过迭代器访问
    int deleteRecord(int lowerBound, int upperBound);       
                                             // 删除某个范围内的记录
    int insertRecord(int key, const RID &rid); 
                                             // 插入某个记录的位置
    int updateRecord(int oldKey, const RID &oldRid, int newKey, const RID &newRid);
                                             // 更新特定记录的关键字或位置
};

#endif

