#ifndef INDEX_HANDLER_H
#define INDEX_HANDLER_H

#include <cstring>
#include "../buf_manager/buf_manager.h"
#include "ix.h"

class IndexHandler {
private:
    BufManager* bm;
    int id;
    short attrType;
    int attrLength;
    char* std_zero;
    void getNode(int pageID, int slotID, IndexPage* retPage);
    int keyCompare(void* lhs, void* rhs);
    void insertData(IndexPage* node, void* val, const RID& rid);
    void splitPage(IndexPage* node, void* val);
    void writeBack(IndexPage* node);
    int maxIndex(IndexPage* node);
    void shiftRight(IndexPage* node, int st, int num);
    IndexPage createPage(int pageID, int slotID);
public:
    IndexHandler(BufManager*);                    
    ~IndexHandler();
    int createIndex(const char *fileName, short attrType, int attrLen);
    int destroyIndex(const char *fileName);
    int openIndex(const char *fileName); 
    int closeIndex();
    void insertIndex(void* val, const RID& pos);
};

#endif

