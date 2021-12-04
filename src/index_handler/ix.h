#ifndef IX_H
#define IX_H

#include "../utils/rid.h"
#include "index_file_handler.h"
#include "../record_handler/record_handler.h"
#include <memory>

typedef char* key_ptr;

const int maxIndexPerPage = 400;

namespace ix{
    enum NodeType {INTERNAL, LEAF, OVRFLOW};
    enum DataType {INT, FLOAT, VARCHAR};
}

struct IndexRecord{
    RID keyPos;
    RID value;  
    int count;
};
    //for leaf page: count>1 -> overflowpage id; count==1 -> RID
    //for intermediate page: pointer right to this page

struct BPlusNode{
    ix::NodeType nodeType;
    int pageId;
    int recs;
    int prevPage;
    int nextPage;   //-1 for nonexistent
    IndexRecord data[maxIndexPerPage];
};

struct BPlusOverflowPage{
    ix::NodeType nodeType;
    int pageId;
    int recs;
    int prevPage;
    int nextPage;
    int fatherPage;
    RID data[maxIndexPerPage];
};

struct IndexFileHeader{
    int rootPageId;
    int pageCount;
    int firstLeaf;
    int lastLeaf;
    int sum;
};

#endif