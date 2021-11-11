#ifndef IX_H
#define IX_H
#include "../utils/rid.h"

const int node_cap = 6;

enum DataType{
    INT,
    FLOAT,
    VARCHAR
};

struct IndexFileHeader{
    short type;
    int attrLength;
};

struct IndexPage{
    short isLeaf;
    int size;
    RID pos;
    RID child[node_cap];
    RID parent;
    RID sibling;
    char* entry;
};

struct IndexScan{
    RID pos;
    int indexNo;
};
#endif