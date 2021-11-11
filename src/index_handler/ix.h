#ifndef IX_H
#define IX_H
#include "../utils/rid.h"

const int node_cap = 3;
struct IndexPage{
    short isLeaf;
    RID pos;
    RID child[node_cap << 1];
    RID parent;
    RID sibling;
    char* entry;
};

struct IndexScan{
    RID pos;
    int indexNo;
};
#endif