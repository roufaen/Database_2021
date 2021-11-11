#ifndef IX_H
#define IX_H

#include <memory>

typedef std::shared_ptr<char> key_ptr;

const int maxIndexPerPage = 650;
enum NodeType {INTERNAL, LEAF, OVERFLOW};
enum DataType {INT, FLOAT, VARCHAR};

struct IndexRecord{
    int keyPos;
    int value;  
    int count;
};
    //for leaf page: count>1 -> overflowpage id; count==1 -> RID
    //for intermediate page: pointer right to this page

struct BPlusNode{
    NodeType nodeType;
    int pageId;
    int recCount;
    int prevPage;
    int nextPage;   //-1 for nonexistent
    IndexRecord data[maxIndexPerPage];
};

struct BPlusOverflowPage{
    NodeType nodeType;
    int pageId;
    int recCount;
    int prevPage;
    int nextPage;
    int fatherPage;
    int data[maxIndexPerPage];
};

struct IndexFileHeader{
    int rootPageId;
    int pageCount;
    int firstLeaf;
    int lastLeaf;
    int sum;
};

#endif