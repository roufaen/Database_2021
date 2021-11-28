#ifndef INDEX_SCAN_H
#define INDEX_SCAN_H
#include "ix.h"
#include "index_handler.h"

class IndexScan{
public:
    IndexScan(IndexHandler* ih):tree(ih){}
    IndexScan(IndexHandler* ih, BPlusNode* bn, int keyn, int valn):tree(ih), currentNode(bn), currentKeyPos(keyn), currentValuePos(valn)
    {}

    char* getKey();
    RID getValue();
    void next();
    void previous();
    void setToBegin();
    bool equals(const IndexScan &other);
    inline bool available();
    void nextKey();
    void previousKey();

private:
    IndexHandler* tree;
    BPlusNode* currentNode;
    BPlusOverflowPage* currentOverflowPage;
    int currentKeyPos, currentValuePos, currentCumulation;
};
#endif
