#ifndef INDEX_SCAN_H
#define INDEX_SCAN_H
#include "index_handler.h"
class IndexScan{
public:
    IndexScan(IndexHandler* ih, BPlusNode* bn, int keyn, int valn):tree(ih), currentNode(bn), currentKeyPos(keyn), currentValuePos(valn)
    {}

    key_ptr getKey();
    int getValue();
    void next();
    void previous();
    void setToBegin();
    bool equals(const IndexScan &other);

private:
    void nextKey();
    void previousKey();
    bool available();
    IndexHandler* tree;
    BPlusNode* currentNode;
    BPlusOverflowPage* currentOverflowPage;
    int currentKeyPos, currentValuePos, currentCumulation;
};
#endif
