#include "index_scan.h"
key_ptr IndexScan::getKey(){
    RID temp = currentNode->data[currentKeyPos].keyPos;
    char* key;
    if (! tree->keyFile->getRecord(temp, key)) {
        std::cerr << "Did not get the key\n";
    };
    return make_shared<char>(key);
}

RID IndexScan::getValue(){
    if(currentNode->data[currentKeyPos].count == 1) return currentNode->data[currentKeyPos].value;
    if(currentValuePos == 0 || currentOverflowPage == nullptr) {
        currentCumulation = 0;
        int index; //index is useless
        currentOverflowPage = (BPlusOverflowPage*) tree->treeFile->getPage(currentNode->data[currentKeyPos].value.pageID, index);
    }

    while(currentCumulation + currentOverflowPage->recCount <= currentValuePos){
        currentCumulation += currentOverflowPage->recCount;
        int index;//index is useless
        currentOverflowPage = (BPlusOverflowPage*) tree->treeFile->getPage(currentOverflowPage->nextPage, index);
    }

    while(currentCumulation + currentOverflowPage->recCount > currentValuePos){
        currentCumulation -= currentOverflowPage->recCount;
        int index;//index is useless
        currentOverflowPage = (BPlusOverflowPage*) tree->treeFile->getPage(currentOverflowPage->prevPage, index);
    }

    return currentOverflowPage->data[currentValuePos - currentCumulation];
}

void IndexScan::next(){
    int c = currentNode->data[currentKeyPos].count;
    if(currentValuePos < c - 1) currentValuePos++;
        else nextKey();
}

void IndexScan::nextKey(){
    currentCumulation = 0;
    currentOverflowPage = nullptr;
    int c=  currentNode-> recCount;
    if(currentKeyPos < c-1){
        currentKeyPos++;
        currentValuePos = 0;
    }else{
        int nextPage = currentNode -> nextPage;
        if(nextPage <= 0) {
            currentNode = nullptr;
            currentKeyPos = 0;
            currentValuePos = 0;
        }else {
            int index; //index is useless
            this->currentNode = (BPlusNode*) tree->treeFile->getPage(nextPage, index);
            currentKeyPos = 0;
            currentValuePos = 0;
        }
    }
}

void IndexScan::previous(){
    if(currentValuePos) currentValuePos--;
        else previousKey();
}

void IndexScan::previousKey(){
    currentCumulation = 0;
    currentOverflowPage = nullptr;
    int c=  currentNode-> recCount;
    if(currentKeyPos){
        currentKeyPos--;
        currentValuePos = 0;
    }else{
        int prevPage = currentNode -> prevPage;
        if(prevPage <= 0) {
            currentNode = nullptr;
            currentKeyPos = 0;
            currentValuePos = 0;
        }else {
            int index; //index is useless
            this->currentNode = (BPlusNode*) tree->treeFile->getPage(prevPage, index);
            currentKeyPos = currentNode->recCount - 1;
            currentValuePos = 0;
        }
    }
}

void IndexScan::setToBegin(){
    currentKeyPos = 0;
    currentValuePos = 0;
    currentCumulation = 0;
    currentOverflowPage = nullptr;
    int index; //index is useless
    currentNode = (BPlusNode*)tree->treeFile->getPage(tree->treeFile->header->firstLeaf, index);
}

inline bool IndexScan::available(){
    return currentNode != nullptr;
}

bool IndexScan::equals(const IndexScan& that){
    if(!available()) return false;
    return ((currentKeyPos == that.currentKeyPos) && (currentValuePos == that.currentValuePos) && (currentNode == that.currentNode));
}
