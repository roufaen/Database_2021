#include "index_handler.h"
int IndexScan::getKey(char* key){
    RID temp = currentNode->data[currentKeyPos].keyPos;
    if (tree->keyFile->getRecord(temp, key) == -1) {
        std::cerr << "Did not get the key " << temp.pageID << " " << temp.slotID << std::endl;
        return -1;
    };
    return 0;
}

RID IndexScan::getValue(){
    if(currentNode->data[currentKeyPos].count == 1) return currentNode->data[currentKeyPos].value;
    // std::cout << "Enter #0 " << currentValuePos << " " << currentNode->data[currentKeyPos].count << std::endl;
    if(currentValuePos == 0 || currentOverflowPage == nullptr) {
        currentCumulation = 0;
        // std::cout << "Enter #1 " << currentNode->data[currentKeyPos].value.pageID <<  std::endl;
        int index; //index is useless
        currentOverflowPage = (BPlusOverflowPage*) tree->treeFile->getPage(currentNode->data[currentKeyPos].value.pageID, index);
    }
    while(currentCumulation + currentOverflowPage->recs <= currentValuePos){
        currentCumulation += currentOverflowPage->recs;
        // std::cout << "Enter #2 " << currentCumulation << " " << currentOverflowPage->recs << std::endl;
        int index;//index is useless
        currentOverflowPage = (BPlusOverflowPage*) tree->treeFile->getPage(currentOverflowPage->nextPage, index);
    }

    while(currentCumulation > currentValuePos){
        currentCumulation -= currentOverflowPage->recs;
        // std::cout << "Enter #3 " << currentCumulation << " " << currentOverflowPage->recs << std::endl;
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
    int c=  currentNode-> recs;
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
    // unused variable
    // int c=  currentNode-> recs;
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
            currentKeyPos = currentNode->recs - 1;
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

bool IndexScan::equals(const IndexScan& that){
    if(!available()) return false;
    return ((currentKeyPos == that.currentKeyPos) && (currentValuePos == that.currentValuePos) && (currentNode == that.currentNode));
}
