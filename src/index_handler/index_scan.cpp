#include "index_handler.h"
int IndexScan::getKey(char* key){
    RID temp = currentNode->data[currentKeyPos].keyPos;
    if (tree->keyFile->getRecord(temp, key) == -1) {
        std::cerr << "Did not get the key " << temp.pageID << " " << temp.slotID << std::endl;
        return -1;
    };
    return 0;
}

void IndexScan::revaildate(){
    int index; //index is useless
    if(currentNodeId>0) currentNode = (BPlusNode*)tree->treeFile->getPage(currentNodeId, index);
    if(currentOverflowPageId>0 && currentOverflowPage!=nullptr) 
        currentOverflowPage = (BPlusOverflowPage*)tree->treeFile->getPage(currentOverflowPageId, index);
}

RID IndexScan::getValue(){
    if (currentNodeId<=0) return RID{0,0};
    revaildate();
    // char* nowdata = new char[MAX_RECORD_LEN];
    // getKey(nowdata);
    // std::cout << currentNodeId << "GETVALUE" << *((int*)nowdata) << std::endl;
    if(currentNode->data[currentKeyPos].count == 1) return currentNode->data[currentKeyPos].value;
    if(currentOverflowPage == nullptr) {
        currentCumulation = 0;
        int index; //index is useless
        currentOverflowPageId = currentNode->data[currentKeyPos].value.pageID;
        currentOverflowPage = (BPlusOverflowPage*) tree->treeFile->getPage(currentOverflowPageId, index);
    }
    while(currentCumulation + currentOverflowPage->recs <= currentValuePos){
        currentCumulation += currentOverflowPage->recs;
        if(currentOverflowPage->recs == 0) {
            cout << "Enter #2 " << currentOverflowPageId << " " << currentOverflowPage->recs << std::endl;
            exit(-1);
        }
        int index;//index is useless
        currentOverflowPageId = currentOverflowPage->nextPage;
        currentOverflowPage = (BPlusOverflowPage*) tree->treeFile->getPage(currentOverflowPageId, index);
    }

    while(currentCumulation > currentValuePos){
        currentCumulation -= currentOverflowPage->recs;
        // std::cout << "Enter #3 " << currentCumulation << " " << currentOverflowPage->recs << std::endl;
        int index;//index is useless
        currentOverflowPageId = currentOverflowPage->prevPage;
        currentOverflowPage = (BPlusOverflowPage*) tree->treeFile->getPage(currentOverflowPageId, index);
    }

    return currentOverflowPage->data[currentValuePos - currentCumulation];
}

void IndexScan::next(){
    int c = currentNode->data[currentKeyPos].count;
    // std::cout << "OVER 400 " << currentValuePos << " " << c << std::endl;
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
        if(currentNodeId == tree->treeFile->header->lastLeaf) {
            currentNode = nullptr;
            currentNodeId = -1;
            currentKeyPos = 0;
            currentValuePos = 0;
        }else {
            int index; //index is useless
            currentNodeId = nextPage;
            this->currentNode = (BPlusNode*) tree->treeFile->getPage(nextPage, index);
            currentKeyPos = 0;
            currentValuePos = 0;
            currentOverflowPage = nullptr;
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
    if(currentKeyPos){
        currentKeyPos--;
        revaildate();
        currentValuePos = currentNode->data[currentKeyPos].count - 1;
    }else{
        int prevPage = currentNode -> prevPage;
        //std::cout << "Prevpage is " << prevPage << " " << tree->treeFile->header->firstLeaf << std::endl;
        if(currentNodeId == tree->treeFile->header->firstLeaf) {
            currentNode = nullptr;
            currentNodeId = -1;
            currentKeyPos = 0;
            currentValuePos = 0;
        }else {
            int index; //index is useless
            currentNodeId = prevPage;
            currentNode = (BPlusNode*) tree->treeFile->getPage(prevPage, index);
            currentKeyPos = currentNode->recs - 1;
            currentValuePos = currentNode->data[currentKeyPos].count - 1;
        }
    }
}

void IndexScan::setToBegin(){
    currentKeyPos = 0;
    currentValuePos = 0;
    currentCumulation = 0;
    currentOverflowPageId = 0;
    currentOverflowPage = nullptr;
    int index; //index is useless
    currentNodeId = tree->treeFile->header->firstLeaf;
    currentNode = (BPlusNode*)tree->treeFile->getPage(currentNodeId, index);
}

void IndexScan::setToEnd(){
    currentCumulation = 0;
    currentOverflowPageId = 0;
    currentOverflowPage = nullptr;
    int index; //index is useless
    currentNodeId = tree->treeFile->header->lastLeaf;
    currentNode = (BPlusNode*)tree->treeFile->getPage(currentNodeId, index);
    currentKeyPos = currentNode->recs - 1;
    currentValuePos = currentNode->data[currentKeyPos].count - 1;
}

bool IndexScan::equals(const IndexScan& that){
    if(!available()) return false;
    return ((currentKeyPos == that.currentKeyPos) && (currentValuePos == that.currentValuePos) && (currentNode == that.currentNode));
}
