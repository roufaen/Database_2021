#include "index_handler.h"

int getLen(key_ptr key, VarType _type){
    switch (_type){
        case FLOAT: return 4;
        case INT: return 4;
        case VARCHAR: return strlen(key);
        default: return 0;
    }
}
int compare(VarType _type, key_ptr a, char* b){
    switch (_type) {
        case FLOAT: {
            float diff = (*(float*)a - *(float*) b);
            return (diff>0)?1:(diff==0)?0:-1;
        }
        case INT: {
            int diff = (*(int*)a - *(int*) b);
            return (diff>0)?1:(diff==0)?0:-1;
        }
        case VARCHAR: {
            int lena = strlen(a);
            int lenb = strlen(b);
            int len = min(lena, lenb);
            for(int i=0; i<len; i++){
                if(a[i]<b[i]) return -1;
                if(a[i]>b[i]) return 1;
            }
            if(lena > len) return 1;
            if(lenb > len) return -1;
            return 0;
        }
    }
    return 0;
}

void IndexHandler::openIndex(std::string _tableName, std::string _colName, VarType _type){
    tableName = _tableName;
    colName = _colName;
    type = _type;
    std::string treeFileName = tableName + colName + ".tree";
    std::string keyFileName = tableName + colName + ".key";
    if (keyFile->openFile(keyFileName) == -1) { //Still some bugs left, is record_hdl able to deal with no file? SHOULD HAVE BEEN FIXED
        keyFile->createFile(keyFileName);
        keyFile->openFile(keyFileName);
    }
    treeFile->openFile(treeFileName.c_str());
}

void IndexHandler::removeIndex(std::string _tableName, std::string _colName){
    std::string treeFileName = _tableName + _colName + ".tree";
    std::string keyFileName = _tableName + _colName + ".key";
    bm->removeFile(treeFileName.c_str());
    bm->removeFile(keyFileName.c_str());
}

void IndexHandler::insert(key_ptr key, RID rid){
    int rootIndex;
    BPlusNode* root = (BPlusNode*)treeFile->getPage(treeFile->header->rootPageId, rootIndex);
    //std::cout << "root is " << treeFile->header->rootPageId << std::endl;
    if(root->recs == maxIndexPerPage){
        if(root->nodeType == ix::NodeType::LEAF){
            treeFile->markPageDirty(rootIndex);
            int index; //index is useless
            BPlusNode* newRoot = (BPlusNode*)treeFile->newPage(index);
            treeFile->header->rootPageId = newRoot->pageId;
            newRoot->nodeType = ix::NodeType::INTERNAL;
            newRoot->nextPage = 0;
            newRoot->prevPage = 0;
            newRoot->recs = 2;
            treeFile->markPageDirty(index);

            BPlusNode* newNode = (BPlusNode*)treeFile->newPage(index);
            root->nextPage = newNode->pageId;
            root->recs >>= 1;
            newNode->prevPage = root->pageId;
            newNode->recs = maxIndexPerPage - root->recs;
            newNode->nodeType = ix::NodeType::LEAF;
            treeFile->markPageDirty(index);

            int st = root->recs;
            for(int i = root->recs; i < maxIndexPerPage; i++) 
                newNode->data[i-st] = root->data[i];
            newRoot->data[0].keyPos = RID{0,0};
            newRoot->data[0].value = RID{root->pageId,0};
            newRoot->data[1].keyPos = newNode->data[0].keyPos;
            newRoot->data[1].value = RID{newNode->pageId,0};

            newRoot->data[0].count = 0;
            for(int i=0; i < root->recs; i++)
                newRoot->data[0].count += root->data[i].count;

            newRoot->data[1].count = 0;
            for(int i=0; i < newNode->recs; i++)
                newRoot->data[1].count += newNode->data[i].count;

            treeFile->header->lastLeaf = newNode->pageId;
            treeFile->header->rootPageId = newRoot->pageId;
            insertIntoNonFullPage(key, rid, newRoot->pageId);
        } else if(root->nodeType == ix::NodeType::INTERNAL){
            treeFile->markPageDirty(rootIndex);
            int index; //index is useless
            BPlusNode* newRoot = (BPlusNode*)treeFile->newPage(index);
            treeFile->header->rootPageId = newRoot->pageId;
            newRoot->nodeType = ix::NodeType::INTERNAL;
            newRoot->nextPage = 0;
            newRoot->prevPage = 0;
            newRoot->recs = 2;
            treeFile->markPageDirty(index);

            BPlusNode* newNode = (BPlusNode*)treeFile->newPage(index);
            root->nextPage = 0;
            root->recs >>= 1;
            newNode->prevPage = 0;
            newNode->recs = maxIndexPerPage - root->recs + 1;
            newNode->nodeType = ix::NodeType::INTERNAL;
            treeFile->markPageDirty(index);

            newNode->data[0].count = 0; //Record the left most node
            newNode->data[0].keyPos = RID{0,0};
            int st = root->recs - 1;
            newNode->data[0].value = root->data[st].value;
            for(int i=root->recs; i<maxIndexPerPage; i++){
                newNode->data[i-st] = root->data[i];
            }
         
            newRoot->data[0].keyPos = RID{0,0};
            newRoot->data[0].value = RID{root->pageId,0};
            newRoot->data[1].keyPos = newNode->data[1].keyPos;
            newRoot->data[1].value = RID{newNode->pageId,0};
            
            newRoot->data[0].count = 0;
            for(int i=0; i < root->recs; i++)
                newRoot->data[0].count += root->data[i].count;

            newRoot->data[1].count = 0;
            for(int i=0; i < newNode->recs; i++)
                newRoot->data[1].count += newNode->data[i].count;

            insertIntoNonFullPage(key, rid, newRoot->pageId);

            treeFile->header->rootPageId = newRoot->pageId;
        }
        else{
            std::cerr << "No suck index B+ tree nodeType\n"; 
        }
    } else { //The root still has enough room
        insertIntoNonFullPage(key, rid, treeFile->header->rootPageId);
    }

    treeFile->header->sum++;
    treeFile->markHeaderPageDirty();
}

bool IndexHandler::has(key_ptr key){
    return (count(key) > 0);
}

int IndexHandler::count(key_ptr key){
    int ret = getCountIn(treeFile->header->rootPageId, key);
    //std::cout << *((int*)key) << " " << ret << std::endl;
    return ret;
}

int IndexHandler::lesserCount(key_ptr key){
    return getLesserCountIn(treeFile->header->rootPageId, key);
}

int IndexHandler::greaterCount(key_ptr key){
    return getGreaterCountIn(treeFile->header->rootPageId, key);
}

IndexScan IndexHandler::begin(){
    IndexScan ret(this);
    ret.setToBegin();
    return ret;
}

IndexScan IndexHandler::lowerBound(key_ptr key){ //Weakly small
    if(totalCount() == 0) return IndexScan(this);
    else return getLowerBound(treeFile->header->rootPageId, key);
}

IndexScan IndexHandler::upperBound(key_ptr key){ //Weakly big
    if(totalCount() == 0) return IndexScan(this);
    IndexScan it = lowerBound(key);
    if(it.available()) {
        it.getKey(nowdata);
        if(compare(type, key, nowdata) == -1) it.nextKey();
    }
    else it.setToBegin();
    return it;
}

IndexScan IndexHandler::lesserBound(key_ptr key){ //Strictly small
    if(totalCount() == 0) return IndexScan(this);
    IndexScan it = lowerBound(key);
    if(it.available()) {
        it.getKey(nowdata);
        if(compare(type, key, nowdata) == 0) it.previousKey();
    }
    else it.setToBegin();
    return it;
}

IndexScan IndexHandler::greaterBound(key_ptr key){ //Strictly big
    if(totalCount() == 0) return IndexScan(this);
    IndexScan it = upperBound(key);
    if(it.available()) {
        it.getKey(nowdata);
        if(compare(type, key, nowdata) == 0) it.nextKey();
    }
    else it.setToBegin();
    return it;
}

int IndexHandler::totalCount() {return treeFile->header->sum;}

void IndexHandler::remove(key_ptr key, RID rid){
    if(!has(key)){
        cerr << "No such key to remove\n";
        return;
    }
    // std::cout << "Remove here step #0" << std::endl;
    vector<RID> rids = getRIDs(key);
    bool flag = false;
    for(unsigned int i=0; i<rids.size(); i++){
        // std::cout << rids[i].pageID << " " <<  rids[i].slotID << std::endl;
        if(rids[i].pageID == rid.pageID && rids[i].slotID == rid.slotID) {
            flag = true;
            break;
        }
    }
    if(!flag) {
        cerr << "No such record to remove\n";
        return;
    }
    int rootIndex;
    BPlusNode* root = (BPlusNode*)treeFile->getPage(treeFile->header->rootPageId, rootIndex);
    // std::cout << "Remove here step #1" << std::endl;
    if((root->nodeType == ix::NodeType::INTERNAL) && (root->recs == 2)) {
        int index;
        BPlusNode* c0 = (BPlusNode*) treeFile->getPage(root->data[0].value.pageID, index);
        BPlusNode* c1 = (BPlusNode*) treeFile->getPage(root->data[1].value.pageID, index);
        if((c0->recs == c1->recs) && (c0->recs == (maxIndexPerPage >> 1))){
            // std::cout << "Remove here step #2" << std::endl;
            mergePage(root, 0);
            treeFile->header->rootPageId = root->data[0].value.pageID;
        }
    }
    treeFile->markPageDirty(rootIndex);
    // std::cout << "Remove here step #3" << std::endl;
    deleteFromLegalPage(key, rid, treeFile->header->rootPageId);
    treeFile->header->sum--;
    treeFile->markHeaderPageDirty();
}

vector<RID> IndexHandler::getRIDs(key_ptr key){
    vector<RID> ret;
    ret.clear();
    if(totalCount() == 0) return ret;
    
    IndexScan lb = lowerBound(key);  
    if(!lb.available()) return ret;
    
    IndexScan hb = lb;
    hb.nextKey();
    
    if(lb.available() && compare(type, key, nowdata) != 0) return ret;
    
    while(lb.available() && !lb.equals(hb)){
        ret.push_back(lb.getValue());
        lb.next();
    }
    return ret;
}

void IndexHandler::closeIndex(){
    keyFile->closeFile();
    treeFile->closeFile();
}

void IndexHandler::removeIndex() {
    closeIndex();
    bm->removeFile(getTreeFilename().c_str());
    bm->removeFile(getKeyFilename().c_str());
}

// void IndexHandler::debug(){
//     int index;
//     BPlusNode* node = (BPlusNode*)(treeFile->getPage(treeFile->header->rootPageId, index));
//     for(int i=0; i<49; i++){
//         keyFile->getRecord(node->data[i].keyPos,nowdata);
//         cout << node->data[i].keyPos.pageID << " " << *((int*)nowdata) << std::endl;
//     }
//     treeFile->markPageDirty(index);
// }
void IndexHandler::insertIntoNonFullPage(key_ptr key,RID rid, int pageID){
    int index;
    BPlusNode* node = (BPlusNode*)(treeFile->getPage(pageID, index));
    // std::cout << "into non full page" << pageID << std::endl;
    if (node -> nodeType == ix::NodeType::LEAF) {
        int i = 0;
        for(;i<node->recs; i++){
            keyFile->getRecord(node->data[i].keyPos, nowdata);
            if(compare(type, key, nowdata) < 0) break;
        }
        i--;
        if(i>=0) keyFile->getRecord(node->data[i].keyPos, nowdata);
        if(i>=0 && i<node->recs && compare(type, key, nowdata) == 0){    
            // std::cout << "We meet the overflow page " << *((int*)key) <<std::endl;    
            insertIntoOverflowPage(key, rid, node, i);
            treeFile->markPageDirty(index);
        } else {
            RID keyPos = keyFile->insertRecord(key, getLen(key, type));
            for(int j=node->recs-1; j>i; j--) node->data[j+1] = node->data[j];
            node->data[i+1].count = 1;
            node->data[i+1].keyPos = keyPos;
            node->data[i+1].value = rid;
            node->recs++;
            treeFile->markPageDirty(index);
        }
    } else if (node->nodeType == ix::NodeType::INTERNAL) {
        int i = 1;
        for(;i<node->recs;i++) {
            keyFile->getRecord(node->data[i].keyPos, nowdata);
            if(compare(type, key, nowdata) < 0) break;
            treeFile->access(index);
        }
        i--;
        int childIndex; //Useless index
        BPlusNode* child = (BPlusNode*)treeFile->getPage(node->data[i].value.pageID, childIndex);
        if (child->recs == maxIndexPerPage) {
            splitPage(node, i);
            treeFile->markPageDirty(index);
            keyFile->getRecord(node->data[i].keyPos, nowdata);
            if(compare(type, key, nowdata) >= 0) i++;
        }
        node->data[i].count++;
        treeFile->markPageDirty(index);
        insertIntoNonFullPage(key, rid, node->data[i].value.pageID);
    } else {
        cerr << "UNKNOWN LEAF TYPE" << node->nodeType;
    }
}

void IndexHandler::splitPage(BPlusNode* node, int index) {
    int childIndex;
    int newIndex; //a useless index
    BPlusNode* child = (BPlusNode*)treeFile->getPage(node->data[index].value.pageID, childIndex);
    BPlusNode* newNode = (BPlusNode*)treeFile->newPage(newIndex);
    if(child->nodeType == ix::NodeType::LEAF) {
        if(child->nextPage > 0) {
            int nextIndex;
            BPlusNode* next = (BPlusNode*) treeFile->getPage(child->nextPage, nextIndex);
            newNode->nextPage = child->nextPage;
            next->prevPage = newNode->pageId;
            treeFile->markPageDirty(nextIndex);
        }
        child->nextPage = newNode->pageId;
        newNode->prevPage = child->pageId;
        child->recs >>= 1;
        newNode->recs = maxIndexPerPage - child->recs;
        newNode->nodeType = child->nodeType;
        for(int i=0; i<newNode->recs; i++){
            newNode->data[i] = child->data[i+child->recs];
        }
        if(treeFile->header->lastLeaf == child->pageId) {
            treeFile->header->lastLeaf = newNode->pageId;
            treeFile->markHeaderPageDirty();
        }
    } 
    else if(child->nodeType == ix::NodeType::INTERNAL){
        child->nextPage = 0;
        newNode->prevPage = 0;
        newNode->data[0].count = 0;
        newNode->data[0].keyPos = RID{0,0};
        newNode->data[0].value = child->data[child->recs - 1].value;
        child->recs >>= 1;
        newNode->recs = maxIndexPerPage - child->recs + 1;
        newNode->nodeType = child->nodeType;
        for(int i=1; i<newNode->recs; i++){
            newNode->data[i]=child->data[i+child->recs-1];
        }
    } else cerr << "No such tree node type\n";
    
    for(int i=node->recs-1; i>index; i--){
        node->data[i+1] = node->data[i];
    }
    node->recs++;
    node->data[index].count = 0;
    node->data[index+1].count = 0;
    node->data[index+1].keyPos = newNode->data[0].keyPos;
    node->data[index+1].value = RID{newNode->pageId,0};

    for(int i=0; i<child->recs; i++)
        node->data[index].count += child->data[i].count;
    for(int i=0; i<newNode->recs; i++)
        node->data[index+1].count += newNode->data[i].count;
    treeFile->markPageDirty(childIndex);
    treeFile->markPageDirty(newIndex);
}

void IndexHandler::insertIntoOverflowPage(key_ptr key, RID rid, BPlusNode* fa, int index){
    if(fa->data[index].count == 1){
        int newIndex;
        BPlusOverflowPage* newOP = (BPlusOverflowPage*)treeFile->newPage(newIndex,true);
        newOP->nodeType = ix::NodeType::OVRFLOW;
        newOP->nextPage = 0;
        newOP->prevPage = 0;
        newOP->recs = 2;
        newOP->fatherPage = fa->pageId;
        newOP->data[0] = fa->data[index].value;
        newOP->data[1] = rid;
        fa->data[index].value = RID{newOP->pageId, 0};
        // std::cout << "page id is" << newOP->pageId << std::endl;
        fa->data[index].count = 2;
        treeFile->markPageDirty(newIndex);
    } else {
        int pageIndex;
        BPlusOverflowPage* page = (BPlusOverflowPage*)treeFile->getPage(fa->data[index].value.pageID, pageIndex);
        while((page->nextPage) && (page->recs == maxIndexPerPage)) //TO BE FIEXED maxKeyperOverflowPage
        {
            page = (BPlusOverflowPage*)treeFile->getPage(page->nextPage, pageIndex);
        }
        if(page->recs == maxIndexPerPage) {
            int newIndex;
            BPlusOverflowPage* newOP = (BPlusOverflowPage*)treeFile->newPage(newIndex);
            newOP->nodeType = ix::NodeType::OVRFLOW;
            newOP->nextPage = 0;
            newOP->prevPage = page->pageId;
            newOP->recs = 1;
            newOP->fatherPage = fa->pageId;
            newOP->data[0] = rid;
            page->nextPage = newOP->pageId;
            fa->data[index].count++;
        } else {
            page->data[page->recs++] = rid;
            fa->data[index].count++;
        }
        treeFile->markPageDirty(pageIndex);
    }
}

void IndexHandler::deleteFromLegalPage(key_ptr key, RID rid, int pageID){
    int index;
    // std::cout << pageID << std::endl;
    BPlusNode* node = (BPlusNode*)treeFile->getPage(pageID, index);
    if(node->nodeType == ix::NodeType::LEAF){
        int i = 0;
        for(;i<node->recs;i++) {
            keyFile->getRecord(node->data[i].keyPos, nowdata);
            if(compare(type, key, nowdata) < 0) break;
        }
        i--;
        // std::cout << "Delete from i "<< node->data[i].count << std::endl;
        if(node->data[i].count > 1){
            deleteFromOverflowPage(key, rid, node, i);
        } else {
            keyFile->deleteRecord(node->data[i].keyPos);
            node->recs--;
            for(;i<node->recs;i++) //WARNING: I make some modification here
                node->data[i] = node->data[i+1];
        }
    } else if (node->nodeType == ix::NodeType::INTERNAL) {
        int i = 1;
        for(;i<node->recs;i++) {
            keyFile->getRecord(node->data[i].keyPos, nowdata);
            if(compare(type, key, nowdata) < 0) break;
        }
        i--;
        int childIndex;
        BPlusNode* child = (BPlusNode*) treeFile->getPage(node->data[i].value.pageID, childIndex);
        if(child->recs <= (maxIndexPerPage >> 1)) {
            if ((i>0) && (node->data[i-1].keyPos.pageID != 0)) {
                int tempIndex;
                BPlusNode* bro = (BPlusNode*)treeFile->getPage(node->data[i-1].value.pageID, tempIndex);
                if(bro->recs > (maxIndexPerPage >> 1)) borrowFromForward(node, i);
                    else mergePage(node, --i);
            } else {
                int tempIndex;
                BPlusNode* bro = (BPlusNode*)treeFile->getPage(node->data[i+1].value.pageID, tempIndex);
                if(bro->recs > (maxIndexPerPage >> 1)) borrowFromBackward(node, i);
                    else mergePage(node, i);
            }
        }
        deleteFromLegalPage(key, rid, node->data[i].value.pageID);
        node->data[i].count--;
        if (i>0) {
            child = (BPlusNode*)treeFile->getPage(node->data[i].value.pageID, childIndex); 
            if(child->nodeType == ix::NodeType::INTERNAL) node->data[i].keyPos = child->data[1].keyPos;
                else node->data[i].keyPos = child->data[0].keyPos;
        }
    }
    treeFile->markPageDirty(index);
}

void IndexHandler::mergePage(BPlusNode* node, int index){
    int m1Index;
    BPlusNode* m1 = (BPlusNode*)treeFile->getPage(node->data[index].value.pageID, m1Index);
    int m2Index;
    BPlusNode* m2 = (BPlusNode*)treeFile->getPage(node->data[index + 1].value.pageID, m2Index);

    treeFile->markPageDirty(m1Index);
    treeFile->markPageDirty(m2Index);

    if(m1->nodeType == ix::NodeType::LEAF) {
        for(int i = 0; i < m2->recs; i++) m1->data[m1->recs + i] = m2->data[i];
        m1->recs += m2->recs;

        m1->nextPage = m2->nextPage;
        if(m2->nextPage){
            int brobroIndex;
            BPlusNode* brobro = (BPlusNode*) treeFile->getPage(m2->nextPage, brobroIndex);
            brobro->prevPage = m1->pageId;
            treeFile->markPageDirty(brobroIndex);
        } else {
            treeFile->header->lastLeaf = m1->pageId;
            treeFile->markHeaderPageDirty();
        }
    } else if(m1->nodeType == ix::NodeType::INTERNAL) {
        for(int i = 1; i < m2->recs; i++) m1->data[m1->recs + i - 1] = m2->data[i];
        m1->recs += m2->recs;
    } else {
        cerr << "OVRFLOW PAGE SHOULD NOT BE MERGED\n";
        return;
    }

    node->data[index].count += node->data[index+1].count;
    node->recs--;
    for(int i=index+1; i<node->recs; i++){
        node->data[i] = node->data[i+1];
    }
}

void IndexHandler::borrowFromBackward(BPlusNode* node, int index){
    int childIndex;
    BPlusNode* child = (BPlusNode*)treeFile->getPage(node->data[index].value.pageID, childIndex);
    int rightIndex;
    BPlusNode* right = (BPlusNode*)treeFile->getPage(node->data[index+1].value.pageID, rightIndex);
    if(child->nodeType == ix::NodeType::LEAF) {
        child->data[child->recs] = right->data[0];
        for(int i=0; i < right->recs - 1; i++)
            right->data[i] = right->data[i+1];
        node->data[index+1].keyPos = right->data[0].keyPos;
        
    } else {
        child->data[child->recs] = right->data[1];
        right->data[0].count = 0;
        right->data[0].keyPos = RID{0,0};
        right->data[0].value = right->data[1].value;
        for(int i=1; i < right->recs -1; i++)
            right->data[i] = right->data[i+1];
        node->data[index+1].keyPos = right->data[1].keyPos;
    }
    node->data[index+1].count -= child->data[child->recs].count;
    node->data[index].count +=  child->data[child->recs++].count;
    right->recs--;
    treeFile->markPageDirty(childIndex);
    treeFile->markPageDirty(rightIndex);
}

void IndexHandler::borrowFromForward(BPlusNode* node, int index){
    int childIndex;
    BPlusNode* child = (BPlusNode*)treeFile->getPage(node->data[index].value.pageID, childIndex);
    int leftIndex;
    BPlusNode* left = (BPlusNode*)treeFile->getPage(node->data[index-1].value.pageID, leftIndex);
    for(int i = child->recs-1; i >= 0; i--) child->data[i+1] = child->data[i];
    if(child->nodeType == ix::NodeType::LEAF) {
        child->data[0] = left->data[left->recs-1];
    } else {
        child->data[1] = left->data[left->recs-1];
        child->data[0].count = 0;
        child->data[0].keyPos = RID{0,0};
        child->data[0].value = left->data[left->recs-2].value;
    }
    node->data[index].keyPos = left->data[left->recs-1].keyPos;
    node->data[index-1].count -= left->data[left->recs-1].count;
    node->data[index].count += left->data[--left->recs].count;
    child->recs++;
    treeFile->markPageDirty(childIndex);
    treeFile->markPageDirty(leftIndex);
}

void IndexHandler::deleteFromOverflowPage(key_ptr key, RID rid, BPlusNode *fa, int index){
    int pageIndex;
    // std::cout << "delete from overflow page" << fa->data[index].value.pageID << std::endl;
    BPlusOverflowPage* op = (BPlusOverflowPage*)treeFile->getPage(fa->data[index].value.pageID,pageIndex);
    int pos = -1;
    while(pos == -1){
        for (int  i=0; i < op->recs; i++)
            if (op->data[i].pageID == rid.pageID && op->data[i].slotID == rid.slotID) {
                pos = i;
                break;
            }
        if(op->nextPage && pos==-1) {
            op = (BPlusOverflowPage*)treeFile->getPage(op->nextPage, pageIndex);
        } else break;
    }
    if(pos==-1) return;//Or maybe assert(flase)?
    for(int i=pos; i<op->recs-1; i++)
        op->data[i] = op->data[i+1];
    op->recs--;
    if(!op->recs){
        if(op->prevPage){
            int prevIndex;
            BPlusOverflowPage* prevPage = (BPlusOverflowPage*)treeFile->getPage(op->prevPage, prevIndex);
            prevPage->nextPage = op->nextPage;
            treeFile->markPageDirty(prevIndex);
        }
        if(op->nextPage){
            int nextIndex;
            BPlusOverflowPage* nextPage = (BPlusOverflowPage*)treeFile->getPage(op->nextPage, nextIndex);
            nextPage->prevPage = op->prevPage;
            treeFile->markPageDirty(nextIndex);
        }
        if(fa->data[index].value.pageID == op->pageId) fa->data[index].value = RID{op->nextPage,0};
    }
    fa->data[index].count--;
    if( fa->data[index].count == 1) {
        int tempIndex;
        op = (BPlusOverflowPage*) treeFile->getPage(fa->data[index].value.pageID, tempIndex);
        fa->data[index].value = op->data[0];
    }
    treeFile->markPageDirty(pageIndex);
}

int IndexHandler::getCountIn(int pageID, key_ptr key){
    int tempIndex;
    BPlusNode* node = (BPlusNode*)treeFile->getPage(pageID, tempIndex);
    if(node->nodeType == ix::NodeType::LEAF){
        for(int i=0; i<node->recs; i++){
            keyFile->getRecord(node->data[i].keyPos,nowdata);
            if(compare(type, key, nowdata) == 0) return node->data[i].count;
        }
        return 0; //Maybe -1 is better here
    } else if (node->nodeType == ix::NodeType::INTERNAL){
        int i = 1;
        for(; i<node->recs; i++){
            keyFile->getRecord(node->data[i].keyPos,nowdata);
            if(compare(type, key, nowdata) < 0) break;
        }
        i--;
        //std::cout<<i<<" " << node->data[i].value.pageID << std::endl;
        return getCountIn(node->data[i].value.pageID, key);
    } 
    return -1;
}

int IndexHandler::getLesserCountIn(int pageID, key_ptr key){
    int tempIndex;
    BPlusNode* node = (BPlusNode*)treeFile->getPage(pageID, tempIndex);
    int ret = 0;
    if(node->nodeType == ix::NodeType::LEAF){
        for(int i=0; i<node->recs; i++){
            keyFile->getRecord(node->data[i].keyPos,nowdata);
            if(compare(type, key, nowdata) <= 0) return ret;
            ret += node->data[i].count;
        }
        return ret; //Maybe -1 is better here
    } else if (node->nodeType == ix::NodeType::INTERNAL){
        int i = 1;
        for(; i<node->recs; i++){
            keyFile->getRecord(node->data[i].keyPos,nowdata);
            if(compare(type, key, nowdata) < 0) break;
            ret += node->data[i].count;
        }
        i--;
        ret -= node->data[i].count;
        return ret + getLesserCountIn(node->data[i].value.pageID, key);
    } 
    return -1;
}

int IndexHandler::getGreaterCountIn(int pageID, key_ptr key){
    int tempIndex;
    BPlusNode* node = (BPlusNode*)treeFile->getPage(pageID, tempIndex);
    int ret = 0;
    if(node->nodeType == ix::NodeType::LEAF){
        for(int i=node->recs-1; i>=0; i--){
            keyFile->getRecord(node->data[i].keyPos,nowdata);
            // unused variable
            //int* nowd = (int*)nowdata;
            if(compare(type, key, nowdata) >= 0) return ret;
            ret += node->data[i].count;
        }
        return ret; //Maybe -1 is better here
    } else if (node->nodeType == ix::NodeType::INTERNAL){
        int i = 1;
        for(; i<node->recs; i++){
            keyFile->getRecord(node->data[i].keyPos,nowdata);
            if(compare(type, key, nowdata) < 0) break;
        }
        i--;
        for(int j=i+1; j<node->recs; j++)
            ret += node->data[j].count;
        return ret + getGreaterCountIn(node->data[i].value.pageID, key);
    } 
    return -1;
}

IndexScan IndexHandler::getLowerBound(int pageID, key_ptr key){
    int tempIndex;
    BPlusNode* node = (BPlusNode*)treeFile->getPage(pageID, tempIndex);
    if(node->nodeType == ix::NodeType::LEAF){
        for(int i=0; i<node->recs; i++){
            keyFile->getRecord(node->data[i].keyPos, nowdata);
            int result = compare(type, key, nowdata);
            if(result == 0) return IndexScan(this, node, i, 0);
            else if(result < 0) {
                if(i==0) return IndexScan(this);
                else return IndexScan(this, node, i-1, 0);
            }
        }
        return IndexScan(this, node, node->recs-1, 0);
    } else if(node->nodeType == ix::NodeType::INTERNAL){
        int i = 1;
        for(; i<node->recs; i++){
            keyFile->getRecord(node->data[i].keyPos,nowdata);
            if(compare(type, key, nowdata) < 0) break;
        }
        i--;
        return getLowerBound(node->data[i].value.pageID, key);
    } else return IndexScan(this);
}
