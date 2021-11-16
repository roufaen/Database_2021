#include "index_handler.h"

IndexHandler::IndexHandler(std::string _tableName, std::string _colName, DataType _type){
    tableName = _tableName;
    colName = _colName;
    type = _type;
    std::string treeFileName = tableName + colName + ".tree";
    std::string keyFileName = tableName + colName + ".key";
    treeFileBm = new BufManager();
    keyFileBm = new BufManager();
    treeFile = make_shared<IndexFileHandler>(treeFileName.c_str(), treeFileBm);
}

IndexHandler::~IndexHandler(){
    closeIndex();
    delete treeFileBm;
    delete keyFileBm;
}

void IndexHandler::insert(key_ptr key, RID rid){
    int rootIndex;
    BPlusNode* root = (BPlusNode*)treeFile->getPage(treeFile->header->rootPageId, rootIndex);

    if(root->recCount == maxIndexPerPage){
        if(root->nodeType == NodeType::LEAF){
            treeFile->markPageDirty(rootIndex);
            int index; //index is useless
            BPlusNode* newRoot = (BPlusNode*)treeFile->newPage(index);
            treeFile->header->rootPageId = newRoot->pageId;
            newRoot->nodeType = NodeType::INTERNAL;
            newRoot->nextPage = 0;
            newRoot->prevPage = 0;
            newRoot->recCount = 2;

            BPlusNode* newNode = (BPlusNode*)treeFile->newPage(index);
            root->nextPage = newNode->pageId;
            root->recCount >>= 1;
            newNode->prevPage = root->pageId;
            newNode->recCount = maxIndexPerPage - root->recCount;
            newNode->nodeType = NodeType::LEAF;

            int st = root->recCount;
            for(int i = root->recCount; i < maxIndexPerPage; i++) 
                newNode->data[i-st] = root->data[i];
            newRoot->data[0].keyPos = RID{0,0};
            newRoot->data[0].value = RID{root->pageId,0};
            newRoot->data[1].keyPos = newNode->data[0].keyPos;
            newRoot->data[1].value = RID{newNode->pageId,0};

            newRoot->data[0].count = 0;
            for(int i=0; i < root->recCount; i++)
                newRoot->data[0].count += root->data[i].count;

            newRoot->data[1].count = 0;
            for(int i=0; i < newNode->recCount; i++)
                newRoot->data[1].count += newNode->data[i].count;

            treeFile->header->lastLeaf = newNode->pageId;
            insertIntoNonFullPage(key, rid, newRoot->pageId);
        } else if(root->nodeType == NodeType::INTERNAL){
            treeFile->markPageDirty(rootIndex);
            int index; //index is useless
            BPlusNode* newRoot = (BPlusNode*)treeFile->newPage(index);
            treeFile->header->rootPageId = newRoot->pageId;
            newRoot->nodeType = NodeType::INTERNAL;
            newRoot->nextPage = 0;
            newRoot->prevPage = 0;
            newRoot->recCount = 2;

            BPlusNode* newNode = (BPlusNode*)treeFile->newPage(index);
            root->nextPage = 0;
            root->recCount >>= 1;
            newNode->prevPage = 0;
            newNode->recCount = maxIndexPerPage - root->recCount + 1;
            newNode->nodeType = NodeType::INTERNAL;

            newNode->data[0].count = 0; //Record the left most node
            newNode->data[0].keyPos = RID{0,0};
            int st = root->recCount - 1;
            newNode->data[0].value = root->data[st].value;
            for(int i=root->recCount; i<maxIndexPerPage; i++){
                newNode->data[i-st] = root->data[i];
            }

            
            newRoot->data[0].keyPos = RID{0,0};
            newRoot->data[0].value = RID{root->pageId,0};
            newRoot->data[1].keyPos = newNode->data[1].keyPos;
            newRoot->data[1].value = RID{newNode->pageId,0};
            
            newRoot->data[0].count = 0;
            for(int i=0; i < root->recCount; i++)
                newRoot->data[0].count += root->data[i].count;

            newRoot->data[1].count = 0;
            for(int i=0; i < newNode->recCount; i++)
                newRoot->data[1].count += newNode->data[i].count;

            insertIntoNonFullPage(key, rid, newRoot->pageId);

        }
        else{
            std::cerr << "No suck index B+ tree nodeType\n"; 
        }
    } else { //The root still has enough room
        insertIntoNonFullPage(ket, rid, treeFile->header->rootPageId)
    }

    treeFile->header->sum++;
    treeFile->markHeaderPageDirty();
}

bool IndexHandler::has(key_ptr key){
    return count(key) > 0;
}

int IndexHandler::count(key_ptr key){
    return getCountIn(treeFile->header->rootPageId, key);
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

IndexScan IndexHandler::lowerBound(key_ptr key){
    if(totalCount() == 0) return IndexScan(this);

}