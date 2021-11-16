#include "index_file_handler.h"

IndexFileHandler::IndexFileHandler(const char* fileName, BufManager* _bm){
    bm = _bm;
    fileID = bm->openFile(fileName);
    if (fileID == -1){
        bm->createFile(fileName);
        fileID = bm->openFile(fileName);
        header = (IndexFileHeader*)bm->getPage(fileID, 0, headerIndex);
        header->rootPageId = 1;
        header->pageCount = 1;
        header->firstLeaf = 1;
        header->lastLeaf = 1;
        header->sum = 0;
        bm->markDirty(headerIndex);

        int index;
        BPlusNode* root = (BPlusNode*)getPage(1, index);
        root->nextPage = 0;
        root->prevPage = 0;
        root->nodeType = LEAF;
        root->pageId = 1;
        root->recCount = 0;
        markPageDirty(index);

    }
    else header = (IndexFileHeader*)bm->getPage(fileID, 0, headerIndex);
}

IndexFileHandler::~IndexFileHandler(){
    closeFile();
}

char* IndexFileHandler::newPage(int &index){
    header->pageCount++;
    char* res = bm->getPage(fileID, header->pageCount, index);
    ((BPlusNode*)res)->pageId = header->pageCount;
    markPageDirty(index);
    markHeaderPageDirty(); 
    return res;
}

char* IndexFileHandler::getPage(int pageID, int& index){
    return bm->getPage(fileID, pageID, index);
}


void IndexFileHandler::markHeaderPageDirty(){
    bm->markDirty(headerIndex);
}

void IndexFileHandler::markPageDirty(int index){
    bm->markDirty(index);
}

void IndexFileHandler::closeFile(){
    if (bm != nullptr){
        bm->close();
        bm->closeFile(fileID);
        bm=nullptr;
    }
}