#include "index_file_handler.h"

IndexFileHandler::IndexFileHandler(BufManager* _bm){
    bm = _bm;
    header = new IndexFileHeader;
}
void IndexFileHandler::openFile(const char* fileName){
    fileID = bm->openFile(fileName);
    int headerIndex;
    if (fileID == -1){
        bm->createFile(fileName);
        fileID = bm->openFile(fileName);
        /*IndexFileHeader* tempHeader = (IndexFileHeader*)*/bm->allocPage(fileID, 0, headerIndex);
        headerChanged = true;
        header->rootPageId = 1;
        header->pageCount = 1;
        header->firstLeaf = 1;
        header->lastLeaf = 1;
        header->sum = 0;

        int index;
        BPlusNode* root = (BPlusNode*)bm->allocPage(fileID, 1, index);
        root->nextPage = 0;
        root->prevPage = 0;
        root->nodeType = ix::LEAF;
        root->pageId = 1;
        root->recs = 0;
        bm->markDirty(index);
    }
    else {
        IndexFileHeader* tempHeader = (IndexFileHeader*)bm->getPage(fileID, 0, headerIndex);
        memcpy(header, tempHeader, sizeof(IndexFileHeader));
    }
}

IndexFileHandler::~IndexFileHandler(){
    delete header;
    closeFile();
}

void IndexFileHandler::access(int index){
    bm->access(index);
}
char* IndexFileHandler::newPage(int &index){
    // std::cout << "Apply for a new page" << std::endl;
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
    headerChanged = true;
}

void IndexFileHandler::markPageDirty(int index){
    bm->markDirty(index);
}

void IndexFileHandler::closeFile(){
    if (bm != nullptr){
        int headerIndex;
        IndexFileHeader* tempHeader = (IndexFileHeader*)bm->getPage(fileID, 0, headerIndex);
        memcpy(tempHeader, header, sizeof(IndexFileHeader));
        this->markPageDirty(headerIndex);
        bm->closeFile(fileID);
    }
}
