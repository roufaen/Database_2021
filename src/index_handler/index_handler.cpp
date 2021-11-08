#include "index_handler.h"

#include <string>
#include <sstream>

IndexHandler::IndexHandler(){
    bm = new BufManager();
    id = -1;
}

IndexHandler::~IndexHandler(){

}

int IndexHandler::createIndex(const char* filename){
    bm->createFile(filename);
    return 0;
}

int IndexHandler::destroyIndex(const char* filename){
    bm->removeFile(filename);
    return 0;
}

int IndexHandler::openIndex(const char* filename){
    if(id != -1) bm->closeFile(id);
    id = bm->openFile(filename);
    
    return id;
}

int IndexHandler::closeIndex(){
    bm->closeFile(id);
    id = -1;
}

int IndexHandler::search(int lowerBound, int upperBound, IndexScan &indexScan){

}

int IndexHandler::insertRecord(int key, const RID &rid){

}

