#include "index_handler.h"

#include <string>
#include <sstream>

IndexHandler::IndexHandler(BufManager* bufManager){
    bm = bufManager;
    id = -1;
}

IndexHandler::~IndexHandler(){}

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
    bm->close();
    bm->closeFile(id);
    id = -1;
    return 1;
}


