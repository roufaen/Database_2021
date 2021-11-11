#ifndef INDEX_HANDLER_H
#define INDEX_HANDLER_H

#include <cstring>
#include "../buf_manager/buf_manager.h"
#include "./btree/btree.h"

class IndexHandler {
private:
    BufManager* bm;
    int id;
    Btree<int>* btree;

public:
    IndexHandler(BufManager*);                    
    ~IndexHandler();
    int createIndex(const char *fileName);
    int destroyIndex(const char *fileName);
    int openIndex(const char *fileName); 
    int closeIndex();
};

#endif

