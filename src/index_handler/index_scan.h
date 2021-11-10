#ifndef INDEX_SCAN_H
#define INDEX_SCAN_H

#include "../utils/rid.h"
    
class IndexScan{
    private:
        int nextEntry;
    public:
    IndexScan ();
    ~IndexScan ();
    int getNextEntry(RID &rid);
};

#endif