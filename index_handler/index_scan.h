#ifndef INDEX_SCAN_H
#define INDEX_SCAN_H

#include "../record_handler/rid.h"
    
class IndexScan{
    private:
        int nextEntry;
    public:
    IndexScan ();
    ~IndexScan ();
    int getNextEntry(RID &rid);
};

#endif