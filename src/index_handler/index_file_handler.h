#ifndef INDEX_FILE_HANDLER_H
#define INDEX_FILE_HANDLER_H

#include "ix.h"
#include "../buf_manager/buf_manager.h"

class IndexFileHandler{
public:
    IndexFileHandler(const char* fileName, BufManager* _bm);
    ~IndexFileHandler();

    BufManager* bm;
    int fileID, headerIndex, currentPage, currentIndex;
    struct IndexFileHeader* header;

    char* newPage(int &index);
    char* getPage(int pageID, int &index);
    void markHeaderPageDirty();
    void markPageDirty(int index);

    void closeFile();
};

#endif