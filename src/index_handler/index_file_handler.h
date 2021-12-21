#ifndef INDEX_FILE_HANDLER_H
#define INDEX_FILE_HANDLER_H

#include "ix.h"
#include "../buf_manager/buf_manager.h"

class IndexFileHandler{
public:
    IndexFileHandler(BufManager* _bm);
    ~IndexFileHandler();

    BufManager* bm;
    int fileID, headerIndex, currentPage, currentIndex;
    struct IndexFileHeader* header;

    void openFile(const char* fileName);
    void access(int index);
    char* newPage(int &index);
    char* getPage(int pageID, int &index);
    void markHeaderPageDirty();
    void markPageDirty(int index);

    void closeFile();
};

#endif
