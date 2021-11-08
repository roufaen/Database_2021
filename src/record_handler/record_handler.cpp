# include "record_handler.h"

RecordHandler::RecordHandler(BufManager *bufManager) : bufManager(bufManager) {
}

RecordHandler::~RecordHandler() {
}

int RecordHandler::createFile(const char *fileName) {
    bufManager->createFile(fileName);
    int newFileID = bufManager->openFile(fileName);
    int idx = -1;
    BufType page = bufManager->allocPage(newFileID, 0, idx, false);
    bufManager->markDirty(idx);
    FileHeader newHeader;
    newHeader.pageNum = 0;
    newHeader.recordNum = 0;
    newHeader.recordPerPage = 3;
    newHeader.recordNumPageOffset = PAGE_SIZE - 4;
    newHeader.recordSize = MAX_RECORD_LEN;
    newHeader.slotMapOffset = MAX_RECORD_LEN * 3;
    newHeader.slotMapSize = ceil(newHeader.recordPerPage / 8.0);
    memcpy(page, &newHeader, sizeof(newHeader));
    bufManager->close();
    bufManager->closeFile(newFileID);
    return 0;
}

int RecordHandler::destroyFile(const char *fileName) {
    bufManager->removeFile(fileName);
    return 0;
}

int RecordHandler::openFile(const char *fileName) {
    if (fileID != -1) {
        return -1;
    } else {
        fileID = bufManager->openFile(fileName);
        int idx = -1;
        BufType headerPage = bufManager->getPage(fileID, 0, idx);
        memcpy(&header, headerPage, sizeof(header));
        while (!availablePage.empty()) {
            availablePage.pop();
        }
        for (int i = 1; i <= header.pageNum; i++) {
            BufType page = bufManager->getPage(fileID, i, idx);
            bufManager->access(i);
            int occupiedNum = 0;
            memcpy(&occupiedNum, page + PAGE_SIZE - 4, 4);
            if (occupiedNum != header.recordPerPage) {
                availablePage.push(i);
            }
        }
        return 0;
    }
}

int RecordHandler::closeFile() {
    if (fileID == -1) {
        return -1;
    } else {
        bufManager->closeFile(fileID);
        fileID = -1;
        return 0;
    }
}

int RecordHandler::getRecord(const RID &rid, char *&pData) {
    int idx = -1;
    char *page = (char*)(bufManager->getPage(fileID, rid.pageID, idx));
    pData = new char[header.recordSize];
    memcpy(pData, page + rid.slotID * header.recordSize, header.recordSize);
    return 0;
}

int RecordHandler::deleteRecord(const RID &rid) {
    int idx = -1;
    char *page = (char*)(bufManager->getPage(fileID, rid.pageID, idx));
    if ((page[header.slotMapOffset + rid.slotID / 8] & (1 << (rid.slotID % 8))) == 0) {
        return -1;
    } else {
        page[header.slotMapOffset + rid.slotID / 8] &= ~(1 << (rid.slotID % 8));
        int recordNumPage = 0;
        memcpy(&recordNumPage, page + header.recordNumPageOffset, sizeof(int));
        recordNumPage--;
        memcpy(page + header.recordNumPageOffset, &recordNumPage, sizeof(int));
        header.recordNum--;
        if (recordNumPage == header.recordPerPage - 1) {
            availablePage.push(rid.pageID);
        }
        return 0;
    }
}

RID RecordHandler::insertRecord(const char *pData, int len) {
    int idx = -1;
    RID rid = {-1, -1};
    if (availablePage.empty()) {
        header.pageNum++;
        bufManager->allocPage(fileID, header.pageNum, idx);
        availablePage.push(header.pageNum);
    }
    int pageID = availablePage.top();
    char *page = (char*)(bufManager->getPage(fileID, pageID, idx));
    bufManager->markDirty(idx);
    for (int i = 0; i < header.slotMapSize; i++) {
        if (page[header.slotMapOffset + i] != 2 ^ 8 - 1) {
            for (int j = 0; j < 8; j++) {
                if (page[header.slotMapOffset + i] & (1 << j) == 0) {
                    page[header.slotMapOffset + i] |= (1 << j);
                    memcpy(page + (i * 8 + j) * header.recordSize, pData, MAX_RECORD_LEN);
                    int recordNumPage = 0;
                    memcpy(&recordNumPage, page + header.recordNumPageOffset, sizeof(int));
                    recordNumPage++;
                    memcpy(page + header.recordNumPageOffset, &recordNumPage, sizeof(int));
                    header.recordNum++;
                    if (recordNumPage == header.recordPerPage) {
                        availablePage.pop();
                    }
                    rid.pageID = pageID;
                    rid.slotID = i * 8 + j;
                    return rid;
                }
            }
        }
    }
    return rid;
}

RID RecordHandler::updateRecord(const RID &rid, const char *pData, int len) {
    deleteRecord(rid);
    return insertRecord(pData, len);
}
