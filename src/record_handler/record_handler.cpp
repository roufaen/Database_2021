# include "record_handler.h"

RecordHandler::RecordHandler(BufManager *_bufManager) : bufManager(_bufManager) {
    this->fileID = -1;
}

RecordHandler::~RecordHandler() {
}

int RecordHandler::createFile(string fileName) {
    if (this->fileID != -1) {
        return -1;
    } else {
        this->bufManager->createFile(fileName.c_str());
        int newFileID = this->bufManager->openFile(fileName.c_str());
        int idx = -1;
        BufType page = this->bufManager->allocPage(newFileID, 0, idx, false);
        this->bufManager->markDirty(idx);
        FileHeader newHeader;
        newHeader.pageNum = 0;
        newHeader.recordNum = 0;
        newHeader.recordPerPage = 3;
        newHeader.recordNumPageOffset = PAGE_SIZE - 4;
        newHeader.recordSize = MAX_RECORD_LEN;
        newHeader.slotMapOffset = MAX_RECORD_LEN * 3;
        newHeader.slotMapSize = ceil(newHeader.recordPerPage / 8.0);
        memcpy(page, &newHeader, sizeof(newHeader));
        this->bufManager->closeFile(newFileID);
        return 0;
    }
}

int RecordHandler::destroyFile(string fileName) {
    if (this->fileID != -1) {
        return -1;
    } else {
        this->bufManager->removeFile(fileName.c_str());
        return 0;
    }
}

int RecordHandler::openFile(string fileName) {
    if (this->fileID != -1) {
        return -1;
    } else {
        this->fileID = this->bufManager->openFile(fileName.c_str());
        if (this->fileID == -1) {
            return -1;
        }
        int idx = -1;
        BufType headerPage = this->bufManager->getPage(this->fileID, 0, idx);
        memcpy(&this->header, headerPage, sizeof(this->header));
        while (!availablePage.empty()) {
            availablePage.pop();
        }
        for (int i = 1; i <= this->header.pageNum; i++) {
            cout << i << endl;
            if (i == 60083) {
                cout << i;
            }
            BufType page = this->bufManager->getPage(this->fileID, i, idx);
            this->bufManager->access(i);
            int occupiedNum = 0;
            memcpy(&occupiedNum, page + PAGE_SIZE - 4, 4);
            if (occupiedNum != this->header.recordPerPage) {
                availablePage.push(i);
            }
        }
        return 0;
    }
}

int RecordHandler::closeFile() {
    if (this->fileID == -1) {
        return -1;
    } else {
        int idx = -1;
        BufType headerPage = this->bufManager->getPage(this->fileID, 0, idx);
        this->bufManager->markDirty(idx);
        memcpy(headerPage, &this->header, sizeof(this->header));
        this->bufManager->closeFile(this->fileID);
        this->fileID = -1;
        return 0;
    }
}

int RecordHandler::readHeader(char *pData) {
    int idx = -1;
    BufType page = this->bufManager->getPage(this->fileID, 0, idx);
    memcpy(pData, page + sizeof(header), MAX_RECORD_LEN);
    return 0;
}

int RecordHandler::writeHeader(char *pData) {
    int idx = -1;
    BufType page = this->bufManager->getPage(this->fileID, 0, idx);
    memcpy(page + sizeof(header), pData, MAX_RECORD_LEN);
    this->bufManager->markDirty(idx);
    this->bufManager->writeBack(idx);
    return 0;
}

int RecordHandler::getRecord(const RID &rid, char *pData) {
    if (this->fileID == -1 || rid.pageID <= 0 || rid.pageID > this->header.pageNum) {
        return -1;
    } else {
        int idx = -1;
        BufType page = this->bufManager->getPage(this->fileID, rid.pageID, idx);
        this->bufManager->access(idx);
        if ((page[this->header.slotMapOffset + rid.slotID / 8] & (1 << (rid.slotID % 8))) == 0) {
            return -1;
        }
        memcpy(pData, page + rid.slotID * this->header.recordSize, this->header.recordSize);
        return 0;
    }
}

int RecordHandler::deleteRecord(const RID &rid) {
    if (this->fileID == -1) {
        return -1;
    } else {
        int idx = -1;
        BufType page = this->bufManager->getPage(this->fileID, rid.pageID, idx);
        this->bufManager->markDirty(idx);
        if ((page[this->header.slotMapOffset + rid.slotID / 8] & (1 << (rid.slotID % 8))) == 0) {
            return -1;
        } else {
            page[this->header.slotMapOffset + rid.slotID / 8] &= ~(1 << (rid.slotID % 8));
            int recordNumPage = 0;
            memcpy(&recordNumPage, page + this->header.recordNumPageOffset, sizeof(int));
            recordNumPage--;
            memcpy(page + this->header.recordNumPageOffset, &recordNumPage, sizeof(int));
            this->header.recordNum--;
            if (recordNumPage == this->header.recordPerPage - 1) {
                this->availablePage.push(rid.pageID);
            }
            return 0;
        }
    }
}

RID RecordHandler::insertRecord(const char *pData, int len) {
    int idx = -1;
    RID rid = {-1, -1};
    if (this->fileID != -1) {
        if (availablePage.empty()) {
            this->header.pageNum++;
            BufType page = this->bufManager->allocPage(fileID, this->header.pageNum, idx);
            this->bufManager->markDirty(idx);
            memset(page, 0, PAGE_SIZE);
            this->bufManager->writeBack(idx);
            this->availablePage.push(this->header.pageNum);
        }
        int pageID = this->availablePage.top();
        BufType page = this->bufManager->getPage(this->fileID, pageID, idx);
        this->bufManager->markDirty(idx);
        for (int i = 0; i < this->header.slotMapSize; i++) {
            if (page[this->header.slotMapOffset + i] != (2 ^ 8) - 1) {
                for (int j = 0; j < 8; j++) {
                    if ((page[this->header.slotMapOffset + i] & (1 << j)) == 0) {
                        page[this->header.slotMapOffset + i] |= (1 << j);
                        memcpy(page + (i * 8 + j) * this->header.recordSize, pData, MAX_RECORD_LEN);
                        int recordNumPage = 0;
                        memcpy(&recordNumPage, page + this->header.recordNumPageOffset, sizeof(int));
                        recordNumPage++;
                        memcpy(page + this->header.recordNumPageOffset, &recordNumPage, sizeof(int));
                        this->header.recordNum++;
                        if (recordNumPage == this->header.recordPerPage) {
                            this->availablePage.pop();
                        }
                        rid.pageID = pageID;
                        rid.slotID = i * 8 + j;
                        return rid;
                    }
                }
            }
        }
    }
    return rid;
}

RID RecordHandler::updateRecord(const RID &rid, const char *pData, int len) {
    if (deleteRecord(rid) == -1) {
        RID rid;
        rid.pageID = -1;
        rid.slotID = -1;
        return rid;
    } else {
        return insertRecord(pData, len);
    }
}

vector <RID> RecordHandler::getRecordList() {
    vector <RID> rids;
    RID rid;
    for (int i = 1, idx = -1; i <= this->header.pageNum; i++) {
        BufType page = this->bufManager->getPage(this->fileID, i, idx);
        for (int j = 0; j < this->header.recordPerPage; j++) {
            if ((page[this->header.slotMapOffset + j / 8] & (1 << (j % 8))) != 0) {
                rid.pageID = i, rid.slotID = j;
                rids.push_back(rid);
            }
        }
    }
    return rids;
}
