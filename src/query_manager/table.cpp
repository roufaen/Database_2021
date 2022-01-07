# include "table.h"

Table::Table(string dbName, string tableName, BufManager *bufManager) {
    this->dbName = dbName, this->tableName = tableName;
    // open file
    string fileName = "table_" + dbName + "_" + tableName + ".dat";
    this->recordHandler = new RecordHandler(bufManager);
    this->recordHandler->openFile(fileName);
    this->headerList = getHeaderList();
}

Table::Table(string dbName, string tableName, BufManager *bufManager, vector <TableHeader> headerList) {
    this->dbName = dbName, this->tableName = tableName;
    // create file
    string fileName = "table_" + dbName + "_" + tableName + ".dat";
    this->recordHandler = new RecordHandler(bufManager);
    this->recordHandler->createFile(fileName);
    this->recordHandler->openFile(fileName);
    writeHeaderList(headerList);
    this->headerList = headerList;
}

Table::~Table() {
    recordHandler->closeFile();
    delete this->recordHandler;
}

vector <Data> Table::exeSelect(RID rid) {
    char pData[MAX_RECORD_LEN];
    char *ptr = pData;
    vector <Data> res;
    Data data;
    if (this->recordHandler->getRecord(rid, pData) != 0) {
        return res;
    } else {
        for (int i = 0, siz = 0; i < (int)this->headerList.size(); i++) {
            memcpy(&siz, ptr, sizeof(int));
            ptr += sizeof(int);
            memcpy(&data.refCount, ptr, sizeof(int));
            ptr += sizeof(int);
            data.varType = this->headerList[i].varType;
            if (siz == 0) {
                data.isNull = 1;
            } else {
                data.isNull = 0;
                if (data.varType == CHAR || data.varType == VARCHAR) {
                    char ch = *(ptr + siz);
                    *(ptr + siz) = 0;
                    data.stringVal = ptr;
                    *(ptr + siz) = ch;
                } else if (data.varType == INT || data.varType == DATE) {
                    memcpy(&data.intVal, ptr, sizeof(int));
                } else if (data.varType == FLOAT) {
                    memcpy(&data.floatVal, ptr, sizeof(double));
                }
            }
            ptr += siz;
            res.push_back(data);
        }
        return res;
    }
}

RID Table::exeInsert(vector <Data> data) {
    RID rid = {-1, -1};
    char pData[MAX_RECORD_LEN];
    memset(pData, 0, sizeof(pData));
    if (this->headerList.size() != data.size()) {
        return rid;
    }

    char *ptr = pData;
    for (int i = 0, size = data.size(); i < size; i++) {
        int siz = this->headerList[i].varType == VARCHAR ? data[i].stringVal.size() : this->headerList[i].len;
        if (data[i].isNull == true) {
            siz = 0;
        }
        memcpy(ptr, &siz, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &data[i].refCount, sizeof(int));
        ptr += sizeof(int);

        if (data[i].isNull == true) {
            ptr += 0;
        } else if (this->headerList[i].varType == CHAR) {
            memcpy(ptr, data[i].stringVal.c_str(), this->headerList[i].len);
            ptr += this->headerList[i].len;
        } else if (this->headerList[i].varType == VARCHAR) {
            memcpy(ptr, data[i].stringVal.c_str(), data[i].stringVal.size());
            ptr += data[i].stringVal.size();
        } else if (this->headerList[i].varType == INT || this->headerList[i].varType == DATE) {
            memcpy(ptr, &data[i].intVal, sizeof(int));
            ptr += sizeof(int);
        } else if (this->headerList[i].varType == FLOAT) {
            memcpy(ptr, &data[i].floatVal, sizeof(double));
            ptr += sizeof(double);
        }
    }

    rid = this->recordHandler->insertRecord(pData, ptr - pData);
    return rid;
}

int Table::exeDelete(RID rid) {
    return this->recordHandler->deleteRecord(rid);
}

RID Table::exeUpdate(vector <Data> data, RID rid) {
    if (this->exeDelete(rid) != 0) {
        return {-1, -1};
    } else {
        return this->exeInsert(data);
    }
}

vector <RID> Table::getRecordList() {
    return this->recordHandler->getRecordList();
}

vector <TableHeader> Table::getHeaderList() {
    vector <TableHeader> headerList;
    TableHeader header;
    header.tableName = tableName;
    char *headerChar = new char[MAX_RECORD_LEN];
    this->recordHandler->readHeader(headerChar);
    int headerNum = *((int*)headerChar);
    char *ptr = headerChar + sizeof(int);
    for (int i = 0; i < headerNum; i++) {
        int stringLen = *((int*)ptr);
        ptr += sizeof(int);
        char ch = *(ptr + stringLen);
        *(ptr + stringLen) = 0;
        header.headerName = ptr;
        *(ptr + stringLen) = ch;
        ptr += stringLen;

        stringLen = *((int*)ptr);
        ptr += sizeof(int);
        ch = *(ptr + stringLen);
        *(ptr + stringLen) = 0;
        header.foreignTableName = ptr;
        *(ptr + stringLen) = ch;
        ptr += stringLen;

        stringLen = *((int*)ptr);
        ptr += sizeof(int);
        ch = *(ptr + stringLen);
        *(ptr + stringLen) = 0;
        header.foreignHeaderName = ptr;
        *(ptr + stringLen) = ch;
        ptr += stringLen;

        header.varType = *((int*)ptr);
        ptr += sizeof(int);
        header.len = *((int*)ptr);
        ptr += sizeof(int);
        header.refCount = *((int*)ptr);
        ptr += sizeof(int);
        header.foreignGroup = *((int*)ptr);
        ptr += sizeof(int);
        header.uniqueGroup = *((int*)ptr);
        ptr += sizeof(int);
        header.isPrimary = *((bool*)ptr);
        ptr += sizeof(bool);
        header.isForeign = *((bool*)ptr);
        ptr += sizeof(bool);
        header.isUnique = *((bool*)ptr);
        ptr += sizeof(bool);
        header.permitNull = *((bool*)ptr);
        ptr += sizeof(bool);
        header.hasIndex = *((bool*)ptr);
        ptr += sizeof(bool);
        headerList.push_back(header);
    }
    delete[] headerChar;
    return headerList;
}

int Table::writeHeaderList(vector <TableHeader> headerList) {
    char *headerChar = new char[MAX_RECORD_LEN];
    int headerNum = headerList.size();
    memcpy(headerChar, &headerNum, sizeof(int));
    char *ptr = headerChar + sizeof(int);
    for (int i = 0; i < headerNum; i++) {
        int stringLen = headerList[i].headerName.size();
        memcpy(ptr, &stringLen, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, headerList[i].headerName.c_str(), stringLen);
        ptr += stringLen;

        stringLen = headerList[i].foreignTableName.size();
        memcpy(ptr, &stringLen, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, headerList[i].foreignTableName.c_str(), stringLen);
        ptr += stringLen;

        stringLen = headerList[i].foreignHeaderName.size();
        memcpy(ptr, &stringLen, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, headerList[i].foreignHeaderName.c_str(), stringLen);
        ptr += stringLen;

        memcpy(ptr, &headerList[i].varType, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &headerList[i].len, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &headerList[i].refCount, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &headerList[i].foreignGroup, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &headerList[i].uniqueGroup, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &headerList[i].isPrimary, sizeof(bool));
        ptr += sizeof(bool);
        memcpy(ptr, &headerList[i].isForeign, sizeof(bool));
        ptr += sizeof(bool);
        memcpy(ptr, &headerList[i].isUnique, sizeof(bool));
        ptr += sizeof(bool);
        memcpy(ptr, &headerList[i].permitNull, sizeof(bool));
        ptr += sizeof(bool);
        memcpy(ptr, &headerList[i].hasIndex, sizeof(bool));
        ptr += sizeof(bool);
    }
    this->recordHandler->writeHeader(headerChar);
    delete[] headerChar;
    this->headerList = headerList;
    return 0;
}

string Table::getTableName() {
    return this->tableName;
}
