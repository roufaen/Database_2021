# include "table.h"

Table::Table(string dbName, string tableName, BufManager *bufManager) {
    this->dbName = dbName, this->tableName = tableName;
    // open file
    string fileName = "table_" + dbName + "_" + tableName;
    this->recordHandler = new RecordHandler(bufManager);
    this->recordHandler->openFile(fileName);
    this->headers = getHeaders();
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
        for (int i = 0, siz = 0; i < this->headers.size(); i++) {
            memcpy(&siz, ptr, sizeof(int));
            ptr += sizeof(int);
            memcpy(&data.refCount, ptr, sizeof(int));
            ptr += sizeof(int);
            data.varType = this->headers[i].varType;
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
                    memcpy(&data.floatVal, ptr, sizeof(int));
                }
            }
            res.push_back(data);
        }
        return res;
    }
}

RID Table::exeInsert(vector <Data> data) {
    RID rid = {-1, -1};
    char pData[MAX_RECORD_LEN];
    memset(pData, 0, sizeof(pData));
    if (this->headers.size() != data.size()) {
        return rid;
    }

    char *ptr = pData;
    for (int i = 0, size = data.size(); i < size; i++) {
        int siz = this->headers[i].varType == VARCHAR ? data[i].stringVal.size() : this->headers[i].len;
        if (data[i].isNull == true) {
            siz = 0;
        }
        memcpy(ptr, &siz, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &data[i].refCount, sizeof(int));
        ptr += sizeof(int);

        if (data[i].isNull == true) {
            ptr += 0;
        } else if (this->headers[i].varType == CHAR) {
            memcpy(ptr, data[i].stringVal.c_str(), this->headers[i].len);
            ptr += this->headers[i].len;
        } else if (this->headers[i].varType == VARCHAR) {
            memcpy(ptr, data[i].stringVal.c_str(), data[i].stringVal.size());
            ptr += data[i].stringVal.size();
        } else if (this->headers[i].varType == INT || this->headers[i].varType == DATE) {
            memcpy(ptr, &data[i].intVal, sizeof(int));
            ptr += sizeof(int);
        } else if (this->headers[i].varType == FLOAT) {
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
    RID rid = {-1, -1};
    if (this->exeDelete(rid) != 0) {
        return rid;
    } else {
        return this->exeInsert(data);
    }
}

vector <RID> Table::allRecords() {
    return this->recordHandler->allRecords();
}

vector <TableHeader> Table::getHeaders() {
    vector <TableHeader> headers;
    TableHeader header;
    header.tableName = tableName;
    char *headerChar = new char[MAX_RECORD_LEN];
    recordHandler->readHeader(headerChar);
    int headerNum = *((int*)headerChar);
    char *ptr = headerChar + sizeof(int);
    for (int i = 0; i < headerNum; i++) {
        int stringLen = *((int*)ptr);
        char ch = *(ptr + stringLen);
        *(ptr + stringLen) = 0;
        header.headerName = ptr;
        *(ptr + stringLen) = ch;
        ptr += stringLen;

        stringLen = *((int*)ptr);
        ch = *(ptr + stringLen);
        *(ptr + stringLen) = 0;
        header.foreignTableName = ptr;
        *(ptr + stringLen) = ch;
        ptr += stringLen;

        stringLen = *((int*)ptr);
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
        header.isPrimary = *((bool*)ptr);
        ptr += sizeof(bool);
        header.isForeign = *((bool*)ptr);
        ptr += sizeof(bool);
        header.permitNull = *((bool*)ptr);
        ptr += sizeof(bool);
        headers.push_back(header);
    }
    delete[] headerChar;
    return headers;
}

int Table::writeHeaders(vector <TableHeader> headers) {
    vector <TableHeader> headers;
    char *headerChar = new char[MAX_RECORD_LEN];
    int headerNum = headers.size();
    memcpy(headerChar, &headerNum, sizeof(int));
    char *ptr = headerChar + sizeof(int);
    for (int i = 0; i < headerNum; i++) {
        int stringLen = headers[i].headerName.size();
        memcpy(ptr, &stringLen, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, headers[i].headerName.c_str(), stringLen);
        ptr += stringLen;

        stringLen = headers[i].foreignTableName.size();
        memcpy(ptr, &stringLen, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, headers[i].foreignTableName.c_str(), stringLen);
        ptr += stringLen;

        stringLen = headers[i].foreignHeaderName.size();
        memcpy(ptr, &stringLen, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, headers[i].foreignHeaderName.c_str(), stringLen);
        ptr += stringLen;

        memcpy(ptr, &headers[i].varType, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &headers[i].len, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &headers[i].refCount, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &headers[i].isPrimary, sizeof(bool));
        ptr += sizeof(bool);
        memcpy(ptr, &headers[i].isForeign, sizeof(bool));
        ptr += sizeof(bool);
        memcpy(ptr, &headers[i].permitNull, sizeof(bool));
        ptr += sizeof(bool);
    }
    recordHandler->writeHeader(headerChar);
    delete[] headerChar;
    this->headers = headers;
    return 0;
}
