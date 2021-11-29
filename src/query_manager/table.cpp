# include "table.h"

Table::Table(string dbName, string tableName, RecordHandler *recordHandler) {
    this->recordHandler = recordHandler;
    this->dbName = dbName, this->tableName = tableName;
    // open file
    string fileName = "table_" + dbName + "_" + tableName;
    recordHandler->openFile(fileName);
    // get header
    RID rid = {0, 0};
    TableHeader header;
    header.tableName = tableName;
    char *headerChar = new char[MAX_RECORD_LEN];
    recordHandler->getRecord(rid, headerChar);
    int headerNum = *((int*)headerChar);
    char *ptr = headerChar + sizeof(int);
    for (int i = 0; i < headerNum; i++) {
        header.headerName = ptr;
        ptr += header.headerName.size();
        header.varType = *((int*)ptr);
        ptr += sizeof(int);
        header.len = *((int*)ptr);
        ptr += sizeof(int);
        header.isPrimary = *((bool*)ptr);
        ptr += sizeof(bool);
        header.isForeign = *((bool*)ptr);
        ptr += sizeof(bool);
        header.permitNull = *((bool*)ptr);
        ptr += sizeof(bool);
        this->headers.push_back(header);
    }
    delete[] headerChar;
}

Table::~Table() {
    recordHandler->closeFile();
}

vector <TableHeader> Table::getHeaders() {
    return this->headers;
}

vector <Data> Table::exeSelect(RID rid) {
}

RID Table::exeInsert(vector <Data> data) {
    RID rid;
    char *pData[MAX_RECORD_LEN];
    for (int i = 0, size = data.size(); i < size; i++) {
        
    }
}

int Table::exeDelete(RID rid) {
}

RID Table::exeUpdate(vector <Data> data, RID rid) {
}
