# include "name_handler.h"

NameHandler::NameHandler(BufManager *bufManager) {
    this->bufManager = bufManager;
    this->recordHandler = new RecordHandler(bufManager);
    if (this->recordHandler->openFile("header.dat") == -1) {
        this->recordHandler->createFile("header.dat");
        this->recordHandler->openFile("header.dat");
        this->elementNameList.clear();
        writeElementList(this->elementNameList);
    } else {
        this->elementNameList = readElementList();
    }
    this->dbName = "";
}

NameHandler::NameHandler(BufManager *bufManager, string dbName) {
    this->bufManager = bufManager;
    this->recordHandler = new RecordHandler(bufManager);
    if (this->recordHandler->openFile("header_" + dbName + ".dat") == -1) {
        this->recordHandler->createFile("header_" + dbName + ".dat");
        this->recordHandler->openFile("header_" + dbName + ".dat");
        this->elementNameList.clear();
        writeElementList(this->elementNameList);
    } else {
        this->elementNameList = readElementList();
    }
    this->dbName = dbName;
}

NameHandler::~NameHandler() {
    this->recordHandler->closeFile();
    delete this->recordHandler;
}

vector <string> NameHandler::getElementList() {
    return this->readElementList();
}

bool NameHandler::hasElement(string elementName) {
    // 检查是否有相同名字
    for (int i = 0; i < (int)this->elementNameList.size(); i++) {
        if (this->elementNameList[i] == elementName) {
            return true;
        }
    }
    return false;
}

int NameHandler::createElement(string elementName) {
    // 检查是否有相同名字
    for (int i = 0; i < (int)this->elementNameList.size(); i++) {
        if (this->elementNameList[i] == elementName) {
            return -1;
        }
    }
    // 新增名字
    this->elementNameList.push_back(elementName);
    writeElementList(this->elementNameList);
    return 0;
}

int NameHandler::dropElement(string elementName) {
    // 查询名字
    for (int i = 0; i < (int)this->elementNameList.size(); i++) {
        if (this->elementNameList[i] == elementName) {
            // 删除名字
            this->elementNameList.erase(elementNameList.begin() + i);
            writeElementList(this->elementNameList);
            return 0;
        }
    }
    // 未找到名字，删除失败
    return -1;
}

vector <string> NameHandler::readElementList() {
    vector <string> headerList;
    char *headerChar = new char[MAX_RECORD_LEN];
    this->recordHandler->readHeader(headerChar);
    int headerNum = *((int*)headerChar);
    char *ptr = headerChar + sizeof(int);
    for (int i = 0; i < headerNum; i++) {
        int stringLen = *((int*)ptr);
        ptr += sizeof(int);
        char ch = *(ptr + stringLen);
        *(ptr + stringLen) = 0;
        headerList.push_back(ptr);
        *(ptr + stringLen) = ch;
        ptr += stringLen;
    }
    delete[] headerChar;
    return headerList;
}

int NameHandler::writeElementList(vector <string> elementNameList) {
    char *headerChar = new char[MAX_RECORD_LEN];
    int headerNum = elementNameList.size();
    memcpy(headerChar, &headerNum, sizeof(int));
    char *ptr = headerChar + sizeof(int);
    for (int i = 0; i < headerNum; i++) {
        int stringLen = elementNameList[i].size();
        memcpy(ptr, &stringLen, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, elementNameList[i].c_str(), stringLen);
        ptr += stringLen;
    }
    this->recordHandler->writeHeader(headerChar);
    delete[] headerChar;
    this->elementNameList = elementNameList;
    return 0;
}
