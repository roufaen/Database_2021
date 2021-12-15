# include "system_manager.h"

SystemManager::SystemManager(IndexHandler *indexHandler, BufManager *bufManager) {
    this->indexHandler = indexHandler;
    this->bufManager = bufManager;
    this->dbNameHandler = new NameHandler(this->bufManager);
}

SystemManager::~SystemManager() {
    delete this->dbNameHandler;
}

int SystemManager::createDb(string dbName) {
    if (this->dbNameHandler->hasElement(dbName)) {
        return -1;
    } else {
        // 添加至数据库名列表
        this->dbNameHandler->createElement(dbName);
        // 创建数据库信息文件
        NameHandler tableNameHandler(this->bufManager, dbName);
        return 0;
    }
}

int SystemManager::dropDb(string dbName) {
    // 检查数据库是否存在
    if (!this->dbNameHandler->hasElement(dbName)) {
        return -1;
    } else {
        // 从数据库名列表中移除
        this->dbNameHandler->dropElement(dbName);
        RecordHandler recordHandler(bufManager);
        NameHandler tableNameHandler(this->bufManager, dbName);
        vector <string> tableNameList = tableNameHandler.getElementList();
        for (int i = 0; i < (int)tableNameList.size(); i++) {
            // 删除索引
            Table table(dbName, tableNameList[i], this->bufManager);
            vector <TableHeader> headerList = table.getHeaderList();
            for (int j = 0; j < (int)headerList.size(); j++) {
                if (headerList[j].isPrimary == true) {
                    VarType type = headerList[j].varType == DATE ? INT : (headerList[j].varType == CHAR ? VARCHAR : headerList[j].varType);
                    indexHandler->openIndex("index_" + this->dbName + "_" + tableNameList[i], headerList[j].headerName, type, this->bufManager);
                    indexHandler->removeIndex();
                }
            }
            // 删除 table
            recordHandler.destroyFile("table_" + this->dbName + "_" + tableNameList[i] + ".dat");
        }
        // 删除数据库信息文件
        recordHandler.destroyFile("header_" + this->dbName + ".dat");
        return 0;
    }
}

int SystemManager::openDb(string dbName) {
    // 检查数据库是否存在
    if (!this->dbNameHandler->hasElement(dbName)) {
        return -1;
    } else {
        this->dbName = dbName;
        this->tableNameHandler = new NameHandler(this->bufManager, dbName);
        vector <string> tableNameList = this->tableNameHandler->getElementList();
        for (int i = 0; i < (int)tableNameList.size(); i++) {
            Table *table = new Table(dbName, tableNameList[i], this->bufManager);
            this->tableList.push_back(table);
        }
        return 0;
    }
}

int SystemManager::closeDb() {
    // 检查是否已打开数据库
    if (this->dbName == "") {
        return -1;
    } else {
        this->dbName = "";
        delete this->tableNameHandler;
        this->tableNameHandler = NULL;
        while (this->tableList.size() > 0) {
            this->tableList.pop_back();
        }
        return 0;
    }
}

string SystemManager::getDbName() {
    return this->dbName;
}

int SystemManager::createTable(string tableName, vector <TableHeader> headerList) {
    map <string, int> nameMap;
    for (int i = 0, hasPrimary = 0; i < (int)headerList.size(); i++) {
        headerList[i].tableName = tableName;
        headerList[i].refCount = 0;
        // 名称不能重复
        if (nameMap.count(headerList[i].headerName) != 0) {
            return -1;
        } else {
            nameMap[headerList[i].headerName] = 1;
        }
        // 数据类型和长度必须合法
        if (headerList[i].varType == INT || headerList[i].varType == DATE) {
            headerList[i].len = sizeof(int);
        } else if (headerList[i].varType == FLOAT) {
            headerList[i].len = sizeof(double);
        } else if (headerList[i].varType == CHAR || headerList[i].varType == VARCHAR) {
            if (headerList[i].len <= 0) {
                return -1;
            }
        }

        if (headerList[i].isPrimary == true) {
            // 不能同时为主键和外键，主键不能为空，主键不能重复
            if (headerList[i].isForeign == true || headerList[i].permitNull == true || hasPrimary == 1) {
                return -1;
            }
            hasPrimary = 1;
        } else if (headerList[i].isForeign == true) {
            // 检查 table 是否存在
            if (!hasTable(headerList[i].foreignTableName)) {
                return -1;
            }
            Table *foreignTable = getTable(headerList[i].foreignTableName);
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            int findForeignHeader = 0;
            for (int j = 0; j < (int)foreignHeaderList.size(); j++) {
                if (headerList[i].foreignHeaderName == foreignHeaderList[j].headerName) {
                    // 检查外键信息是否正确
                    if (headerList[i].varType != foreignHeaderList[j].varType || headerList[i].len != foreignHeaderList[j].len) {
                        return -1;
                    }
                    findForeignHeader = 1;
                }
            }
            // 检查是否找到外键
            if (findForeignHeader == 0) {
                return -1;
            }
        }
    }

    // 新建 table
    Table *table = new Table(this->dbName, tableName, bufManager, headerList);
    this->tableNameHandler->createElement(tableName);
    this->tableList.push_back(table);

    // 修改外键信息
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true) {
            Table *foreignTable = getTable(headerList[i].foreignTableName);
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            for (int j = 0; j < (int)foreignHeaderList.size(); i++) {
                if (headerList[i].foreignHeaderName == foreignHeaderList[j].headerName) {
                    foreignHeaderList[j].refCount++;
                }
            }
            foreignTable->writeHeaderList(foreignHeaderList);
        }
    }

    return 0;
}

int SystemManager::dropTable(string tableName) {
    // 检查 table 是否存在
    if (!this->tableNameHandler->hasElement(tableName)) {
        return -1;
    }
    // 检查是否有键被引用
    for (int i = 0; i < (int)this->tableList.size(); i++) {
        if (tableName == this->tableList[i]->getTableName()) {
            vector <TableHeader> headerList = this->tableList[i]->getHeaderList();
            for (int j = 0; j < (int)headerList.size(); j++) {
                if (headerList[i].refCount != 0) {
                    return -1;
                }
            }
        }
    }

    RecordHandler recordHandler(this->bufManager);
    Table table(dbName, tableName, this->bufManager);
    vector <TableHeader> headerList = table.getHeaderList();
    // 修改外键信息
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true) {
            Table *foreignTable = getTable(headerList[i].foreignTableName);
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            for (int j = 0; j < (int)foreignHeaderList.size(); i++) {
                if (headerList[i].foreignHeaderName == foreignHeaderList[j].headerName) {
                    foreignHeaderList[j].refCount--;
                }
            }
            foreignTable->writeHeaderList(foreignHeaderList);
        }
    }
    // 删除索引
    for (int j = 0; j < (int)headerList.size(); j++) {
        if (headerList[j].isPrimary == true) {
            VarType type = headerList[j].varType == DATE ? INT : (headerList[j].varType == CHAR ? VARCHAR : headerList[j].varType);
            indexHandler->openIndex("index_" + this->dbName + "_" + tableName, headerList[j].headerName, type, this->bufManager);
            indexHandler->removeIndex();
        }
    }
    // 删除 table
    this->tableNameHandler->dropElement(tableName);
    recordHandler.destroyFile("table_" + this->dbName + "_" + tableName + ".dat");
    for (int i = 0; i < (int)this->tableList.size(); i++) {
        if (tableName == this->tableList[i]->getTableName()) {
            this->tableList.erase(this->tableList.begin() + i);
        }
    }

    return 0;
}

bool SystemManager::hasTable(string tableName) {
    for (int i = 0; i < (int)this->tableList.size(); i++) {
        if (this->tableList[i]->getTableName() == tableName) {
            return true;
        }
    }
    return false;
}

Table* SystemManager::getTable(string tableName) {
    for (int i = 0; i < (int)this->tableList.size(); i++) {
        if (this->tableList[i]->getTableName() == tableName) {
            return this->tableList[i];
        }
    }
    return NULL;
}

/*int SystemManager::createIndex(string tableName, string headerName) {
}

int SystemManager::dropIndex(string tableName, string headerName) {
}*/
