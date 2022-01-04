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
    if (this->dbNameHandler->hasElement(dbName) == true) {
        cerr << "Database " << dbName << " already exists. Operation failed." << endl;
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
    if (this->dbNameHandler->hasElement(dbName) == false) {
        cerr << "Database " << dbName << " doesn't exist. Operation failed." << endl;
        return -1;
    } else if (this->dbName == dbName) {
        cerr << "Please close database before drop it." << endl;
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
                if (headerList[j].hasIndex == true) {
                    indexHandler->removeIndex("index_" + this->dbName + "_" + tableNameList[i], headerList[j].headerName);
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
        cerr << "Database " << dbName << " doesn't exist. Operation failed." << endl;
        return -1;
    } else if (this->dbName != "") {
        cerr << "Database " << this->dbName << " is opened. Can't open two databases at the same time. Operation failed." << endl;
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
        cerr << "No database is opened currently. Operation failed." << endl;
        return -1;
    } else {
        this->dbName = "";
        delete this->tableNameHandler;
        this->tableNameHandler = NULL;
        while (this->tableList.size() > 0) {
            delete this->tableList.back();
            this->tableList.pop_back();
        }
        return 0;
    }
}

string SystemManager::getDbName() {
    return this->dbName;
}

int SystemManager::createTable(string tableName, vector <TableHeader> headerList) {
    // 检查 table 是否存在，检查数据库是否存在
    if (hasTable(tableName) || this->dbName == "") {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    for (int i = 0; i < (int)headerList.size(); i++) {
        headerList[i].tableName = tableName;
        headerList[i].refCount = 0;
        headerList[i].hasIndex = false;
        headerList[i].isPrimary = false;
        headerList[i].isForeign = false;
        headerList[i].isUnique = false;
        if (headerList[i].varType == INT || headerList[i].varType == DATE) {
            headerList[i].len = sizeof(int);
        } else if (headerList[i].varType == FLOAT) {
            headerList[i].len = sizeof(double);
        }

        /*if (headerList[i].isPrimary == true) {
            // 主键不能重复，主键不能为空
            headerList[i].isUnique = true;
            headerList[i].permitNull = false;
        } else if (headerList[i].isForeign == true) {
            // 外键允许为空
            headerList[i].permitNull = true;
        }*/
    }

    if (!headerListLegal(headerList)) {
        cerr << "Header list illegal. Operation failed." << endl;
        return -1;
    }

    // 新建 table
    Table *table = new Table(this->dbName, tableName, bufManager, headerList);
    this->tableNameHandler->createElement(tableName);
    this->tableList.push_back(table);

    /*// 创建索引
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isUnique == true) {
            createIndex(headerList[i].tableName, headerList[i].headerName);
        }
    }

    // 修改外键信息
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true) {
            Table *foreignTable = getTable(headerList[i].foreignTableName);
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            for (int j = 0; j < (int)foreignHeaderList.size(); j++) {
                if (headerList[i].foreignHeaderName == foreignHeaderList[j].headerName) {
                    foreignHeaderList[j].refCount++;
                }
            }
            foreignTable->writeHeaderList(foreignHeaderList);
        }
    }*/

    return 0;
}

int SystemManager::dropTable(string tableName) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }
    // 检查是否有键被引用
    for (int i = 0; i < (int)this->tableList.size(); i++) {
        if (tableName == this->tableList[i]->getTableName()) {
            vector <TableHeader> headerList = this->tableList[i]->getHeaderList();
            for (int j = 0; j < (int)headerList.size(); j++) {
                if (headerList[i].isPrimary == true && headerList[i].refCount != 0) {
                    cerr << "Column " << headerList[j].headerName << " is referenced. Operation failed." << endl;
                    return -1;
                }
            }
        }
    }

    RecordHandler recordHandler(this->bufManager);
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    // 修改外键信息
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true) {
            Table *foreignTable = getTable(headerList[i].foreignTableName);
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            for (int j = 0; j < (int)foreignHeaderList.size(); j++) {
                if (headerList[i].foreignHeaderName == foreignHeaderList[j].headerName) {
                    foreignHeaderList[j].refCount--;
                }
            }
            foreignTable->writeHeaderList(foreignHeaderList);
        }
    }
    // 删除索引
    for (int j = 0; j < (int)headerList.size(); j++) {
        if (headerList[j].hasIndex == true) {
            this->indexHandler->removeIndex("index_" + this->dbName + "_" + tableName, headerList[j].headerName);
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

int SystemManager::createIndex(string tableName, string headerName) {
    // 检查 table 是否存在
    if (!hasTable(tableName) || this->dbName == "") {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    vector <RID> ridList = table->getRecordList();
    int idxPos = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].headerName == headerName) {
            idxPos = i;
        }
    }
    // 检查 header 是否存在
    if (idxPos == -1) {
        cerr << "Column " << headerName << " doesn't exist. Operation failed." << endl;
        return -1;
    } else if (headerList[idxPos].hasIndex == true) {
        cerr << "Column " << headerName << " has index already. Operation failed." << endl;
        return -1;
    }
    headerList[idxPos].hasIndex = true;
    table->writeHeaderList(headerList);
    VarType type = headerList[idxPos].varType == DATE ? INT : (headerList[idxPos].varType == CHAR ? VARCHAR : headerList[idxPos].varType);
    this->indexHandler->openIndex("index_" + getDbName() + "_" + tableName, headerName, type);
    for (int i = 0; i < (int)ridList.size(); i++) {
        vector <Data> dataList = table->exeSelect(ridList[i]);
        if (dataList[idxPos].isNull == false) {
            key_ptr keyPtr;
            char str[MAX_RECORD_LEN];
            memset(str, 0, sizeof(str));
            if (type == INT) {
                keyPtr = (char*)&dataList[idxPos].intVal;
            } else if (type == FLOAT) {
                keyPtr = (char*)&dataList[idxPos].floatVal;
            } else {
                memcpy(str, dataList[idxPos].stringVal.c_str(), dataList[i].stringVal.size());
                keyPtr = str;
            }
            this->indexHandler->insert(keyPtr, ridList[i]);
        }
    }
    this->indexHandler->closeIndex();

    return 0;
}

int SystemManager::dropIndex(string tableName, string headerName) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    vector <RID> ridList = table->getRecordList();
    int idxPos = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].headerName == headerName) {
            idxPos = i;
        }
    }
    // 检查 header 是否存在且已有索引
    if (idxPos == -1) {
        cerr << "Column " << headerName << " doesn't exist. Operation failed." << endl;
        return -1;
    } else if (headerList[idxPos].hasIndex == false) {
        cerr << "Column " << headerName << " doesn't have index yet. Operation failed." << endl;
        return -1;
    }
    headerList[idxPos].hasIndex = false;
    table->writeHeaderList(headerList);
    this->indexHandler->removeIndex(tableName, headerName);

    return 0;
}

int SystemManager::createColumn(string tableName, TableHeader header, Data defaultData) {
    // 检查 table 是否存在
    /*if (!hasTable(tableName)) {
        return -1;
    }

    header.tableName = tableName;
    header.refCount = 0;
    header.hasIndex = false;
    if (header.varType == INT || header.varType == DATE) {
        header.len = sizeof(int);
    } else if (header.varType == FLOAT) {
        header.len = sizeof(double);
    }

    if (header.isPrimary == true) {
        // 主键不能重复，主键不能为空
        header.isUnique = true;
        header.permitNull = false;
    } else if (header.isForeign == true) {
        // 外键允许为空
        header.permitNull = true;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    headerList.push_back(header);
    vector <RID> ridList = table->getRecordList();

    // 判断 header 是否符合要求
    if (!headerListLegal(headerList)) {
        return -1;
    }

    // 是否非法空值
    if (defaultData.isNull == true && header.permitNull == false) {
        return -1;
    // 是否类型不符
    } else if (defaultData.varType != header.varType && !((defaultData.varType == CHAR || defaultData.varType == VARCHAR) && (header.varType == CHAR || header.varType == VARCHAR))) {
        return -1;
    // 字符串长度是否非法
    } else if ((header.varType == CHAR || header.varType == VARCHAR) && (int)defaultData.stringVal.size() > header.len) {
        return -1;
    }

    // 判断 Unique 冲突
    if (header.isUnique == true && defaultData.isNull == false && ridList.size() > 1) {
        return -1;
    }
    if (header.isForeign == true && defaultData.isNull == false) {
        int count = countKey(header.foreignTableName, header.foreignHeaderName, header.varType, defaultData.intVal, defaultData.floatVal, defaultData.stringVal);
        // 外键不能引用不存在的键
        if (count != 1 && defaultData.isNull == false) {
            return -1;
        }
    }

    // 创建索引
    if (header.isUnique == true) {
        createIndex(tableName, header.headerName);
    }

    // 修改外键信息
    if (header.isForeign == true) {
        Table *foreignTable = getTable(header.foreignTableName);
        vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
        for (int i = 0; i < (int)foreignHeaderList.size(); i++) {
            if (header.foreignHeaderName == foreignHeaderList[i].headerName) {
                foreignHeaderList[i].refCount++;
            }
        }
        foreignTable->writeHeaderList(foreignHeaderList);
    }

    // 修改数据
    vector <vector <Data> > dataLists;
    for (int i = 0; i < (int)ridList.size(); i++) {
        vector <Data> dataList = table->exeSelect(ridList[i]);
        opDelete(tableName, dataList, ridList[i]);
        dataList.push_back(defaultData);
        dataLists.push_back(dataList);
    }
    table->writeHeaderList(headerList);
    for (int i = 0; i < (int)dataLists.size(); i++) {
        opInsert(tableName, dataLists[i]);
    }*/

    return 0;
}

int SystemManager::dropColumn(string tableName, string headerName) {
    // 检查 table 是否存在
    /*if (!hasTable(tableName)) {
        return -1;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    int idxPos = -1;
    // 检查是否有键被引用
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerName == headerList[i].headerName) {
            if (headerList[i].refCount != 0) {
                return -1;
            } else {
                idxPos = i;
            }
        }
    }

    // 检查是否找到 header
    if (idxPos == -1) {
        return -1;
    }

    RecordHandler recordHandler(this->bufManager);
    // 删除索引
    if (headerList[idxPos].hasIndex == true) {
        this->indexHandler->removeIndex("index_" + this->dbName + "_" + tableName, headerList[idxPos].headerName);
    }

    // 修改外键信息
    if (headerList[idxPos].isForeign == true) {
        Table *foreignTable = getTable(headerList[idxPos].foreignTableName);
        vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
        for (int i = 0; i < (int)foreignHeaderList.size(); i++) {
            if (headerList[idxPos].foreignHeaderName == foreignHeaderList[i].headerName) {
                foreignHeaderList[i].refCount--;
            }
        }
        foreignTable->writeHeaderList(foreignHeaderList);
    }

    // 修改数据
    vector <vector <Data> > dataLists;
    vector <RID> ridList = table->getRecordList();
    for (int i = 0; i < (int)ridList.size(); i++) {
        vector <Data> dataList = table->exeSelect(ridList[i]);
        opDelete(tableName, dataList, ridList[i]);
        dataList.erase(dataList.begin() + idxPos);
        dataLists.push_back(dataList);
    }
    headerList.erase(headerList.begin() + idxPos);
    table->writeHeaderList(headerList);
    for (int i = 0; i < (int)dataLists.size(); i++) {
        opInsert(tableName, dataLists[i]);
    }*/

    return 0;
}

int SystemManager::createPrimary(string tableName, vector <string> headerNameList) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    // 检查是否已经有主键
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isPrimary == true) {
            cerr << "Table " << tableName << " has primary key already. Operation failed." << endl;
            return -1;
        }
    }

    for (int i = 0; i < (int)headerNameList.size(); i++) {
        int find = 0;
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                // 检查是否满足 unique 要求
                if (columnUnique(tableName, headerList[j].headerName) == false) {
                    cerr << "Column " << headerList[j].headerName << " is not unique. Operation failed." << endl;
                    return -1;
                } else if (headerList[j].permitNull == true) {
                    cerr << "Column " << headerList[j].headerName << " permits null. Operation failed." << endl;
                    return -1;
                }
                find = 1;
                break;
            }
        }
        // 检查列是否存在
        if (find == 0) {
            cerr << "Column " << headerNameList[i] << " doesn't exist. Operation failed." << endl;
            return -1;
        }
    }

    for (int i = 0; i < (int)headerNameList.size(); i++) {
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                headerList[j].isPrimary = true;
                headerList[j].id = i;
            }
        }
    }
    table->writeHeaderList(headerList);

    return 0;
}

int SystemManager::dropPrimary(string tableName, vector <string> headerNameList) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();

    int counter = 0;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isPrimary == true) {
            counter++;
        }
    }
    if ((int)headerNameList.size() != counter) {
        cerr << "Primary key doesn't exist. Operation failed." << endl;
        return -1;
    }

    for (int i = 0; i < (int)headerNameList.size(); i++) {
        int find = 0;
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                // 检查是否为主键
                if (headerList[j].isPrimary == false) {
                    cerr << "Primary key doesn't exist. Operation failed." << endl;
                    return -1;
                }
                // 检查是否被引用
                if (headerList[j].refCount > 0) {
                    cerr << "Primary key is referenced. Operation failed." << endl;
                    return -1;
                }
                find = 1;
                break;
            }
        }
        // 检查列是否存在
        if (find == 0) {
            cerr << "Primary key doesn't exist. Operation failed." << endl;
            return -1;
        }
    }

    for (int i = 0; i < (int)headerNameList.size(); i++) {
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                headerList[j].isPrimary = false;
                headerList[j].id = -1;
            }
        }
    }
    table->writeHeaderList(headerList);

    return 0;
}

int SystemManager::createForeign(string tableName, string foreignTableName, vector <TableHeader> updateHeaderList) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    } else if (!hasTable(foreignTableName)) {
        cerr << "Table " << foreignTableName << " doesn't exist. Operation failed." << endl;
        return -1;
    // 检查是否引用自己
    } else if (tableName == foreignTableName) {
        cerr << "Table " << tableName << " can't refer to itself." << endl;
        return -1;
    }

    Table *table = getTable(tableName);
    Table *foreignTable = getTable(foreignTableName);
    vector <TableHeader> headerList = table->getHeaderList();
    vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
    vector <int> updatePos;
    int counter = 0;

    for (int i = 0; i < (int)updateHeaderList.size(); i++) {
        int find = 0;
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (updateHeaderList[i].headerName == headerList[j].headerName) {
                // 不能同时为主键和外键
                if (headerList[j].isPrimary == true) {
                    cerr << "Column " << updateHeaderList[i].headerName << " is part of primary key. Operation failed." << endl;
                    return -1;
                }
                updatePos.push_back(j);
                find = 1;
                break;
            }
        }
        // 检查列是否存在
        if (find == 0) {
            cerr << "Column " << updateHeaderList[i].headerName << " doesn't exist. Operation failed." << endl;
            return -1;
        }
        find = 0;
        for (int j = 0; j < (int)foreignHeaderList.size(); j++) {
            if (updateHeaderList[i].foreignHeaderName == foreignHeaderList[j].headerName && foreignHeaderList[j].isPrimary == true) {
                find = 1;
                break;
            }
        }
        // 检查引用列是否存在
        if (find == 0) {
            cerr << "Column " << updateHeaderList[i].foreignHeaderName << " doesn't exist. Operation failed." << endl;
            return -1;
        }
    }

    for (int i = 0; i < (int)foreignHeaderList.size(); i++) {
        if (foreignHeaderList[i].isPrimary == true) {
            counter++;
        }
    }
    // 检查是否正确引用了主键
    if ((int)updateHeaderList.size() != counter) {
        cerr << "Primary key doesn't exist. Operation failed." << endl;
        return -1;
    }

    int maxNum = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true) {
            maxNum = headerList[i].id > maxNum ? headerList[i].id : maxNum;
        }
    }

    // 修改外键信息
    for (int i = 0; i < (int)updateHeaderList.size(); i++) {
        headerList[updatePos[i]].isForeign = true;
        headerList[updatePos[i]].foreignTableName = foreignTableName;
        headerList[updatePos[i]].foreignHeaderName = updateHeaderList[i].foreignHeaderName;
        headerList[updatePos[i]].id = maxNum + 1;
    }
    table->writeHeaderList(headerList);

    // 修改引用信息
    for (int i = 0; i < (int)foreignHeaderList.size(); i++) {
        if (foreignHeaderList[i].isPrimary == true) {
            foreignHeaderList[i].refCount++;
        }
    }
    foreignTable->writeHeaderList(foreignHeaderList);

    return 0;
}

int SystemManager::dropForeign(string tableName, vector <string> headerNameList) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    int counter = 0, id = -1;
    string foreignTableName;

    for (int i = 0; i < (int)headerNameList.size(); i++) {
        int find = 0;
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                // 检查删除的 key 是否为一组
                if (id == -1) {
                    id = headerList[j].id;
                    foreignTableName = headerList[j].foreignTableName;
                } else if (headerList[j].id != id) {
                    cerr << "Foreign key doesn't exist. Operation failed." << endl;
                    return -1;
                }
                find = 1;
                break;
            }
        }
        // 检查列是否存在
        if (find == 0) {
            cerr << "Column " << headerNameList[i] << " doesn't exist. Operation failed." << endl;
            return -1;
        }
    }

    Table *foreignTable = getTable(foreignTableName);
    vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();

    for (int i = 0; i < (int)foreignHeaderList.size(); i++) {
        if (foreignHeaderList[i].isPrimary == true) {
            counter++;
        }
    }
    // 检查是否完整删除外键
    if ((int)headerNameList.size() != counter) {
        cerr << "Foreign key doesn't exist. Operation failed." << endl;
        return -1;
    }

    // 修改外键信息
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true && headerList[i].id == id) {
            headerList[i].isForeign = false;
            headerList[i].foreignTableName = "";
            headerList[i].foreignHeaderName = "";
            headerList[i].id = -1;
        }
    }

    table->writeHeaderList(headerList);

    // 修改引用信息
    for (int i = 0; i < (int)foreignHeaderList.size(); i++) {
        if (foreignHeaderList[i].isPrimary == true) {
            foreignHeaderList[i].refCount--;
        }
    }
    foreignTable->writeHeaderList(foreignHeaderList);

    return 0;
}

int SystemManager::createUnique(string tableName, string headerName) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    int idxPos = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerName == headerList[i].headerName) {
            idxPos = i;
            // 检查是否已经 unique
            if (headerList[i].isUnique == true) {
                cerr << "Column " << headerName << " is already unique. Operation failed." << endl;
                return -1;
            }
        }
    }
    // 检查列是否存在
    if (idxPos == -1) {
        cerr << "Column " << headerName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    // 检查元素是否有重复
    if (!columnUnique(tableName, headerName)) {
        cerr << "Column " << headerName << " has duplicate elements. Operation failed." << endl;
        return -1;
    }

    headerList[idxPos].isUnique = true;
    table->writeHeaderList(headerList);

    return 0;
}

int SystemManager::dropUnique(string tableName, string headerName) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    int idxPos = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerName == headerList[i].headerName) {
            idxPos = i;
            // 检查是否未 unique
            if (headerList[i].isUnique == false) {
                cerr << "Column " << headerName << " isn't unique yet. Operation failed." << endl;
                return -1;
            }
        }
    }
    // 检查列是否存在
    if (idxPos == -1) {
        cerr << "Column " << headerName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    headerList[idxPos].isUnique = false;
    table->writeHeaderList(headerList);

    return 0;
}

bool SystemManager::headerListLegal(vector <TableHeader> headerList) {
    map <string, int> nameMap;
    for (int i = 0, hasPrimary = 0; i < (int)headerList.size(); i++) {
        // 名称不能重复
        if (nameMap.count(headerList[i].headerName) != 0) {
            return false;
        } else {
            nameMap[headerList[i].headerName] = 1;
        }
        // 数据类型和长度必须合法
        if ((headerList[i].varType == CHAR || headerList[i].varType == VARCHAR) && headerList[i].len <= 0) {
            return false;
        }

        if (headerList[i].isPrimary == true) {
            // 不能同时为主键和外键，主键不能为空，主键不能有多个
            if (headerList[i].isForeign == true || hasPrimary == 1) {
                return false;
            }
            hasPrimary = 1;
        } else if (headerList[i].isForeign == true) {
            // 检查 table 是否存在
            if (!hasTable(headerList[i].foreignTableName)) {
                return false;
            }
            Table *foreignTable = getTable(headerList[i].foreignTableName);
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            int findForeignHeader = 0;
            for (int j = 0; j < (int)foreignHeaderList.size(); j++) {
                if (headerList[i].foreignHeaderName == foreignHeaderList[j].headerName) {
                    // 检查外键信息是否正确
                    if (foreignHeaderList[j].isPrimary == false || headerList[i].varType != foreignHeaderList[j].varType || headerList[i].len != foreignHeaderList[j].len) {
                        return false;
                    }
                    findForeignHeader = 1;
                }
            }
            // 检查是否找到外键
            if (findForeignHeader == 0) {
                return false;
            }
        }
    }

    return true;
}

bool SystemManager::columnUnique(string tableName, string headerName) {
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    vector <RID> ridList = table->getRecordList();
    map <int, int> mapInt;
    map <double, int> mapFloat;
    map <string, int> mapString;
    int idxPos = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerName == headerList[i].headerName) {
            idxPos = i;
        }
    }

    // 用 map 判断是否有重复元素
    for (int i = 0; i < (int)ridList.size(); i++) {
        vector <Data> dataList = table->exeSelect(ridList[i]);
        if (headerList[idxPos].varType == INT || headerList[idxPos].varType == DATE) {
            if (mapInt.count(dataList[idxPos].intVal) != 0) {
                return false;
            } else {
                mapInt[dataList[idxPos].intVal] = 1;
            }
        } else if (headerList[idxPos].varType == FLOAT) {
            if (mapFloat.count(dataList[idxPos].floatVal) != 0) {
                return false;
            } else {
                mapFloat[dataList[idxPos].floatVal] = 1;
            }
        } else if (headerList[idxPos].varType == CHAR || headerList[idxPos].varType == VARCHAR) {
            if (mapString.count(dataList[idxPos].stringVal) != 0) {
                return false;
            } else {
                mapString[dataList[idxPos].stringVal] = 1;
            }
        }
    }

    return true;
}

void SystemManager::opInsert(string tableName, vector <Data> dataList) {
    // 插入数据
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    RID rid = table->exeInsert(dataList);

    // 插入索引
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].hasIndex == true && dataList[i].isNull == false) {
            VarType type = headerList[i].varType == DATE ? INT : (headerList[i].varType == CHAR ? VARCHAR : headerList[i].varType);
            this->indexHandler->openIndex("index_" + this->dbName + "_" + tableName, headerList[i].headerName, type);
            key_ptr keyPtr;
            char str[MAX_RECORD_LEN];
            memset(str, 0, sizeof(str));
            if (type == INT) {
                keyPtr = (char*)&dataList[i].intVal;
            } else if (type == FLOAT) {
                keyPtr = (char*)&dataList[i].floatVal;
            } else {
                memcpy(str, dataList[i].stringVal.c_str(), dataList[i].stringVal.size());
                keyPtr = str;
            }
            this->indexHandler->insert(keyPtr, rid);
            this->indexHandler->closeIndex();
        }
    }

    // 更新外键对应的表的信息
    foreignKeyProcess(headerList, dataList, 1);
}

void SystemManager::opDelete(string tableName, vector <Data> dataList, RID rid) {
    // 删除数据
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    table->exeDelete(rid);

    // 删除索引
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].hasIndex == true && dataList[i].isNull == false) {
            VarType type = headerList[i].varType == DATE ? INT : (headerList[i].varType == CHAR ? VARCHAR : headerList[i].varType);
            this->indexHandler->openIndex("index_" + this->dbName + "_" + tableName, headerList[i].headerName, type);
            key_ptr keyPtr;
            char str[MAX_RECORD_LEN];
            memset(str, 0, sizeof(str));
            if (type == INT) {
                keyPtr = (char*)&dataList[i].intVal;
            } else if (type == FLOAT) {
                keyPtr = (char*)&dataList[i].floatVal;
            } else {
                memcpy(str, dataList[i].stringVal.c_str(), dataList[i].stringVal.size());
                keyPtr = str;
            }
            this->indexHandler->remove(keyPtr, rid);
            this->indexHandler->closeIndex();
        }
    }

    // 更新外键对应的表的信息
    foreignKeyProcess(headerList, dataList, -1);
}

void SystemManager::foreignKeyProcess(vector <TableHeader> headerList, vector <Data> dataList, int delta) {
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true && dataList[i].isNull == false) {
            Table *foreignTable = getTable(headerList[i].foreignTableName);
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            vector <RID> ridList = foreignTable->getRecordList();
            int refPos = -1;
            for (int j = 0; j < (int)foreignHeaderList.size(); j++) {
                if (headerList[i].foreignHeaderName == foreignHeaderList[j].headerName) {
                    refPos = j;
                }
            }
            for (int j = 0; j < (int)ridList.size(); j++) {
                vector <Data> foreignDataList = foreignTable->exeSelect(ridList[j]);
                if (((headerList[i].varType == INT || headerList[i].varType == DATE) && dataList[i].intVal == foreignDataList[refPos].intVal)
                    || (headerList[i].varType == FLOAT && dataList[i].floatVal == foreignDataList[refPos].floatVal)
                    || ((headerList[i].varType == CHAR || headerList[i].varType == VARCHAR) && dataList[i].stringVal == foreignDataList[refPos].stringVal)) {
                    foreignDataList[refPos].refCount += delta;
                    RID foreignNewRid = foreignTable->exeUpdate(foreignDataList, ridList[j]);
                    if (foreignHeaderList[refPos].hasIndex == true) {
                        VarType type = headerList[i].varType == DATE ? INT : (headerList[i].varType == CHAR ? VARCHAR : headerList[i].varType);
                        this->indexHandler->openIndex("index_" + this->dbName + "_" + headerList[i].foreignTableName, headerList[i].foreignHeaderName, type);
                        char str[MAX_RECORD_LEN];
                        memset(str, 0, sizeof(str));
                        key_ptr keyPtr;
                        if (type == INT) {
                            keyPtr = (char*)&dataList[i].intVal;
                        } else if (type == FLOAT) {
                            keyPtr = (char*)&dataList[i].floatVal;
                        } else {
                            memcpy(str, dataList[i].stringVal.c_str(), dataList[i].stringVal.size());
                            keyPtr = str;
                        }
                        this->indexHandler->remove(keyPtr, ridList[j]);
                        this->indexHandler->insert(keyPtr, foreignNewRid);
                        this->indexHandler->closeIndex();
                    }
                    break;
                }
            }
            // 索引中的类型只有 INT FLOAT VARCHAR 三种
            /*VarType type = headerList[i].varType == DATE ? INT : (headerList[i].varType == CHAR ? VARCHAR : headerList[i].varType);
            // 打开索引
            this->indexHandler->openIndex("index_" + this->dbName + "_" + headerList[i].foreignTableName, headerList[i].foreignHeaderName, type);
            vector <RID> rids;
            // 根据数据类型得到索引的 key
            char str[MAX_RECORD_LEN];
            memset(str, 0, sizeof(str));
            key_ptr keyPtr;
            if (type == INT) {
                rids = this->indexHandler->getRIDs((char*)&dataList[i].intVal);
                keyPtr = (char*)&dataList[i].intVal;
            } else if (type == FLOAT) {
                rids = this->indexHandler->getRIDs((char*)&dataList[i].floatVal);
                keyPtr = (char*)&dataList[i].floatVal;
            } else {
                memcpy(str, dataList[i].stringVal.c_str(), dataList[i].stringVal.size());
                rids = this->indexHandler->getRIDs(str);
                keyPtr = str;
            }

            // 计算外键引用的数据的 refCount
            Table *foreignTable = getTable(headerList[i].foreignTableName);
            vector <Data> foreignDataList = foreignTable->exeSelect(rids[0]);
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            for (int j = 0; j < (int)foreignDataList.size(); j++) {
                if (foreignHeaderList[j].headerName == headerList[i].foreignHeaderName) {
                    foreignDataList[j].refCount += delta;
                }
            }

            // 更新外键引用的数据
            RID foreignNewRid = foreignTable->exeUpdate(foreignDataList, rids[0]);

            // 更新引用的数据的索引
            this->indexHandler->remove(keyPtr, rids[0]);
            this->indexHandler->insert(keyPtr, foreignNewRid);
            // 关闭索引
            this->indexHandler->closeIndex();*/
        }
    }
}

/*int SystemManager::countKey(string tableName, string headerName, VarType type, int intVal, double floatVal, string stringVal) {
    VarType indexType = type == DATE ? INT : (type == CHAR ? VARCHAR : type);
    indexHandler->openIndex("index_" + this->dbName + "_" + tableName, headerName, indexType);
    int count = 0;
    if (indexType == INT) {
        count = indexHandler->count((char*)&intVal);
    } else if (indexType == FLOAT) {
        count = indexHandler->count((char*)&floatVal);
    } else {
        char str[MAX_RECORD_LEN];
        memcpy(str, stringVal.c_str(), stringVal.size());
        count = indexHandler->count(str);
    }
    indexHandler->closeIndex();
    return count;
}*/
