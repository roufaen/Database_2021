# include "system_manager.h"

SystemManager::SystemManager(IndexHandler *indexHandler, BufManager *bufManager) {
    this->indexHandler = indexHandler;
    this->bufManager = bufManager;
    this->dbNameHandler = new NameHandler(this->bufManager);
}

SystemManager::~SystemManager() {
    delete this->dbNameHandler;
}

int SystemManager::getDbNameList(vector <string>& nameList) {
    nameList = this->dbNameHandler->getElementList();
    return 0;
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
                if (headerList[j].isPrimary == true || headerList[j].isUnique == true || headerList[j].hasIndex == true) {
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
        closeDb();
    }
    this->dbName = dbName;
    this->tableNameHandler = new NameHandler(this->bufManager, dbName);
    vector <string> tableNameList = this->tableNameHandler->getElementList();
    for (int i = 0; i < (int)tableNameList.size(); i++) {
        Table *table = new Table(dbName, tableNameList[i], this->bufManager);
        this->tableList.push_back(table);
    }
    return 0;
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

int SystemManager::getTableNameList(vector <string>& nameList) {
    if (this->dbName == "") {
        cerr << "No database opened. Operation failed." << endl;
        return -1;
    }
    nameList = this->tableNameHandler->getElementList();
    return 0;
}

int SystemManager::createTable(string tableName, vector <TableHeader> headerList) {
    // 检查 table 是否存在，检查数据库是否存在
    if (this->dbName == "") {
        cerr << "No database opened. Operation failed." << endl;
        return -1;
    } else if (hasTable(tableName)) {
        cerr << "Table " << tableName << " already exists. Operation failed." << endl;
        return -1;
    }

    for (int i = 0; i < (int)headerList.size(); i++) {
        headerList[i].tableName = tableName;
        headerList[i].refCount = 0;
        headerList[i].hasIndex = false;
        headerList[i].isPrimary = false;
        headerList[i].isForeign = false;
        headerList[i].foreignGroup = -1;
        headerList[i].isUnique = false;
        headerList[i].uniqueGroup = -1;
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
            vector <RID> ridList = table->getRecordList();
            vector <vector <Data>> dataLists;
            for (int j = 0; j < (int)ridList.size(); j++) {
                opDelete(tableName, table->exeSelect(ridList[j]), ridList[j]);
            }
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
        if (headerList[j].isPrimary == true || headerList[j].isUnique == true || headerList[j].hasIndex == true) {
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

int SystemManager::getHeaderList(string tableName, vector <TableHeader>& headerList) {
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }
    Table *table = getTable(tableName);
    headerList = table->getHeaderList();
    return 0;
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
    if (headerList[idxPos].isPrimary == false && headerList[idxPos].isUnique == false) {
        opCreateIndex(tableName, headerName);
    }

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
    if (headerList[idxPos].isPrimary == false && headerList[idxPos].isUnique == false) {
        opDropIndex(tableName, headerName);
    }

    return 0;
}

int SystemManager::createColumn(string tableName, TableHeader header, Data defaultData) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    header.tableName = tableName;
    header.refCount = 0;
    header.hasIndex = false;
    header.isPrimary = false;
    header.isForeign = false;
    header.foreignGroup = -1;
    header.isUnique = false;
    header.uniqueGroup = -1;
    if (header.varType == INT || header.varType == DATE) {
        header.len = sizeof(int);
    } else if (header.varType == FLOAT) {
        header.len = sizeof(double);
    }

    /*if (header.isPrimary == true) {
        // 主键不能重复，主键不能为空
        header.isUnique = true;
        header.permitNull = false;
    } else if (header.isForeign == true) {
        // 外键允许为空
        header.permitNull = true;
    }*/

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
        cerr << "Column " << header.headerName << " doesn't permit NULL, but default data is NULL. Operation failed." << endl;
        return -1;
    // 是否类型不符
    } else if (defaultData.isNull == false && defaultData.varType != header.varType && !((defaultData.varType == CHAR || defaultData.varType == VARCHAR) && (header.varType == CHAR || header.varType == VARCHAR))) {
        if (defaultData.varType == INT && header.varType == FLOAT) {
            defaultData.varType = FLOAT;
            defaultData.floatVal = defaultData.intVal;
        } else {
            cerr << "Default data type error. Operation failed." << endl;
            return -1;
        }
    // 字符串长度是否非法
    } else if ((header.varType == CHAR || header.varType == VARCHAR) && (int)defaultData.stringVal.size() > header.len) {
        cerr << "String \"" << defaultData.stringVal << "\" too long. Operation failed." << endl;
        return -1;
    }

    /*// 判断 Unique 冲突
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
    }*/

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
    }

    return 0;
}

int SystemManager::dropColumn(string tableName, string headerName) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    int idxPos = -1;
    // 检查是否有键被引用
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerName == headerList[i].headerName) {
            if (headerList[i].isPrimary == true) {
                cerr << "Column \"" << headerList[i].headerName << "\" is part of a primary key. Operation failed." << endl;
                return -1;
            } else if (headerList[i].isForeign == true) {
                cerr << "Column \"" << headerList[i].headerName << "\" is part of a foreign key. Operation failed." << endl;
                return -1;
            } else {
                idxPos = i;
            }
        }
    }

    // 检查是否找到 header
    if (idxPos == -1) {
        cerr << "Column \"" << headerName << "\" doesn't exist. Operation failed." << endl;
        return -1;
    }

    // 清除相关 unique 标记
    if (headerList[idxPos].isUnique == true) {
        vector <string> uniqueColNameList;
        for (int i = 0; i < (int)headerList.size(); i++) {
            if (headerList[i].isUnique == true && headerList[i].uniqueGroup == headerList[idxPos].uniqueGroup) {
                uniqueColNameList.push_back(headerList[i].headerName);
            }
        }
        dropUnique(tableName, uniqueColNameList);
    }

    RecordHandler recordHandler(this->bufManager);
    // 删除索引
    if (headerList[idxPos].isPrimary == true || headerList[idxPos].isUnique == true || headerList[idxPos].hasIndex == true) {
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
    headerList = table->getHeaderList();
    headerList.erase(headerList.begin() + idxPos);
    table->writeHeaderList(headerList);
    for (int i = 0; i < (int)dataLists.size(); i++) {
        opInsert(tableName, dataLists[i]);
    }

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

    // 检查是否满足 unique 要求
    if (columnUnique(tableName, headerNameList) == false) {
        cerr << "Column(s) you selected have duplicate elements. Operation failed." << endl;
        return -1;
    }
    vector <RID> ridList = table->getRecordList();
    for (int i = 0; i < (int)headerNameList.size(); i++) {
        int find = 0;
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                /*// 不能同时为主键和外键
                if (headerList[j].isForeign == true) {
                    cerr << "Column " << headerList[j].headerName << " is already part of a foreign key. Operation failed." << endl;
                    return -1;
                // 主键不能为 null
                } else if (headerList[j].permitNull == true) {
                    cerr << "Column " << headerList[j].headerName << " permits null. Operation failed." << endl;
                    return -1;
                }*/
                for (int k = 0; k < (int)ridList.size(); k++) {
                    vector <Data> dataList = table->exeSelect(ridList[k]);
                    if (dataList[j].isNull == true) {
                        cerr << "Column " << headerList[j].headerName << " has null elements. Operation failed." << endl;
                        return -1;
                    }
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

    /*// 添加主键标记并创建索引
    for (int i = 0; i < (int)headerNameList.size(); i++) {
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                headerList[j].isPrimary = true;
                if (headerList[j].isUnique == false && headerList[j].hasIndex == false) {
                    opCreateIndex(tableName, headerList[j].headerName);
                }
                //headerList[j].id = i;
            }
        }
    }
    table->writeHeaderList(headerList);*/
    opCreatePrimary(tableName, headerNameList);

    return 0;
}

void SystemManager::opCreatePrimary(string tableName, vector <string> headerNameList) {
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    // 添加主键标记并创建索引
    for (int i = 0; i < (int)headerNameList.size(); i++) {
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                headerList[j].isPrimary = true;
                if (headerList[j].isUnique == false && headerList[j].hasIndex == false) {
                    opCreateIndex(tableName, headerList[j].headerName);
                }
                //headerList[j].id = i;
            }
        }
    }
    table->writeHeaderList(headerList);
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
    if (headerNameList.size() != 0 && (int)headerNameList.size() != counter) {
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

    // 清除主键标记并删除索引
    for (int j = 0; j < (int)headerList.size(); j++) {
        if (headerList[j].isPrimary == true) {
            headerList[j].isPrimary = false;
            if (headerList[j].isUnique == false && headerList[j].hasIndex == false) {
                opDropIndex(tableName, headerList[j].headerName);
            }
            //headerList[j].id = -1;
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
    vector <int> updatePos, foreignPos;
    int counter = 0;

    for (int i = 0; i < (int)updateHeaderList.size(); i++) {
        int find = 0;
        VarType headerType, foreignHeaderType;
        for (int j = 0; j < (int)headerList.size(); j++) {
            //VarType updateHeaderType = updateHeaderList[i].varType == VARCHAR ? CHAR : updateHeaderList[i].varType;
            headerType = headerList[j].varType == VARCHAR ? CHAR : headerList[j].varType;
            if (updateHeaderList[i].headerName == headerList[j].headerName) {
                // 不能同时为主键和外键
                if (headerList[j].isPrimary == true) {
                    cerr << "Column " << updateHeaderList[i].headerName << " is part of primary key. Operation failed." << endl;
                    return -1;
                }
                // 不能同时为两个外键
                if (headerList[j].isForeign == true) {
                    cerr << "Column " << updateHeaderList[i].headerName << " is already part of foreign key. Operation failed." << endl;
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
                foreignHeaderType = foreignHeaderList[j].varType == VARCHAR ? CHAR : foreignHeaderList[j].varType;
                foreignPos.push_back(j);
                find = 1;
                break;
            }
        }
        // 检查引用列是否存在
        if (find == 0) {
            cerr << "Column " << updateHeaderList[i].foreignHeaderName << " doesn't exist. Operation failed." << endl;
            return -1;
        }
        // 检查类型是否正确
        if (headerType != foreignHeaderType) {
            cerr << "Column " << updateHeaderList[i].foreignHeaderName << " and the column it references have different type. Operation failed." << endl;
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

    vector <RID> ridList = table->getRecordList();
    vector <RID> foreignRidList = foreignTable->getRecordList();

    for (int i = 0; i < (int)ridList.size(); i++) {
        int find = 0;
        for (int j = 0; j < (int)foreignRidList.size(); j++) {
            vector <Data> dataList = table->exeSelect(ridList[i]);
            vector <Data> foreignDataList = foreignTable->exeSelect(foreignRidList[j]);
            int equal = 1;
            for (int k = 0; k < (int)updatePos.size(); k++) {
                if (dataList[updatePos[k]].varType == INT || dataList[updatePos[k]].varType == DATE) {
                    if (dataList[updatePos[k]].intVal != foreignDataList[foreignPos[k]].intVal) {
                        equal = 0;
                    }
                } else if (dataList[updatePos[k]].varType == FLOAT) {
                    if (dataList[updatePos[k]].floatVal != foreignDataList[foreignPos[k]].floatVal) {
                        equal = 0;
                    }
                } else if (dataList[updatePos[k]].varType == CHAR || dataList[updatePos[k]].varType == VARCHAR) {
                    if (dataList[updatePos[k]].stringVal != foreignDataList[foreignPos[k]].stringVal) {
                        equal = 0;
                    }
                }
            }
            if (equal == 1) {
                find = 1;
            }
        }
        if (find == 0) {
            cerr << "Foreign key has something that primary key doesn't have. Operation failed." << endl;
            return -1;
        }
    }

    int maxNum = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true) {
            maxNum = headerList[i].foreignGroup > maxNum ? headerList[i].foreignGroup : maxNum;
        }
    }

    vector <vector <Data>> dataLists;
    for (int i = 0; i < (int)ridList.size(); i++) {
        dataLists.push_back(table->exeSelect(ridList[i]));
        opDelete(tableName, dataLists[i], ridList[i]);
    }
    // 修改外键信息
    for (int i = 0; i < (int)updateHeaderList.size(); i++) {
        headerList[updatePos[i]].isForeign = true;
        headerList[updatePos[i]].foreignTableName = foreignTableName;
        headerList[updatePos[i]].foreignHeaderName = updateHeaderList[i].foreignHeaderName;
        headerList[updatePos[i]].foreignGroup = maxNum + 1;
    }
    table->writeHeaderList(headerList);
    // 修改引用信息
    for (int i = 0; i < (int)foreignHeaderList.size(); i++) {
        if (foreignHeaderList[i].isPrimary == true) {
            foreignHeaderList[i].refCount++;
        }
    }
    foreignTable->writeHeaderList(foreignHeaderList);
    for (int i = 0; i < (int)ridList.size(); i++) {
        opInsert(tableName, dataLists[i]);
    }

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
    int counter = 0, groupId = -1;
    string foreignTableName;

    for (int i = 0; i < (int)headerNameList.size(); i++) {
        int find = 0;
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                // 检查是否为外键
                if (headerList[j].isForeign == false) {
                    cerr << "Foreign key doesn't exist. Operation failed." << endl;
                    return -1;
                // 检查删除的 key 是否为一组
                } if (groupId == -1) {
                    groupId = headerList[j].foreignGroup;
                    foreignTableName = headerList[j].foreignTableName;
                } else if (headerList[j].foreignGroup != groupId) {
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

    vector <RID> ridList = table->getRecordList();
    vector <vector <Data>> dataLists;
    for (int i = 0; i < (int)ridList.size(); i++) {
        dataLists.push_back(table->exeSelect(ridList[i]));
        opDelete(tableName, dataLists[i], ridList[i]);
    }
    // 修改外键信息
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true && headerList[i].foreignGroup == groupId) {
            headerList[i].isForeign = false;
            headerList[i].foreignTableName = "";
            headerList[i].foreignHeaderName = "";
            headerList[i].foreignGroup = -1;
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
    for (int i = 0; i < (int)ridList.size(); i++) {
        opInsert(tableName, dataLists[i]);
    }

    return 0;
}

int SystemManager::createUnique(string tableName, vector <string> headerNameList) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();

    for (int i = 0; i < (int)headerNameList.size(); i++) {
        int find = 0;
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                // 检查是否已经 unique
                if (headerList[j].isUnique == true) {
                    cerr << "Column " << headerNameList[i] << " is already unique. Operation failed." << endl;
                    return -1;
                }
                find = 1;
            }
        }
        // 检查列是否存在
        if (find == 0) {
            cerr << "Column " << headerNameList[i] << " doesn't exist. Operation failed." << endl;
            return -1;
        }
    }

    // 检查元素是否有重复
    if (!columnUnique(tableName, headerNameList)) {
        cerr << "Column(s) selected have duplicate elements. Operation failed." << endl;
        return -1;
    }

    int maxNum = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isUnique == true) {
            maxNum = headerList[i].uniqueGroup > maxNum ? headerList[i].uniqueGroup : maxNum;
        }
    }

    // 修改 unique 信息
    for (int i = 0; i < (int)headerNameList.size(); i++) {
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                headerList[j].isUnique = true;
                headerList[j].uniqueGroup = maxNum + 1;
                if (headerList[j].isPrimary == false && headerList[j].hasIndex == false) {
                    opCreateIndex(tableName, headerList[j].headerName);
                }
            }
        }
    }
    table->writeHeaderList(headerList);

    return 0;
}

int SystemManager::dropUnique(string tableName, vector <string> headerNameList) {
    // 检查 table 是否存在
    if (!hasTable(tableName)) {
        cerr << "Table " << tableName << " doesn't exist. Operation failed." << endl;
        return -1;
    }

    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    int counter = 0, groupId = -1;

    for (int i = 0; i < (int)headerNameList.size(); i++) {
        int find = 0;
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                // 检查是否 unique
                if (headerList[j].isUnique == false) {
                    cerr << "Column(s) selected aren't unique. Operation failed." << endl;
                    return -1;
                }
                // 检查删除的 key 是否为一组
                if (groupId == -1) {
                    groupId = headerList[j].uniqueGroup;
                } else if (headerList[j].uniqueGroup != groupId) {
                    cerr << "Column(s) selected aren't unique. Operation failed." << endl;
                    return -1;
                }
                find = 1;
                break;
            }
        }
        // 检查列是否存在
        if (find == 0) {
            cerr << "Column(s) selected aren't unique. Operation failed." << endl;
            return -1;
        }
    }

    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isUnique == true && headerList[i].uniqueGroup == groupId) {
            counter++;
        }
    }
    // 检查是否完整删除外键
    if ((int)headerNameList.size() != counter) {
        cerr << "Column(s) selected aren't unique. Operation failed." << endl;
        return -1;
    }

    // 修改 unique 信息
    for (int i = 0; i < (int)headerNameList.size(); i++) {
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                headerList[j].isUnique = false;
                headerList[j].uniqueGroup = -1;
                if (headerList[j].isPrimary == false && headerList[j].hasIndex == false) {
                    opDropIndex(tableName, headerList[j].headerName);
                }
            }
        }
    }
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

bool SystemManager::columnUnique(string tableName, vector <string> headerNameList) {
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    vector <RID> ridList = table->getRecordList();
    vector <int> posList;
    map <string, int> mp;
    for (int i = 0; i < (int)headerNameList.size(); i++) {
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerNameList[i] == headerList[j].headerName) {
                posList.push_back(j);
            }
        }
    }

    for (int i = 0; i < (int)ridList.size(); i++) {
        vector <Data> dataList = table->exeSelect(ridList[i]);
        string str = "";
        for (int j = 0; j < (int)posList.size(); j++) {
            if (headerList[posList[j]].varType == INT || headerList[posList[j]].varType == DATE) {
                str += to_string(dataList[posList[j]].intVal);
            } else if (headerList[posList[j]].varType == FLOAT) {
                str += to_string(dataList[posList[j]].floatVal);
            } else if (headerList[posList[j]].varType == CHAR || headerList[posList[j]].varType == VARCHAR) {
                str += dataList[posList[j]].stringVal;
            }
        }
        if (mp.count(str) == 0) {
            mp[str] = 1;
        } else {
            return false;
        }
    }

    return true;
}

void SystemManager::opCreateIndex(string tableName, string headerName) {
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    vector <RID> ridList = table->getRecordList();
    int idxPos = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].headerName == headerName) {
            idxPos = i;
        }
    }

    VarType type = headerList[idxPos].varType == DATE ? INT : (headerList[idxPos].varType == CHAR ? VARCHAR : headerList[idxPos].varType);
    this->indexHandler->openIndex("index_" + getDbName() + "_" + tableName, headerName, type);
    for (int i = 0; i < (int)ridList.size(); i++) {
        /*if (i % 10000 == 0) {
            cout << i << endl;
        }*/
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
                memcpy(str, dataList[idxPos].stringVal.c_str(), dataList[idxPos].stringVal.size());
                keyPtr = str;
            }
            this->indexHandler->insert(keyPtr, ridList[i]);
        }
    }
    this->indexHandler->closeIndex();
}

void SystemManager::opDropIndex(string tableName, string headerName) {
    this->indexHandler->removeIndex(tableName, headerName);
}

RID SystemManager::opInsert(string tableName, vector <Data> dataList) {
    // 插入数据
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    RID rid = table->exeInsert(dataList);

    // 插入索引
    for (int i = 0; i < (int)headerList.size(); i++) {
        if ((headerList[i].isPrimary == true || headerList[i].isUnique == true || headerList[i].hasIndex == true) && dataList[i].isNull == false) {
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
    return rid;
}

void SystemManager::opDelete(string tableName, vector <Data> dataList, RID rid) {
    // 删除数据
    Table *table = getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    table->exeDelete(rid);

    // 删除索引
    for (int i = 0; i < (int)headerList.size(); i++) {
        if ((headerList[i].isPrimary == true || headerList[i].isUnique == true || headerList[i].hasIndex == true) && dataList[i].isNull == false) {
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
    int maxGroup = -1;
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true) {
            maxGroup = headerList[i].foreignGroup > maxGroup ? headerList[i].foreignGroup : maxGroup;
        }
    }
    for (int i = 0; i <= maxGroup; i++) {
        vector <int> refPos;
        Table *foreignTable;
        int groupExist = 0;
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerList[j].isForeign == true && headerList[j].foreignGroup == i) {
                foreignTable = getTable(headerList[j].foreignTableName);
                groupExist = 1;
                vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
                for (int k = 0; k < (int)foreignHeaderList.size(); k++) {
                    if (headerList[j].foreignHeaderName == foreignHeaderList[k].headerName) {
                        refPos.push_back(k);
                        break;
                    }
                }
            } else {
                refPos.push_back(-1);
            }
        }
        if (groupExist == 1) {
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            vector <RID> ridList = foreignTable->getRecordList();
            for (int j = 0; j < (int)ridList.size(); j++) {
                int equal = 1;
                vector <Data> foreignDataList = foreignTable->exeSelect(ridList[j]);
                for (int k = 0; k < (int)headerList.size(); k++) {
                    if (headerList[k].isForeign == true && headerList[k].foreignGroup == i) {
                        if (headerList[k].varType == INT || headerList[k].varType == DATE) {
                            if (dataList[k].intVal != foreignDataList[refPos[k]].intVal) {
                                equal = 0;
                            }
                        } else if (headerList[k].varType == FLOAT) {
                            if (dataList[k].floatVal != foreignDataList[refPos[k]].floatVal) {
                                equal = 0;
                            }
                        } else if (headerList[k].varType == CHAR || headerList[k].varType == VARCHAR) {
                            if (dataList[k].stringVal != foreignDataList[refPos[k]].stringVal) {
                                equal = 0;
                            }
                        }
                    }
                }
                if (equal == 1) {
                    for (int k = 0; k < (int)refPos.size(); k++) {
                        if (refPos[k] != -1) {
                            foreignDataList[refPos[k]].refCount += delta;
                        }
                    }
                    RID foreignNewRid = foreignTable->exeUpdate(foreignDataList, ridList[j]);
                    for (int k = 0; k < (int)refPos.size(); k++) {
                        if (refPos[k] != -1) {
                            if (foreignHeaderList[refPos[k]].isPrimary == true || foreignHeaderList[refPos[k]].isUnique == true || foreignHeaderList[refPos[k]].hasIndex == true) {
                                VarType type = headerList[k].varType == DATE ? INT : (headerList[k].varType == CHAR ? VARCHAR : headerList[k].varType);
                                this->indexHandler->openIndex("index_" + this->dbName + "_" + headerList[k].foreignTableName, headerList[k].foreignHeaderName, type);
                                char str[MAX_RECORD_LEN];
                                memset(str, 0, sizeof(str));
                                key_ptr keyPtr;
                                if (type == INT) {
                                    keyPtr = (char*)&dataList[k].intVal;
                                } else if (type == FLOAT) {
                                    keyPtr = (char*)&dataList[k].floatVal;
                                } else {
                                    memcpy(str, dataList[k].stringVal.c_str(), dataList[k].stringVal.size());
                                    keyPtr = str;
                                }
                                this->indexHandler->remove(keyPtr, ridList[j]);
                                this->indexHandler->insert(keyPtr, foreignNewRid);
                                this->indexHandler->closeIndex();
                            }
                        }
                    }
                }
            }
        }
    }
    /*for (int i = 0; i < (int)headerList.size(); i++) {
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
                    if (foreignHeaderList[refPos].isPrimary == true || foreignHeaderList[refPos].isUnique == true || foreignHeaderList[refPos].hasIndex == true) {
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
            VarType type = headerList[i].varType == DATE ? INT : (headerList[i].varType == CHAR ? VARCHAR : headerList[i].varType);
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
            this->indexHandler->closeIndex();
        }
    }*/
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
