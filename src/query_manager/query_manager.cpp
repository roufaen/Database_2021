# include "query_manager.h"

QueryManager::QueryManager(IndexHandler *indexHandler, SystemManager *systemManager, BufManager *bufManager) {
    this->indexHandler = indexHandler;
    this->systemManager = systemManager;
    this->bufManager = bufManager;
}

QueryManager::~QueryManager() {
}

int QueryManager::exeSelect(vector <string> tableNameList, vector <string> selectorList, vector <Condition> conditionList, vector <vector <Data> >& resData) {
    vector <Table*> tableList;
    vector <TableHeader> headerList;
    vector <vector <RID> > ridList;
    map <string, int> tableMap;
    string dbName = this->systemManager->getDbName();
    int totNum = 1;

    // 获取用到的所有 table ，其 header 进行连接
    for (int i = 0; i < (int)tableNameList.size(); i++) {
        // 判断 table 是否存在
        if (!this->systemManager->hasTable(tableNameList[i])) {
            return -1;
        }

        if (tableMap.count(tableNameList[i]) == 0) {
            tableMap[tableNameList[i]] = i;
            Table *table = this->systemManager->getTable(tableNameList[i]);
            tableList.push_back(table);
            vector <TableHeader> originalHeaderList = table->getHeaderList();
            for (int j = 0; j < (int)originalHeaderList.size(); i++) {
                headerList.push_back(originalHeaderList[i]);
            }
            vector <RID> rids = table->getRecordList();
            ridList.push_back(rids);
            totNum *= rids.size();
        }

        // 判断 header 是否存在
        vector <TableHeader> originalHeaderList = tableList[tableMap[tableNameList[i]]]->getHeaderList();
        bool headerFind = false;
        for (int j = 0; j < (int)originalHeaderList.size(); j++) {
            if (originalHeaderList[i].headerName == selectorList[i]) {
                headerFind = true;
            }
        }
        if (headerFind == false) {
            return -1;
        }
    }

    // data 进行连接
    for (int i = 0; i < totNum; i++) {
        vector <Data> jointData;
        for (int j = 0, tmp = i; j < (int)ridList.size(); j++) {
            RID rid = ridList[j][tmp % ridList[j].size()];
            tmp /= ridList[j].size();
            vector <Data> data = tableList[j]->exeSelect(rid);
            for (int k = 0; k < (int)data.size(); k++) {
                jointData.push_back(data[k]);
            }
        }
        // 判断是否满足选择条件，若是则选择要求的列输出
        if (conditionJudge(headerList, jointData, conditionList)) {
            vector <Data> res;
            for (int j = 0; j < (int)selectorList.size(); j++) {
                for (int k = 0; k < (int)headerList.size(); k++) {
                    if (headerList[k].tableName == tableNameList[j] && headerList[k].headerName == selectorList[j]) {
                        res.push_back(jointData[k]);
                    }
                }
            }
            resData.push_back(res);
        }
    }

    return 0;
}

int QueryManager::exeInsert(string tableName, vector <Data> dataList) {
    // 判断 table 是否存在
    if (!this->systemManager->hasTable(tableName)) {
        return -1;
    }
    Table *table = this->systemManager->getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    // 数据长度不符
    if (dataList.size() != headerList.size()) {
        return -1;
    }
    for (int i = 0; i < (int)headerList.size(); i++) {
        // 是否非法空值
        if (dataList[i].isNull == true && headerList[i].permitNull == false) {
            return -1;
        // 是否类型不符
        } else if (dataList[i].varType != headerList[i].varType && !((dataList[i].varType == CHAR || dataList[i].varType == VARCHAR) && (headerList[i].varType == CHAR || headerList[i].varType == VARCHAR))) {
            return -1;
        // 字符串长度是否非法
        } else if ((headerList[i].varType == CHAR || headerList[i].varType == VARCHAR) && (int)dataList[i].stringVal.size() > headerList[i].len) {
            return -1;
        }
    }
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isUnique == true) {
            int count = countKey(tableName, headerList[i].headerName, headerList[i].varType, dataList[i].intVal, dataList[i].floatVal, dataList[i].stringVal);
            // Unique 键不能重复
            if (count != 0) {
                return -1;
            }
        } else if (headerList[i].isForeign == true) {
            int count = countKey(headerList[i].foreignTableName, headerList[i].foreignHeaderName, headerList[i].varType, dataList[i].intVal, dataList[i].floatVal, dataList[i].stringVal);
            // 外键不能引用不存在的键
            if (count != 1) {
                return -1;
            }
        }
    }

    // 插入数据和索引
    opInsert(tableName, dataList);

    return 0;
}

int QueryManager::exeDelete(string tableName, vector <Condition> conditionList) {
    // 判断 table 是否存在
    if (!this->systemManager->hasTable(tableName)) {
        return -1;
    }
    Table *table = this->systemManager->getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    vector <RID> ridList = table->getRecordList();
    int primaryIdx = -1;
    // 寻找主键位置
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isPrimary == true) {
            primaryIdx = i;
        }
    }

    // 主键不能被引用，否则无法删除
    for (int i = 0; i < (int)ridList.size(); i++) {
        vector <Data> dataList = table->exeSelect(ridList[i]);
        if (conditionJudge(headerList, dataList, conditionList)) {
            if (primaryIdx != -1 && dataList[primaryIdx].refCount != 0) {
                return -1;
            }
        }
    }

    // 删除数据和索引
    for (int i = 0; i < (int)headerList.size(); i++) {
        vector <Data> dataList = table->exeSelect(ridList[i]);
        if (conditionJudge(headerList, dataList, conditionList)) {
            opDelete(tableName, dataList, ridList[i]);
        }
    }

    return 0;
}

int QueryManager::exeUpdate(string tableName, vector <string> updateHeaderNameList, vector <Data> originalUpdateDataList, vector <Condition> conditionList) {
    // 判断 table 是否存在
    if (!this->systemManager->hasTable(tableName)) {
        return -1;
    }
    // 判断输入数据格式是否正确
    if (updateHeaderNameList.size() != originalUpdateDataList.size()) {
        return -1;
    }
    Table *table = this->systemManager->getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    vector <RID> ridList = table->getRecordList();
    vector <int> updatePos;
    vector <Data> updateDataList;
    // 预处理
    int updateCount = 0;
    for (int i = 0; i < (int)headerList.size(); i++) {
        int updateHere = 0;
        for (int j = 0; j < (int)updateHeaderNameList.size(); j++) {
            if (headerList[i].headerName == updateHeaderNameList[j] && updateHere == 0) {
                updateHere = 1;
                updatePos.push_back(1);
                updateDataList.push_back(originalUpdateDataList[j]);
                updateCount++;
            // 判断是否重复修改某些列的值
            } else if (headerList[i].headerName == updateHeaderNameList[j] && updateHere == 1) {
                return -1;
            }
        }
        if (updateHere == 0) {
            updatePos.push_back(0);
            updateDataList.push_back(originalUpdateDataList[0]);
        }
    }
    // 要求修改的某些列不存在
    if (updateCount < (int)updateHeaderNameList.size()) {
        return -1;
    }

    // 更新的数据本身是否存在问题
    for (int i = 0; i < (int)headerList.size(); i++) {
        // 是否非法空值
        if (updatePos[i] == 1 && updateDataList[i].isNull == true && headerList[i].permitNull == false) {
            return -1;
        // 是否类型不符
        } else if (updatePos[i] == 1 && updateDataList[i].varType != headerList[i].varType && !((updateDataList[i].varType == CHAR || updateDataList[i].varType == VARCHAR) && (headerList[i].varType == CHAR || headerList[i].varType == VARCHAR))) {
            return -1;
        // 字符串长度是否非法
        } else if (updatePos[i] == 1 && headerList[i].varType == CHAR && (int)updateDataList[i].stringVal.size() > headerList[i].len) {
            return -1;
        }
    }

    // 寻找符合条件的数据
    vector <vector <Data> > dataLists;
    vector <RID> updateRidList;
    for (int i = 0; i < (int)ridList.size(); i++) {
        vector <Data> dataList = table->exeSelect(ridList[i]);
        if (conditionJudge(headerList, dataList, conditionList)) {
            dataLists.push_back(dataList);
            updateRidList.push_back(ridList[i]);
        }
    }

    // 判断 Unique 冲突
    for (int i = 0; i < headerList.size(); i++) {
        if (updatePos[i] == 1 && headerList[i].isUnique == true) {
            // 多个数据相同，冲突
            if (dataLists.size() > 1) {
                return -1;
            // 只有一个数据修改
            } else {
                int count = countKey(tableName, headerList[i].headerName, headerList[i].varType, updateDataList[i].intVal, updateDataList[i].floatVal, updateDataList[i].stringVal);
                // 若修改后的数值本不存在，则可直接修改，否则判断修改前后数值是否一样，若不一样则说明有冲突
                if (count != 0) {
                    VarType updateType = (headerList[i].varType == VARCHAR) ? CHAR : ((headerList[i].varType == DATE) ? INT : headerList[i].varType);
                    if (updateType == INT && dataLists[0][i].intVal != updateDataList[i].intVal) {
                        return -1;
                    } else if (updateType == FLOAT && dataLists[0][i].floatVal != updateDataList[i].floatVal) {
                        return -1;
                    } else if (updateType == CHAR && dataLists[0][i].stringVal != updateDataList[i].stringVal) {
                        return -1;
                    }
                }
            }
        }
    }

    // 判断外键冲突
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (updatePos[i] == true && headerList[i].isForeign == true) {
            int count = countKey(headerList[i].foreignTableName, headerList[i].foreignHeaderName, headerList[i].varType, updateDataList[i].intVal, updateDataList[i].floatVal, updateDataList[i].stringVal);
            // 外键不能引用不存在的键
            if (count != 1) {
                return -1;
            }
        }
    }

    // 更新数据
    for (int i = 0; i < (int)ridList.size(); i++) {
        opDelete(tableName, dataLists[i], updateRidList[i]);
        for (int j = 0; j < (int)updateDataList.size(); j++) {
            if (updatePos[j] == 1) {
                dataLists[i][j] = updateDataList[j];
            }
        }
        opInsert(tableName, dataLists[i]);
    }

    return 0;
}

void QueryManager::opInsert(string tableName, vector <Data> dataList) {
    // 插入数据
    Table *table = this->systemManager->getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    RID rid = table->exeInsert(dataList);

    // 插入索引
    for (int i = 0; i < headerList.size(); i++) {
        if (headerList[i].hasIndex == true) {
            VarType type = headerList[i].varType == DATE ? INT : (headerList[i].varType == CHAR ? VARCHAR : headerList[i].varType);
            this->indexHandler->openIndex("index_" + this->systemManager->getDbName() + "_" + tableName, headerList[i].headerName, type, this->bufManager);
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

void QueryManager::opDelete(string tableName, vector <Data> dataList, RID rid) {
    // 删除数据
    Table *table = this->systemManager->getTable(tableName);
    vector <TableHeader> headerList = table->getHeaderList();
    table->exeDelete(rid);

    // 删除索引
    for (int i = 0; i < headerList.size(); i++) {
        if (headerList[i].hasIndex == true) {
            VarType type = headerList[i].varType == DATE ? INT : (headerList[i].varType == CHAR ? VARCHAR : headerList[i].varType);
            this->indexHandler->openIndex("index_" + this->systemManager->getDbName() + "_" + tableName, headerList[i].headerName, type, this->bufManager);
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

bool QueryManager::compare(int lInt, double lFloat, string lString, int rInt, double rFloat, string rString, ConditionType cond) {
    bool intEqual = (lInt == rInt), floatEqual = (lFloat == rFloat), stringEqual = (lString == rString);
    bool intNotEqual = (lInt != rInt), floatNotEqual = (lFloat != rFloat), stringNotEqual = (lString != rString);
    bool intLess = (lInt < rInt), floatLess = (lFloat < rFloat);
    bool intLessEqual = (lInt <= rInt), floatLessEqual = (lFloat <= rFloat);
    bool intGreater = (lInt > rInt), floatGreater = (lFloat > rFloat);
    bool intGreaterEqual = (lInt >= rInt), floatGreaterEqual = (lFloat >= rFloat);
    // 对其中一种数据类型判断时，输入的其余数据均相等
    if (cond == EQUAL) {
        return intEqual && floatEqual && stringEqual;
    } else if (cond == NOT_EQUAL) {
        return intNotEqual || floatNotEqual || stringNotEqual;
    } else if (cond == LESS) {
        return intLess || floatLess;
    } else if (cond == LESS_EQUAL) {
        return intLessEqual && floatLessEqual;
    } else if (cond == GREATER) {
        return intGreater || floatGreater;
    } else if (cond == GREATER_EQUAL) {
        return intGreaterEqual && floatGreaterEqual;
    }
    return false;
}

bool QueryManager::conditionJudge(vector <TableHeader> headerList, vector <Data> dataList, vector <Condition> conditionList) {
    Data leftData, rightData;
    bool leftDataFind = false, rightDataFind = false;
    // 对每个条件检查
    for (int i = 0; i < (int)conditionList.size(); i++) {
        // 寻找要求的列
        for (int j = 0; j < (int)headerList.size(); j++) {
            if (headerList[j].tableName == conditionList[i].leftTableName && headerList[j].headerName == conditionList[i].leftCol) {
                leftData = dataList[j];
                leftDataFind = true;
            }
            if (headerList[j].tableName == conditionList[i].rightTableName && headerList[j].headerName == conditionList[i].rightCol) {
                rightData = dataList[j];
                rightDataFind = true;
            }
        }

        // 未找到要求的列，判断失败
        if (leftDataFind == false) {
            return false;
        }

        // char 和 varchar 看作同一种类型
        leftData.varType = leftData.varType == VARCHAR ? CHAR : leftData.varType;
        if (conditionList[i].useColumn == false) {
            rightData.varType = conditionList[i].rightType == VARCHAR ? CHAR : conditionList[i].rightType;
            rightData.intVal = conditionList[i].rightIntVal;
            rightData.floatVal = conditionList[i].rightFloatVal;
            rightData.stringVal = conditionList[i].rightStringVal;
            rightData.isNull = conditionList[i].rightNull;
        } else {
            // 等号右侧的要求的列未找到，判断失败
            if (rightDataFind == false) {
                return false;
            }
            rightData.varType = rightData.varType == VARCHAR ? CHAR : rightData.varType;
        }

        // 两列均为空，只能判等不能比较
        if (leftData.isNull == 1 && rightData.isNull == 1) {
            if (conditionList[i].condType == EQUAL) {
                continue;
            } else {
                return false;
            }
        // 其中一列为空，只能判等不能比较
        } else if ((leftData.isNull ^ rightData.isNull) == 1) {
            if (conditionList[i].condType == NOT_EQUAL) {
                continue;
            } else {
                return false;
            }
        // 分不同的类型比较
        } else if ((leftData.varType == INT && rightData.varType == INT) || (leftData.varType == DATE && rightData.varType == DATE)) {
            if (!compare(leftData.intVal, 0, "", rightData.intVal, 0, "", conditionList[i].condType)) {
                return false;
            }
        } else if (leftData.varType == FLOAT && rightData.varType == FLOAT) {
            if (!compare(0, leftData.floatVal, "", 0, rightData.floatVal, "", conditionList[i].condType)) {
                return false;
            }
        } else if (leftData.varType == CHAR && rightData.varType == CHAR) {
            if (!compare(0, 0, leftData.stringVal, 0, 0, rightData.stringVal, conditionList[i].condType)) {
                return false;
            }
        // 类型不同，判断失败
        } else {
            return false;
        }
    }

    // 通过所有条件，判断成功
    return true;
}

void QueryManager::foreignKeyProcess(vector <TableHeader> headerList, vector <Data> dataList, int delta) {
    for (int i = 0; i < (int)headerList.size(); i++) {
        if (headerList[i].isForeign == true) {
            // 索引中的类型只有 INT FLOAT VARCHAR 三种
            VarType type = headerList[i].varType == DATE ? INT : (headerList[i].varType == CHAR ? VARCHAR : headerList[i].varType);
            // 打开索引
            this->indexHandler->openIndex("index_" + this->systemManager->getDbName() + "_" + headerList[i].foreignTableName, headerList[i].foreignHeaderName, type, this->bufManager);
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
            Table *foreignTable = this->systemManager->getTable(headerList[i].foreignTableName);
            vector <Data> foreignDataList = foreignTable->exeSelect(rids[0]);
            vector <TableHeader> foreignHeaderList = foreignTable->getHeaderList();
            for (int j = 0; j < (int)foreignDataList.size(); j++) {
                if (foreignHeaderList[j].headerName == headerList[j].foreignHeaderName) {
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
    }
}

int QueryManager::countKey(string tableName, string headerName, VarType type, int intVal, double floatVal, string stringVal) {
    VarType indexType = type == DATE ? INT : (type == CHAR ? VARCHAR : type);
    indexHandler->openIndex("index_" + this->systemManager->getDbName() + "_" + tableName, headerName, indexType, this->bufManager);
    int count = 0;
    if (type == INT) {
        count = indexHandler->count((char*)&intVal);
    } else if (type == FLOAT) {
        count = indexHandler->count((char*)&floatVal);
    } else {
        char str[MAX_RECORD_LEN];
        memcpy(str, stringVal.c_str(), stringVal.size());
        count = indexHandler->count(str);
    }
    indexHandler->closeIndex();
    return count;
}
