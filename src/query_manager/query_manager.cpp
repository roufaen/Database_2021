# include "query_manager.h"

QueryManager::QueryManager(RecordHandler *recordHandler, IndexHandler *indexHandler, SystemManager *systemManager) {
    this->recordHandler = recordHandler;
    this->indexHandler = indexHandler;
    this->systemManager = systemManager;
}

QueryManager::~QueryManager() {
}

int QueryManager::exeSelect(vector <string> tableNameList, vector <string> selectorList, vector <Condition> conditionList, vector <vector <Data> >& resData) {
    vector <Table> tableList;
    vector <TableHeader> headerList;
    vector <vector <RID> > ridList;
    map <string, int> tableMap;
    string dbName = this->systemManager->dbName();
    int totNum = 1;

    // 获取用到的所有 table ，其 header 进行连接
    for (int i = 0; i < tableNameList.size(); i++) {
        // 判断 table 是否存在
        if (!this->systemManager->hasTable(tableNameList[i])) {
            return -1;
        }

        if (tableMap.count(tableNameList[i]) == 0) {
            tableMap[tableNameList[i]] = i;
            Table table(dbName, tableNameList[i], this->recordHandler);
            tableList.push_back(table);
            vector <TableHeader> headers = table.getHeaders();
            for (int j = 0; j < headers.size(); i++) {
                headerList.push_back(headers[i]);
            }
            vector <RID> rids = table.allRecords();
            ridList.push_back(rids);
            totNum *= rids.size();
        }

        // 判断 header 是否存在
        vector <TableHeader> headers = tableList[tableMap[tableNameList[i]]].getHeaders();
        bool headerFind = false;
        for (int j = 0; j < headers.size(); j++) {
            if (headers[i].headerName == selectorList[i]) {
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
        for (int j = 0, tmp = i; j < ridList.size(); j++) {
            RID rid = ridList[j][tmp % ridList[j].size()];
            tmp /= ridList[j].size();
            vector <Data> data = tableList[j].exeSelect(rid);
            for (int k = 0; k < data.size(); k++) {
                jointData.push_back(data[k]);
            }
        }
        // 判断是否满足选择条件，若是则选择要求的列输出
        if (conditionJudge(headerList, jointData, conditionList)) {
            vector <Data> res;
            for (int j = 0; j < selectorList.size(); j++) {
                for (int k = 0; k < headerList.size(); k++) {
                    if (headerList[k].tableName == tableNameList[j] && headerList[k].headerName == selectorList[j]) {
                        res.push_back(jointData[k]);
                    }
                }
            }
            resData.push_back(res);
        }
    }
}

int QueryManager::exeInsert(string tableName, vector <Data> dataList) {
}

int QueryManager::exeDelete(string tableName, vector <Condition> conditionList) {
}

int QueryManager::exeUpdate(string tableName, vector <TableHeader> headerList, vector <Data> dataList, vector <Condition> conditionList) {
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
}

bool QueryManager::conditionJudge(vector <TableHeader> headerList, vector <Data> dataList, vector <Condition> conditionList) {
    Data leftData, rightData;
    bool leftDataFind = false, rightDataFind = false;
    // 对每个条件检查
    for (int i = 0; i < conditionList.size(); i++) {
        // 寻找要求的列
        for (int j = 0; j < headerList.size(); j++) {
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
        } else if (leftData.isNull ^ rightData.isNull == 1) {
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
