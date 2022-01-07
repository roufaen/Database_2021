#include "query_manager/query_manager.h"
#include "system_manager/system_manager.h"
#include <assert.h>
#include <stdlib.h>
#include <iostream>
using namespace std;

unsigned char MyBitMap::ha[] = {0};

BufManager* bufManager = new BufManager();
IndexHandler* indexHandler = new IndexHandler(bufManager);
SystemManager* systemManager = new SystemManager(indexHandler, bufManager);
QueryManager* queryManager = new QueryManager(indexHandler, systemManager, bufManager);
vector <TableHeader> vecTableHeader;
TableHeader tableHeader;
vector <Data> vecData;
vector <vector <Data> > vecResult;
Data data;
vector <Condition> vecCondition;
vector <string> vecString[2];

// 运行结束后有空的已打开的 courses 数据库
void testDatabaseAndTable() {
    cout << "[-----Test Database And Table-----]" << endl;
    // 打开不存在的数据库
    assert(systemManager->openDb("courses") == -1);
    // 创建数据库
    assert(systemManager->createDb("courses") == 0);
    // 打开存在的数据库
    assert(systemManager->openDb("courses") == 0);
    // 在打开数据库的情况下创建数据库
    assert(systemManager->createDb("students") == 0);
    // 重复创建数据库
    assert(systemManager->createDb("students") == -1);
    // 关闭数据库
    assert(systemManager->closeDb() == 0);
    // 在数据库未打开的情况下建表
    assert(systemManager->createTable("teacher", vecTableHeader) == -1);
    // 切换数据库
    assert(systemManager->openDb("courses") == 0);
    // 打开多个数据库
    assert(systemManager->openDb("students") == -1);
    // 删除打开的数据库
    assert(systemManager->dropDb("courses") == -1);
    // 删除数据库
    assert(systemManager->dropDb("students") == 0);
    // 删除不存在的数据库
    assert(systemManager->dropDb("students") == -1);
    // 创建表
    assert(systemManager->createTable("teacher", vecTableHeader) == 0);
    // 重复创建表
    assert(systemManager->createTable("teacher", vecTableHeader) == -1);
    // 删除表
    assert(systemManager->dropTable("teacher") == 0);
    // 删除不存在的表
    assert(systemManager->dropTable("teacher") == -1);
    cout << "[-----Test passed-----]" << endl << endl;
}

void testSingleTable() {
    cout << "[-----Test Single Table-----]" << endl;
    // 表头
    vecTableHeader.clear();
    tableHeader.tableName = "teacher", tableHeader.headerName = "id", tableHeader.varType = INT, tableHeader.permitNull = false;  vecTableHeader.push_back(tableHeader);
    tableHeader.tableName = "teacher", tableHeader.headerName = "name", tableHeader.varType = VARCHAR, tableHeader.len = 10, tableHeader.permitNull = false;  vecTableHeader.push_back(tableHeader);
    tableHeader.tableName = "teacher", tableHeader.headerName = "data", tableHeader.varType = FLOAT, tableHeader.permitNull = true;  vecTableHeader.push_back(tableHeader);
    // 创建表
    assert(systemManager->createTable("teacher", vecTableHeader) == 0);
    // 向不存在的表中插入数据
    assert(queryManager->exeInsert("student", vecData) == -1);
    // 插入数据
    vecData.clear();
    data.varType = INT, data.intVal = 10000, data.isNull = false;  vecData.push_back(data);
    data.varType = VARCHAR, data.stringVal = "teacher", data.isNull = false;  vecData.push_back(data);
    data.varType = FLOAT, data.floatVal = 1.05, data.isNull = false;  vecData.push_back(data);
    // 数据类型不正确
    vecData[0].varType = FLOAT;  assert(queryManager->exeInsert("teacher", vecData) == -1);  vecData[0].varType = INT;
    // 数据格式不正确
    vecData.push_back(data);  assert(queryManager->exeInsert("teacher", vecData) == -1);  vecData.pop_back();
    // 数据长度不正确
    vecData[1].stringVal = "teacher12345";  assert(queryManager->exeInsert("teacher", vecData) == -1);  vecData[1].stringVal = "teacher";
    // 正确插入数据
    assert(queryManager->exeInsert("teacher", vecData) == 0);
    // 错误插入带 NULL 的数据
    vecData[1].isNull = true;  assert(queryManager->exeInsert("teacher", vecData) == -1);  vecData[1].isNull = false;
    // 正确插入带 NULL 的数据
    vecData[1].stringVal = "teacher_", vecData[2].isNull = true;  assert(queryManager->exeInsert("teacher", vecData) == 0);  vecData[1].stringVal = "teacher", vecData[2].isNull = false;
    // 尝试增加 unique 失败
    vecString[0].clear();  vecString[0].push_back("id");
    assert(systemManager->createUnique("teacher", vecString[0]) == -1);
    // 读取数据
    vecString[0].clear();  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");
    vecString[1].clear();  vecString[1].push_back("id");  vecString[1].push_back("name");  vecString[1].push_back("data");
    vecCondition.clear();
    vecCondition.push_back(Condition{EQUAL, INT, false, false, "teacher", "", "id", "", "", 10000, 0});
    assert(queryManager->exeSelect(vecString[0], vecString[1], vecCondition, vecResult) == 0);
    cout << "[--check--] There should be two lines below." << endl;
    for (int i = 0; i < (int)vecResult.size(); i++) {
        cout << vecResult[i][0].intVal << " " << vecResult[i][1].stringVal << " " << vecResult[i][2].floatVal << endl;
    }
    // 移除数据
    vecCondition.clear();
    vecCondition.push_back(Condition{EQUAL, CHAR, false, false, "teacher", "", "name", "", "teacher_", 0, 0});
    assert(queryManager->exeDelete("teacher", vecCondition) == 0);
    // 移除后读取数据
    vecString[0].clear();  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");
    vecString[1].clear();  vecString[1].push_back("id");  vecString[1].push_back("name");  vecString[1].push_back("data");
    vecCondition.clear();
    vecCondition.push_back(Condition{EQUAL, INT, false, false, "teacher", "", "id", "", "", 10000, 0});
    vecResult.clear();
    assert(queryManager->exeSelect(vecString[0], vecString[1], vecCondition, vecResult) == 0);
    cout << "[--check--] There should be one line below." << endl;
    for (int i = 0; i < (int)vecResult.size(); i++) {
        cout << vecResult[i][0].intVal << " " << vecResult[i][1].stringVal << " " << vecResult[i][2].floatVal << endl;
    }
    // 尝试增加 unique 失败
    vecString[0].clear();  vecString[0].push_back("id");  vecString[0].push_back("id_");
    assert(systemManager->createUnique("teacher", vecString[0]) == -1);
    // 尝试增加 unique 成功
    vecString[0].clear();  vecString[0].push_back("name");  vecString[0].push_back("id");
    assert(systemManager->createUnique("teacher", vecString[0]) == 0);
    // 插入数据导致非 unique
    assert(queryManager->exeInsert("teacher", vecData) == -1);
    // 正确插入数据
    vecData[0].intVal = 10001;  assert(queryManager->exeInsert("teacher", vecData) == 0);  vecData[0].intVal = 10000;
    vecData[1].stringVal = "teacher_";  assert(queryManager->exeInsert("teacher", vecData) == 0);  vecData[0].stringVal = "teacher";
    assert(queryManager->exeInsert("teacher", vecData) == -1);
    vecString[0].clear();  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");
    vecString[1].clear();  vecString[1].push_back("id");  vecString[1].push_back("name");  vecString[1].push_back("data");
    vecCondition.clear();
    vecCondition.push_back(Condition{GREATER_EQUAL, INT, false, false, "teacher", "", "id", "", "", 10000, 0});
    vecResult.clear();
    assert(queryManager->exeSelect(vecString[0], vecString[1], vecCondition, vecResult) == 0);
    cout << "[--check--] There should be three lines below." << endl;
    for (int i = 0; i < (int)vecResult.size(); i++) {
        cout << vecResult[i][0].intVal << " " << vecResult[i][1].stringVal << " " << vecResult[i][2].floatVal << endl;
    }
    // 移除unique标记并插入数据
    vecString[0].clear();  vecString[0].push_back("name");
    assert(systemManager->dropUnique("teacher", vecString[0]) == -1);
    vecString[0].clear();  vecString[0].push_back("name");  vecString[0].push_back("id");
    assert(systemManager->dropUnique("teacher", vecString[0]) == 0);
    assert(queryManager->exeInsert("teacher", vecData) == 0);
    vecString[0].clear();  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");
    vecString[1].clear();  vecString[1].push_back("id");  vecString[1].push_back("name");  vecString[1].push_back("data");
    vecCondition.clear();
    vecCondition.push_back(Condition{GREATER_EQUAL, INT, false, false, "teacher", "", "id", "", "", 10000, 0});
    vecResult.clear();
    assert(queryManager->exeSelect(vecString[0], vecString[1], vecCondition, vecResult) == 0);
    cout << "[--check--] There should be four lines below." << endl;
    for (int i = 0; i < (int)vecResult.size(); i++) {
        cout << vecResult[i][0].intVal << " " << vecResult[i][1].stringVal << " " << vecResult[i][2].floatVal << endl;
    }
    // 移除后读取数据
    vecString[0].clear();  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");
    vecString[1].clear();  vecString[1].push_back("id");  vecString[1].push_back("name");  vecString[1].push_back("data");
    vecCondition.clear();
    vecCondition.push_back(Condition{GREATER_EQUAL, INT, false, false, "teacher", "", "id", "", "", 10001, 0});
    assert(queryManager->exeDelete("teacher", vecCondition) == 0);
    vecResult.clear();
    vecCondition.clear();
    vecCondition.push_back(Condition{LESS_EQUAL, INT, false, false, "teacher", "", "id", "", "", 10000, 0});
    assert(queryManager->exeSelect(vecString[0], vecString[1], vecCondition, vecResult) == 0);
    cout << "[--check--] There should be nothing below." << endl;
    for (int i = 0; i < (int)vecResult.size(); i++) {
        cout << vecResult[i][0].intVal << " " << vecResult[i][1].stringVal << " " << vecResult[i][2].floatVal << endl;
    }
    cout << "[-----Test passed-----]" << endl << endl;
}

void testPrimaryAndForeign() {
    cout << "[-----Test Primary And Foreign-----]" << endl;
    // 插入数据
    vecData.clear();
    data.varType = INT, data.intVal = 0, data.isNull = false;  vecData.push_back(data);
    data.varType = VARCHAR, data.stringVal = "teacher", data.isNull = false;  vecData.push_back(data);
    data.varType = FLOAT, data.floatVal = 0.0, data.isNull = false;  vecData.push_back(data);
    for (int i = 0; i < 3; i++) {
        vecData[0].intVal = i;
        vecData[2].floatVal = i * 1.2;
        assert(queryManager->exeInsert("teacher", vecData) == 0);
    }
    vecData[1].stringVal = "teacher_";
    for (int i = 0; i < 3; i++) {
        vecData[0].intVal = i;
        vecData[2].floatVal = i * 1.2;
        assert(queryManager->exeInsert("teacher", vecData) == 0);
    }
    vecData[0].intVal = 0;  vecData[1].stringVal = "teacher";  vecData[2].floatVal = 0.0;
    // 表头
    vecTableHeader.clear();
    tableHeader.tableName = "course", tableHeader.headerName = "teacher_id", tableHeader.varType = INT, tableHeader.permitNull = false;  vecTableHeader.push_back(tableHeader);
    tableHeader.tableName = "course", tableHeader.headerName = "teacher_name", tableHeader.varType = VARCHAR, tableHeader.len = 10, tableHeader.permitNull = false;  vecTableHeader.push_back(tableHeader);
    tableHeader.tableName = "course", tableHeader.headerName = "data", tableHeader.varType = FLOAT, tableHeader.permitNull = true;  vecTableHeader.push_back(tableHeader);
    // 创建表
    assert(systemManager->createTable("course", vecTableHeader) == 0);
    for (int i = 0; i < 3; i++) {
        vecData[0].intVal = i;
        vecData[2].floatVal = i * 1.5;
        assert(queryManager->exeInsert("course", vecData) == 0);
    }
    vecData[1].stringVal = "teacher__";  vecData[2].floatVal = 2000;  assert(queryManager->exeInsert("course", vecData) == 0);  vecData[1].stringVal = "teacher";
    // 添加主键
    vecString[0].clear();
    vecString[0].push_back("name");  vecString[0].push_back("id");
    assert(systemManager->createPrimary("teacher", vecString[0]) == 0);
    // 因类型不符引用失败
    vecTableHeader.clear();
    tableHeader.headerName = "teacher_id";  tableHeader.foreignHeaderName = "name";  vecTableHeader.push_back(tableHeader);
    tableHeader.headerName = "teacher_name";  tableHeader.foreignHeaderName = "id";  vecTableHeader.push_back(tableHeader);
    assert(systemManager->createForeign("course", "teacher", vecTableHeader) == -1);
    // 引用不正确的主键
    vecTableHeader.clear();
    tableHeader.headerName = "teacher_id";  tableHeader.foreignHeaderName = "id";  vecTableHeader.push_back(tableHeader);
    tableHeader.headerName = "teacher_name";  tableHeader.foreignHeaderName = "id";  vecTableHeader.push_back(tableHeader);
    assert(systemManager->createForeign("course", "teacher", vecTableHeader) == -1);
    // 因列内有非引用的内容而无法引用
    vecTableHeader.clear();
    tableHeader.headerName = "teacher_id";  tableHeader.foreignHeaderName = "id";  vecTableHeader.push_back(tableHeader);
    tableHeader.headerName = "teacher_name";  tableHeader.foreignHeaderName = "name";  vecTableHeader.push_back(tableHeader);
    assert(systemManager->createForeign("course", "teacher", vecTableHeader) == -1);
    // 删除非引用内容再次添加外键成功
    vecCondition.clear();
    vecCondition.push_back(Condition{GREATER_EQUAL, FLOAT, false, false, "course", "", "data", "", "", 0, 1000});
    assert(queryManager->exeDelete("course", vecCondition) == 0);
    assert(systemManager->createForeign("course", "teacher", vecTableHeader) == 0);
    // 加外键后插入数据
    for (int i = 0; i < 3; i++) {
        vecData[0].intVal = i;
        vecData[2].floatVal = 2000 + i * 1.5;
        assert(queryManager->exeInsert("course", vecData) == 0);
    }
    // 双表连接
    vecString[0].clear();  vecString[0].push_back("teacher");  vecString[0].push_back("course");  vecString[0].push_back("teacher");  vecString[0].push_back("course");
    vecString[1].clear();  vecString[1].push_back("id");  vecString[1].push_back("teacher_name");  vecString[1].push_back("data");  vecString[1].push_back("data");
    vecCondition.clear();
    vecCondition.push_back(Condition{EQUAL, INT, false, true, "teacher", "course", "id", "teacher_id", "", 0, 0});
    vecCondition.push_back(Condition{EQUAL, CHAR, false, true, "teacher", "course", "name", "teacher_name", "", 0, 0});
    vecResult.clear();
    assert(queryManager->exeSelect(vecString[0], vecString[1], vecCondition, vecResult) == 0);
    cout << "[--check--] There should be six lines below." << endl;
    for (int i = 0; i < (int)vecResult.size(); i++) {
        cout << vecResult[i][0].intVal << " " << vecResult[i][1].stringVal << " " << vecResult[i][2].floatVal<< " " << vecResult[i][3].floatVal << endl;
    }
    // 删除未被引用的元素成功
    vecCondition.clear();
    vecCondition.push_back(Condition{EQUAL, VARCHAR, false, false, "teacher", "", "name", "", "teacher_", 0, 0});
    assert(queryManager->exeDelete("teacher", vecCondition) == 0);
    // 删除被引用的元素失败
    vecCondition.clear();
    vecCondition.push_back(Condition{EQUAL, VARCHAR, false, false, "teacher", "", "name", "", "teacher", 0, 0});
    assert(queryManager->exeDelete("teacher", vecCondition) == -1);
    // 删除被引用的元素失败
    vecCondition.clear();
    vecCondition.push_back(Condition{GREATER_EQUAL, FLOAT, false, false, "course", "", "data", "", "", 0, 1000});
    assert(queryManager->exeDelete("course", vecCondition) == 0);
    vecCondition.clear();
    vecCondition.push_back(Condition{EQUAL, VARCHAR, false, false, "teacher", "", "name", "", "teacher", 0, 0});
    assert(queryManager->exeDelete("teacher", vecCondition) == -1);
    // 删除被引用的元素成功
    vecCondition.clear();
    vecCondition.push_back(Condition{LESS_EQUAL, FLOAT, false, false, "course", "", "data", "", "", 0, 1});
    assert(queryManager->exeDelete("course", vecCondition) == 0);
    vecCondition.clear();
    vecCondition.push_back(Condition{LESS_EQUAL, FLOAT, false, false, "teacher", "", "data", "", "", 0, 1});
    assert(queryManager->exeDelete("teacher", vecCondition) == 0);
    // 去除主键失败
    vecString[0].clear();
    vecString[0].push_back("id");  vecString[0].push_back("name");
    assert(systemManager->dropPrimary("teacher", vecString[0]) == -1);
    // 去除外键成功
    vecString[0].clear();
    vecString[0].push_back("teacher_id");  vecString[0].push_back("teacher_name");
    assert(systemManager->dropForeign("course", vecString[0]) == 0);
    // 去除外键后删除元素成功
    vecCondition.clear();
    vecCondition.push_back(Condition{LESS_EQUAL, FLOAT, false, false, "teacher", "", "data", "", "", 0, 2});
    assert(queryManager->exeDelete("teacher", vecCondition) == 0);
    // 去除主键失败
    vecString[0].clear();
    vecString[0].push_back("id");  vecString[0].push_back("name");  vecString[0].push_back("data");
    assert(systemManager->dropPrimary("teacher", vecString[0]) == -1);
    // 去除主键成功
    vecString[0].clear();
    vecString[0].push_back("name");  vecString[0].push_back("id");
    assert(systemManager->dropPrimary("teacher", vecString[0]) == 0);
    // 确认数据无误
    vecString[0].clear();  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");  vecString[0].push_back("teacher");
    vecString[1].clear();  vecString[1].push_back("id");  vecString[1].push_back("name");  vecString[1].push_back("data");
    vecResult.clear();
    vecCondition.clear();
    vecCondition.push_back(Condition{GREATER_EQUAL, INT, false, false, "teacher", "", "id", "", "", 0, 0});
    assert(queryManager->exeSelect(vecString[0], vecString[1], vecCondition, vecResult) == 0);
    cout << "[--check--] There should be one line below." << endl;
    for (int i = 0; i < (int)vecResult.size(); i++) {
        cout << vecResult[i][0].intVal << " " << vecResult[i][1].stringVal << " " << vecResult[i][2].floatVal << endl;
    }
    cout << "[-----Test passed-----]" << endl << endl;
}

/*Table *table2 = getTable("teacher");
vector <RID> ridList2 = table2->getRecordList();
for (int i = 0; i < (int)ridList2.size(); i++) {
    vector <Data> dataList = table2->exeSelect(ridList2[i]);
    int a = 1;
}*/

int main(){
    system("rm -rf *.dat *.key *.tree");
    MyBitMap::initConst();
    bufManager = new BufManager();
    indexHandler = new IndexHandler(bufManager);
    systemManager = new SystemManager(indexHandler, bufManager);
    queryManager = new QueryManager(indexHandler, systemManager, bufManager);

    testDatabaseAndTable();
    testSingleTable();
    testPrimaryAndForeign();
    
    /*tableHeader.clear();
    tableHeader.push_back(TableHeader{"teacher", "id", "", "", INT, 4, 0, 0, true, false, false, false, false});
    tableHeader.push_back(TableHeader{"teacher", "name", "", "", VARCHAR, 10, 0, 0, false, false, false, false, false});
    tableHeader.push_back(TableHeader{"teacher", "score", "", "", FLOAT, 4, 0, 0, false, false, false, false, false});
    sm->createTable("teacher", tableHeader);
    
    nameHeader.push_back("id");
    sm->createPrimary("teacher", nameHeader);
    for(int i=0;i<10;i++){
        vector<Data> data;
        data.clear();
        Data dt;
        dt.intVal = 10*i + 5;
        dt.isNull = false;
        dt.varType = INT;
        data.push_back(dt);
        dt.stringVal = "teacher" + to_string(i);
        dt.varType = VARCHAR;
        data.push_back(dt);
        dt.floatVal = (float)100/i;
        dt.varType = FLOAT;
        data.push_back(dt);
        assert(qm->exeInsert("teacher", data) == 0);
        assert(qm->exeInsert("student", data) == -1);
    }

    std::vector<std::string> tableName;
    tableName.clear();
    tableName.push_back("teacher");
    tableName.push_back("teacher");
    std::vector<std::string> selectorList;
    selectorList.clear();
    selectorList.push_back("name");
    selectorList.push_back("score");
    std::vector<Condition> conditionList;
    conditionList.clear();
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "teacher", "",  "id", "", "", 25, 0});
    std::vector<std::vector<Data>> resData;
    resData.clear();
    qm->exeSelect(tableName, selectorList, conditionList, resData);
    for(auto i:resData){
        cout << i[0].stringVal <<  " " << i[1].floatVal << endl;
    }

    conditionList.clear();
    conditionList.push_back(Condition{ConditionType::LESS, INT, false, true, "teacher", "teacher",  "id", "id", "", 0, 0});
    //std::vector<std::vector<Data>> resData;
    resData.clear();
    qm->exeSelect(tableName, selectorList, conditionList, resData);
    cout << resData.size() << endl;

    vector<Data> data;
    data.clear();
    Data dt;
    dt.intVal = 10 + 5;
    dt.isNull = false;
    dt.varType = INT;
    data.push_back(dt);
    dt.stringVal = "teacher" + to_string(111);
    dt.varType = VARCHAR;
    data.push_back(dt);
    dt.floatVal = (float)111;
    dt.varType = FLOAT;
    data.push_back(dt);
    assert(qm->exeInsert("teacher", data) == -1);

    tableHeader.clear();
    tableHeader.push_back(TableHeader{"course", "id", "", "", INT, 4, 0, 0, false, false, false, false, true});
    tableHeader.push_back(TableHeader{"course", "course_name", "", "", VARCHAR, 10, 0, 0, false, false, false, false, false});
    tableHeader.push_back(TableHeader{"course", "teacher", "teacher", "id", INT, 4, 0, 0, false, false, false, false, false});
    assert(sm->createTable("course", tableHeader) == 0);
    nameHeader.clear();
    nameHeader.push_back("id");
    tableHeader.clear();
    tableHeader.push_back(TableHeader{"", "teacher", "", "id", INT, 0, 0, 0, false, false, false, false, false});
    sm->createPrimary("course", nameHeader);
    sm->createForeign("course", "teacher", tableHeader);
    
    for(int i=0;i<10;i++){
        vector<Data> data;
        data.clear();
        Data dt;
        dt.intVal = 10*i + 5;
        dt.isNull = false;
        dt.varType = INT;
        data.push_back(dt);
        dt.stringVal = "course" + to_string(i);
        dt.varType = VARCHAR;
        data.push_back(dt);
        dt.intVal = 10*i+5;
        dt.varType = INT;
        data.push_back(dt);
        assert(qm->exeInsert("course", data) == 0);
    }

    data.clear();
    dt.intVal = 101;
    dt.isNull = false;
    dt.varType = INT;
    data.push_back(dt);
    dt.stringVal = "course" + to_string(111);
    dt.varType = VARCHAR;
    data.push_back(dt);
    dt.intVal = 11;
    dt.varType = INT;
    data.push_back(dt);
    assert(qm->exeInsert("course", data) == -1);

    tableName.clear();
    tableName.push_back("teacher");
    tableName.push_back("course");
    selectorList.clear();
    selectorList.push_back("name");
    selectorList.push_back("course_name");
    //std::vector<Condition> conditionList;
    conditionList.clear();
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "teacher", "",  "id", "", "", 85, 0});
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "course", "",  "id", "", "", 85, 0});
    //std::vector<std::vector<Data>> resData;
    resData.clear();
    qm->exeSelect(tableName, selectorList, conditionList, resData);
    for(auto i:resData){
        cout << i[0].stringVal <<  " " << i[1].stringVal << endl;
    }

    conditionList.clear();
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "teacher", "",  "id", "", "", 85, 0});
    assert(qm->exeDelete("teacher", conditionList) == -1);

    conditionList.clear();
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "course", "",  "id", "", "", 85, 0});
    assert(qm->exeDelete("course", conditionList) == 0);

    conditionList.clear();
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "teacher", "",  "id", "", "", 85, 0});
    assert(qm->exeDelete("teacher", conditionList) == 0);

    conditionList.clear();
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "teacher", "",  "id", "", "", 75, 0});
    assert(qm->exeDelete("teacher", conditionList) == -1);*/

    delete queryManager;
    delete systemManager;
    delete indexHandler;
    delete bufManager;

    return 0;
}
