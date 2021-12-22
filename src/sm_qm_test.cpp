#include "query_manager/query_manager.h"
#include "system_manager/system_manager.h"
#include <assert.h>
#include <stdlib.h>
using namespace std;

unsigned char MyBitMap::ha[] = {0};
int main(){
    system("rm -rf *.dat");
    system("rm -rf *.key");
    system("rm -rf *.tree");
    MyBitMap::initConst();
    BufManager* bm = new BufManager();
    //RecordHandler* rh = new RecordHandler(bm);
    IndexHandler* ih = new IndexHandler(bm);
    SystemManager* sm = new SystemManager(ih, bm);
    QueryManager* qm = new QueryManager(ih, sm, bm);

    sm->createDb("courses");
    assert(sm->openDb("student"));
    assert(sm->openDb("courses") == 0);
    vector<TableHeader> tableHeader;
    tableHeader.clear();
    tableHeader.push_back(TableHeader{"teacher", "id", "", "", INT, 4, 0, true, false, true, false, true});
    tableHeader.push_back(TableHeader{"teacher", "name", "", "", VARCHAR, 10, 0, false, false, false, false, false});
    tableHeader.push_back(TableHeader{"teacher", "score", "", "", FLOAT, 4, 0, false, false, false, false, false});
    sm->createTable("teacher", tableHeader);
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
        assert(qm->exeInsert("student", data));
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
    assert(qm->exeInsert("teacher", data));

    tableHeader.clear();
    tableHeader.push_back(TableHeader{"course", "id", "", "", INT, 4, 0, true, false, true, false, true});
    tableHeader.push_back(TableHeader{"course", "course_name", "", "", VARCHAR, 10, 0, false, false, false, false, false});
    tableHeader.push_back(TableHeader{"course", "teacher", "teacher", "id", INT, 4, 0, false, true, false, false, false});
    assert(sm->createTable("course", tableHeader)==0);
    
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
    assert(qm->exeInsert("course", data));

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
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "teacher", "",  "id", "", "", 105, 0});
    assert(qm->exeDelete("teacher", conditionList));

    conditionList.clear();
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "course", "",  "id", "", "", 105, 0});
    assert(qm->exeDelete("course", conditionList) == 0);

    conditionList.clear();
    conditionList.push_back(Condition{ConditionType::GREATER_EQUAL, INT, false, false, "teacher", "",  "id", "", "", 105, 0});
    assert(qm->exeDelete("teacher", conditionList) == 0);   

    return 0; 
}