# ifndef QUERY_MANAGER_H
# define QUERY_MANAGER_H

# include "../record_handler/record_handler.h"
# include "../index_handler/index_handler.h"
# include "../system_manager/system_manager.h"
# include "./condition.h"
# include "./data.h"
# include <string>
# include <map>

using namespace std;

class QueryManager {
public:
    QueryManager(RecordHandler *recordHandler, IndexHandler *indexHandler, SystemManager *systemManager);
    ~QueryManager();
    int exeSelect(vector <string> tableNameList, vector <string> selectorList, vector <Condition> conditionList, vector <vector <Data>>& resData);
    int exeInsert(string tableName, vector <Data> dataList);
    int exeDelete(string tableName, vector <Condition> conditionList);
    int exeUpdate(string tableName, vector <TableHeader> headerList, vector <Data> dataList, vector <Condition> conditionList);

private:
    bool compare(int lInt, double lFloat, string lString, int rInt, double rFloat, string rString, ConditionType cond);
    bool conditionJudge(vector <TableHeader> headerList, vector <Data> dataList, vector <Condition> conditionList);
    RecordHandler *recordHandler;
    IndexHandler *indexHandler;
    SystemManager *systemManager;
};

# endif
