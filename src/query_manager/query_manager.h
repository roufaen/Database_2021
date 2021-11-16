# ifndef QUERY_MANAGER_H
# define QUERY_MANAGER_H

# include "../record_handler/record_handler.h"
# include "../index_handler/index_handler.h"
# include "../system_manager/system_manager.h"
# include "./condition.h"
# include "./data.h"
# include "./update_data.h"

class QueryManager {
public:
    QueryManager(RecordHandler *recordHandler, IndexHandler *indexHandler, SystemManager *systemManager);
    ~QueryManager();
    int exeSelect(vector <const char*> tableNameList, vector <const char*> selectorList, vector <Condition> conditionList);
    int exeInsert(const char *tableName, vector <Data> dataList);
    int exeDelete(const char *tableName, vector <Condition> conditionList);
    int exeUpdate(const char *tableName, vector <UpdateData> dataList, vector <Condition> conditionList);
};

# endif
