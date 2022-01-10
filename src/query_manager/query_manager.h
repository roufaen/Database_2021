# ifndef QUERY_MANAGER_H
# define QUERY_MANAGER_H

# include "../index_handler/index_handler.h"
# include "../system_manager/system_manager.h"
# include "./condition.h"
# include "./data.h"
# include <string>
# include <map>
# include <float.h>

using namespace std;

class QueryManager {
public:
    QueryManager(IndexHandler *indexHandler, SystemManager *systemManager, BufManager *bufManager);
    ~QueryManager();
    // 从 tableNameList 中读取数据， selectorList 中每三个元素分别为 tableName headerName aggregator ， conditionList 为条件列表， aggregation 标记是否聚集查询， resData 保存返回数据
    int exeSelect(vector <string> tableNameList, vector <pair <pair <string, string>, string> > selectorList, vector <Condition> conditionList, bool aggregation, vector <vector <Data> >& resData);
    // 向名为 tableName 的表中插入一条数据 dataList
    int exeInsert(string tableName, vector <Data> dataList);
    // 向名为 tableName 的表中插入一条数据 dataList
    int exeInsert(string tableName, vector <Data> dataList, RID& rid);
    // 从名为 tableName 的表中删除符合筛选条件的数据
    int exeDelete(string tableName, vector <Condition> conditionList);
    // 在名为 tableName 的表中更新数据，修改 updateHeaderNameList 中的列，每列数据修改为 updateDataList 中对应列的数据，筛选条件为 conditionList
    int exeUpdate(string tableName, vector <string> updateHeaderNameList, vector <Data> updateDataList, vector <Condition> conditionList);

private:
    // 对两组 data 输出比较结果，要求对于不比较的内容输入必须相同，例如希望比较 lInt 和 rInt 则 lFloat 和 rFloat 必须相等且 lString 和 rString 必须相等
    bool compare(int lInt, double lFloat, string lString, int rInt, double rFloat, string rString, ConditionType cond);
    // 列头为 headerList ，数据为 dataList ，判断是否符合 conditionList ，符合则返回 true ，否则返回 false
    bool conditionJudge(vector <TableHeader> headerList, vector <Data> dataList, vector <Condition> conditionList);
    // 判断外键是否存在
    bool foreignKeyExistJudge(TableHeader header, Data data);
    // 判断是否符合 unique 条件
    bool judgeUnique(string tableName, vector <string> judgeHeaderList, vector <Data> judgeDataList);
    IndexHandler *indexHandler;
    SystemManager *systemManager;
    BufManager *bufManager;
};

# endif
