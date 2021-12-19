# ifndef SYSTEM_MANAGER_H
# define SYSTEM_MANAGER_H

# include "../record_handler/record_handler.h"
# include "../index_handler/index_handler.h"
# include "../query_manager/table.h"
# include "./name_handler.h"
# include <vector>
# include <string>
# include <map>

using namespace std;

class SystemManager {
public:
    SystemManager(IndexHandler *indexHandler, BufManager *bufManager);
    ~SystemManager();
    int createDb(string dbName);
    int dropDb(string dbName);
    int openDb(string dbName);
    int closeDb();
    string getDbName();
    int createTable(string tableName, vector <TableHeader> headerList);
    int dropTable(string tableName);
    bool hasTable(string tableName);
    Table *getTable(string tableName);
    int createIndex(string tableName, string headerName);
    int dropIndex(string tableName, string headerName);
    int createColumn(string tableName, TableHeader header, Data defaultData);
    int dropColumn(string tableName, string headerName);
    int createPrimary(string tableName, string headerName);
    int dropPrimary(string tableName, string headerName);
    int createUnique(string tableName, string headerName);
    int dropUnique(string tableName, string headerName);

    // 执行插入操作，对名为 tableName 的表格插入 dataList
    void opInsert(string tableName, vector <Data> dataList);
    // 执行删除操作，从名为 tableName 的表格删除编号为 rid 的记录
    void opDelete(string tableName, vector <Data> dataList, RID rid);
    // 列头为 header ，数据为 data ，对引用的外键，其被引用次数 +1 （新增）或 -1 （删除）
    void foreignKeyProcess(vector <TableHeader> headerList, vector <Data> dataList, int delta);
    // 输入 table 名、列名、数据，返回 index 中 key 的计数
    int countKey(string tableName, string headerName, VarType type, int intVal, double floatVal, string stringVal);

private:
    bool headerListLegal(vector <TableHeader> headerList);
    IndexHandler *indexHandler;
    BufManager *bufManager;
    NameHandler *dbNameHandler, *tableNameHandler;
    vector <Table*> tableList;
    string dbName;
};

# endif
