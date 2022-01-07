# ifndef SYSTEM_MANAGER_H
# define SYSTEM_MANAGER_H

# include "../record_handler/record_handler.h"
# include "../index_handler/index_handler.h"
# include "../query_manager/table.h"
# include "./name_handler.h"
# include <vector>
# include <string>
# include <map>
# include <iostream>

using namespace std;

class SystemManager {
public:
    SystemManager(IndexHandler *indexHandler, BufManager *bufManager);
    ~SystemManager();
    int getDbNameList(vector <string>& nameList);
    int createDb(string dbName);
    int dropDb(string dbName);
    int openDb(string dbName);
    int closeDb();
    string getDbName();
    int getTableNameList(vector <string>& nameList);
    int createTable(string tableName, vector <TableHeader> headerList);
    int dropTable(string tableName);
    bool hasTable(string tableName);
    Table *getTable(string tableName);
    int getHeaderList(string tableName, vector <TableHeader>& headerList);
    int createIndex(string tableName, string headerName);
    int dropIndex(string tableName, string headerName);
    int createColumn(string tableName, TableHeader header, Data defaultData);
    int dropColumn(string tableName, string headerName);
    int createPrimary(string tableName, vector <string> headerNameList);
    void opCreatePrimary(string tableName, vector <string> headerNameList); 
    int dropPrimary(string tableName, vector <string> headerNameList);
    int createForeign(string tableName, string foreignTableName, vector <TableHeader> updateHeaderList);
    int dropForeign(string tableName, vector <string> headerNameList);
    int createUnique(string tableName, vector <string> headerNameList);
    int dropUnique(string tableName, vector <string> headerNameList);

    // 执行插入操作，对名为 tableName 的表格插入 dataList
    RID opInsert(string tableName, vector <Data> dataList);
    // 执行删除操作，从名为 tableName 的表格删除编号为 rid 的记录
    void opDelete(string tableName, vector <Data> dataList, RID rid);
    // 列头为 header ，数据为 data ，对引用的外键，其被引用次数 +1 （新增）或 -1 （删除）
    void foreignKeyProcess(vector <TableHeader> headerList, vector <Data> dataList, int delta);
    // 输入 table 名、列名、数据，返回 index 中 key 的计数
    //int countKey(string tableName, string headerName, VarType type, int intVal, double floatVal, string stringVal);

private:
    bool headerListLegal(vector <TableHeader> headerList);
    bool columnUnique(string tableName, vector <string> headerNameList);
    void opCreateIndex(string tableName, string headerName);
    void opDropIndex(string tableName, string headerName);
    IndexHandler *indexHandler;
    BufManager *bufManager;
    NameHandler *dbNameHandler, *tableNameHandler;
    vector <Table*> tableList;
    string dbName;
};

# endif
