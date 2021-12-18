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

private:
    IndexHandler *indexHandler;
    BufManager *bufManager;
    NameHandler *dbNameHandler, *tableNameHandler;
    vector <Table*> tableList;
    string dbName;
};

# endif
