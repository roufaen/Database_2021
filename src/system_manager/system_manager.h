# ifndef SYSTEM_MANAGER_H
# define SYSTEM_MANAGER_H

# include "../query_manager/table.h"
# include <vector>
# include <string>

using namespace std;

class SystemManager {
public:
    SystemManager();
    ~SystemManager();
    int createDb(string dbName);
    int dropDb(string dbName);
    int openDb(string dbName);
    int closeDb();
    string dbName();
    int createTable(string tableName, vector <TableHeader> headers);
    int dropTable(string tableName);
    bool hasTable(string tableName);
    //vector <TableHeader> getHeaders(string tableName);
    Table *getTable(string tableName);
    int createIndex(string tableName, string headerName);
    int dropIndex(string tableName, string headerName);
};

# endif
