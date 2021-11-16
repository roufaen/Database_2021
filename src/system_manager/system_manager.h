# ifndef SYSTEM_MANAGER_H
# define SYSTEM_MANAGER_H

# include "../query_manager/table.h"
# include <vector>

using namespace std;

class SystemManager {
public:
    SystemManager();
    ~SystemManager();
    int createDb(const char *name);
    int dropDb(const char *name);
    int openDb(const char *name);
    int closeDb();
    int createTable(const char *name, vector <TableHeader> headers);
    int dropTable(const char *name);
    vector <TableHeader> getHeaders(const char *name);
    int createIndex(const char *name);
    int dropIndex(const char *name);
};

# endif
