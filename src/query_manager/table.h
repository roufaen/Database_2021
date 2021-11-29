# ifndef TABLE_H
# define TABLE_H

# include "../record_handler/record_handler.h"
# include "./table_header.h"
# include "./data.h"
# include <vector>

using namespace std;

class Table {
public:
    Table(string dbName, string tableName, RecordHandler *recordHandler);
    ~Table();
    vector <TableHeader> getHeaders();
    vector <Data> exeSelect(RID rid);
    RID exeInsert(vector <Data> data);
    int exeDelete(RID rid);
    RID exeUpdate(vector <Data> data, RID rid);

private:
    RecordHandler *recordHandler;
    string dbName, tableName;
    vector <TableHeader> headers;
};

# endif
