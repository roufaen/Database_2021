# ifndef TABLE_H
# define TABLE_H

# include "../record_handler/record_handler.h"
# include "./table_header.h"
# include "./data.h"
# include <vector>

using namespace std;

class Table {
public:
    Table(string dbName, string tableName, BufManager *bufManager);
    ~Table();
    vector <TableHeader> getHeaders();
    vector <Data> exeSelect(RID rid);
    RID exeInsert(vector <Data> data);
    int exeDelete(RID rid);
    RID exeUpdate(vector <Data> data, RID rid);
    vector <RID> allRecords();

private:
    vector <TableHeader> readHeaders();
    int writeHeaders(vector <TableHeader> headers); 
    RecordHandler *recordHandler;
    string dbName, tableName;
    vector <TableHeader> headers;
};

# endif
