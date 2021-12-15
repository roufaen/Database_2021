# ifndef TABLE_H
# define TABLE_H

# include "../record_handler/record_handler.h"
# include "./table_header.h"
# include "./data.h"
# include <vector>

using namespace std;

// 在 RecordHandler 的基础上进行封装，无数据合法性检查功能
class Table {
public:
    Table(string dbName, string tableName, BufManager *bufManager);
    Table(string dbName, string tableName, BufManager *bufManager, vector <TableHeader> headerList);
    ~Table();
    vector <TableHeader> getHeaderList();
    int writeHeaderList(vector <TableHeader> headerList); 
    vector <Data> exeSelect(RID rid);
    RID exeInsert(vector <Data> data);
    int exeDelete(RID rid);
    RID exeUpdate(vector <Data> data, RID rid);
    vector <RID> getRecordList();
    string getTableName();

private:
    RecordHandler *recordHandler;
    string dbName, tableName;
    vector <TableHeader> headerList;
};

# endif
