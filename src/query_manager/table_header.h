# ifndef TABLE_HEADER_H
# define TABLE_HEADER_H

# include "../utils/var_type.h"
# include <string>

using namespace std;

struct TableHeader {
    string tableName, headerName;
    VarType varType;
    int len;
    bool isPrimary, isForeign, permitNull;
};

# endif
