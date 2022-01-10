# ifndef TABLE_HEADER_H
# define TABLE_HEADER_H

# include "../utils/var_type.h"
# include <string>

using namespace std;

struct TableHeader {
    string tableName, headerName, foreignTableName, foreignHeaderName;
    VarType varType;
    int len, refCount = 0, foreignGroup, uniqueGroup;
    bool isPrimary, isForeign, isUnique, permitNull, hasIndex;
};

# endif
