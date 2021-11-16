# ifndef DATA_H
# define DATA_H

# include "../utils/var_type.h"

using namespace std;

struct Data {
    char *data;
    VarType varType;
    int len;
};

# endif
