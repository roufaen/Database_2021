# ifndef DATA_H
# define DATA_H

# include "../utils/var_type.h"

using namespace std;

struct Data {
    int intVal;
    double floatVal;
    string stringVal;
    VarType varType;
    bool isNull;
};

# endif
