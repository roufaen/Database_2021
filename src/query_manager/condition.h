# ifndef CONDITION_H
# define CONDITION_H

# include "../utils/var_type.h"
# include "./table_header.h"
# include <vector>

using namespace std;

struct Condition {
    ConditionType condType;
    TableHeader leftCol, rightCol;
    int rightVal;
    vector <char*> limitList;
};

enum ConditionType { EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, IN };

# endif
