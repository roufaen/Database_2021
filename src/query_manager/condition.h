# ifndef CONDITION_H
# define CONDITION_H

# include "../utils/var_type.h"
# include "./table_header.h"
# include <vector>

using namespace std;

enum ConditionType { EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, IN, IS, ISNOT };

struct Condition {
    enum ConditionType condType;
    VarType rightType;
    bool rightNull, useColumn;
    string leftTableName, rightTableName, leftCol, rightCol, rightStringVal;
    int rightIntVal;
    double rightFloatVal;
};

# endif
