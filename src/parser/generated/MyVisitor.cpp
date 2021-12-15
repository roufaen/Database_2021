#include "MyVisitor.h"
#include <sstream>

VarType getVarType(SQLParser::Type_Context* tc, int& len){
    int type = tc->getAltNumber(); //WARNING: THERE MIGHT EXIST SOME PROBLEM HERE
    switch(type){
        case 1: len=4;
            return INT;
        case 2: len=getValue<int>(tc->Integer()->getText());
            return VARCHAR;
        case 3: len=4;
            return FLOAT;
    }
    len=0;
    return VARCHAR;
}

ConditionType getCondType(SQLParser::OperateContext* oc){
    if(oc->EqualOrAssign()) return ConditionType::EQUAL;
    if(oc->NotEqual()) return ConditionType::NOT_EQUAL;
    if(oc->Less()) return ConditionType::LESS;
    if(oc->LessEqual()) return ConditionType::LESS_EQUAL;
    if(oc->Greater()) return ConditionType::GREATER;
    if(oc->GreaterEqual()) return ConditionType::GREATER_EQUAL;
    return ConditionType::IN;
}

template <class Type>  
Type getValue(const string& str){
    std::istringstream iss(str);  
    Type num;  
    iss >> num;  
    return num;  
}

void getFromValue(Data& dt, SQLParser::ValueContext* data){
    try
    { 
        dt.floatVal = getValue<float>(data->Float()->getText());
    }
    catch(const std::exception& e) {}
    try
    { 
        dt.intVal = getValue<int>(data->Float()->getText());
    }
    catch(const std::exception& e) {}
    try
    { 
        dt.stringVal = data->String()->getText();
    }
    catch(const std::exception& e) {}
    try
    { 
        dt.isNull = ( data->Null() != nullptr );
    }
    catch(const std::exception& e) {}
}

template <class Type>
std::vector<Type> castVector(antlrcpp::Any& x){
    std::vector<Type> vec;
    vec.clear();
    try
    {
        vec = x.as<std::vector<Type>>();
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    return vec;
}