#include "MyVisitor.h"
#include <sstream>

VarType getVarType(SQLParser::Type_Context*, int& len){

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
