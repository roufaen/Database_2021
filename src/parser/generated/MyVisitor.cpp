#include "MyVisitor.h"
#include <sstream>
#include <math.h>

VarType getVarType(SQLParser::Type_Context* tc, int& len){
    int type = tc->getAltNumber();
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
        dt.varType = FLOAT;
    }
    catch(const std::exception& e) {}
    try
    { 
        dt.intVal = getValue<int>(data->Integer()->getText());
        dt.varType = INT;
    }
    catch(const std::exception& e) {}
    try
    { 
        dt.stringVal = data->String()->getText();
        dt.varType = VARCHAR;
    }
    catch(const std::exception& e) {}
    try
    { 
        dt.isNull = ( data->Null() != nullptr );
    }
    catch(const std::exception& e) {}
}

inline int max(int a, int b) {return a>b?a:b; }

inline int getNumLen(int a) {
    int sign = (a<0);
    int b = abs(a);
    if(b<10) return sign + 1;
    if(b<100) return sign + 2;
    if(b<1000) return sign + 3;
    if(b<10000) return sign + 4;
    if(b<100000) return sign + 5;
    if(b<1000000) return sign + 6;
    if(b<10000000) return sign + 7;
    if(b<100000000) return sign + 8;
    if(b<1000000000) return sign + 9;
    return sign + 10;
}

void print(const vector<string>& tableName, const vector<string>& colName, const vector<vector<Data>>& data){
    auto t_i = tableName.begin();
    auto t_j = colName.begin();
    vector<int> len;
    while(t_i!=tableName.end() && t_j!=colName.end()){
        len.push_back((*t_i).length() + (*t_j).length());
        t_i++; t_j++;
    }
    for(auto dt:data){
        int index = 0;
        for(auto d:dt){
            switch (d.varType)
            {
            case INT:
                len[index] = max(len[index], getNumLen(d.intVal));
                break;
            case FLOAT:
                double ftval = (d.floatVal < 0) ? -d.floatVal : d.floatVal;
                len[index] = max(len[index], max( ceil(log10(ftval)) + (d.floatVal < 0) , 7));
                break;
            case VARCHAR:
            case CHAR:
                len[index] = max(len[index], d.stringVal.length());
                break;
            case DATE:
                len[index] = max(len[index],8);
                break;
            default:
                std::cerr << "ERROR TYPE IN PRINTING" << std::endl;
                break;
            }
            index++;
        }
    }
    int num_of_col = tableName.size();
    std::cout<<"+";
    for(int i=0; i<num_of_col; i++)
    {
        for(int j=0; j<len[i]; j++)
            std::cout<<"-";
       std::cout<<"+";
    }
    std::cout<<std::endl;
    std::cout<<"|";
    for(int i=0; i<num_of_col; i++)
    {
        std::cout<<setw(len[i])<<(tableName[i] + "." + colName[i]);
        std::cout<<"|";
    }
    std::cout << std::endl;
    std::cout<<"+";
    for(int i=0; i<num_of_col; i++)
    {
        for(int j=0; j<len[i]; j++)
            std::cout<<"-";
       std::cout<<"+";
    }
    std::cout<<std::endl;
    std::cout << "|";
    for(auto dt:data){
        int index = 0;
        for(auto d:dt){
            switch (d.varType)
            {
            case INT:
            case DATE:
                std::cout << setw(len[index]) << d.intVal;
                break;
            case FLOAT:
                std::cout << setw(len[index]) << d.floatVal;
                break;
            case VARCHAR:
            case CHAR:
                std::cout << setw(len[index]) << d.stringVal;
                break;
            default:
                std::cerr << "ERROR TYPE IN PRINTING" << std::endl;
                break;
            }
            index++;
        }
        std::cout << "|";
    }
    std::cout << std::endl;
    std::cout<<"+";
    for(int i=0; i<num_of_col; i++)
    {
        for(int j=0; j<len[i]; j++)
            std::cout<<"-";
       std::cout<<"+";
    }
    std::cout<<std::endl;
}