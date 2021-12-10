#include "Myparser.h"

auto parse(std::string sSQL) {
    ANTLRInputStream sInputStream(sSQL);
    SQLLexer iLexer(&sInputStream);
    CommonTokenStream sTokenStream(&iLexer);
    SQLParser iParser(&sTokenStream);
    auto iTree = iParser.program();
    MyVisitor myVisitor; 
    auto iRes = myVisitor.visit(iTree);
    return iRes;  
}

int main(int argc, const char* argv[]){
    parse(argv[1]);
}