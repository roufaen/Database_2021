#include "MyParser.h"

void parse(std::string sSQL, QueryManager* qm, RecordHandler* rh, IndexHandler* ih, SystemManager* sm) {
    ANTLRInputStream sInputStream(sSQL);
    SQLLexer iLexer(&sInputStream);
    CommonTokenStream sTokenStream(&iLexer);
    SQLParser iParser(&sTokenStream);
    auto iTree = iParser.program();
    MyVisitor myVisitor(qm,rh,ih,sm); 
    myVisitor.visit(iTree);;  
}
