#include "MyParser.h"

void parse(std::string sSQL, QueryManager* qm, IndexHandler* ih, SystemManager* sm) {
    ANTLRInputStream sInputStream(sSQL);
    SQLLexer iLexer(&sInputStream);
    CommonTokenStream sTokenStream(&iLexer);
    SQLParser iParser(&sTokenStream);
    if(iParser.getNumberOfSyntaxErrors() != 0) return;
    auto iTree = iParser.program();
    MyVisitor myVisitor(qm,ih,sm); 
    try
    {
        myVisitor.visit(iTree);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
}
