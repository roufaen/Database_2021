#include "MyParser.h"

void parse(std::string sSQL, QueryManager* qm, IndexHandler* ih, SystemManager* sm) {
    ANTLRInputStream sInputStream(sSQL);
    SQLLexer iLexer(&sInputStream);
    CommonTokenStream sTokenStream(&iLexer);
    SQLParser iParser(&sTokenStream);
    auto iTree = iParser.program();
    if(iParser.getNumberOfSyntaxErrors() != 0) return;
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
