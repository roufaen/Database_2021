#include "MyParser.h"

auto parse(std::string sSQL, QueryManager* qm, RecordHandler* rh, IndexHandler* ih, SystemManager* sm) {
    ANTLRInputStream sInputStream(sSQL);
    SQLLexer iLexer(&sInputStream);
    CommonTokenStream sTokenStream(&iLexer);
    SQLParser iParser(&sTokenStream);
    auto iTree = iParser.program();
    MyVisitor myVisitor(qm,rh,ih,sm); 
    auto iRes = myVisitor.visit(iTree);
    return iRes;  
}

int main(int argc, const char* argv[]){
    BufManager* bm = new BufManager();
    RecordHandler* rh = new RecordHandler(bm);
    IndexHandler* ih = new IndexHandler(bm);
    SystemManager* sm = new SystemManager(ih, bm);
    QueryManager* qm = new QueryManager(ih, sm, bm);
    parse(argv[1], qm, rh, ih, sm);
}