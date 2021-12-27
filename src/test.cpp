#include "MyParser.h"
#include <string>

unsigned char MyBitMap::ha[] = {0};
int main(){
    BufManager* bm = new BufManager();
    RecordHandler* rh = new RecordHandler(bm);
    IndexHandler* ih = new IndexHandler(bm);
    SystemManager* sm = new SystemManager(ih, bm);
    QueryManager* qm = new QueryManager(ih, sm, bm);
    std::string command;
    while(getline(cin, command)){
        parse(command, qm, rh, ih, sm);
    }
    return 0;
}