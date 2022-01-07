#include "MyParser.h"
#include <string>

unsigned char MyBitMap::ha[] = {0};
int main(){
    system("rm -rf *.dat *.key *.tree");
    MyBitMap::initConst();
    BufManager* bm = new BufManager();
    IndexHandler* ih = new IndexHandler(bm);
    SystemManager* sm = new SystemManager(ih, bm);
    QueryManager* qm = new QueryManager(ih, sm, bm);
    std::cout<<"$";
    std::string command;
    while(getline(cin, command)){
        parse(command, qm, ih, sm);
        std::cout<<"$";
    }
    return 0;
}