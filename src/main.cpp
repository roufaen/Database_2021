#include "MyParser.h"
#include <string>

unsigned char MyBitMap::ha[] = {0};
int main(){
    MyBitMap::initConst();
    BufManager* bm = new BufManager();
    IndexHandler* ih = new IndexHandler(bm);
    SystemManager* sm = new SystemManager(ih, bm);
    QueryManager* qm = new QueryManager(ih, sm, bm);
    std::cout<<"$> ";
    std::string command = "";
    std::string lineStr;
    while(getline(cin, lineStr)){
        if(lineStr.find(";") != string::npos) {
            parse(command + lineStr, qm, ih, sm);
            std::cout<<"$> ";
            command = "";
        } else command = command + lineStr;
    }
    return 0;
}
