#include "index_handler.h"
#include <cstdio>
#include <iostream>
unsigned char MyBitMap::ha[]={0};
int main(){
    std::cout << sizeof(BPlusNode) << std::endl;
    MyBitMap::initConst();
    BufManager* bm = new BufManager();
    IndexHandler* ih = new IndexHandler(bm);
    ih->openIndex("db","col",INT);
    printf("Built\n");
    for(int i=0; i<600; i++){
        ih->insert((char*)&i, RID{i,i});
        printf("Inserted %d\n", i);
    }
    ih->closeIndex();
    ih->openIndex("db","col",INT);
    int index = 588;
    IndexScan indexScan = ih->upperBound((char*)&index);
    char* nowdata = new char[MAX_RECORD_LEN];
    while(indexScan.available()){
        indexScan.getKey(nowdata);
        std::cout<<*((int*)nowdata) <<"*" << indexScan.getValue().pageID << " " << indexScan.getValue().slotID << std::endl;
        indexScan.next();
    }
    ih->insert((char*)&index, RID{1,1});
    printf("Is there any 1? %d\n", ih->count((char*)&index));
    ih->remove((char*)&index, RID{1,1});
    printf("Is there any 1? %d\n", ih->count((char*)&index));
    index = 10;
    printf("Is there any 10? %d\n", ih->has((char*)&index));
    index = 1;
    ih->insert((char*)&index, RID{10,20});
    printf("How many 1? %d\n", ih->count((char*)&index));
    printf("How many bigger than 1? %d\n", ih->greaterCount((char*)&index));
    ih->closeIndex();
    return 0;
}