#include "index_handler.h"
#include <cstdio>
#include <iostream>
int main(){
    IndexHandler* ih = new IndexHandler("test", "name", ix::DataType::INT);
    printf("Built\n");
    for(int i=0; i<10000; i++){
        ih->insert((char*)&i, RID{i,i});
        printf("Inserted %d\n", i);
        int index = 1;
        if( i>=1 && ih->greaterCount((char*)&index) != i-1) break;
    }
    int index = 1;
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