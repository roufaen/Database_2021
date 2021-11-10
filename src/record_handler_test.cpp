# include "utils/RID.h"
# include "record_handler/record_handler.h"
# include "buf_manager/buf_manager.h"
# include <stdio.h>

using namespace std;

int main() {
    char *dat = new char[2048];
    RID rid[20];
    BufManager *bufManager = new BufManager();
    RecordHandler *recordHandler = new RecordHandler(bufManager);
    printf("%d\n", recordHandler->createFile("file1.dat"));
    printf("%d\n", recordHandler->createFile("file2.dat"));
    recordHandler->openFile("file1.dat");
    printf("%d\n", recordHandler->createFile("file2.dat"));
    for (int i = 0; i < 10; i++) {
        rid[i] = recordHandler->insertRecord("init_data");
    }
    recordHandler->getRecord(rid[0], dat);
    printf("%s\n", dat);
    recordHandler->getRecord(rid[4], dat);
    printf("%s\n", dat);
    recordHandler->getRecord(rid[9], dat);
    printf("%s\n", dat);
    recordHandler->deleteRecord(rid[1]);
    recordHandler->deleteRecord(rid[2]);
    recordHandler->deleteRecord(rid[3]);
    recordHandler->deleteRecord(rid[4]);
    recordHandler->deleteRecord(rid[5]);
    printf("%d\n", recordHandler->deleteRecord(rid[1]));
    rid[10] = recordHandler->updateRecord(rid[6], "updated_data");
    printf("%d\n", recordHandler->getRecord(rid[6], dat));
    printf("%d %d\n", rid[10].pageID, rid[10].slotID);
    recordHandler->getRecord(rid[6], dat);
    printf("%s\n", dat);
    recordHandler->closeFile();
    printf("%d\n", recordHandler->destroyFile("file1.dat"));
    printf("%d\n", recordHandler->destroyFile("file2.dat"));

    delete recordHandler;
    delete bufManager;
    delete[] dat;
    return 0;
}