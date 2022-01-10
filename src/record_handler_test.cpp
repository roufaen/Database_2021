# include "utils/rid.h"
# include "record_handler/record_handler.h"
# include "buf_manager/buf_manager.h"
# include <stdio.h>
# include <cstdlib>
# include <random>
# include <map>
# include <assert.h>
# include <algorithm>

using namespace std;
unsigned char MyBitMap::ha[] = {0};
int main() {
    system("rm -rf *.dat");
    MyBitMap::initConst();
    char *dat = new char[20];
    BufManager *bufManager = new BufManager();
    RecordHandler *recordHandler = new RecordHandler(bufManager);
    recordHandler->createFile("test.dat", 10);
    recordHandler->openFile("test.dat");
    vector <RID> ridList;
    for (int i = 0; i < 100000; i++) {
        memset(dat, 0, 20), memcpy(dat, to_string(i).c_str(), to_string(i).size());
        ridList.push_back(recordHandler->insertRecord(dat));
    }
    srand(time(0));
    for (int i = 0; i < 5000; i++) {
        vector <string> vecString;
        for (int j = 0; j < 20; j++) {
            int randNum = rand() % ridList.size();
            recordHandler->getRecord(ridList[randNum], dat);
            vecString.push_back(dat);
            recordHandler->deleteRecord(ridList[randNum]);
            ridList.erase(ridList.begin() + randNum);
        }
        random_shuffle(vecString.begin(), vecString.end());
        for (int j = 0; j < 20; j++) {
            memset(dat, 0, 20), memcpy(dat, vecString[j].c_str(), vecString[j].size());
            ridList.push_back(recordHandler->insertRecord(dat));
        }
    }
    map <int, int> chkMap;
    for (int i = 0; i < 100000; i++) {
        recordHandler->getRecord(ridList[i], dat);
        chkMap[atoi(dat)] = 1;
    }
    for (int i = 0; i < 100000; i++) {
        assert(chkMap[i] == 1);
        chkMap[i] = 0;
    }
    recordHandler->closeFile();
    recordHandler->openFile("test.dat");
    ridList = recordHandler->getRecordList();
    for (int i = 0; i < (int)ridList.size(); i++) {
        recordHandler->getRecord(ridList[i], dat);
        chkMap[atoi(dat)] = 1;
    }
    for (int i = 0; i < 100000; i++) {
        assert(chkMap[i] == 1);
    }

    recordHandler->closeFile();
    recordHandler->destroyFile("test.dat");
    delete recordHandler;
    delete bufManager;
    delete[] dat;
    return 0;
}
