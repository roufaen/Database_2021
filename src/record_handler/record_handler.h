# ifndef RECORD_HANDLER_H
# define RECORD_HANDLER_H

# include "../utils/RID.h"
# include "../utils/file_header.h"
# include "../utils/pagedef.h"
# include "../buf_manager/buf_manager.h"
# include <cstring>
# include <cmath>
# include <queue>

using namespace std;

class RecordHandler {
public:
    RecordHandler(BufManager *bufManager);
    ~RecordHandler();
    int createFile(string fileName);                                                  // 创建文件
    int destroyFile(string fileName);                                                 // 删除文件
    int openFile(string fileName);                                                    // 通过缓存管理模块打开文件，并获取其句柄
    int closeFile();                                                                  // 关闭fileID对应文件
    int getRecord(const RID &rid, char *&pData);                                      // 通过页号和槽号访问记录后，相应字节序列可以通过pData访问
    int deleteRecord(const RID &rid);                                                 // 删除特定记录
    RID insertRecord(const char *pData, int len = MAX_RECORD_LEN);                    // 将字节序列pData插入特定位置
    RID updateRecord(const RID &rid, const char *pData, int len = MAX_RECORD_LEN);    // 将特定位置记录更新为字节序列pData

private:
    BufManager *bufManager;
    FileHeader header;
    priority_queue <int, vector<int>, greater<int> > availablePage;
    int fileID;
};

# endif
