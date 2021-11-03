# ifndef RECORD_HANDLER_H
# define RDCORD_HANDLER_H

# include "rid.h"

class RecordHandler {
public:
    RecordHandler();
    ~RecordHandler();
    int CreateFile(const char *fileName);                  // 创建文件
    int DestroyFile(const char *fileName);                 // 删除文件
    int OpenFile(const char *fileName);                    // 通过缓存管理模块打开文件，并获取其句柄
    int CloseFile();                                       // 关闭fileID对应文件
    int GetRecord(const RID &rid, char *&pData);           // 通过页号和槽号访问记录后，相应字节序列可以通过pData访问
    int DeleteRecord(const RID &rid);                      // 删除特定记录
    int InsertRecord(const RID &rid, const char *pData);   // 将字节序列pData插入特定位置
    int UpdateRecord(const RID &rid, const char *pData);   // 将特定位置记录更新为字节序列pData
};

# endif
