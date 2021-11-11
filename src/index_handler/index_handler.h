#ifndef INDEX_HANDLER_H
#define INDEX_HANDLER_H

#include "../utils/rid.h"
#include "index_file_handler.h"
#include "ix.h"
#include <cstring>
#include <vector>

class IndexScan;

class IndexHandler{
public:
    IndexHandler(std::string tableName, string colName, DataType type);
    ~IndexHandler();

    void insert(key_ptr key, int rid);
    void remove(key_ptr key, int rid);
    bool has(key_ptr key);
    int count(key_ptr key);
    int lesserCount(key_ptr key);
    int greaterCount(key_ptr key);
    IndexScan begin();
    IndexScan lowerBound(key_ptr key);
    IndexScan upperBound(key_ptr key);
    std::vector<RID> getRIDs(key_ptr key);
    int totalCount();
    void closeIndex();
    void removeIndex();

private:
    string tableName, colName;
    DataType type;

    shared_ptr<IndexFileHandler> treeFile = nullptr;

    void insertIntoNonFullPage(key_ptr key, int rid, int pageID); 
    void splitChildPageOn(BPlusNode* node, int index); 
    void insertIntoOverflowPage(key_ptr key, int rid, BPlusNode* fatherPage, int x);

    void deleteFromLegalPage(key_ptr key, int rid, int pageID);
    void mergeChildPageOn(BPlusNode* node, int index);  //合并node上index和index+1号节点
    void borrowFromBackward(BPlusNode* node, int index);
    void borrowFromForward(BPlusNode* node, int index);
    void deleteFromOverflowPage(key_ptr key, int rid, BPlusNode* fatherPage, int x);

    int getCountIn(int pageID, key_ptr key);
    int getLesserCountIn(int pageID, key_ptr key);
    int getGreaterCountIn(int pageID, key_ptr key);
    IndexScan getLowerBound(int pageID, key_ptr key);
};

#endif