#ifndef INDEX_HANDLER_H
#define INDEX_HANDLER_H

#include "ix.h"
#include <cstring>
#include <vector>

class IndexScan;

class IndexHandler{
public:
    IndexHandler(std::string tableName, std::string colName, ix::DataType type);
    ~IndexHandler();

    void insert(key_ptr key, RID rid);
    void remove(key_ptr key, RID rid);
    bool has(key_ptr key);
    int count(key_ptr key);
    int lesserCount(key_ptr key);
    int greaterCount(key_ptr key);
    IndexScan begin();
    IndexScan lowerBound(key_ptr key);
    IndexScan upperBound(key_ptr key);
    std::vector<RID> getRIDs(key_ptr key);
    inline int totalCount();
    void closeIndex();
    void removeIndex();
    friend class IndexScan;

private:
    string tableName, colName;
    ix::DataType type;

    shared_ptr<IndexFileHandler> treeFile = nullptr;
    shared_ptr<RecordHandler> keyFile = nullptr;
    BufManager* treeFileBm;
    BufManager* keyFileBm;
    char* nowdata;

    void insertIntoNonFullPage(key_ptr key, RID rid, int pageID); 
    void splitPage(BPlusNode* node, int index); 
    void insertIntoOverflowPage(key_ptr key, RID rid, BPlusNode* fatherPage, int x);

    void deleteFromLegalPage(key_ptr key, RID rid, int pageID);
    void mergePage(BPlusNode* node, int index);  //合并node上index和index+1号节点
    void borrowFromBackward(BPlusNode* node, int index);
    void borrowFromForward(BPlusNode* node, int index);
    void deleteFromOverflowPage(key_ptr key, RID rid, BPlusNode* fatherPage, int x);

    int getCountIn(int pageID, key_ptr key);
    int getLesserCountIn(int pageID, key_ptr key);
    int getGreaterCountIn(int pageID, key_ptr key);
    IndexScan getLowerBound(int pageID, key_ptr key);

    inline std::string getKeyFilename() {return tableName + colName + ".tree";}
    inline std::string getTreeFilename() {return tableName + colName + ".key";}

};

class IndexScan{
public:
    IndexScan(IndexHandler *ih):tree(ih){}
    IndexScan(IndexHandler *ih, BPlusNode* bn, int keyn, int valn):tree(ih), currentNode(bn), currentKeyPos(keyn), currentValuePos(valn)
    {}

    char* getKey();
    RID getValue();
    void next();
    void previous();
    void setToBegin();
    bool equals(const IndexScan &other);
    inline bool available(){return currentNode != nullptr;}
    void nextKey();
    void previousKey();

private:
    IndexHandler* tree;
    BPlusNode* currentNode;
    BPlusOverflowPage* currentOverflowPage;
    int currentKeyPos, currentValuePos, currentCumulation;
};

#endif
