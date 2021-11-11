#include "index_handler.h"

#include <string>
#include <sstream>

IndexHandler::IndexHandler(BufManager* bufManager){
    bm = bufManager;
    id = -1;
}

IndexHandler::~IndexHandler(){}

int IndexHandler::keyCompare(void* lhs, void*rhs){
    if(attrType == INT){
        int* ls = (int*) lhs;
        int* rs = (int*) rhs;
        return ((*ls) < (*rs)) ? -1 : ((*ls) == (*rs)) ? 0 : 1;
    }else if(attrType == FLOAT) {
        float* ls = (float*) lhs;
        float* rs = (float*) rhs;
        return ((*ls) < (*rs)) ? -1 : ((*ls) == (*rs)) ? 0 : 1;
    }else if(attrType == VARCHAR) {
        const char* ls = (const char*)lhs;
        const char* rs = (const char*)rhs;
        return strncmp(ls, rs, attrLength);
    }else {
        cerr << "NO SUCH TYPE\n";
    }
}

int IndexHandler::createIndex(const char* filename, short attrType, int attrLen){
    bm->createFile(filename);
    int tempFileID;
    tempFileID = bm->openFile(filename);

    //create file header
    BufType fileHeader;
    int tempPageID;
    fileHeader = bm->allocPage(tempFileID, 0, tempPageID);
    IndexFileHeader ifh;
    ifh.type = attrType;
    ifh.attrLength = attrLen;
    memcpy(fileHeader,&ifh, sizeof(ifh));
    bm->markDirty(tempPageID);
    bm->writeBack(tempPageID);

    //create root
    IndexPage* root = (IndexPage*) bm->allocPage(tempFileID, 1, tempPageID);
    root->isLeaf = 1;
    root->pos.pageID = 1;
    root->pos.slotID = 0;
    root->size = 0;
    for(int i=0; i<node_cap; i++)
    {
        root->child[i].pageID = 0;
        root->child[i].slotID = 0;
    }

    root->parent.pageID = 0;
    root->parent.slotID = 0;
    root->sibling.pageID = 0;
    root->sibling.slotID = 0;
    root->entry = new char[node_cap * attrLen];
    memset(root->entry, 0, node_cap * attrLen); 

    bm->markDirty(tempPageID);
    bm->writeBack(tempPageID);
    return 0;
}

IndexPage IndexHandler::createPage(int pageID, int slotID){
    IndexPage node;
    node.isLeaf = 1;
    node.pos.pageID = pageID;
    node.pos.slotID = slotID;
    node.size = 0;
    for(int i=0; i<node_cap; i++)
    {
        node.child[i].pageID = 0;
        node.child[i].slotID = 0;
    }

    node.parent.pageID = pageID;
    node.parent.slotID = slotID;
    node.sibling.pageID = pageID;
    node.sibling.slotID = slotID;
    node.entry = new char[node_cap * attrLength];
    memset(node.entry, 0, node_cap * attrLength);
    return node;

}
int IndexHandler::destroyIndex(const char* filename){
    bm->removeFile(filename);
    return 0;
}

int IndexHandler::openIndex(const char* filename){
    if(id != -1) bm->closeFile(id);
    id = bm->openFile(filename);
    BufType headerPage;
    int pageID = -1;
    headerPage = bm->getPage(id, 0, pageID);
    IndexFileHeader ifh;
    std::memcpy(&ifh, headerPage, sizeof(ifh));
    attrLength = ifh.attrLength;
    attrType = ifh.type;
    if(std_zero) delete[] std_zero;
    std_zero = new char[attrLength * node_cap];
    memset(std_zero, 0, sizeof(std_zero));
    return id;
}

int IndexHandler::closeIndex(){
    bm->close();
    bm->closeFile(id);
    id = -1;
    return 1;
}

void IndexHandler::getNode(int pageID, int slotID, IndexPage* retPage) {
    int index;
    BufType currentPage = bm->getPage(id, pageID, index);
    IndexPage* indexPage = (IndexPage*) currentPage[slotID];
    memcpy(retPage, currentPage, offsetof(IndexPage, entry));
    memcpy(retPage->entry, indexPage->entry, attrLength * node_cap); //I am not sure if this will make gcc comfortable?
}

void IndexHandler::splitPage(IndexPage* node, void* val){
    //char* middle = node->entry[];
}

int IndexHandler::maxIndex(IndexPage* node){
    char* entry = node->entry;
    for(int i=0; i < node_cap; i++) {
        if(keyCompare(entry, std_zero) == 0) return(i-1);
        entry += attrLength;
    }
    return node_cap - 1;
}
void IndexHandler::shiftRight(IndexPage* node, int st, int offset){
    int max = maxIndex(node);
    int index = max;
    for(; index >= st; index--){
        memcpy(node->entry+(index+offset)*attrLength, node->entry+index*attrLength, attrLength);
        node->child[index + offset] = node->child[index];
    }
    writeBack(node);
}
void IndexHandler::writeBack(IndexPage* node){
    int index;
    int slotID = node->pos.slotID;
    BufType currentPage = bm->getPage(id, node->pos.pageID, index);
    IndexPage* indexPage = (IndexPage*) currentPage[slotID];
    memcpy(currentPage, node, offsetof(IndexPage, entry));
    memcpy(indexPage->entry, node->entry, attrLength * node_cap);
    bm->markDirty(index);
}

void IndexHandler::insertData(IndexPage* node, void* val, const RID& rid){
    if(node->size == node_cap - 1) {
        splitPage(node, val);
    }
    int index = 0;
    char* entry = node->entry;
    for(; index < node_cap - 1; index++){
        if(keyCompare(entry, std_zero) == 0 || keyCompare(entry ,val) == 1) break;
        entry += attrLength;
    }
    if(node->isLeaf == 0){
        if(!(node->child[index].pageID == 0 && node->child[index].pageID == 0)) {
            getNode(node->child[index].pageID, node->child[index].slotID, node);
            insertData(node, val, rid);
        } else {
            memcpy(node->entry + (index - 1) * attrLength, (const char*)val, attrLength);  
            writeBack(node); 
            getNode(node->child[index - 1].pageID, node->child[index - 1].slotID, node);
            insertData(node, val, rid);
        }
    }else{
        if(node->size > 0) shiftRight(node, index, 1);

        getNode(node->pos.pageID, node->pos.slotID, node);
        memcpy(node->entry + index * attrLength, (const char*)val, attrLength);
        node->child[index] = rid;
        node->size++;
        
        writeBack(node);
    }
}

void IndexHandler::insertIndex(void* val, const RID& pos){
    IndexPage root;
    root.entry = new char[ attrLength  * node_cap];
    getNode(1, 0, &root);
    insertData(&root, val, pos);
    delete[] root.entry;
}



