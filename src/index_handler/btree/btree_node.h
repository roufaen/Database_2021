#ifndef BTREE_NODE_H
#define BTREE_NODE_H
#include "btree.h"

template<typename T>
class BtreeNode{
private:
    int maxSize;
    int size;
    T* key;
    BtreeNode<T>* parent;
    BtreeNode<T>** child;
public:
    friend BTree<T>;
    T getKey(int);
    bool isFull();
    void destroy(BtreeNode<T>*);
    BtreeNode();
    BtreeNode(int);
    ~BtreeNode();
};

template<typename T>
class Triple{
public:
    BtreeNode<T> *node;
    int id;
    int tag;
};
#endif