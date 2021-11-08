#ifndef BTREE_H
#define BTREE_H

#include "btree_node.h"
template<typename T>
class Btree{
private:
    BtreeNode<T> *root;
    const int maxSize;

public:
    Btree(int);
    ~Btree();
    Triple<T> search(const T item);
    void load(const char* con);
    bool insert(const T item);
    bool remove(const T item);
    void print();
    BtreeNode<T> *getParent(const T item);
};

#endif