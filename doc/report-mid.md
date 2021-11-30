<font face="宋体">

# <center>数据库系统概论中期报告</center>
<center>罗富文 计91 2019011409  任彦羽 计91 2019011215</center>

## 1. 小组人员分工
罗富文：记录管理模块；查询处理和系统管理模块基本框架搭建；
任彦羽：文件系统；索引模块；

## 2. 目前的项目进度

### 文件系统
本项目文件系统基于下发的文件系统改造。最主要的区别为 file manager 与 buffer manager 被整合，使上层模块调用接口更加方便。文件系统主要接口如下，该系统目前已能正常工作。

```c++
class BufManager {
public:
    // 为文件中的某一个页面获取一个缓存中的页面
    BufType allocPage(int fileID, int pageID, int& index, bool ifRead = false);
    // 为文件中的某一个页面在缓存中找到对应的缓存页面
    BufType getPage(int fileID, int pageID, int& index);
    // 标记 index 代表的缓存页面被访问过，为替换算法提供信息
    void access(int index);
    // 标记 index 代表的缓存页面被写过，保证替换算法在执行时能进行必要的写回操作，保证数据的正确性
    void markDirty(int index);
    // 将 index 代表的缓存页面归还给缓存管理器，在归还前，缓存页面中的数据不标记写回
    void release(int index);
    // 将 index 代表的缓存页面归还给缓存管理器，在归还前，缓存页面中的数据需要根据脏页标记决定是否写到对应的文件页面中
    void writeBack(int index);
    // 将所有缓存页面归还给缓存管理器，归还前需要根据脏页标记决定是否写到对应的文件页面中
    void close();

    // 创建新文件
    void createFile(const char* filename);
    // 打开文件
    int openFile(const char* filename);
    // 关闭文件
    void closeFile(int fd);
    // 删除文件
    void removeFile(const char* filename);
};
```

### 记录管理模块
RecordHandler 接口参照文档中提供的接口设计。目前记录存储模式为定长，但在接口设计上为后续扩展为变长存储作了预留。 RecordHandler 类声明的主要部分如下，该模块已编写完毕，正在测试中。

```c++
class RecordHandler {
public:
    // 创建文件
    int createFile(string fileName);
    // 删除文件
    int destroyFile(string fileName);
    // 通过缓存管理模块打开文件
    int openFile(string fileName);
    // 关闭文件
    int closeFile();
    // 通过页号和槽号访问记录后，相应字节序列可以通过 pData 访问
    int getRecord(const RID &rid, char *&pData);
    // 删除特定记录
    int deleteRecord(const RID &rid);
    // 将字节序列 pData 插入特定位置
    RID insertRecord(const char *pData, int len = MAX_RECORD_LEN);
    // 将特定位置记录更新为字节序列 pData
    RID updateRecord(const RID &rid, const char *pData, int len = MAX_RECORD_LEN);
};
```

### 索引管理模块

索引管理模块IndexHandler内部维护了一个B+树。为使得索引能够适应无Unique约束，B+树中还有一类Overflow节点，用于存储同一键值的不同节点信息。该模块中，键值使用记录管理模块储存，因而具有较好的变长存储拓展性。该模块已编写完毕，正在测试中。

```c++
class IndexHandler{
public:
    // 创建IndexHandler时自动生成或者打开文件
    IndexHandler(std::string tableName, std::string colName, DataType type);
    ~IndexHandler();
	// 插入键值对
    void insert(key_ptr key, RID rid);
    // 移除键值对
    void remove(key_ptr key, RID rid);
    // 查询是否有该键值
    bool has(key_ptr key);
    // 查询键值等于该键值的键值对有多少个
    int count(key_ptr key);
    // 查询键值小于给定键值对的键值对有多少个
    int lesserCount(key_ptr key);
    // 查询键值大于给定键值对的键值对有多少个
    int greaterCount(key_ptr key);
    // 获得B+树第一个叶子节点上第一个键值对的迭代器
    IndexScan begin();
    // 获得恰好小于等于给定键值的键值对的迭代器
    IndexScan lowerBound(key_ptr key);
    // 获得恰好大于等于给定键值的键值对的迭代器
    IndexScan upperBound(key_ptr key);
    // 获得所有键值等于给定键值的键值对对应的记录的位置
    std::vector<RID> getRIDs(key_ptr key);
    // 获得键值对数量
    inline int totalCount();
    // 关闭索引文件
    void closeIndex();
    // 删除索引文件
    void removeIndex();
}
```



### 查询解析模块
为了代码编写方便，本项目新增 Table 类以实现对单张表的增删查改等基本操作。 Table 类声明的主要部分如下，该模块正在编写中。

```c++
class Table {
public:
    // 建立 Table 类的同时打开特定表格
    Table(string dbName, string tableName, RecordHandler *recordHandler);
    ~Table();
    // 获取表格 Header
    vector <TableHeader> getHeaders();
    // 从表格中选择特定 RID 的数据
    vector <Data> exeSelect(RID rid);
    // 向表格中插入一行数据
    RID exeInsert(vector <Data> data);
    // 从表格中删除一行数据
    int exeDelete(RID rid);
    // 在表格中更新一行数据
    RID exeUpdate(vector <Data> data, RID rid);
};
```

QueryManager 类在 Table 类的基础上增加更复杂的操作（主要针对涉及多表的操作），包括主键和外键检查、多表数据筛选与整合等，能完成对表的增、删、查、改、连接等操作。在进行增删改查等操作时， QueryManager 还会调用 IndexHandler 类创建或删除索引。 QueryManager 类声明的主要部分如下，该模块未开始编写。

```c++
class QueryManager {
public:
    // 查询，包括单表查询和多表连接等
    int exeSelect(vector <const char*> tableNameList, vector <const char*> selectorList, vector <Condition> conditionList);
    // 向表格中插入数据
    int exeInsert(const char *tableName, vector <Data> dataList);
    // 从表格中删除满足条件的数据
    int exeDelete(const char *tableName, vector <Condition> conditionList);
    // 在表格中更新满足条件的数据
    int exeUpdate(const char *tableName, vector <TableHeader> headerList, vector <Data> dataList, vector <Condition> conditionList);
};
```

### 系统管理模块
SystemManager 类主要执行创建、删除、打开、关闭数据库和创建、删除表格与索引等操作。 SystemManager 类声明的主要部分如下，该模块未开始编写。

```c++
class SystemManager {
public:
    // 创建数据库
    int createDb(string dbName);
    // 删除数据库
    int dropDb(string dbName);
    // 打开数据库
    int openDb(string dbName);
    // 关闭数据库
    int closeDb();
    // 获取数据库名字
    int dbName(string& dbName);
    // 创建表格
    int createTable(string tableName, vector <TableHeader> headers);
    // 删除表格
    int dropTable(string tableName);
    // 获取表格 Header
    vector <TableHeader> getHeaders(string tableName);
    // 创建索引
    int createIndex(string tableName, string headerName);
    // 删除索引
    int dropIndex(string tableName, string headerName);
};
```

## 3. 存在的问题与困难
 - 对数据库系统的基本架构不了解，不知道从何下手。
 - 部分模块测试困难，不知道会出现哪些特殊情况。

## 4. 对课程的建议
 - 希望能提供符合本课程验收要求的基本框架的参考（参考接口）。
 - 希望能提供部分测试建议。
 - 希望能给出明确的最终验收的评分标准。
