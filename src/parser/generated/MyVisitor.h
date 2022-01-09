#pragma once
#include "SQLBaseVisitor.h"
#include "SQLLexer.h"
#include "antlr4-runtime.h"
#include "../../query_manager/query_manager.h"
#include <time.h>

VarType getVarType(SQLParser::Type_Context*, int& len);
ConditionType getCondType(SQLParser::OperateContext*);
void print(const vector<string>& tableName, const vector<string>& colName, const vector<vector<Data>>& data);
void print(const string header, const vector<string>& data);
void print(const vector<string>& tableName, const vector<vector<string>>& data);
bool isDate(string dateStr, int& date);

template <class Type>  
Type getValue(const string& str);

bool getFromValue(Data& dt, SQLParser::ValueContext* data);

class MyVisitor: public SQLBaseVisitor {
    private:
      QueryManager* qm;
      RecordHandler* rh;
      IndexHandler* ih;
      SystemManager* sm;
      std::vector<Condition> conditionList;
      bool alwaysFalse;

    public:
  MyVisitor(QueryManager* _qm, IndexHandler* _ih, SystemManager* _sm){
    qm = _qm;
    ih = _ih;
    sm = _sm;
  }
  virtual antlrcpp::Any visitCreate_db(SQLParser::Create_dbContext *ctx) override {
    if(!ctx->Identifier()) return defaultResult();
    if (sm->createDb(ctx->Identifier()->getText()) == 0) printf("Successfully create the DB\n");
    else printf("The creation fails.\n");
    return defaultResult();
  }

  virtual antlrcpp::Any visitDrop_db(SQLParser::Drop_dbContext *ctx) override {
    if(!ctx->Identifier()) return defaultResult();
    if (sm->dropDb(ctx->Identifier()->getText()) == 0) printf("Successfully drop the DB\n");
    else printf("The drop fails.\n");
    return defaultResult();
  }

  virtual antlrcpp::Any visitShow_dbs(SQLParser::Show_dbsContext *ctx) override {
    std::vector<string> dbnames;
    sm->getDbNameList(dbnames);
    print("Database", dbnames);
    return defaultResult();
  }

  virtual antlrcpp::Any visitUse_db(SQLParser::Use_dbContext *ctx) override {
    if(!ctx->Identifier()) return defaultResult();
    if (sm->openDb(ctx->Identifier()->getText()) == 0) printf("Enter the db\n");
    else printf("Enter fails.\n");
    return defaultResult();
  }

  virtual antlrcpp::Any visitShow_tables(SQLParser::Show_tablesContext *ctx) override {
    std::vector<std::string> tablenames;
    if(sm->getTableNameList(tablenames)==0) print("Table", tablenames);
    return defaultResult();
  }

  virtual antlrcpp::Any visitShow_indexes(SQLParser::Show_indexesContext *ctx) override {
    std::string tableName = ctx->Identifier()->getText();
    std::vector<TableHeader> th;
    th.clear();
    sm->getHeaderList(tableName, th);
    std::vector<string> vec;
    for(auto it:th){
      if(it.isUnique || it.isPrimary || it.hasIndex) vec.push_back(it.tableName+"."+it.headerName);
    }
    print("Cols with Index", vec);
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLoad_data(SQLParser::Load_dataContext *ctx) override {
    std::string tableName = ctx->Identifier()->getText();
    std::vector<TableHeader> th;
    th.clear();
    sm->getHeaderList(tableName, th);
    vector<string> vec;
    vec.clear();
    for(auto i:th){
      if(i.isPrimary) vec.push_back(i.headerName);
    }
    sm->dropPrimary(tableName,vec);
    std::string fileID = ctx->String()->getText();
    fileID = fileID.substr(1, fileID.length() - 2);
    ifstream inFile(fileID, ios::in);
    string lineStr;
    std::vector<Data> datalist;
    int accumulate = 0;
    while (getline(inFile, lineStr)){
      //std::cout << (accumulate++) << std::endl;
      datalist.clear();
      istringstream sin(lineStr);
      string field;
      auto it = th.begin();
      while(getline(sin, field, ',')){
        Data dt = {0,0,0, "",0,false};
        dt.varType = (*it).varType;
        if(field=="NULL") dt.isNull = true;
        else if (dt.varType == INT){
          dt.intVal = getValue<int>(field);
          dt.varType = INT;
        } else if(dt.varType == FLOAT){
          dt.floatVal = getValue<float>(field);
          dt.varType = FLOAT;
        } else if(dt.varType == DATE) {
          field = "'" + field + "'";
          isDate(field, dt.intVal);
          dt.varType = DATE;
        }
        else {
          dt.varType = VARCHAR;
          dt.stringVal = field;
        }
        it++;
        datalist.push_back(dt);
      }
      sm->opInsert(tableName.c_str(), datalist); 
    }
    if(vec.size()) sm->opCreatePrimary(tableName.c_str(),vec);
    std::cout << "Finish to load the data" << std::endl;
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitStore_data(SQLParser::Store_dataContext *ctx) override {
    //TODO
    printf("Interface not supported.\n");
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreate_table(SQLParser::Create_tableContext *ctx) override {
    if(!(ctx->Identifier() && ctx->field_list())) return defaultResult(); 
    std::string tableName = ctx->Identifier()->getText();
    std::vector<TableHeader> tableHeader;
    tableHeader.clear();
    std::vector<SQLParser::FieldContext *> fc = ctx->field_list()->field();
    TableHeader th;
    th.tableName = tableName;
    bool isCreated = false;
    std::vector<std::string> PF_1;
    for(auto i:fc){ 
      SQLParser::Normal_fieldContext* pointer = dynamic_cast<SQLParser::Normal_fieldContext*>(i);
      if(pointer != nullptr) {
        th.headerName = pointer->Identifier()->getText();
        th.isForeign = false;
        th.isPrimary = false;
        th.isUnique = false;
        th.varType = getVarType(pointer->type_(), th.len);
        th.hasIndex = false;
        th.permitNull = (pointer->Null() == nullptr);
        //th.defaultValue
        tableHeader.push_back(th);
      } else { 
        if(!isCreated) {
          if(sm->createTable(tableName, tableHeader)) {
            printf("Fail to create the table\n");
            return defaultResult();
          }
          isCreated = true;
        }
        SQLParser::Primary_key_fieldContext* pointer = dynamic_cast<SQLParser::Primary_key_fieldContext*>(i);
        if(pointer != nullptr) {
          PF_1.clear();
          auto list = pointer->identifiers()->Identifier();
          for(auto id:list){
              PF_1.push_back(id->getText());
          }
          if(sm->createPrimary(tableName, PF_1)) {
            sm->dropTable(tableName);
            printf("Fail to create the table\n");
            return defaultResult();
          };
        } else {
          SQLParser::Foreign_key_fieldContext* pointer = dynamic_cast<SQLParser::Foreign_key_fieldContext*>(i);
          if(pointer != nullptr) {
              tableHeader.clear();
              // One tag here
              std::string foreignTable = pointer->Identifier()->getText();
              auto selfid= pointer->identifiers(0)->Identifier();
              auto othid = pointer->identifiers(1)->Identifier();
              for(int i = 0; i < selfid.size(); i++){
                  th.foreignHeaderName = othid[i]->getText();
                  th.foreignTableName = foreignTable;
                  th.headerName = selfid[i]->getText();
                  th.isForeign = true;
                  tableHeader.push_back(th);
              }
              if (sm->createForeign(tableName,foreignTable, tableHeader)) {
                sm->dropTable(tableName);
                printf("Fail to create the table\n");
                return defaultResult();
              }
          } else{
             cerr << "UNKNOWN TYPE IN PARSING CREATE TABLE\n";
             sm->dropTable(tableName);
             return defaultResult();
          }
        }
      } 
    }
    if(!isCreated) {
      if(sm->createTable(tableName, tableHeader)) return defaultResult();
    }
    printf("Successfully create the new table\n");
    return defaultResult();
  }

  virtual antlrcpp::Any visitDrop_table(SQLParser::Drop_tableContext *ctx) override {
    if(!ctx->Identifier()) return defaultResult();
    sm->dropTable(ctx->Identifier()->getText());
    return defaultResult();
  }

  virtual antlrcpp::Any visitDescribe_table(SQLParser::Describe_tableContext *ctx) override {
    std::vector<TableHeader> th;
    th.clear();
    sm->getHeaderList(ctx->Identifier()->getText(), th);
    std::vector<string> tb;
    tb.clear();
    tb.push_back("Field");
    tb.push_back("Type");
    tb.push_back("Null");
    tb.push_back("Key");
    tb.push_back("Extra");
    std::vector<vector<std::string>> vec;
    for(auto t:th){
      vector<string> ve;
      ve.clear();
      ve.push_back(t.headerName);
      switch(t.varType){
        case INT:
          ve.push_back("INT");
          break;
        case FLOAT:
          ve.push_back("FLOAT");
          break;
        case VARCHAR:
          ve.push_back("VARCHAR("+to_string(t.len)+")");
          break;
        case DATE:
          ve.push_back("DATE");
          break;
      }
      if(t.permitNull) ve.push_back("YES");
      else ve.push_back("NO");
      std::string key;
      int fig = (int)t.isForeign + (int)t.isUnique + (int)t.isPrimary;
      if(fig>1) key = "MUL";
      else if(t.isForeign) key = "FOR";
      else if(t.isUnique) key = "UNI";
      else if(t.isPrimary) key = "PRI";
      ve.push_back(key);
      ve.push_back("");
      vec.push_back(ve);
    }
    print(tb,vec);
    return defaultResult();
  }

  virtual antlrcpp::Any visitInsert_into_table(SQLParser::Insert_into_tableContext *ctx) override {
    if(!(ctx->Identifier() && ctx->value_lists())) return defaultResult();
    std::string tableName = ctx->Identifier()->getText();
    auto list = ctx->value_lists()->value_list();
    // 为了降低运行时间，以下代码被注释
    // for(auto i:list){
    //   for(auto data:i->value()){
    //     Data dt = {0,0,0, "",0,false};
    //     if(!getFromValue(dt, data)) {
    //       std::cerr << "ERROR INPUT VALUE\n";
    //       return defaultResult();
    //     }
    //   }
    // }
    for(auto i:list){
      std::vector<Data> datalist;
      datalist.clear();
      for(auto data:i->value()){
        Data dt = {0,0,0, "",0,false};
        if(!getFromValue(dt, data)) {
          std::cerr << "ERROR INPUT VALUE\n";
          return defaultResult();
        }
        datalist.push_back(dt);
      }
      qm->exeInsert(tableName.c_str(), datalist); 
    }
    return defaultResult();
  }

  virtual antlrcpp::Any visitDelete_from_table(SQLParser::Delete_from_tableContext *ctx) override {
    if(!(ctx->Identifier() && ctx->where_and_clause())) return defaultResult();
    std::string tableName = ctx->Identifier()->getText();
    visitWhere_and_clause(ctx->where_and_clause());
    if(alwaysFalse) {
      std::cout << "Nothing to do\n";
      return defaultResult();
    }
    if(conditionList.size()) qm->exeDelete(tableName.c_str(), conditionList);
    else cerr << "The condition fails\n";
    return defaultResult();
  }

  virtual antlrcpp::Any visitUpdate_table(SQLParser::Update_tableContext *ctx) override {
    if(!(ctx->Identifier() && ctx->where_and_clause())) return defaultResult();
    std::string tableName = ctx->Identifier()->getText();
    std::vector<std::string> headerList;
    std::vector<Data> dataList;
    visitWhere_and_clause(ctx->where_and_clause());
    if(alwaysFalse) {
      std::cout << "Nothing to do\n";
      return defaultResult();
    }
    SQLParser::Set_clauseContext* sc = ctx->set_clause();
    auto sc_eq = sc->EqualOrAssign();
    int size = sc_eq.size();
    for(int i = 0; i < size; i++)
    {
      Data dt;
      if(!getFromValue(dt, sc->value(i))) {
          std::cerr << "ERROR INPUT VALUE\n";
          return defaultResult();
        }
      headerList.push_back(sc->Identifier(i)->getText());
      dataList.push_back(dt);
    }
    if (qm->exeUpdate(tableName, headerList, dataList, conditionList)==0) printf("Successfully updated.\n");
      else printf("Fail to update.\n");
    return defaultResult();
  }

  virtual antlrcpp::Any visitSelect_table(SQLParser::Select_tableContext *ctx) override {
    if(!(ctx->identifiers())) return defaultResult();
    std::vector<std::vector<Data>> resData;
    resData.clear();
    std::vector<std::string> tableNameList;
    std::vector<std::string> selectorList;
    visitWhere_and_clause(ctx->where_and_clause());
    if(alwaysFalse) {
      std::cout << "Nothing to do\n";
      return defaultResult();
    }
    tableNameList.clear();
    selectorList.clear();
    //NO GROUPED SEARCH SUPPORTED
    if(ctx->selectors() != nullptr && (ctx->selectors()->selector()).size() > 0) {
        auto sls = ctx->selectors()->selector();
        for(auto i:sls)
        {
          tableNameList.push_back(i->column()->Identifier(0)->getText());
          selectorList.push_back(i->column()->Identifier(1)->getText());
        }
    } else {
      auto sls = ctx->identifiers()->Identifier();
      std::vector<TableHeader> th;
      for(auto i:sls){
        std::string tbN = i->getText();
        th.clear();
        sm->getHeaderList(tbN, th);
        for(auto j:th)
        {
          tableNameList.push_back(tbN);
          selectorList.push_back(j.headerName);
        }
      }
    }
    time_t first = time(NULL);
    if (qm->exeSelect(tableNameList, selectorList, conditionList, resData) == 0) {
      time_t second = time(NULL);
      print(tableNameList, selectorList, resData);
      printf("Get return in %f seconds\n",std::difftime(second,first));
    } else printf("Fail to select the data\n");
    return defaultResult();
  }

  virtual antlrcpp::Any visitAlter_add_index(SQLParser::Alter_add_indexContext *ctx) override {
    if(!(ctx->Identifier() && ctx->identifiers())) return defaultResult();
    std::string tableName = ctx->Identifier()->getText();
    auto headerTable = ctx->identifiers()->Identifier();
    // 以下部分用于支持联合索引（？）
    // for(auto i:headerTable){
      // sm->createIndex(tableName, i->getText());
    // }
    sm->createIndex(tableName, headerTable[0]->getText());
    return defaultResult();
  }

  virtual antlrcpp::Any visitAlter_drop_index(SQLParser::Alter_drop_indexContext *ctx) override {
    if(!(ctx->Identifier() && ctx->identifiers())) return defaultResult();
    std::string tableName = ctx->Identifier()->getText();
    auto headerTable = ctx->identifiers()->Identifier();
    // 以下部分用于支持联合索引（？）
    // for(auto i:headerTable){
      // sm->createIndex(tableName, i->getText());
    // }
    sm->dropIndex(tableName, headerTable[0]->getText());
    return defaultResult();
  }

  virtual antlrcpp::Any visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext *ctx) override {
    if(ctx->Identifier()) {
      return defaultResult();
    }
    auto id = ctx->identifiers()->Identifier();
    std::vector<std::string> identifiers;
    identifiers.clear();
    for(int i=1; i<id.size(); i++)
      identifiers.push_back(id[i]->getText());
    sm->dropPrimary(id[0]->getText(), identifiers);
    return defaultResult();
  }

  virtual antlrcpp::Any visitAlter_table_drop_foreign_key(SQLParser::Alter_table_drop_foreign_keyContext *ctx) override {
    //Here is the bug located, only initial foreign col name is supported, no foreign name supported.
    if(ctx->Identifier()) {
      return defaultResult();
    }
    std::string tableName = ctx->Identifier()->getText();
    auto id = ctx->identifiers()->Identifier();
    std::vector<std::string> listName;
    listName.clear();
    listName.push_back(id[1]->getText());
    sm->dropForeign(tableName,listName);
    return defaultResult();
  }

  virtual antlrcpp::Any visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext *ctx) override {
    auto identi = ctx->Identifier();
    if(identi.size() != 2) {
      return defaultResult();
    }
    string tableName = identi[0]->getText(); 
    string keyName = identi[1]->getText();
    auto identifiers = ctx->identifiers()->Identifier();
    std::vector<std::string> headerName;
    headerName.clear();
    for(auto i:identifiers){
      headerName.push_back(i->getText());
    }
    sm->createPrimary(tableName, headerName);
    return defaultResult();
  }

  virtual antlrcpp::Any visitAlter_table_add_foreign_key(SQLParser::Alter_table_add_foreign_keyContext *ctx) override {
    auto identi = ctx->Identifier();
    auto selfid = ctx->identifiers(0)->Identifier();
    auto othid = ctx->identifiers(1)->Identifier();
    if(identi.size() != 3 &&  ctx->identifiers().size() != 2) {
      return defaultResult();
    }
    if(selfid.size() != othid.size()) {
      return defaultResult();
    }
    string tableName = identi[0]->getText(); 
    string keyName = identi[1]->getText();
    string foreignTable = identi[2]->getText();

    TableHeader th;
    th.tableName = tableName;
    std::vector<TableHeader> headerName;
    headerName.clear();
    for(int i = 0; i < selfid.size(); i++){
      th.foreignHeaderName = othid[i]->getText();
      th.foreignTableName = foreignTable;
      th.headerName = selfid[i]->getText();
      th.isForeign = true;
      headerName.push_back(th);
    }
    sm->createForeign(tableName, foreignTable, headerName);
    return defaultResult();
  }

  virtual antlrcpp::Any visitAlter_table_add_unique(SQLParser::Alter_table_add_uniqueContext *ctx) override {
    if(!(ctx->Identifier() && ctx->identifiers())) return defaultResult();
    std::vector<string> listName;
    listName.clear();
    auto list = ctx->identifiers()->Identifier();
    for(auto i:list){
      listName.push_back(i->getText());
    }
    sm->createUnique(ctx->Identifier()->getText(), listName);
    return defaultResult();
  }

   virtual antlrcpp::Any visitAlter_table_drop_unique(SQLParser::Alter_table_drop_uniqueContext *ctx) override {
    if(!(ctx->Identifier() && ctx->identifiers())) return defaultResult();
    std::vector<string> listName;
    listName.clear();
    auto list = ctx->identifiers()->Identifier();
    for(auto i:list){
      listName.push_back(i->getText());
    }
    sm->dropUnique(ctx->Identifier()->getText(), listName);
    return defaultResult();
  }

  virtual antlrcpp::Any visitAlter_add_col(SQLParser::Alter_add_colContext *ctx) override {
    TableHeader th;
    th.tableName = ctx->Identifier(0)->getText();
    th.headerName = ctx->Identifier(1)->getText();
    th.isForeign = false;
    th.isPrimary = false;
    th.isUnique = false;
    th.varType = getVarType(ctx->type_(), th.len);
    th.hasIndex = false;
    th.permitNull = true;
    Data defaultData;
    defaultData.isNull = true;
    defaultData.varType = th.varType;
    sm->createColumn(ctx->Identifier(0)->getText(), th, defaultData);
    return defaultResult();
  }

  virtual antlrcpp::Any visitAlter_drop_col(SQLParser::Alter_drop_colContext *ctx) override {
    sm->dropColumn(ctx->Identifier(0)->getText(), ctx->Identifier(1)->getText());
    return defaultResult();
  }

  virtual antlrcpp::Any visitWhere_and_clause(SQLParser::Where_and_clauseContext *ctx) override{
    conditionList.clear();
    alwaysFalse = false;
    if(ctx==nullptr) return defaultResult();
    std::vector<SQLParser::Where_clauseContext *> wax = ctx->where_clause();
    for(auto i:wax){
      Condition cond;
      SQLParser::Where_operator_expressionContext* woe = dynamic_cast<SQLParser::Where_operator_expressionContext*>(i);
      if(woe==nullptr) continue;
      SQLParser::OperateContext* oc = woe->operate();
      cond.condType = getCondType(oc);
      SQLParser::ColumnContext* cc = woe->column();
      cond.leftTableName = cc->Identifier(0)->getText();
      cond.leftCol = cc->Identifier(1)->getText();
      SQLParser::ExpressionContext* ec = woe->expression();
      if(ec->value() != nullptr){
        Data dt = {0,0,0,"",0,false};
        if(!getFromValue(dt, ec->value())) {
          std::cerr << "ERROR INPUT VALUE\n";
          return defaultResult();
        }
        cond.rightFloatVal = dt.floatVal;
        cond.rightStringVal = dt.stringVal;
        cond.rightIntVal = dt.intVal;
        cond.rightNull = dt.isNull;
        cond.rightType = dt.varType;
        cond.useColumn = false;
        if(cond.rightNull == true && cond.condType != ConditionType::IS && cond.condType != ConditionType::ISNOT) alwaysFalse = true;
        if (cond.condType == ConditionType::IS) cond.condType = ConditionType::EQUAL;
        if (cond.condType == ConditionType::ISNOT) cond.condType = ConditionType::NOT_EQUAL;
      } else if (ec->column() != nullptr) {
        cond.rightTableName = ec->column()->Identifier(0)->getText();
        cond.rightCol = ec->column()->Identifier(1)->getText();
        cond.useColumn = true;
      } else {
        cerr << "Get error in your right input side\n";
        return defaultResult();
      }
      conditionList.push_back(cond);
    }
    return defaultResult();
  }
};