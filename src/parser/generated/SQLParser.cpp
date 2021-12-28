
// Generated from ./SQL.g4 by ANTLR 4.9.3


#include "SQLVisitor.h"

#include "SQLParser.h"


using namespace antlrcpp;
using namespace antlr4;

SQLParser::SQLParser(TokenStream *input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA, _sharedContextCache);
}

SQLParser::~SQLParser() {
  delete _interpreter;
}

std::string SQLParser::getGrammarFileName() const {
  return "SQL.g4";
}

const std::vector<std::string>& SQLParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& SQLParser::getVocabulary() const {
  return _vocabulary;
}


//----------------- ProgramContext ------------------------------------------------------------------

SQLParser::ProgramContext::ProgramContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SQLParser::ProgramContext::EOF() {
  return getToken(SQLParser::EOF, 0);
}

std::vector<SQLParser::StatementContext *> SQLParser::ProgramContext::statement() {
  return getRuleContexts<SQLParser::StatementContext>();
}

SQLParser::StatementContext* SQLParser::ProgramContext::statement(size_t i) {
  return getRuleContext<SQLParser::StatementContext>(i);
}


size_t SQLParser::ProgramContext::getRuleIndex() const {
  return SQLParser::RuleProgram;
}


antlrcpp::Any SQLParser::ProgramContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitProgram(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::ProgramContext* SQLParser::program() {
  ProgramContext *_localctx = _tracker.createInstance<ProgramContext>(_ctx, getState());
  enterRule(_localctx, 0, SQLParser::RuleProgram);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(49);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SQLParser::T__1)
      | (1ULL << SQLParser::T__3)
      | (1ULL << SQLParser::T__4)
      | (1ULL << SQLParser::T__6)
      | (1ULL << SQLParser::T__9)
      | (1ULL << SQLParser::T__14)
      | (1ULL << SQLParser::T__19)
      | (1ULL << SQLParser::T__20)
      | (1ULL << SQLParser::T__22)
      | (1ULL << SQLParser::T__24)
      | (1ULL << SQLParser::T__26)
      | (1ULL << SQLParser::T__31)
      | (1ULL << SQLParser::Null))) != 0) || _la == SQLParser::Annotation) {
      setState(46);
      statement();
      setState(51);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(52);
    match(SQLParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StatementContext ------------------------------------------------------------------

SQLParser::StatementContext::StatementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SQLParser::Db_statementContext* SQLParser::StatementContext::db_statement() {
  return getRuleContext<SQLParser::Db_statementContext>(0);
}

SQLParser::Io_statementContext* SQLParser::StatementContext::io_statement() {
  return getRuleContext<SQLParser::Io_statementContext>(0);
}

SQLParser::Table_statementContext* SQLParser::StatementContext::table_statement() {
  return getRuleContext<SQLParser::Table_statementContext>(0);
}

SQLParser::Alter_statementContext* SQLParser::StatementContext::alter_statement() {
  return getRuleContext<SQLParser::Alter_statementContext>(0);
}

tree::TerminalNode* SQLParser::StatementContext::Annotation() {
  return getToken(SQLParser::Annotation, 0);
}

tree::TerminalNode* SQLParser::StatementContext::Null() {
  return getToken(SQLParser::Null, 0);
}


size_t SQLParser::StatementContext::getRuleIndex() const {
  return SQLParser::RuleStatement;
}


antlrcpp::Any SQLParser::StatementContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitStatement(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::StatementContext* SQLParser::statement() {
  StatementContext *_localctx = _tracker.createInstance<StatementContext>(_ctx, getState());
  enterRule(_localctx, 2, SQLParser::RuleStatement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(70);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(54);
      db_statement();
      setState(55);
      match(SQLParser::T__0);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(57);
      io_statement();
      setState(58);
      match(SQLParser::T__0);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(60);
      table_statement();
      setState(61);
      match(SQLParser::T__0);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(63);
      alter_statement();
      setState(64);
      match(SQLParser::T__0);
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(66);
      match(SQLParser::Annotation);
      setState(67);
      match(SQLParser::T__0);
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(68);
      match(SQLParser::Null);
      setState(69);
      match(SQLParser::T__0);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Db_statementContext ------------------------------------------------------------------

SQLParser::Db_statementContext::Db_statementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t SQLParser::Db_statementContext::getRuleIndex() const {
  return SQLParser::RuleDb_statement;
}

void SQLParser::Db_statementContext::copyFrom(Db_statementContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- Show_dbsContext ------------------------------------------------------------------

SQLParser::Show_dbsContext::Show_dbsContext(Db_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Show_dbsContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitShow_dbs(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Drop_dbContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Drop_dbContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Drop_dbContext::Drop_dbContext(Db_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Drop_dbContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitDrop_db(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Show_tablesContext ------------------------------------------------------------------

SQLParser::Show_tablesContext::Show_tablesContext(Db_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Show_tablesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitShow_tables(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Create_dbContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Create_dbContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Create_dbContext::Create_dbContext(Db_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Create_dbContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitCreate_db(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Use_dbContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Use_dbContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Use_dbContext::Use_dbContext(Db_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Use_dbContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitUse_db(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Show_indexesContext ------------------------------------------------------------------

SQLParser::Show_indexesContext::Show_indexesContext(Db_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Show_indexesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitShow_indexes(this);
  else
    return visitor->visitChildren(this);
}
SQLParser::Db_statementContext* SQLParser::db_statement() {
  Db_statementContext *_localctx = _tracker.createInstance<Db_statementContext>(_ctx, getState());
  enterRule(_localctx, 4, SQLParser::RuleDb_statement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(86);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 2, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<SQLParser::Create_dbContext>(_localctx);
      enterOuterAlt(_localctx, 1);
      setState(72);
      match(SQLParser::T__1);
      setState(73);
      match(SQLParser::T__2);
      setState(74);
      match(SQLParser::Identifier);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<SQLParser::Drop_dbContext>(_localctx);
      enterOuterAlt(_localctx, 2);
      setState(75);
      match(SQLParser::T__3);
      setState(76);
      match(SQLParser::T__2);
      setState(77);
      match(SQLParser::Identifier);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<SQLParser::Show_dbsContext>(_localctx);
      enterOuterAlt(_localctx, 3);
      setState(78);
      match(SQLParser::T__4);
      setState(79);
      match(SQLParser::T__5);
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<SQLParser::Use_dbContext>(_localctx);
      enterOuterAlt(_localctx, 4);
      setState(80);
      match(SQLParser::T__6);
      setState(81);
      match(SQLParser::Identifier);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<SQLParser::Show_tablesContext>(_localctx);
      enterOuterAlt(_localctx, 5);
      setState(82);
      match(SQLParser::T__4);
      setState(83);
      match(SQLParser::T__7);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<SQLParser::Show_indexesContext>(_localctx);
      enterOuterAlt(_localctx, 6);
      setState(84);
      match(SQLParser::T__4);
      setState(85);
      match(SQLParser::T__8);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Io_statementContext ------------------------------------------------------------------

SQLParser::Io_statementContext::Io_statementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t SQLParser::Io_statementContext::getRuleIndex() const {
  return SQLParser::RuleIo_statement;
}

void SQLParser::Io_statementContext::copyFrom(Io_statementContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- Store_dataContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Store_dataContext::String() {
  return getToken(SQLParser::String, 0);
}

tree::TerminalNode* SQLParser::Store_dataContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Store_dataContext::Store_dataContext(Io_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Store_dataContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitStore_data(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Load_dataContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Load_dataContext::String() {
  return getToken(SQLParser::String, 0);
}

tree::TerminalNode* SQLParser::Load_dataContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Load_dataContext::Load_dataContext(Io_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Load_dataContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitLoad_data(this);
  else
    return visitor->visitChildren(this);
}
SQLParser::Io_statementContext* SQLParser::io_statement() {
  Io_statementContext *_localctx = _tracker.createInstance<Io_statementContext>(_ctx, getState());
  enterRule(_localctx, 6, SQLParser::RuleIo_statement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(102);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SQLParser::T__9: {
        _localctx = _tracker.createInstance<SQLParser::Load_dataContext>(_localctx);
        enterOuterAlt(_localctx, 1);
        setState(88);
        match(SQLParser::T__9);
        setState(89);
        match(SQLParser::T__10);
        setState(90);
        match(SQLParser::T__11);
        setState(91);
        match(SQLParser::String);
        setState(92);
        match(SQLParser::T__12);
        setState(93);
        match(SQLParser::T__13);
        setState(94);
        match(SQLParser::Identifier);
        break;
      }

      case SQLParser::T__14: {
        _localctx = _tracker.createInstance<SQLParser::Store_dataContext>(_localctx);
        enterOuterAlt(_localctx, 2);
        setState(95);
        match(SQLParser::T__14);
        setState(96);
        match(SQLParser::T__10);
        setState(97);
        match(SQLParser::T__15);
        setState(98);
        match(SQLParser::String);
        setState(99);
        match(SQLParser::T__16);
        setState(100);
        match(SQLParser::T__13);
        setState(101);
        match(SQLParser::Identifier);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Table_statementContext ------------------------------------------------------------------

SQLParser::Table_statementContext::Table_statementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t SQLParser::Table_statementContext::getRuleIndex() const {
  return SQLParser::RuleTable_statement;
}

void SQLParser::Table_statementContext::copyFrom(Table_statementContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- Delete_from_tableContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Delete_from_tableContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Where_and_clauseContext* SQLParser::Delete_from_tableContext::where_and_clause() {
  return getRuleContext<SQLParser::Where_and_clauseContext>(0);
}

SQLParser::Delete_from_tableContext::Delete_from_tableContext(Table_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Delete_from_tableContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitDelete_from_table(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Insert_into_tableContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Insert_into_tableContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Value_listsContext* SQLParser::Insert_into_tableContext::value_lists() {
  return getRuleContext<SQLParser::Value_listsContext>(0);
}

SQLParser::Insert_into_tableContext::Insert_into_tableContext(Table_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Insert_into_tableContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitInsert_into_table(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Create_tableContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Create_tableContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Field_listContext* SQLParser::Create_tableContext::field_list() {
  return getRuleContext<SQLParser::Field_listContext>(0);
}

SQLParser::Create_tableContext::Create_tableContext(Table_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Create_tableContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitCreate_table(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Describe_tableContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Describe_tableContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Describe_tableContext::Describe_tableContext(Table_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Describe_tableContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitDescribe_table(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Select_table_Context ------------------------------------------------------------------

SQLParser::Select_tableContext* SQLParser::Select_table_Context::select_table() {
  return getRuleContext<SQLParser::Select_tableContext>(0);
}

SQLParser::Select_table_Context::Select_table_Context(Table_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Select_table_Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitSelect_table_(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Drop_tableContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Drop_tableContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Drop_tableContext::Drop_tableContext(Table_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Drop_tableContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitDrop_table(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Update_tableContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Update_tableContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Set_clauseContext* SQLParser::Update_tableContext::set_clause() {
  return getRuleContext<SQLParser::Set_clauseContext>(0);
}

SQLParser::Where_and_clauseContext* SQLParser::Update_tableContext::where_and_clause() {
  return getRuleContext<SQLParser::Where_and_clauseContext>(0);
}

SQLParser::Update_tableContext::Update_tableContext(Table_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Update_tableContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitUpdate_table(this);
  else
    return visitor->visitChildren(this);
}
SQLParser::Table_statementContext* SQLParser::table_statement() {
  Table_statementContext *_localctx = _tracker.createInstance<Table_statementContext>(_ctx, getState());
  enterRule(_localctx, 8, SQLParser::RuleTable_statement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(134);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SQLParser::T__1: {
        _localctx = _tracker.createInstance<SQLParser::Create_tableContext>(_localctx);
        enterOuterAlt(_localctx, 1);
        setState(104);
        match(SQLParser::T__1);
        setState(105);
        match(SQLParser::T__13);
        setState(106);
        match(SQLParser::Identifier);
        setState(107);
        match(SQLParser::T__17);
        setState(108);
        field_list();
        setState(109);
        match(SQLParser::T__18);
        break;
      }

      case SQLParser::T__3: {
        _localctx = _tracker.createInstance<SQLParser::Drop_tableContext>(_localctx);
        enterOuterAlt(_localctx, 2);
        setState(111);
        match(SQLParser::T__3);
        setState(112);
        match(SQLParser::T__13);
        setState(113);
        match(SQLParser::Identifier);
        break;
      }

      case SQLParser::T__19: {
        _localctx = _tracker.createInstance<SQLParser::Describe_tableContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(114);
        match(SQLParser::T__19);
        setState(115);
        match(SQLParser::Identifier);
        break;
      }

      case SQLParser::T__20: {
        _localctx = _tracker.createInstance<SQLParser::Insert_into_tableContext>(_localctx);
        enterOuterAlt(_localctx, 4);
        setState(116);
        match(SQLParser::T__20);
        setState(117);
        match(SQLParser::T__12);
        setState(118);
        match(SQLParser::Identifier);
        setState(119);
        match(SQLParser::T__21);
        setState(120);
        value_lists();
        break;
      }

      case SQLParser::T__22: {
        _localctx = _tracker.createInstance<SQLParser::Delete_from_tableContext>(_localctx);
        enterOuterAlt(_localctx, 5);
        setState(121);
        match(SQLParser::T__22);
        setState(122);
        match(SQLParser::T__16);
        setState(123);
        match(SQLParser::Identifier);
        setState(124);
        match(SQLParser::T__23);
        setState(125);
        where_and_clause();
        break;
      }

      case SQLParser::T__24: {
        _localctx = _tracker.createInstance<SQLParser::Update_tableContext>(_localctx);
        enterOuterAlt(_localctx, 6);
        setState(126);
        match(SQLParser::T__24);
        setState(127);
        match(SQLParser::Identifier);
        setState(128);
        match(SQLParser::T__25);
        setState(129);
        set_clause();
        setState(130);
        match(SQLParser::T__23);
        setState(131);
        where_and_clause();
        break;
      }

      case SQLParser::T__26: {
        _localctx = _tracker.createInstance<SQLParser::Select_table_Context>(_localctx);
        enterOuterAlt(_localctx, 7);
        setState(133);
        select_table();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Select_tableContext ------------------------------------------------------------------

SQLParser::Select_tableContext::Select_tableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SQLParser::SelectorsContext* SQLParser::Select_tableContext::selectors() {
  return getRuleContext<SQLParser::SelectorsContext>(0);
}

SQLParser::IdentifiersContext* SQLParser::Select_tableContext::identifiers() {
  return getRuleContext<SQLParser::IdentifiersContext>(0);
}

SQLParser::Where_and_clauseContext* SQLParser::Select_tableContext::where_and_clause() {
  return getRuleContext<SQLParser::Where_and_clauseContext>(0);
}

SQLParser::ColumnContext* SQLParser::Select_tableContext::column() {
  return getRuleContext<SQLParser::ColumnContext>(0);
}

std::vector<tree::TerminalNode *> SQLParser::Select_tableContext::Integer() {
  return getTokens(SQLParser::Integer);
}

tree::TerminalNode* SQLParser::Select_tableContext::Integer(size_t i) {
  return getToken(SQLParser::Integer, i);
}


size_t SQLParser::Select_tableContext::getRuleIndex() const {
  return SQLParser::RuleSelect_table;
}


antlrcpp::Any SQLParser::Select_tableContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitSelect_table(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::Select_tableContext* SQLParser::select_table() {
  Select_tableContext *_localctx = _tracker.createInstance<Select_tableContext>(_ctx, getState());
  enterRule(_localctx, 10, SQLParser::RuleSelect_table);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(136);
    match(SQLParser::T__26);
    setState(137);
    selectors();
    setState(138);
    match(SQLParser::T__16);
    setState(139);
    identifiers();
    setState(142);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SQLParser::T__23) {
      setState(140);
      match(SQLParser::T__23);
      setState(141);
      where_and_clause();
    }
    setState(147);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SQLParser::T__27) {
      setState(144);
      match(SQLParser::T__27);
      setState(145);
      match(SQLParser::T__28);
      setState(146);
      column();
    }
    setState(155);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SQLParser::T__29) {
      setState(149);
      match(SQLParser::T__29);
      setState(150);
      match(SQLParser::Integer);
      setState(153);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == SQLParser::T__30) {
        setState(151);
        match(SQLParser::T__30);
        setState(152);
        match(SQLParser::Integer);
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Alter_statementContext ------------------------------------------------------------------

SQLParser::Alter_statementContext::Alter_statementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t SQLParser::Alter_statementContext::getRuleIndex() const {
  return SQLParser::RuleAlter_statement;
}

void SQLParser::Alter_statementContext::copyFrom(Alter_statementContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- Alter_table_drop_pkContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Alter_table_drop_pkContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::IdentifiersContext* SQLParser::Alter_table_drop_pkContext::identifiers() {
  return getRuleContext<SQLParser::IdentifiersContext>(0);
}

SQLParser::Alter_table_drop_pkContext::Alter_table_drop_pkContext(Alter_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Alter_table_drop_pkContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitAlter_table_drop_pk(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Alter_table_add_foreign_keyContext ------------------------------------------------------------------

std::vector<tree::TerminalNode *> SQLParser::Alter_table_add_foreign_keyContext::Identifier() {
  return getTokens(SQLParser::Identifier);
}

tree::TerminalNode* SQLParser::Alter_table_add_foreign_keyContext::Identifier(size_t i) {
  return getToken(SQLParser::Identifier, i);
}

std::vector<SQLParser::IdentifiersContext *> SQLParser::Alter_table_add_foreign_keyContext::identifiers() {
  return getRuleContexts<SQLParser::IdentifiersContext>();
}

SQLParser::IdentifiersContext* SQLParser::Alter_table_add_foreign_keyContext::identifiers(size_t i) {
  return getRuleContext<SQLParser::IdentifiersContext>(i);
}

SQLParser::Alter_table_add_foreign_keyContext::Alter_table_add_foreign_keyContext(Alter_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Alter_table_add_foreign_keyContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitAlter_table_add_foreign_key(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Alter_table_add_uniqueContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Alter_table_add_uniqueContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::IdentifiersContext* SQLParser::Alter_table_add_uniqueContext::identifiers() {
  return getRuleContext<SQLParser::IdentifiersContext>(0);
}

SQLParser::Alter_table_add_uniqueContext::Alter_table_add_uniqueContext(Alter_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Alter_table_add_uniqueContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitAlter_table_add_unique(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Alter_drop_indexContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Alter_drop_indexContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::IdentifiersContext* SQLParser::Alter_drop_indexContext::identifiers() {
  return getRuleContext<SQLParser::IdentifiersContext>(0);
}

SQLParser::Alter_drop_indexContext::Alter_drop_indexContext(Alter_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Alter_drop_indexContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitAlter_drop_index(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Alter_add_indexContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Alter_add_indexContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::IdentifiersContext* SQLParser::Alter_add_indexContext::identifiers() {
  return getRuleContext<SQLParser::IdentifiersContext>(0);
}

SQLParser::Alter_add_indexContext::Alter_add_indexContext(Alter_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Alter_add_indexContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitAlter_add_index(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Alter_table_drop_foreign_keyContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Alter_table_drop_foreign_keyContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::IdentifiersContext* SQLParser::Alter_table_drop_foreign_keyContext::identifiers() {
  return getRuleContext<SQLParser::IdentifiersContext>(0);
}

SQLParser::Alter_table_drop_foreign_keyContext::Alter_table_drop_foreign_keyContext(Alter_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Alter_table_drop_foreign_keyContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitAlter_table_drop_foreign_key(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Alter_table_add_pkContext ------------------------------------------------------------------

std::vector<tree::TerminalNode *> SQLParser::Alter_table_add_pkContext::Identifier() {
  return getTokens(SQLParser::Identifier);
}

tree::TerminalNode* SQLParser::Alter_table_add_pkContext::Identifier(size_t i) {
  return getToken(SQLParser::Identifier, i);
}

SQLParser::IdentifiersContext* SQLParser::Alter_table_add_pkContext::identifiers() {
  return getRuleContext<SQLParser::IdentifiersContext>(0);
}

SQLParser::Alter_table_add_pkContext::Alter_table_add_pkContext(Alter_statementContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Alter_table_add_pkContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitAlter_table_add_pk(this);
  else
    return visitor->visitChildren(this);
}
SQLParser::Alter_statementContext* SQLParser::alter_statement() {
  Alter_statementContext *_localctx = _tracker.createInstance<Alter_statementContext>(_ctx, getState());
  enterRule(_localctx, 12, SQLParser::RuleAlter_statement);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(235);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<SQLParser::Alter_add_indexContext>(_localctx);
      enterOuterAlt(_localctx, 1);
      setState(157);
      match(SQLParser::T__31);
      setState(158);
      match(SQLParser::T__13);
      setState(159);
      match(SQLParser::Identifier);
      setState(160);
      match(SQLParser::T__32);
      setState(161);
      match(SQLParser::T__33);
      setState(162);
      match(SQLParser::T__17);
      setState(163);
      identifiers();
      setState(164);
      match(SQLParser::T__18);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<SQLParser::Alter_drop_indexContext>(_localctx);
      enterOuterAlt(_localctx, 2);
      setState(166);
      match(SQLParser::T__31);
      setState(167);
      match(SQLParser::T__13);
      setState(168);
      match(SQLParser::Identifier);
      setState(169);
      match(SQLParser::T__3);
      setState(170);
      match(SQLParser::T__33);
      setState(171);
      match(SQLParser::T__17);
      setState(172);
      identifiers();
      setState(173);
      match(SQLParser::T__18);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<SQLParser::Alter_table_drop_pkContext>(_localctx);
      enterOuterAlt(_localctx, 3);
      setState(175);
      match(SQLParser::T__31);
      setState(176);
      match(SQLParser::T__13);
      setState(177);
      match(SQLParser::Identifier);
      setState(178);
      match(SQLParser::T__3);
      setState(179);
      match(SQLParser::T__34);
      setState(180);
      match(SQLParser::T__35);
      setState(185);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == SQLParser::T__17) {
        setState(181);
        match(SQLParser::T__17);
        setState(182);
        identifiers();
        setState(183);
        match(SQLParser::T__18);
      }
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<SQLParser::Alter_table_drop_foreign_keyContext>(_localctx);
      enterOuterAlt(_localctx, 4);
      setState(187);
      match(SQLParser::T__31);
      setState(188);
      match(SQLParser::T__13);
      setState(189);
      match(SQLParser::Identifier);
      setState(190);
      match(SQLParser::T__3);
      setState(191);
      match(SQLParser::T__36);
      setState(192);
      match(SQLParser::T__35);
      setState(193);
      match(SQLParser::T__17);
      setState(194);
      identifiers();
      setState(195);
      match(SQLParser::T__18);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<SQLParser::Alter_table_add_pkContext>(_localctx);
      enterOuterAlt(_localctx, 5);
      setState(197);
      match(SQLParser::T__31);
      setState(198);
      match(SQLParser::T__13);
      setState(199);
      match(SQLParser::Identifier);
      setState(200);
      match(SQLParser::T__32);
      setState(201);
      match(SQLParser::T__37);
      setState(202);
      match(SQLParser::Identifier);
      setState(203);
      match(SQLParser::T__34);
      setState(204);
      match(SQLParser::T__35);
      setState(205);
      match(SQLParser::T__17);
      setState(206);
      identifiers();
      setState(207);
      match(SQLParser::T__18);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<SQLParser::Alter_table_add_foreign_keyContext>(_localctx);
      enterOuterAlt(_localctx, 6);
      setState(209);
      match(SQLParser::T__31);
      setState(210);
      match(SQLParser::T__13);
      setState(211);
      match(SQLParser::Identifier);
      setState(212);
      match(SQLParser::T__32);
      setState(213);
      match(SQLParser::T__37);
      setState(214);
      match(SQLParser::Identifier);
      setState(215);
      match(SQLParser::T__36);
      setState(216);
      match(SQLParser::T__35);
      setState(217);
      match(SQLParser::T__17);
      setState(218);
      identifiers();
      setState(219);
      match(SQLParser::T__18);
      setState(220);
      match(SQLParser::T__38);
      setState(221);
      match(SQLParser::Identifier);
      setState(222);
      match(SQLParser::T__17);
      setState(223);
      identifiers();
      setState(224);
      match(SQLParser::T__18);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<SQLParser::Alter_table_add_uniqueContext>(_localctx);
      enterOuterAlt(_localctx, 7);
      setState(226);
      match(SQLParser::T__31);
      setState(227);
      match(SQLParser::T__13);
      setState(228);
      match(SQLParser::Identifier);
      setState(229);
      match(SQLParser::T__32);
      setState(230);
      match(SQLParser::T__39);
      setState(231);
      match(SQLParser::T__17);
      setState(232);
      identifiers();
      setState(233);
      match(SQLParser::T__18);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Field_listContext ------------------------------------------------------------------

SQLParser::Field_listContext::Field_listContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SQLParser::FieldContext *> SQLParser::Field_listContext::field() {
  return getRuleContexts<SQLParser::FieldContext>();
}

SQLParser::FieldContext* SQLParser::Field_listContext::field(size_t i) {
  return getRuleContext<SQLParser::FieldContext>(i);
}


size_t SQLParser::Field_listContext::getRuleIndex() const {
  return SQLParser::RuleField_list;
}


antlrcpp::Any SQLParser::Field_listContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitField_list(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::Field_listContext* SQLParser::field_list() {
  Field_listContext *_localctx = _tracker.createInstance<Field_listContext>(_ctx, getState());
  enterRule(_localctx, 14, SQLParser::RuleField_list);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(237);
    field();
    setState(242);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SQLParser::T__40) {
      setState(238);
      match(SQLParser::T__40);
      setState(239);
      field();
      setState(244);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FieldContext ------------------------------------------------------------------

SQLParser::FieldContext::FieldContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t SQLParser::FieldContext::getRuleIndex() const {
  return SQLParser::RuleField;
}

void SQLParser::FieldContext::copyFrom(FieldContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- Primary_key_fieldContext ------------------------------------------------------------------

SQLParser::IdentifiersContext* SQLParser::Primary_key_fieldContext::identifiers() {
  return getRuleContext<SQLParser::IdentifiersContext>(0);
}

SQLParser::Primary_key_fieldContext::Primary_key_fieldContext(FieldContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Primary_key_fieldContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitPrimary_key_field(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Foreign_key_fieldContext ------------------------------------------------------------------

std::vector<tree::TerminalNode *> SQLParser::Foreign_key_fieldContext::Identifier() {
  return getTokens(SQLParser::Identifier);
}

tree::TerminalNode* SQLParser::Foreign_key_fieldContext::Identifier(size_t i) {
  return getToken(SQLParser::Identifier, i);
}

SQLParser::Foreign_key_fieldContext::Foreign_key_fieldContext(FieldContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Foreign_key_fieldContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitForeign_key_field(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Normal_fieldContext ------------------------------------------------------------------

tree::TerminalNode* SQLParser::Normal_fieldContext::Identifier() {
  return getToken(SQLParser::Identifier, 0);
}

SQLParser::Type_Context* SQLParser::Normal_fieldContext::type_() {
  return getRuleContext<SQLParser::Type_Context>(0);
}

tree::TerminalNode* SQLParser::Normal_fieldContext::Null() {
  return getToken(SQLParser::Null, 0);
}

SQLParser::ValueContext* SQLParser::Normal_fieldContext::value() {
  return getRuleContext<SQLParser::ValueContext>(0);
}

SQLParser::Normal_fieldContext::Normal_fieldContext(FieldContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Normal_fieldContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitNormal_field(this);
  else
    return visitor->visitChildren(this);
}
SQLParser::FieldContext* SQLParser::field() {
  FieldContext *_localctx = _tracker.createInstance<FieldContext>(_ctx, getState());
  enterRule(_localctx, 16, SQLParser::RuleField);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(271);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SQLParser::Identifier: {
        _localctx = _tracker.createInstance<SQLParser::Normal_fieldContext>(_localctx);
        enterOuterAlt(_localctx, 1);
        setState(245);
        match(SQLParser::Identifier);
        setState(246);
        type_();
        setState(249);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SQLParser::T__41) {
          setState(247);
          match(SQLParser::T__41);
          setState(248);
          match(SQLParser::Null);
        }
        setState(253);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SQLParser::T__42) {
          setState(251);
          match(SQLParser::T__42);
          setState(252);
          value();
        }
        break;
      }

      case SQLParser::T__34: {
        _localctx = _tracker.createInstance<SQLParser::Primary_key_fieldContext>(_localctx);
        enterOuterAlt(_localctx, 2);
        setState(255);
        match(SQLParser::T__34);
        setState(256);
        match(SQLParser::T__35);
        setState(257);
        match(SQLParser::T__17);
        setState(258);
        identifiers();
        setState(259);
        match(SQLParser::T__18);
        break;
      }

      case SQLParser::T__36: {
        _localctx = _tracker.createInstance<SQLParser::Foreign_key_fieldContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(261);
        match(SQLParser::T__36);
        setState(262);
        match(SQLParser::T__35);
        setState(263);
        match(SQLParser::T__17);
        setState(264);
        match(SQLParser::Identifier);
        setState(265);
        match(SQLParser::T__18);
        setState(266);
        match(SQLParser::T__38);
        setState(267);
        match(SQLParser::Identifier);
        setState(268);
        match(SQLParser::T__17);
        setState(269);
        match(SQLParser::Identifier);
        setState(270);
        match(SQLParser::T__18);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Type_Context ------------------------------------------------------------------

SQLParser::Type_Context::Type_Context(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SQLParser::Type_Context::Integer() {
  return getToken(SQLParser::Integer, 0);
}


size_t SQLParser::Type_Context::getRuleIndex() const {
  return SQLParser::RuleType_;
}


antlrcpp::Any SQLParser::Type_Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitType_(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::Type_Context* SQLParser::type_() {
  Type_Context *_localctx = _tracker.createInstance<Type_Context>(_ctx, getState());
  enterRule(_localctx, 18, SQLParser::RuleType_);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(280);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SQLParser::T__43: {
        enterOuterAlt(_localctx, 1);
        setState(273);
        match(SQLParser::T__43);
        break;
      }

      case SQLParser::T__44: {
        enterOuterAlt(_localctx, 2);
        setState(274);
        match(SQLParser::T__44);
        setState(275);
        match(SQLParser::T__17);
        setState(276);
        match(SQLParser::Integer);
        setState(277);
        match(SQLParser::T__18);
        break;
      }

      case SQLParser::T__45: {
        enterOuterAlt(_localctx, 3);
        setState(278);
        match(SQLParser::T__45);
        break;
      }

      case SQLParser::T__46: {
        enterOuterAlt(_localctx, 4);
        setState(279);
        match(SQLParser::T__46);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Value_listsContext ------------------------------------------------------------------

SQLParser::Value_listsContext::Value_listsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SQLParser::Value_listContext *> SQLParser::Value_listsContext::value_list() {
  return getRuleContexts<SQLParser::Value_listContext>();
}

SQLParser::Value_listContext* SQLParser::Value_listsContext::value_list(size_t i) {
  return getRuleContext<SQLParser::Value_listContext>(i);
}


size_t SQLParser::Value_listsContext::getRuleIndex() const {
  return SQLParser::RuleValue_lists;
}


antlrcpp::Any SQLParser::Value_listsContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitValue_lists(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::Value_listsContext* SQLParser::value_lists() {
  Value_listsContext *_localctx = _tracker.createInstance<Value_listsContext>(_ctx, getState());
  enterRule(_localctx, 20, SQLParser::RuleValue_lists);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(282);
    value_list();
    setState(287);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SQLParser::T__40) {
      setState(283);
      match(SQLParser::T__40);
      setState(284);
      value_list();
      setState(289);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Value_listContext ------------------------------------------------------------------

SQLParser::Value_listContext::Value_listContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SQLParser::ValueContext *> SQLParser::Value_listContext::value() {
  return getRuleContexts<SQLParser::ValueContext>();
}

SQLParser::ValueContext* SQLParser::Value_listContext::value(size_t i) {
  return getRuleContext<SQLParser::ValueContext>(i);
}


size_t SQLParser::Value_listContext::getRuleIndex() const {
  return SQLParser::RuleValue_list;
}


antlrcpp::Any SQLParser::Value_listContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitValue_list(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::Value_listContext* SQLParser::value_list() {
  Value_listContext *_localctx = _tracker.createInstance<Value_listContext>(_ctx, getState());
  enterRule(_localctx, 22, SQLParser::RuleValue_list);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(290);
    match(SQLParser::T__17);
    setState(291);
    value();
    setState(296);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SQLParser::T__40) {
      setState(292);
      match(SQLParser::T__40);
      setState(293);
      value();
      setState(298);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(299);
    match(SQLParser::T__18);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValueContext ------------------------------------------------------------------

SQLParser::ValueContext::ValueContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SQLParser::ValueContext::Integer() {
  return getToken(SQLParser::Integer, 0);
}

tree::TerminalNode* SQLParser::ValueContext::String() {
  return getToken(SQLParser::String, 0);
}

tree::TerminalNode* SQLParser::ValueContext::Float() {
  return getToken(SQLParser::Float, 0);
}

tree::TerminalNode* SQLParser::ValueContext::Date() {
  return getToken(SQLParser::Date, 0);
}

tree::TerminalNode* SQLParser::ValueContext::Null() {
  return getToken(SQLParser::Null, 0);
}


size_t SQLParser::ValueContext::getRuleIndex() const {
  return SQLParser::RuleValue;
}


antlrcpp::Any SQLParser::ValueContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitValue(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::ValueContext* SQLParser::value() {
  ValueContext *_localctx = _tracker.createInstance<ValueContext>(_ctx, getState());
  enterRule(_localctx, 24, SQLParser::RuleValue);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(301);
    _la = _input->LA(1);
    if (!(((((_la - 62) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 62)) & ((1ULL << (SQLParser::Null - 62))
      | (1ULL << (SQLParser::Integer - 62))
      | (1ULL << (SQLParser::Date - 62))
      | (1ULL << (SQLParser::String - 62))
      | (1ULL << (SQLParser::Float - 62)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Where_and_clauseContext ------------------------------------------------------------------

SQLParser::Where_and_clauseContext::Where_and_clauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SQLParser::Where_clauseContext *> SQLParser::Where_and_clauseContext::where_clause() {
  return getRuleContexts<SQLParser::Where_clauseContext>();
}

SQLParser::Where_clauseContext* SQLParser::Where_and_clauseContext::where_clause(size_t i) {
  return getRuleContext<SQLParser::Where_clauseContext>(i);
}


size_t SQLParser::Where_and_clauseContext::getRuleIndex() const {
  return SQLParser::RuleWhere_and_clause;
}


antlrcpp::Any SQLParser::Where_and_clauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitWhere_and_clause(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::Where_and_clauseContext* SQLParser::where_and_clause() {
  Where_and_clauseContext *_localctx = _tracker.createInstance<Where_and_clauseContext>(_ctx, getState());
  enterRule(_localctx, 26, SQLParser::RuleWhere_and_clause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(303);
    where_clause();
    setState(308);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SQLParser::T__47) {
      setState(304);
      match(SQLParser::T__47);
      setState(305);
      where_clause();
      setState(310);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Where_clauseContext ------------------------------------------------------------------

SQLParser::Where_clauseContext::Where_clauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t SQLParser::Where_clauseContext::getRuleIndex() const {
  return SQLParser::RuleWhere_clause;
}

void SQLParser::Where_clauseContext::copyFrom(Where_clauseContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- Where_operator_expressionContext ------------------------------------------------------------------

SQLParser::ColumnContext* SQLParser::Where_operator_expressionContext::column() {
  return getRuleContext<SQLParser::ColumnContext>(0);
}

SQLParser::OperateContext* SQLParser::Where_operator_expressionContext::operate() {
  return getRuleContext<SQLParser::OperateContext>(0);
}

SQLParser::ExpressionContext* SQLParser::Where_operator_expressionContext::expression() {
  return getRuleContext<SQLParser::ExpressionContext>(0);
}

SQLParser::Where_operator_expressionContext::Where_operator_expressionContext(Where_clauseContext *ctx) { copyFrom(ctx); }


antlrcpp::Any SQLParser::Where_operator_expressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitWhere_operator_expression(this);
  else
    return visitor->visitChildren(this);
}
SQLParser::Where_clauseContext* SQLParser::where_clause() {
  Where_clauseContext *_localctx = _tracker.createInstance<Where_clauseContext>(_ctx, getState());
  enterRule(_localctx, 28, SQLParser::RuleWhere_clause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    _localctx = _tracker.createInstance<SQLParser::Where_operator_expressionContext>(_localctx);
    enterOuterAlt(_localctx, 1);
    setState(311);
    column();
    setState(312);
    operate();
    setState(313);
    expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnContext ------------------------------------------------------------------

SQLParser::ColumnContext::ColumnContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> SQLParser::ColumnContext::Identifier() {
  return getTokens(SQLParser::Identifier);
}

tree::TerminalNode* SQLParser::ColumnContext::Identifier(size_t i) {
  return getToken(SQLParser::Identifier, i);
}


size_t SQLParser::ColumnContext::getRuleIndex() const {
  return SQLParser::RuleColumn;
}


antlrcpp::Any SQLParser::ColumnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitColumn(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::ColumnContext* SQLParser::column() {
  ColumnContext *_localctx = _tracker.createInstance<ColumnContext>(_ctx, getState());
  enterRule(_localctx, 30, SQLParser::RuleColumn);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(315);
    match(SQLParser::Identifier);
    setState(316);
    match(SQLParser::T__48);
    setState(317);
    match(SQLParser::Identifier);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExpressionContext ------------------------------------------------------------------

SQLParser::ExpressionContext::ExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SQLParser::ValueContext* SQLParser::ExpressionContext::value() {
  return getRuleContext<SQLParser::ValueContext>(0);
}

SQLParser::ColumnContext* SQLParser::ExpressionContext::column() {
  return getRuleContext<SQLParser::ColumnContext>(0);
}


size_t SQLParser::ExpressionContext::getRuleIndex() const {
  return SQLParser::RuleExpression;
}


antlrcpp::Any SQLParser::ExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitExpression(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::ExpressionContext* SQLParser::expression() {
  ExpressionContext *_localctx = _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 32, SQLParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(321);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SQLParser::Null:
      case SQLParser::Integer:
      case SQLParser::Date:
      case SQLParser::String:
      case SQLParser::Float: {
        enterOuterAlt(_localctx, 1);
        setState(319);
        value();
        break;
      }

      case SQLParser::Identifier: {
        enterOuterAlt(_localctx, 2);
        setState(320);
        column();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Set_clauseContext ------------------------------------------------------------------

SQLParser::Set_clauseContext::Set_clauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> SQLParser::Set_clauseContext::Identifier() {
  return getTokens(SQLParser::Identifier);
}

tree::TerminalNode* SQLParser::Set_clauseContext::Identifier(size_t i) {
  return getToken(SQLParser::Identifier, i);
}

std::vector<tree::TerminalNode *> SQLParser::Set_clauseContext::EqualOrAssign() {
  return getTokens(SQLParser::EqualOrAssign);
}

tree::TerminalNode* SQLParser::Set_clauseContext::EqualOrAssign(size_t i) {
  return getToken(SQLParser::EqualOrAssign, i);
}

std::vector<SQLParser::ValueContext *> SQLParser::Set_clauseContext::value() {
  return getRuleContexts<SQLParser::ValueContext>();
}

SQLParser::ValueContext* SQLParser::Set_clauseContext::value(size_t i) {
  return getRuleContext<SQLParser::ValueContext>(i);
}


size_t SQLParser::Set_clauseContext::getRuleIndex() const {
  return SQLParser::RuleSet_clause;
}


antlrcpp::Any SQLParser::Set_clauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitSet_clause(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::Set_clauseContext* SQLParser::set_clause() {
  Set_clauseContext *_localctx = _tracker.createInstance<Set_clauseContext>(_ctx, getState());
  enterRule(_localctx, 34, SQLParser::RuleSet_clause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(323);
    match(SQLParser::Identifier);
    setState(324);
    match(SQLParser::EqualOrAssign);
    setState(325);
    value();
    setState(332);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SQLParser::T__40) {
      setState(326);
      match(SQLParser::T__40);
      setState(327);
      match(SQLParser::Identifier);
      setState(328);
      match(SQLParser::EqualOrAssign);
      setState(329);
      value();
      setState(334);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectorsContext ------------------------------------------------------------------

SQLParser::SelectorsContext::SelectorsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SQLParser::SelectorContext *> SQLParser::SelectorsContext::selector() {
  return getRuleContexts<SQLParser::SelectorContext>();
}

SQLParser::SelectorContext* SQLParser::SelectorsContext::selector(size_t i) {
  return getRuleContext<SQLParser::SelectorContext>(i);
}


size_t SQLParser::SelectorsContext::getRuleIndex() const {
  return SQLParser::RuleSelectors;
}


antlrcpp::Any SQLParser::SelectorsContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitSelectors(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::SelectorsContext* SQLParser::selectors() {
  SelectorsContext *_localctx = _tracker.createInstance<SelectorsContext>(_ctx, getState());
  enterRule(_localctx, 36, SQLParser::RuleSelectors);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(344);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SQLParser::T__49: {
        enterOuterAlt(_localctx, 1);
        setState(335);
        match(SQLParser::T__49);
        break;
      }

      case SQLParser::Count:
      case SQLParser::Average:
      case SQLParser::Max:
      case SQLParser::Min:
      case SQLParser::Sum:
      case SQLParser::Identifier: {
        enterOuterAlt(_localctx, 2);
        setState(336);
        selector();
        setState(341);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SQLParser::T__40) {
          setState(337);
          match(SQLParser::T__40);
          setState(338);
          selector();
          setState(343);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectorContext ------------------------------------------------------------------

SQLParser::SelectorContext::SelectorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SQLParser::ColumnContext* SQLParser::SelectorContext::column() {
  return getRuleContext<SQLParser::ColumnContext>(0);
}

SQLParser::AggregatorContext* SQLParser::SelectorContext::aggregator() {
  return getRuleContext<SQLParser::AggregatorContext>(0);
}

tree::TerminalNode* SQLParser::SelectorContext::Count() {
  return getToken(SQLParser::Count, 0);
}


size_t SQLParser::SelectorContext::getRuleIndex() const {
  return SQLParser::RuleSelector;
}


antlrcpp::Any SQLParser::SelectorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitSelector(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::SelectorContext* SQLParser::selector() {
  SelectorContext *_localctx = _tracker.createInstance<SelectorContext>(_ctx, getState());
  enterRule(_localctx, 38, SQLParser::RuleSelector);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(356);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 23, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(346);
      column();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(347);
      aggregator();
      setState(348);
      match(SQLParser::T__17);
      setState(349);
      column();
      setState(350);
      match(SQLParser::T__18);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(352);
      match(SQLParser::Count);
      setState(353);
      match(SQLParser::T__17);
      setState(354);
      match(SQLParser::T__49);
      setState(355);
      match(SQLParser::T__18);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifiersContext ------------------------------------------------------------------

SQLParser::IdentifiersContext::IdentifiersContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> SQLParser::IdentifiersContext::Identifier() {
  return getTokens(SQLParser::Identifier);
}

tree::TerminalNode* SQLParser::IdentifiersContext::Identifier(size_t i) {
  return getToken(SQLParser::Identifier, i);
}


size_t SQLParser::IdentifiersContext::getRuleIndex() const {
  return SQLParser::RuleIdentifiers;
}


antlrcpp::Any SQLParser::IdentifiersContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitIdentifiers(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::IdentifiersContext* SQLParser::identifiers() {
  IdentifiersContext *_localctx = _tracker.createInstance<IdentifiersContext>(_ctx, getState());
  enterRule(_localctx, 40, SQLParser::RuleIdentifiers);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(358);
    match(SQLParser::Identifier);
    setState(363);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SQLParser::T__40) {
      setState(359);
      match(SQLParser::T__40);
      setState(360);
      match(SQLParser::Identifier);
      setState(365);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OperateContext ------------------------------------------------------------------

SQLParser::OperateContext::OperateContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SQLParser::OperateContext::EqualOrAssign() {
  return getToken(SQLParser::EqualOrAssign, 0);
}

tree::TerminalNode* SQLParser::OperateContext::Less() {
  return getToken(SQLParser::Less, 0);
}

tree::TerminalNode* SQLParser::OperateContext::LessEqual() {
  return getToken(SQLParser::LessEqual, 0);
}

tree::TerminalNode* SQLParser::OperateContext::Greater() {
  return getToken(SQLParser::Greater, 0);
}

tree::TerminalNode* SQLParser::OperateContext::GreaterEqual() {
  return getToken(SQLParser::GreaterEqual, 0);
}

tree::TerminalNode* SQLParser::OperateContext::NotEqual() {
  return getToken(SQLParser::NotEqual, 0);
}


size_t SQLParser::OperateContext::getRuleIndex() const {
  return SQLParser::RuleOperate;
}


antlrcpp::Any SQLParser::OperateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitOperate(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::OperateContext* SQLParser::operate() {
  OperateContext *_localctx = _tracker.createInstance<OperateContext>(_ctx, getState());
  enterRule(_localctx, 42, SQLParser::RuleOperate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(366);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SQLParser::EqualOrAssign)
      | (1ULL << SQLParser::Less)
      | (1ULL << SQLParser::LessEqual)
      | (1ULL << SQLParser::Greater)
      | (1ULL << SQLParser::GreaterEqual)
      | (1ULL << SQLParser::NotEqual))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AggregatorContext ------------------------------------------------------------------

SQLParser::AggregatorContext::AggregatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SQLParser::AggregatorContext::Count() {
  return getToken(SQLParser::Count, 0);
}

tree::TerminalNode* SQLParser::AggregatorContext::Average() {
  return getToken(SQLParser::Average, 0);
}

tree::TerminalNode* SQLParser::AggregatorContext::Max() {
  return getToken(SQLParser::Max, 0);
}

tree::TerminalNode* SQLParser::AggregatorContext::Min() {
  return getToken(SQLParser::Min, 0);
}

tree::TerminalNode* SQLParser::AggregatorContext::Sum() {
  return getToken(SQLParser::Sum, 0);
}


size_t SQLParser::AggregatorContext::getRuleIndex() const {
  return SQLParser::RuleAggregator;
}


antlrcpp::Any SQLParser::AggregatorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SQLVisitor*>(visitor))
    return parserVisitor->visitAggregator(this);
  else
    return visitor->visitChildren(this);
}

SQLParser::AggregatorContext* SQLParser::aggregator() {
  AggregatorContext *_localctx = _tracker.createInstance<AggregatorContext>(_ctx, getState());
  enterRule(_localctx, 44, SQLParser::RuleAggregator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(368);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SQLParser::Count)
      | (1ULL << SQLParser::Average)
      | (1ULL << SQLParser::Max)
      | (1ULL << SQLParser::Min)
      | (1ULL << SQLParser::Sum))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

// Static vars and initialization.
std::vector<dfa::DFA> SQLParser::_decisionToDFA;
atn::PredictionContextCache SQLParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN SQLParser::_atn;
std::vector<uint16_t> SQLParser::_serializedATN;

std::vector<std::string> SQLParser::_ruleNames = {
  "program", "statement", "db_statement", "io_statement", "table_statement", 
  "select_table", "alter_statement", "field_list", "field", "type_", "value_lists", 
  "value_list", "value", "where_and_clause", "where_clause", "column", "expression", 
  "set_clause", "selectors", "selector", "identifiers", "operate", "aggregator"
};

std::vector<std::string> SQLParser::_literalNames = {
  "", "';'", "'CREATE'", "'DATABASE'", "'DROP'", "'SHOW'", "'DATABASES'", 
  "'USE'", "'TABLES'", "'INDEXES'", "'LOAD'", "'DATA'", "'INFILE'", "'INTO'", 
  "'TABLE'", "'STORE'", "'OUTFILE'", "'FROM'", "'('", "')'", "'DESC'", "'INSERT'", 
  "'VALUES'", "'DELETE'", "'WHERE'", "'UPDATE'", "'SET'", "'SELECT'", "'GROUP'", 
  "'BY'", "'LIMIT'", "'OFFSET'", "'ALTER'", "'ADD'", "'INDEX'", "'PRIMARY'", 
  "'KEY'", "'FOREIGN'", "'CONSTRAINT'", "'REFERENCES'", "'UNIQUE'", "','", 
  "'NOT'", "'DEFAULT'", "'INT'", "'VARCHAR'", "'FLOAT'", "'DATE'", "'AND'", 
  "'.'", "'*'", "'='", "'<'", "'<='", "'>'", "'>='", "'<>'", "'COUNT'", 
  "'AVG'", "'MAX'", "'MIN'", "'SUM'", "'NULL'"
};

std::vector<std::string> SQLParser::_symbolicNames = {
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "EqualOrAssign", 
  "Less", "LessEqual", "Greater", "GreaterEqual", "NotEqual", "Count", "Average", 
  "Max", "Min", "Sum", "Null", "Identifier", "Integer", "Date", "String", 
  "Float", "Whitespace", "Annotation"
};

dfa::Vocabulary SQLParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> SQLParser::_tokenNames;

SQLParser::Initializer::Initializer() {
	for (size_t i = 0; i < _symbolicNames.size(); ++i) {
		std::string name = _vocabulary.getLiteralName(i);
		if (name.empty()) {
			name = _vocabulary.getSymbolicName(i);
		}

		if (name.empty()) {
			_tokenNames.push_back("<INVALID>");
		} else {
      _tokenNames.push_back(name);
    }
	}

  static const uint16_t serializedATNSegment0[] = {
    0x3, 0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 
       0x3, 0x47, 0x175, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
       0x9, 0x4, 0x4, 0x5, 0x9, 0x5, 0x4, 0x6, 0x9, 0x6, 0x4, 0x7, 0x9, 
       0x7, 0x4, 0x8, 0x9, 0x8, 0x4, 0x9, 0x9, 0x9, 0x4, 0xa, 0x9, 0xa, 
       0x4, 0xb, 0x9, 0xb, 0x4, 0xc, 0x9, 0xc, 0x4, 0xd, 0x9, 0xd, 0x4, 
       0xe, 0x9, 0xe, 0x4, 0xf, 0x9, 0xf, 0x4, 0x10, 0x9, 0x10, 0x4, 0x11, 
       0x9, 0x11, 0x4, 0x12, 0x9, 0x12, 0x4, 0x13, 0x9, 0x13, 0x4, 0x14, 
       0x9, 0x14, 0x4, 0x15, 0x9, 0x15, 0x4, 0x16, 0x9, 0x16, 0x4, 0x17, 
       0x9, 0x17, 0x4, 0x18, 0x9, 0x18, 0x3, 0x2, 0x7, 0x2, 0x32, 0xa, 0x2, 
       0xc, 0x2, 0xe, 0x2, 0x35, 0xb, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x3, 
       0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
       0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
       0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0x49, 0xa, 0x3, 0x3, 0x4, 0x3, 0x4, 
       0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 
       0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x5, 0x4, 
       0x59, 0xa, 0x4, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
       0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 
       0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x69, 0xa, 0x5, 0x3, 0x6, 0x3, 
       0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 
       0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 
       0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 
       0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 
       0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0x89, 0xa, 0x6, 0x3, 0x7, 0x3, 
       0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x91, 0xa, 
       0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x96, 0xa, 0x7, 0x3, 
       0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x9c, 0xa, 0x7, 0x5, 
       0x7, 0x9e, 0xa, 0x7, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
       0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 
       0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
       0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 
       0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 0xbc, 0xa, 0x8, 
       0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
       0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 
       0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
       0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 
       0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
       0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 
       0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
       0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 0xee, 0xa, 0x8, 0x3, 0x9, 0x3, 
       0x9, 0x3, 0x9, 0x7, 0x9, 0xf3, 0xa, 0x9, 0xc, 0x9, 0xe, 0x9, 0xf6, 
       0xb, 0x9, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0xfc, 
       0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x100, 0xa, 0xa, 0x3, 0xa, 
       0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 
       0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 
       0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x112, 0xa, 0xa, 0x3, 0xb, 0x3, 0xb, 
       0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 0x11b, 
       0xa, 0xb, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x7, 0xc, 0x120, 0xa, 0xc, 
       0xc, 0xc, 0xe, 0xc, 0x123, 0xb, 0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 
       0x3, 0xd, 0x7, 0xd, 0x129, 0xa, 0xd, 0xc, 0xd, 0xe, 0xd, 0x12c, 0xb, 
       0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xe, 0x3, 0xe, 0x3, 0xf, 0x3, 0xf, 
       0x3, 0xf, 0x7, 0xf, 0x135, 0xa, 0xf, 0xc, 0xf, 0xe, 0xf, 0x138, 0xb, 
       0xf, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x11, 0x3, 
       0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x12, 0x3, 0x12, 0x5, 0x12, 0x144, 
       0xa, 0x12, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 
       0x3, 0x13, 0x3, 0x13, 0x7, 0x13, 0x14d, 0xa, 0x13, 0xc, 0x13, 0xe, 
       0x13, 0x150, 0xb, 0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 
       0x7, 0x14, 0x156, 0xa, 0x14, 0xc, 0x14, 0xe, 0x14, 0x159, 0xb, 0x14, 
       0x5, 0x14, 0x15b, 0xa, 0x14, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 
       0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 
       0x15, 0x5, 0x15, 0x167, 0xa, 0x15, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 
       0x7, 0x16, 0x16c, 0xa, 0x16, 0xc, 0x16, 0xe, 0x16, 0x16f, 0xb, 0x16, 
       0x3, 0x17, 0x3, 0x17, 0x3, 0x18, 0x3, 0x18, 0x3, 0x18, 0x2, 0x2, 
       0x19, 0x2, 0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 
       0x18, 0x1a, 0x1c, 0x1e, 0x20, 0x22, 0x24, 0x26, 0x28, 0x2a, 0x2c, 
       0x2e, 0x2, 0x5, 0x4, 0x2, 0x40, 0x40, 0x42, 0x45, 0x3, 0x2, 0x35, 
       0x3a, 0x3, 0x2, 0x3b, 0x3f, 0x2, 0x18c, 0x2, 0x33, 0x3, 0x2, 0x2, 
       0x2, 0x4, 0x48, 0x3, 0x2, 0x2, 0x2, 0x6, 0x58, 0x3, 0x2, 0x2, 0x2, 
       0x8, 0x68, 0x3, 0x2, 0x2, 0x2, 0xa, 0x88, 0x3, 0x2, 0x2, 0x2, 0xc, 
       0x8a, 0x3, 0x2, 0x2, 0x2, 0xe, 0xed, 0x3, 0x2, 0x2, 0x2, 0x10, 0xef, 
       0x3, 0x2, 0x2, 0x2, 0x12, 0x111, 0x3, 0x2, 0x2, 0x2, 0x14, 0x11a, 
       0x3, 0x2, 0x2, 0x2, 0x16, 0x11c, 0x3, 0x2, 0x2, 0x2, 0x18, 0x124, 
       0x3, 0x2, 0x2, 0x2, 0x1a, 0x12f, 0x3, 0x2, 0x2, 0x2, 0x1c, 0x131, 
       0x3, 0x2, 0x2, 0x2, 0x1e, 0x139, 0x3, 0x2, 0x2, 0x2, 0x20, 0x13d, 
       0x3, 0x2, 0x2, 0x2, 0x22, 0x143, 0x3, 0x2, 0x2, 0x2, 0x24, 0x145, 
       0x3, 0x2, 0x2, 0x2, 0x26, 0x15a, 0x3, 0x2, 0x2, 0x2, 0x28, 0x166, 
       0x3, 0x2, 0x2, 0x2, 0x2a, 0x168, 0x3, 0x2, 0x2, 0x2, 0x2c, 0x170, 
       0x3, 0x2, 0x2, 0x2, 0x2e, 0x172, 0x3, 0x2, 0x2, 0x2, 0x30, 0x32, 
       0x5, 0x4, 0x3, 0x2, 0x31, 0x30, 0x3, 0x2, 0x2, 0x2, 0x32, 0x35, 0x3, 
       0x2, 0x2, 0x2, 0x33, 0x31, 0x3, 0x2, 0x2, 0x2, 0x33, 0x34, 0x3, 0x2, 
       0x2, 0x2, 0x34, 0x36, 0x3, 0x2, 0x2, 0x2, 0x35, 0x33, 0x3, 0x2, 0x2, 
       0x2, 0x36, 0x37, 0x7, 0x2, 0x2, 0x3, 0x37, 0x3, 0x3, 0x2, 0x2, 0x2, 
       0x38, 0x39, 0x5, 0x6, 0x4, 0x2, 0x39, 0x3a, 0x7, 0x3, 0x2, 0x2, 0x3a, 
       0x49, 0x3, 0x2, 0x2, 0x2, 0x3b, 0x3c, 0x5, 0x8, 0x5, 0x2, 0x3c, 0x3d, 
       0x7, 0x3, 0x2, 0x2, 0x3d, 0x49, 0x3, 0x2, 0x2, 0x2, 0x3e, 0x3f, 0x5, 
       0xa, 0x6, 0x2, 0x3f, 0x40, 0x7, 0x3, 0x2, 0x2, 0x40, 0x49, 0x3, 0x2, 
       0x2, 0x2, 0x41, 0x42, 0x5, 0xe, 0x8, 0x2, 0x42, 0x43, 0x7, 0x3, 0x2, 
       0x2, 0x43, 0x49, 0x3, 0x2, 0x2, 0x2, 0x44, 0x45, 0x7, 0x47, 0x2, 
       0x2, 0x45, 0x49, 0x7, 0x3, 0x2, 0x2, 0x46, 0x47, 0x7, 0x40, 0x2, 
       0x2, 0x47, 0x49, 0x7, 0x3, 0x2, 0x2, 0x48, 0x38, 0x3, 0x2, 0x2, 0x2, 
       0x48, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x48, 0x3e, 0x3, 0x2, 0x2, 0x2, 0x48, 
       0x41, 0x3, 0x2, 0x2, 0x2, 0x48, 0x44, 0x3, 0x2, 0x2, 0x2, 0x48, 0x46, 
       0x3, 0x2, 0x2, 0x2, 0x49, 0x5, 0x3, 0x2, 0x2, 0x2, 0x4a, 0x4b, 0x7, 
       0x4, 0x2, 0x2, 0x4b, 0x4c, 0x7, 0x5, 0x2, 0x2, 0x4c, 0x59, 0x7, 0x41, 
       0x2, 0x2, 0x4d, 0x4e, 0x7, 0x6, 0x2, 0x2, 0x4e, 0x4f, 0x7, 0x5, 0x2, 
       0x2, 0x4f, 0x59, 0x7, 0x41, 0x2, 0x2, 0x50, 0x51, 0x7, 0x7, 0x2, 
       0x2, 0x51, 0x59, 0x7, 0x8, 0x2, 0x2, 0x52, 0x53, 0x7, 0x9, 0x2, 0x2, 
       0x53, 0x59, 0x7, 0x41, 0x2, 0x2, 0x54, 0x55, 0x7, 0x7, 0x2, 0x2, 
       0x55, 0x59, 0x7, 0xa, 0x2, 0x2, 0x56, 0x57, 0x7, 0x7, 0x2, 0x2, 0x57, 
       0x59, 0x7, 0xb, 0x2, 0x2, 0x58, 0x4a, 0x3, 0x2, 0x2, 0x2, 0x58, 0x4d, 
       0x3, 0x2, 0x2, 0x2, 0x58, 0x50, 0x3, 0x2, 0x2, 0x2, 0x58, 0x52, 0x3, 
       0x2, 0x2, 0x2, 0x58, 0x54, 0x3, 0x2, 0x2, 0x2, 0x58, 0x56, 0x3, 0x2, 
       0x2, 0x2, 0x59, 0x7, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x5b, 0x7, 0xc, 0x2, 
       0x2, 0x5b, 0x5c, 0x7, 0xd, 0x2, 0x2, 0x5c, 0x5d, 0x7, 0xe, 0x2, 0x2, 
       0x5d, 0x5e, 0x7, 0x44, 0x2, 0x2, 0x5e, 0x5f, 0x7, 0xf, 0x2, 0x2, 
       0x5f, 0x60, 0x7, 0x10, 0x2, 0x2, 0x60, 0x69, 0x7, 0x41, 0x2, 0x2, 
       0x61, 0x62, 0x7, 0x11, 0x2, 0x2, 0x62, 0x63, 0x7, 0xd, 0x2, 0x2, 
       0x63, 0x64, 0x7, 0x12, 0x2, 0x2, 0x64, 0x65, 0x7, 0x44, 0x2, 0x2, 
       0x65, 0x66, 0x7, 0x13, 0x2, 0x2, 0x66, 0x67, 0x7, 0x10, 0x2, 0x2, 
       0x67, 0x69, 0x7, 0x41, 0x2, 0x2, 0x68, 0x5a, 0x3, 0x2, 0x2, 0x2, 
       0x68, 0x61, 0x3, 0x2, 0x2, 0x2, 0x69, 0x9, 0x3, 0x2, 0x2, 0x2, 0x6a, 
       0x6b, 0x7, 0x4, 0x2, 0x2, 0x6b, 0x6c, 0x7, 0x10, 0x2, 0x2, 0x6c, 
       0x6d, 0x7, 0x41, 0x2, 0x2, 0x6d, 0x6e, 0x7, 0x14, 0x2, 0x2, 0x6e, 
       0x6f, 0x5, 0x10, 0x9, 0x2, 0x6f, 0x70, 0x7, 0x15, 0x2, 0x2, 0x70, 
       0x89, 0x3, 0x2, 0x2, 0x2, 0x71, 0x72, 0x7, 0x6, 0x2, 0x2, 0x72, 0x73, 
       0x7, 0x10, 0x2, 0x2, 0x73, 0x89, 0x7, 0x41, 0x2, 0x2, 0x74, 0x75, 
       0x7, 0x16, 0x2, 0x2, 0x75, 0x89, 0x7, 0x41, 0x2, 0x2, 0x76, 0x77, 
       0x7, 0x17, 0x2, 0x2, 0x77, 0x78, 0x7, 0xf, 0x2, 0x2, 0x78, 0x79, 
       0x7, 0x41, 0x2, 0x2, 0x79, 0x7a, 0x7, 0x18, 0x2, 0x2, 0x7a, 0x89, 
       0x5, 0x16, 0xc, 0x2, 0x7b, 0x7c, 0x7, 0x19, 0x2, 0x2, 0x7c, 0x7d, 
       0x7, 0x13, 0x2, 0x2, 0x7d, 0x7e, 0x7, 0x41, 0x2, 0x2, 0x7e, 0x7f, 
       0x7, 0x1a, 0x2, 0x2, 0x7f, 0x89, 0x5, 0x1c, 0xf, 0x2, 0x80, 0x81, 
       0x7, 0x1b, 0x2, 0x2, 0x81, 0x82, 0x7, 0x41, 0x2, 0x2, 0x82, 0x83, 
       0x7, 0x1c, 0x2, 0x2, 0x83, 0x84, 0x5, 0x24, 0x13, 0x2, 0x84, 0x85, 
       0x7, 0x1a, 0x2, 0x2, 0x85, 0x86, 0x5, 0x1c, 0xf, 0x2, 0x86, 0x89, 
       0x3, 0x2, 0x2, 0x2, 0x87, 0x89, 0x5, 0xc, 0x7, 0x2, 0x88, 0x6a, 0x3, 
       0x2, 0x2, 0x2, 0x88, 0x71, 0x3, 0x2, 0x2, 0x2, 0x88, 0x74, 0x3, 0x2, 
       0x2, 0x2, 0x88, 0x76, 0x3, 0x2, 0x2, 0x2, 0x88, 0x7b, 0x3, 0x2, 0x2, 
       0x2, 0x88, 0x80, 0x3, 0x2, 0x2, 0x2, 0x88, 0x87, 0x3, 0x2, 0x2, 0x2, 
       0x89, 0xb, 0x3, 0x2, 0x2, 0x2, 0x8a, 0x8b, 0x7, 0x1d, 0x2, 0x2, 0x8b, 
       0x8c, 0x5, 0x26, 0x14, 0x2, 0x8c, 0x8d, 0x7, 0x13, 0x2, 0x2, 0x8d, 
       0x90, 0x5, 0x2a, 0x16, 0x2, 0x8e, 0x8f, 0x7, 0x1a, 0x2, 0x2, 0x8f, 
       0x91, 0x5, 0x1c, 0xf, 0x2, 0x90, 0x8e, 0x3, 0x2, 0x2, 0x2, 0x90, 
       0x91, 0x3, 0x2, 0x2, 0x2, 0x91, 0x95, 0x3, 0x2, 0x2, 0x2, 0x92, 0x93, 
       0x7, 0x1e, 0x2, 0x2, 0x93, 0x94, 0x7, 0x1f, 0x2, 0x2, 0x94, 0x96, 
       0x5, 0x20, 0x11, 0x2, 0x95, 0x92, 0x3, 0x2, 0x2, 0x2, 0x95, 0x96, 
       0x3, 0x2, 0x2, 0x2, 0x96, 0x9d, 0x3, 0x2, 0x2, 0x2, 0x97, 0x98, 0x7, 
       0x20, 0x2, 0x2, 0x98, 0x9b, 0x7, 0x42, 0x2, 0x2, 0x99, 0x9a, 0x7, 
       0x21, 0x2, 0x2, 0x9a, 0x9c, 0x7, 0x42, 0x2, 0x2, 0x9b, 0x99, 0x3, 
       0x2, 0x2, 0x2, 0x9b, 0x9c, 0x3, 0x2, 0x2, 0x2, 0x9c, 0x9e, 0x3, 0x2, 
       0x2, 0x2, 0x9d, 0x97, 0x3, 0x2, 0x2, 0x2, 0x9d, 0x9e, 0x3, 0x2, 0x2, 
       0x2, 0x9e, 0xd, 0x3, 0x2, 0x2, 0x2, 0x9f, 0xa0, 0x7, 0x22, 0x2, 0x2, 
       0xa0, 0xa1, 0x7, 0x10, 0x2, 0x2, 0xa1, 0xa2, 0x7, 0x41, 0x2, 0x2, 
       0xa2, 0xa3, 0x7, 0x23, 0x2, 0x2, 0xa3, 0xa4, 0x7, 0x24, 0x2, 0x2, 
       0xa4, 0xa5, 0x7, 0x14, 0x2, 0x2, 0xa5, 0xa6, 0x5, 0x2a, 0x16, 0x2, 
       0xa6, 0xa7, 0x7, 0x15, 0x2, 0x2, 0xa7, 0xee, 0x3, 0x2, 0x2, 0x2, 
       0xa8, 0xa9, 0x7, 0x22, 0x2, 0x2, 0xa9, 0xaa, 0x7, 0x10, 0x2, 0x2, 
       0xaa, 0xab, 0x7, 0x41, 0x2, 0x2, 0xab, 0xac, 0x7, 0x6, 0x2, 0x2, 
       0xac, 0xad, 0x7, 0x24, 0x2, 0x2, 0xad, 0xae, 0x7, 0x14, 0x2, 0x2, 
       0xae, 0xaf, 0x5, 0x2a, 0x16, 0x2, 0xaf, 0xb0, 0x7, 0x15, 0x2, 0x2, 
       0xb0, 0xee, 0x3, 0x2, 0x2, 0x2, 0xb1, 0xb2, 0x7, 0x22, 0x2, 0x2, 
       0xb2, 0xb3, 0x7, 0x10, 0x2, 0x2, 0xb3, 0xb4, 0x7, 0x41, 0x2, 0x2, 
       0xb4, 0xb5, 0x7, 0x6, 0x2, 0x2, 0xb5, 0xb6, 0x7, 0x25, 0x2, 0x2, 
       0xb6, 0xbb, 0x7, 0x26, 0x2, 0x2, 0xb7, 0xb8, 0x7, 0x14, 0x2, 0x2, 
       0xb8, 0xb9, 0x5, 0x2a, 0x16, 0x2, 0xb9, 0xba, 0x7, 0x15, 0x2, 0x2, 
       0xba, 0xbc, 0x3, 0x2, 0x2, 0x2, 0xbb, 0xb7, 0x3, 0x2, 0x2, 0x2, 0xbb, 
       0xbc, 0x3, 0x2, 0x2, 0x2, 0xbc, 0xee, 0x3, 0x2, 0x2, 0x2, 0xbd, 0xbe, 
       0x7, 0x22, 0x2, 0x2, 0xbe, 0xbf, 0x7, 0x10, 0x2, 0x2, 0xbf, 0xc0, 
       0x7, 0x41, 0x2, 0x2, 0xc0, 0xc1, 0x7, 0x6, 0x2, 0x2, 0xc1, 0xc2, 
       0x7, 0x27, 0x2, 0x2, 0xc2, 0xc3, 0x7, 0x26, 0x2, 0x2, 0xc3, 0xc4, 
       0x7, 0x14, 0x2, 0x2, 0xc4, 0xc5, 0x5, 0x2a, 0x16, 0x2, 0xc5, 0xc6, 
       0x7, 0x15, 0x2, 0x2, 0xc6, 0xee, 0x3, 0x2, 0x2, 0x2, 0xc7, 0xc8, 
       0x7, 0x22, 0x2, 0x2, 0xc8, 0xc9, 0x7, 0x10, 0x2, 0x2, 0xc9, 0xca, 
       0x7, 0x41, 0x2, 0x2, 0xca, 0xcb, 0x7, 0x23, 0x2, 0x2, 0xcb, 0xcc, 
       0x7, 0x28, 0x2, 0x2, 0xcc, 0xcd, 0x7, 0x41, 0x2, 0x2, 0xcd, 0xce, 
       0x7, 0x25, 0x2, 0x2, 0xce, 0xcf, 0x7, 0x26, 0x2, 0x2, 0xcf, 0xd0, 
       0x7, 0x14, 0x2, 0x2, 0xd0, 0xd1, 0x5, 0x2a, 0x16, 0x2, 0xd1, 0xd2, 
       0x7, 0x15, 0x2, 0x2, 0xd2, 0xee, 0x3, 0x2, 0x2, 0x2, 0xd3, 0xd4, 
       0x7, 0x22, 0x2, 0x2, 0xd4, 0xd5, 0x7, 0x10, 0x2, 0x2, 0xd5, 0xd6, 
       0x7, 0x41, 0x2, 0x2, 0xd6, 0xd7, 0x7, 0x23, 0x2, 0x2, 0xd7, 0xd8, 
       0x7, 0x28, 0x2, 0x2, 0xd8, 0xd9, 0x7, 0x41, 0x2, 0x2, 0xd9, 0xda, 
       0x7, 0x27, 0x2, 0x2, 0xda, 0xdb, 0x7, 0x26, 0x2, 0x2, 0xdb, 0xdc, 
       0x7, 0x14, 0x2, 0x2, 0xdc, 0xdd, 0x5, 0x2a, 0x16, 0x2, 0xdd, 0xde, 
       0x7, 0x15, 0x2, 0x2, 0xde, 0xdf, 0x7, 0x29, 0x2, 0x2, 0xdf, 0xe0, 
       0x7, 0x41, 0x2, 0x2, 0xe0, 0xe1, 0x7, 0x14, 0x2, 0x2, 0xe1, 0xe2, 
       0x5, 0x2a, 0x16, 0x2, 0xe2, 0xe3, 0x7, 0x15, 0x2, 0x2, 0xe3, 0xee, 
       0x3, 0x2, 0x2, 0x2, 0xe4, 0xe5, 0x7, 0x22, 0x2, 0x2, 0xe5, 0xe6, 
       0x7, 0x10, 0x2, 0x2, 0xe6, 0xe7, 0x7, 0x41, 0x2, 0x2, 0xe7, 0xe8, 
       0x7, 0x23, 0x2, 0x2, 0xe8, 0xe9, 0x7, 0x2a, 0x2, 0x2, 0xe9, 0xea, 
       0x7, 0x14, 0x2, 0x2, 0xea, 0xeb, 0x5, 0x2a, 0x16, 0x2, 0xeb, 0xec, 
       0x7, 0x15, 0x2, 0x2, 0xec, 0xee, 0x3, 0x2, 0x2, 0x2, 0xed, 0x9f, 
       0x3, 0x2, 0x2, 0x2, 0xed, 0xa8, 0x3, 0x2, 0x2, 0x2, 0xed, 0xb1, 0x3, 
       0x2, 0x2, 0x2, 0xed, 0xbd, 0x3, 0x2, 0x2, 0x2, 0xed, 0xc7, 0x3, 0x2, 
       0x2, 0x2, 0xed, 0xd3, 0x3, 0x2, 0x2, 0x2, 0xed, 0xe4, 0x3, 0x2, 0x2, 
       0x2, 0xee, 0xf, 0x3, 0x2, 0x2, 0x2, 0xef, 0xf4, 0x5, 0x12, 0xa, 0x2, 
       0xf0, 0xf1, 0x7, 0x2b, 0x2, 0x2, 0xf1, 0xf3, 0x5, 0x12, 0xa, 0x2, 
       0xf2, 0xf0, 0x3, 0x2, 0x2, 0x2, 0xf3, 0xf6, 0x3, 0x2, 0x2, 0x2, 0xf4, 
       0xf2, 0x3, 0x2, 0x2, 0x2, 0xf4, 0xf5, 0x3, 0x2, 0x2, 0x2, 0xf5, 0x11, 
       0x3, 0x2, 0x2, 0x2, 0xf6, 0xf4, 0x3, 0x2, 0x2, 0x2, 0xf7, 0xf8, 0x7, 
       0x41, 0x2, 0x2, 0xf8, 0xfb, 0x5, 0x14, 0xb, 0x2, 0xf9, 0xfa, 0x7, 
       0x2c, 0x2, 0x2, 0xfa, 0xfc, 0x7, 0x40, 0x2, 0x2, 0xfb, 0xf9, 0x3, 
       0x2, 0x2, 0x2, 0xfb, 0xfc, 0x3, 0x2, 0x2, 0x2, 0xfc, 0xff, 0x3, 0x2, 
       0x2, 0x2, 0xfd, 0xfe, 0x7, 0x2d, 0x2, 0x2, 0xfe, 0x100, 0x5, 0x1a, 
       0xe, 0x2, 0xff, 0xfd, 0x3, 0x2, 0x2, 0x2, 0xff, 0x100, 0x3, 0x2, 
       0x2, 0x2, 0x100, 0x112, 0x3, 0x2, 0x2, 0x2, 0x101, 0x102, 0x7, 0x25, 
       0x2, 0x2, 0x102, 0x103, 0x7, 0x26, 0x2, 0x2, 0x103, 0x104, 0x7, 0x14, 
       0x2, 0x2, 0x104, 0x105, 0x5, 0x2a, 0x16, 0x2, 0x105, 0x106, 0x7, 
       0x15, 0x2, 0x2, 0x106, 0x112, 0x3, 0x2, 0x2, 0x2, 0x107, 0x108, 0x7, 
       0x27, 0x2, 0x2, 0x108, 0x109, 0x7, 0x26, 0x2, 0x2, 0x109, 0x10a, 
       0x7, 0x14, 0x2, 0x2, 0x10a, 0x10b, 0x7, 0x41, 0x2, 0x2, 0x10b, 0x10c, 
       0x7, 0x15, 0x2, 0x2, 0x10c, 0x10d, 0x7, 0x29, 0x2, 0x2, 0x10d, 0x10e, 
       0x7, 0x41, 0x2, 0x2, 0x10e, 0x10f, 0x7, 0x14, 0x2, 0x2, 0x10f, 0x110, 
       0x7, 0x41, 0x2, 0x2, 0x110, 0x112, 0x7, 0x15, 0x2, 0x2, 0x111, 0xf7, 
       0x3, 0x2, 0x2, 0x2, 0x111, 0x101, 0x3, 0x2, 0x2, 0x2, 0x111, 0x107, 
       0x3, 0x2, 0x2, 0x2, 0x112, 0x13, 0x3, 0x2, 0x2, 0x2, 0x113, 0x11b, 
       0x7, 0x2e, 0x2, 0x2, 0x114, 0x115, 0x7, 0x2f, 0x2, 0x2, 0x115, 0x116, 
       0x7, 0x14, 0x2, 0x2, 0x116, 0x117, 0x7, 0x42, 0x2, 0x2, 0x117, 0x11b, 
       0x7, 0x15, 0x2, 0x2, 0x118, 0x11b, 0x7, 0x30, 0x2, 0x2, 0x119, 0x11b, 
       0x7, 0x31, 0x2, 0x2, 0x11a, 0x113, 0x3, 0x2, 0x2, 0x2, 0x11a, 0x114, 
       0x3, 0x2, 0x2, 0x2, 0x11a, 0x118, 0x3, 0x2, 0x2, 0x2, 0x11a, 0x119, 
       0x3, 0x2, 0x2, 0x2, 0x11b, 0x15, 0x3, 0x2, 0x2, 0x2, 0x11c, 0x121, 
       0x5, 0x18, 0xd, 0x2, 0x11d, 0x11e, 0x7, 0x2b, 0x2, 0x2, 0x11e, 0x120, 
       0x5, 0x18, 0xd, 0x2, 0x11f, 0x11d, 0x3, 0x2, 0x2, 0x2, 0x120, 0x123, 
       0x3, 0x2, 0x2, 0x2, 0x121, 0x11f, 0x3, 0x2, 0x2, 0x2, 0x121, 0x122, 
       0x3, 0x2, 0x2, 0x2, 0x122, 0x17, 0x3, 0x2, 0x2, 0x2, 0x123, 0x121, 
       0x3, 0x2, 0x2, 0x2, 0x124, 0x125, 0x7, 0x14, 0x2, 0x2, 0x125, 0x12a, 
       0x5, 0x1a, 0xe, 0x2, 0x126, 0x127, 0x7, 0x2b, 0x2, 0x2, 0x127, 0x129, 
       0x5, 0x1a, 0xe, 0x2, 0x128, 0x126, 0x3, 0x2, 0x2, 0x2, 0x129, 0x12c, 
       0x3, 0x2, 0x2, 0x2, 0x12a, 0x128, 0x3, 0x2, 0x2, 0x2, 0x12a, 0x12b, 
       0x3, 0x2, 0x2, 0x2, 0x12b, 0x12d, 0x3, 0x2, 0x2, 0x2, 0x12c, 0x12a, 
       0x3, 0x2, 0x2, 0x2, 0x12d, 0x12e, 0x7, 0x15, 0x2, 0x2, 0x12e, 0x19, 
       0x3, 0x2, 0x2, 0x2, 0x12f, 0x130, 0x9, 0x2, 0x2, 0x2, 0x130, 0x1b, 
       0x3, 0x2, 0x2, 0x2, 0x131, 0x136, 0x5, 0x1e, 0x10, 0x2, 0x132, 0x133, 
       0x7, 0x32, 0x2, 0x2, 0x133, 0x135, 0x5, 0x1e, 0x10, 0x2, 0x134, 0x132, 
       0x3, 0x2, 0x2, 0x2, 0x135, 0x138, 0x3, 0x2, 0x2, 0x2, 0x136, 0x134, 
       0x3, 0x2, 0x2, 0x2, 0x136, 0x137, 0x3, 0x2, 0x2, 0x2, 0x137, 0x1d, 
       0x3, 0x2, 0x2, 0x2, 0x138, 0x136, 0x3, 0x2, 0x2, 0x2, 0x139, 0x13a, 
       0x5, 0x20, 0x11, 0x2, 0x13a, 0x13b, 0x5, 0x2c, 0x17, 0x2, 0x13b, 
       0x13c, 0x5, 0x22, 0x12, 0x2, 0x13c, 0x1f, 0x3, 0x2, 0x2, 0x2, 0x13d, 
       0x13e, 0x7, 0x41, 0x2, 0x2, 0x13e, 0x13f, 0x7, 0x33, 0x2, 0x2, 0x13f, 
       0x140, 0x7, 0x41, 0x2, 0x2, 0x140, 0x21, 0x3, 0x2, 0x2, 0x2, 0x141, 
       0x144, 0x5, 0x1a, 0xe, 0x2, 0x142, 0x144, 0x5, 0x20, 0x11, 0x2, 0x143, 
       0x141, 0x3, 0x2, 0x2, 0x2, 0x143, 0x142, 0x3, 0x2, 0x2, 0x2, 0x144, 
       0x23, 0x3, 0x2, 0x2, 0x2, 0x145, 0x146, 0x7, 0x41, 0x2, 0x2, 0x146, 
       0x147, 0x7, 0x35, 0x2, 0x2, 0x147, 0x14e, 0x5, 0x1a, 0xe, 0x2, 0x148, 
       0x149, 0x7, 0x2b, 0x2, 0x2, 0x149, 0x14a, 0x7, 0x41, 0x2, 0x2, 0x14a, 
       0x14b, 0x7, 0x35, 0x2, 0x2, 0x14b, 0x14d, 0x5, 0x1a, 0xe, 0x2, 0x14c, 
       0x148, 0x3, 0x2, 0x2, 0x2, 0x14d, 0x150, 0x3, 0x2, 0x2, 0x2, 0x14e, 
       0x14c, 0x3, 0x2, 0x2, 0x2, 0x14e, 0x14f, 0x3, 0x2, 0x2, 0x2, 0x14f, 
       0x25, 0x3, 0x2, 0x2, 0x2, 0x150, 0x14e, 0x3, 0x2, 0x2, 0x2, 0x151, 
       0x15b, 0x7, 0x34, 0x2, 0x2, 0x152, 0x157, 0x5, 0x28, 0x15, 0x2, 0x153, 
       0x154, 0x7, 0x2b, 0x2, 0x2, 0x154, 0x156, 0x5, 0x28, 0x15, 0x2, 0x155, 
       0x153, 0x3, 0x2, 0x2, 0x2, 0x156, 0x159, 0x3, 0x2, 0x2, 0x2, 0x157, 
       0x155, 0x3, 0x2, 0x2, 0x2, 0x157, 0x158, 0x3, 0x2, 0x2, 0x2, 0x158, 
       0x15b, 0x3, 0x2, 0x2, 0x2, 0x159, 0x157, 0x3, 0x2, 0x2, 0x2, 0x15a, 
       0x151, 0x3, 0x2, 0x2, 0x2, 0x15a, 0x152, 0x3, 0x2, 0x2, 0x2, 0x15b, 
       0x27, 0x3, 0x2, 0x2, 0x2, 0x15c, 0x167, 0x5, 0x20, 0x11, 0x2, 0x15d, 
       0x15e, 0x5, 0x2e, 0x18, 0x2, 0x15e, 0x15f, 0x7, 0x14, 0x2, 0x2, 0x15f, 
       0x160, 0x5, 0x20, 0x11, 0x2, 0x160, 0x161, 0x7, 0x15, 0x2, 0x2, 0x161, 
       0x167, 0x3, 0x2, 0x2, 0x2, 0x162, 0x163, 0x7, 0x3b, 0x2, 0x2, 0x163, 
       0x164, 0x7, 0x14, 0x2, 0x2, 0x164, 0x165, 0x7, 0x34, 0x2, 0x2, 0x165, 
       0x167, 0x7, 0x15, 0x2, 0x2, 0x166, 0x15c, 0x3, 0x2, 0x2, 0x2, 0x166, 
       0x15d, 0x3, 0x2, 0x2, 0x2, 0x166, 0x162, 0x3, 0x2, 0x2, 0x2, 0x167, 
       0x29, 0x3, 0x2, 0x2, 0x2, 0x168, 0x16d, 0x7, 0x41, 0x2, 0x2, 0x169, 
       0x16a, 0x7, 0x2b, 0x2, 0x2, 0x16a, 0x16c, 0x7, 0x41, 0x2, 0x2, 0x16b, 
       0x169, 0x3, 0x2, 0x2, 0x2, 0x16c, 0x16f, 0x3, 0x2, 0x2, 0x2, 0x16d, 
       0x16b, 0x3, 0x2, 0x2, 0x2, 0x16d, 0x16e, 0x3, 0x2, 0x2, 0x2, 0x16e, 
       0x2b, 0x3, 0x2, 0x2, 0x2, 0x16f, 0x16d, 0x3, 0x2, 0x2, 0x2, 0x170, 
       0x171, 0x9, 0x3, 0x2, 0x2, 0x171, 0x2d, 0x3, 0x2, 0x2, 0x2, 0x172, 
       0x173, 0x9, 0x4, 0x2, 0x2, 0x173, 0x2f, 0x3, 0x2, 0x2, 0x2, 0x1b, 
       0x33, 0x48, 0x58, 0x68, 0x88, 0x90, 0x95, 0x9b, 0x9d, 0xbb, 0xed, 
       0xf4, 0xfb, 0xff, 0x111, 0x11a, 0x121, 0x12a, 0x136, 0x143, 0x14e, 
       0x157, 0x15a, 0x166, 0x16d, 
  };

  _serializedATN.insert(_serializedATN.end(), serializedATNSegment0,
    serializedATNSegment0 + sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));


  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

SQLParser::Initializer SQLParser::_init;
