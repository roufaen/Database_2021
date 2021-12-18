#include <string>
#include "antlr4-runtime.h"

#include "MyVisitor.h"
#include "SQLLexer.h"
using namespace antlr4;

auto parse(std::string sSQL, QueryManager* qm, RecordHandler* rh, IndexHandler* ih, SystemManager* sm);