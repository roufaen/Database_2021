# ifndef NAME_HANDLER_H
# define NAME_HANDLER_H

# include "../buf_manager/buf_manager.h"
# include "../record_handler/record_handler.h"
# include <vector>
# include <string>

using namespace std;

class NameHandler {
public:
    NameHandler(BufManager *bufManager);
    NameHandler(BufManager *bufManager, string dbName);
    ~NameHandler();
    vector <string> getElementList();
    bool hasElement(string elementName);
    int createElement(string elementName);
    int dropElement(string elementName);

private:
    vector <string> readElementList();
    int writeElementList(vector <string> elementNameList);
    BufManager *bufManager;
    RecordHandler *recordHandler;
    vector <string> elementNameList;
    string dbName;
};

# endif
