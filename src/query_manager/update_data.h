# ifndef UPDATE_DATA_H
# define UPDATE_DATA_H

# include "./data.h"
# include "./table_header.h"

struct UpdateData {
    TableHeader header;
    Data data;
};

# endif
