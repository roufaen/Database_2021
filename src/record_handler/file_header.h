# ifndef FILE_HEADER_H
# define FILE_HEADER_H

struct FileHeader {
    int pageNum;
    int recordSize;
    int recordNum;
    int recordPerPage;
    int recordNumPageOffset;
    int slotMapOffset;
    int slotMapSize;
};

# endif
