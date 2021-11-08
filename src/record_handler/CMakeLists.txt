set(HEADERS
        ${HEADERS}
        ${CMAKE_CURRENT_SOURCE_DIR}/record_handler.h
        ${CMAKE_CURRENT_SOURCE_DIR}/rid.h
        ${CMAKE_CURRENT_SOURCE_DIR}/file_header.h
        PARENT_SCOPE
        )

set(SOURCE
        ${SOURCE}
        ${CMAKE_CURRENT_SOURCE_DIR}/record_handler.cpp
        PARENT_SCOPE
        )
