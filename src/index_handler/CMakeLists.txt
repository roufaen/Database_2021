set(HEADERS
        ${HEADERS}
        ${CMAKE_CURRENT_SOURCE_DIR}/index_handler.h
        ${CMAKE_CURRENT_SOURCE_DIR}/index_file_handler.h
        ${CMAKE_CURRENT_SOURCE_DIR}/ix.h
        PARENT_SCOPE
        )

set(SOURCE
        ${SOURCE}
        ${CMAKE_CURRENT_SOURCE_DIR}/index_handler.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/index_scan.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/index_file_handler.cpp
        PARENT_SCOPE
        )
