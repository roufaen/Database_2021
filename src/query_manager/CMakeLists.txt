set(HEADERS
        ${HEADERS}
        ${CMAKE_CURRENT_SOURCE_DIR}/query_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/table.h
        ${CMAKE_CURRENT_SOURCE_DIR}/table_header.h
        ${CMAKE_CURRENT_SOURCE_DIR}/condition.h
        ${CMAKE_CURRENT_SOURCE_DIR}/data.h
        PARENT_SCOPE
        )

set(SOURCE
        ${SOURCE}
        ${CMAKE_CURRENT_SOURCE_DIR}/query_manager.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/table.cpp
        PARENT_SCOPE
        )
