set(HEADERS
        ${HEADERS}
        ${CMAKE_CURRENT_SOURCE_DIR}/system_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/name_handler.h
        PARENT_SCOPE
        )

set(SOURCE
        ${SOURCE}
        ${CMAKE_CURRENT_SOURCE_DIR}/system_manager.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/name_handler.cpp
        PARENT_SCOPE
        )
