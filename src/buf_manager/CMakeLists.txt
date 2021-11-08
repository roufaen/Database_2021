add_subdirectory(fileio)

set(HEADERS
        ${HEADERS}
        ${CMAKE_CURRENT_SOURCE_DIR}/buf_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/find_replace.h
        PARENT_SCOPE
        )
