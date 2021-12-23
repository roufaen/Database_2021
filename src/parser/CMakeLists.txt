set(LIBRARY_OUTPUT_PATH "${CMAKE_BINARY_DIR}")

add_subdirectory(generated)

set(SOURCE
        ${SOURCE}
        ${CMAKE_CURRENT_SOURCE_DIR}/MyParser.cpp
        PARENT_SCOPE
)
