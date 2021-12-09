// project(database2021)
// set(CMAKE_CXX_STANDARD 14)
add_subdirectory(generated)

include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/generated/
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/runtime/src
)

file(GLOB antlr4-cpp-src
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/runtime/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/runtime/src/atn/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/runtime/src/dfa/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/runtime/src/misc/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/runtime/src/support/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/runtime/src/tree/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/runtime/src/tree/pattern/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/runtime/src/tree/xpath/*.cpp
)

add_library (antlr4-cpp-runtime ${antlr4-cpp-src})
add_executable(SQLHelp ${src_dir} ${SOURCE} MyParser.cpp)
target_link_libraries(SQLHelp antlr4-cpp-runtime)