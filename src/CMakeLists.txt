add_subdirectory(utils)
add_subdirectory(buf_manager)
add_subdirectory(record_handler)
add_subdirectory(index_handler)
add_subdirectory(query_manager)
add_subdirectory(system_manager)
add_subdirectory(parser)

add_definitions(-DDEBUG)
add_definitions("-Wall -g")

include_directories(
       ${CMAKE_CURRENT_SOURCE_DIR}/parser/
       ${CMAKE_CURRENT_SOURCE_DIR}/parser/generated/
       ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/
       ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/runtime/src
)

file(GLOB antlr4-cpp-src
   ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/runtime/src/*.cpp
   ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/runtime/src/atn/*.cpp
   ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/runtime/src/dfa/*.cpp
   ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/runtime/src/misc/*.cpp
   ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/runtime/src/support/*.cpp
   ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/runtime/src/tree/*.cpp
   ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/runtime/src/tree/pattern/*.cpp
   ${CMAKE_CURRENT_SOURCE_DIR}/parser/third_party/runtime/src/tree/xpath/*.cpp
)

add_library(${CMAKE_PROJECT_NAME}_lib ${SOURCE} ${HEADERS} ${antlr4-cpp-src})
add_executable(${CMAKE_PROJECT_NAME} test.cpp)

target_link_libraries(${CMAKE_PROJECT_NAME}  ${CMAKE_PROJECT_NAME}_lib )
