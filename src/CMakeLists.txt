add_subdirectory(utils)
add_subdirectory(buf_manager)
add_subdirectory(record_handler)
add_subdirectory(index_handler)

add_definitions(-DDEBUG)
add_definitions("-Wall -g")

add_library(${CMAKE_PROJECT_NAME}_lib ${SOURCE} ${HEADERS})
add_executable(${CMAKE_PROJECT_NAME} record_handler_test.cpp)
target_link_libraries(${CMAKE_PROJECT_NAME} ${CMAKE_PROJECT_NAME}_lib)
