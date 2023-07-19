add_executable(lab12 main.cpp)

target_link_libraries(lab12 database)
target_include_directories(lab12 PRIVATE ${PROJECT_SOURCE_DIR})
