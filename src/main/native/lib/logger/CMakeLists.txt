add_library(iologger Logging.cc Logger.cc)
target_link_libraries(iologger ${CMAKE_THREAD_LIBS_INIT} rt)
