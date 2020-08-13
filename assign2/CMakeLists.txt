cmake_minimum_required(VERSION 3.2)
project("assign2")
add_executable(test_assign2_1 test_assign2_1.c storage_mgr.c buffer_mgr_stat.c dberror.c buffer_mgr.c)
#add_executable(test_assign2_2 test_assign2_2.c storage_mgr.c buffer_mgr_stat.c dberror.c buffer_mgr.c), no implementation for LRU_k