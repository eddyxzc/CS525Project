CFLAGS = -g -Wall#debug
CC = gcc
RM = /bin/rm -f
all: test_assign2_1

dberror.o :dberror.h dberror.c

storage_mgr.o: storage_mgr.h storage_mgr.c

buffer_mgr.o: buffer_mgr.h buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr.h buffer_mgr.c

test_assign2_1.o: test_helper.h test_assign2_1.c

test_assign2_1: dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o test_assign2_1.o

clean:
	${RM} *.o test_assign2_1