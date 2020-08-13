//To build:
MakeFile was made in the code directory

//General:
Our team implement two strategies, FIFO and LRU.
The code can pass test_assign2_1 testing.

In FIFO, we use one head pointer and the pins array as a circular queue.The next free pointer indicates
the next potential available frame slot.

In LRU, we didn't use the real time like the UNIX format time_t. Because it's not accurate.
Otherwise, we use a relative time. Every time we pin a frame, the clock will +1.

//storage_mgr.c
File Related Functions
1. initStorageManager
- Initialization function

 2. createPageFile
- Create a new page file 'fileName'.

 3. openPageFile
- Opens an existing page file.

 4. closePageFile
 - Close an open page file.

 5. destroyPageFile
 - Delete a page file

 6. readBlock
 - Read the pageNum block defined by fHandle

7. getBlockPos
- Return the current block position in the file

8. readFirstBlock
- Read the first block defined by fHandle

9. readPreviousBlock
- input check in getBlockPos then read the previous block

10. readCurrentBlock
- input check in getBlockPos and readBlock below then read the current block

11. readNextBlock
- input check in getBlockPos then read the next block

12. readLastBlock
- Read the last block defined by fHandle

 13. writeBlock
 - writing blocks to a page file
 - determine the capacity whether expend or not

14. writeCurrentBlock
- determine page valid or not
- write on the current block if the page is valid

 15. appendEmptyBlock
- Increase the number of pages in the file by one with zero bytes.

 16. ensureCapacity
 - If the file has less than numberOfPages pages then increase the size to numberOfPages.

//buffer_mgr.c
1. foundPageIndex
- this function map the page number to the index which page frame cached
- return -1, if it hasn't been cached.

2. initBufferPool
- creates a new buffer pool with numPages page frames using the page replacement strategy strategy.
- The pool is used to cache pages from the page file with name pageFileName.
- For the page replacement strategy, *stratData could pass the parameter k for LRU-k algorithm.

3. shutdownBufferPool
- destroys a buffer pool

4. forceFlushPool
- causes all dirty pages (with fix count 0) from the buffer pool to be written to disk

5. markDirty
- buffer manager interface access pages

6. unpinPage
- unpin a page in frame, note  this will not force the dirty page frame write back

7. forcePage
- write dirty page back

8. pinPage
- use a relative time. Every time we pin a frame, the clock will +1.