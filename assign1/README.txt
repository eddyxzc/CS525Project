
MakeFile was made

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

9. readLastBlock
- Read the last block defined by fHandle

 10. writeBlock
 - writing blocks to a page file
 - determine the capacity whether expend or not

11. writeCurrentBlock
- determine page valid or not
- write on the current block if the page is valid

 12. appendEmptyBlock
- Increase the number of pages in the file by one with zero bytes.

 13. ensureCapacity
 - If the file has less than numberOfPages pages then increase the size to numberOfPages.

