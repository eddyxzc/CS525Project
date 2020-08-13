#include <fcntl.h>
#include <unistd.h>
#include "storage_mgr.h"
#include <stdio.h>
/* manipulating page files */
 void initStorageManager (void)
 {
     // nothing to do
 }
 RC createPageFile (char *fileName)
 {
     FILE* fp=fopen(fileName,"wb+");//"wb+"  creat a new file to write and read as binary file
     if(fp==NULL)
     {
         //open failed
         printf("fopen failed in createPageFile");
         return RC_WRITE_FAILED;
     } else{
        // open succeed
        char zeros[PAGE_SIZE];
         for (int i = 0; i < PAGE_SIZE ; ++i) {
             zeros[i]=0x00;
         }
         unsigned long nRW=fwrite(zeros,1,PAGE_SIZE,fp);
         if (nRW<0)
         {
             printf("fwrite failed in createPageFile\n");
             return RC_WRITE_FAILED;
         }
         //fseek(fp,0L,SEEK_END);
         fclose(fp);
         return RC_OK;
     }
 }

 RC openPageFile (char *fileName, SM_FileHandle *fHandle)
 {
     FILE* fp=fopen(fileName,"rb+");//"rb+" open a creat file to write and read as binary file
     if(fp==NULL)
     {
         //open failed
         printf("fopen failed in openPageFile\n");
         return RC_WRITE_FAILED;

     } else{
         // open succeed
          fHandle->fileName=fileName;
          fseek(fp,0L,SEEK_END);
          long numberbytes=ftell(fp);
          fHandle->curPagePos=0;
          fHandle->totalNumPages=numberbytes/PAGE_SIZE;
          fHandle->mgmtInfo=fp;
         return RC_OK;
     }
 }

 RC closePageFile (SM_FileHandle *fHandle)
 {
     //int rt=close()
     if (fHandle==NULL) {
         printf("fHandle not init in closePageFile\n");
         return RC_FILE_HANDLE_NOT_INIT;
     } else if (fHandle->mgmtInfo==NULL)
     {
         printf("page file pointer doesn't init in closePageFile\n");
         return RC_FILE_NOT_FOUND;
     }
     int rt=fclose(fHandle->mgmtInfo);
     if (rt==0)
     {
         //close successfully
         fHandle->mgmtInfo=NULL;
         fHandle->totalNumPages=0;
         fHandle->fileName="";
         fHandle->curPagePos=0;
         return RC_OK;
     } else{
         //close failed
         printf("fclose failed in closePageFile\n");
         return RC_WRITE_FAILED;
     }
 }
 RC destroyPageFile (char *fileName)
 {
     int rt=remove(fileName);
     if (rt==0)
     {
         //succeed

         return RC_OK;
     } else{
         //fail
         printf("remove file failed in destroyPageFile\n");
         return RC_FILE_NOT_FOUND;
     }
 }

/* reading blocks from disc */
 RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
 {
     if (fHandle==NULL) {
         printf("fHandle not init in readBlock\n");
         return RC_FILE_HANDLE_NOT_INIT;
     } else if (fHandle->mgmtInfo==NULL)
     {
         printf("page file pointer doesn't init in readBlock\n");
         return RC_READ_NON_EXISTING_PAGE;
     }

     //check pageNum
     int totalNumPages=fHandle->totalNumPages;
     if(pageNum<0) return RC_READ_NON_EXISTING_PAGE;
     if(pageNum>totalNumPages-1)
     {
         printf("Block index exceed total blocks in readBlock\n");
         return RC_READ_NON_EXISTING_PAGE;
     }
     FILE* fp=fHandle->mgmtInfo;
     fseek(fp,pageNum*PAGE_SIZE,SEEK_SET);
     unsigned long nRW=fread(memPage,1,PAGE_SIZE,fp);
     fHandle->curPagePos=pageNum;// update current page position
     if (nRW<0)
     {
         //read failed.
         printf("fread failed in readBlock\n");

         return RC_READ_NON_EXISTING_PAGE;
     }else{
         fHandle->curPagePos=pageNum;//change current position
         return RC_OK;
     }
 }
 int getBlockPos (SM_FileHandle *fHandle)
 {
     if (fHandle==NULL) {
         //return RC_FILE_HANDLE_NOT_INIT;
         printf("fHandle not init in getBlockPos\n");
         return -1;
     } else if (fHandle->mgmtInfo==NULL)
     {
         //return RC_READ_NON_EXISTING_PAGE;
         printf("page file pointer doesn't init in getBlockPos\n");
         return -1;
     }
     return fHandle->curPagePos;
 }
 RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
 {
     // head over to readBlock, no need to check input parameters
     return readBlock(0,fHandle,memPage);
 }

 RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
 {
     //input check in getBlockPos and readBlock below
     int currentBlock=getBlockPos(fHandle);
     if(currentBlock==0)
     {
        // current on the first block
        printf("try to call previousBlock of the first block\n");
         return RC_READ_NON_EXISTING_PAGE;
     }else{
         return readBlock(currentBlock-1,fHandle,memPage);
     }

 }

 RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
 {
     //input check in getBlockPos and readBlock below
     int currentBlock=getBlockPos(fHandle);
     return readBlock(currentBlock,fHandle,memPage);

 }

 RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
 {
     int currentBlock=getBlockPos(fHandle);
     int totalBlocks=fHandle->totalNumPages;
     if(currentBlock==totalBlocks-1)// pagenum starts at 0;
     {
         // current on the last block
         printf("try to call readNextBlock of the last block\n");
         return RC_READ_NON_EXISTING_PAGE;
     }else{
         return readBlock(currentBlock+1,fHandle,memPage);
     }
 }
 RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
 {
     if (fHandle==NULL) {
         //return RC_FILE_HANDLE_NOT_INIT;
         printf("fHandle not init in readLastBlock\n");
         return -1;
     }
     int totalBlocks=fHandle->totalNumPages;
     return readBlock(totalBlocks-1,fHandle,memPage);// pagenum starts at 0;
 }

/* writing blocks to a page file */
 RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
 {
     if (fHandle==NULL) {
         printf("fHandle not init in writeBlock\n");
         return RC_FILE_HANDLE_NOT_INIT;
     } else if (fHandle->mgmtInfo==NULL)
     {
         printf("page file pointer doesn't init in writeBlock\n");
         return RC_READ_NON_EXISTING_PAGE;
     }

     //check pageNum
     int totalNumPages=fHandle->totalNumPages;
     if(pageNum<0){
         printf("pageNum <0 in writeBlock\n");
         return RC_WRITE_FAILED;
     }
     FILE* fp=fHandle->mgmtInfo;//get page file pointer

     if(pageNum>totalNumPages-1)
     {
         //expand file first
         ensureCapacity(pageNum+1,fHandle);
         return writeBlock(pageNum,fHandle,memPage);
     } else {
         // no need to expand the file
         fseek(fp,pageNum*PAGE_SIZE,SEEK_SET);
         unsigned long nRW=fwrite(memPage,1,PAGE_SIZE,fp);
         if(nRW<0)
         {
             printf("fwrite failed in writeBlock\n");
             return RC_WRITE_FAILED;
         }
         fHandle->curPagePos=pageNum;// update current page position
         return RC_OK;
     }

 }
 RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
 {
     if (fHandle==NULL) {
         printf("fHandle not init in writeCurrentBlock\n");
         return RC_FILE_HANDLE_NOT_INIT;
     } else if (fHandle->mgmtInfo==NULL)
     {
         printf("page file pointer doesn't init in writeCurrentBlock\n");
         return RC_READ_NON_EXISTING_PAGE;
     }
     // as long as the pagefile is valid, there must be at least one block.
     FILE* fp=fHandle->mgmtInfo;
     fseek(fp,fHandle->curPagePos*PAGE_SIZE,SEEK_SET);
     unsigned long nRW=fwrite(memPage,1,PAGE_SIZE,fp);
     if (nRW<0){
         printf("fwrite failed in writeCurrentBlock\n");
         return RC_WRITE_FAILED;
     }else{
         return RC_OK;
     }
 }

//Increase the number of pages in the file by one. The new last page should be filled with zero bytes.
 RC appendEmptyBlock (SM_FileHandle *fHandle)
 {
     if (fHandle==NULL) {
         printf("fHandle not init in appendEmptyBlock\n");
         return RC_FILE_HANDLE_NOT_INIT;
     } else if (fHandle->mgmtInfo==NULL)
     {
         printf("page file pointer doesn't init in appendEmptyBlock\n");
         return RC_READ_NON_EXISTING_PAGE;
     }

     FILE* fp=fHandle->mgmtInfo;
     fseek(fp,0L,SEEK_END);// go to the end of the file.
     // write zeros to the new page.
     char zeros[PAGE_SIZE];
     for (int i = 0; i < PAGE_SIZE ; ++i) {
         zeros[i]=0x00;
     }
     unsigned long nRW=fwrite(zeros,1,PAGE_SIZE,fp);
     if (nRW<0)
     {
         printf("fwrite failed in appendEmptyBlock\n");
         return RC_WRITE_FAILED;
     }
     //update the page info in fHandle
     fHandle->curPagePos=fHandle->totalNumPages-1;
     fHandle->totalNumPages++;
     return RC_OK;
 }

//If the file has less than numberOfPages pages then increase the size to numberOfPages.
 RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
 {
     if (fHandle==NULL) {
         printf("fHandle not init in ensureCapacity\n");
         return RC_FILE_HANDLE_NOT_INIT;
     } else if (fHandle->mgmtInfo==NULL)
     {
         printf("page file pointer doesn't init in ensureCapacity\n");
         return RC_READ_NON_EXISTING_PAGE;
     }

     //check pageNum
     int totalNumPages=fHandle->totalNumPages;
     if(numberOfPages<0) return RC_READ_NON_EXISTING_PAGE;
     FILE* fp=fHandle->mgmtInfo;
     fseek(fp,0L,SEEK_END);
     if(numberOfPages>totalNumPages) {
         // need to expand file
         for (int i = totalNumPages; i < numberOfPages; ++i) {
             if (appendEmptyBlock(fHandle)!=RC_OK)
                 return RC_WRITE_FAILED;
         }
         return RC_OK;
     } else {
         // no need to expand the file
         return RC_OK;
     }
 }