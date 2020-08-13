//
// Created by eddy on 2/8/20.
//
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
//PageFrame wapper ,extend SM_PageHandle with extra msg.
typedef  struct PageFrame_Content_s{
    bool dirtyflag;

    //pthread_rwlock_t lock; single thread version
    BM_PageHandle* bmPageHandle;
}PF_Content;

//extra data for BM_BufferPool
typedef  struct BP_Content_s{
    int numReadIO;// num of Reading
    int numWriteIO;// num of writing

    SM_FileHandle* fileHandle;// file handle

    int* pincount;// pin counts for every page frame
    bool* dirtyflag;//dirty flags for every page frame
    PageNumber*  pageNumber;// record the pagenumber of the page(block) that stored in the pageframes
                             // This array map the pageNum to pageframes index
    //PF_Content** pageframes;
    BM_PageHandle** pageframe;

    int queueCurr;// this is used in FIFO, indicating next potential free slot

    time_t wallClock;// this is a wall lock, note it is a relative time, not the real time
    time_t * cachetime;// timestamps array for every page frame,used in LRU(Least Recently Used)
                       // the timestamp here is latest ACCESSED,every time pin the page, update the time
}BP_Content;// Anything need dynamic memory here, make sure init and delete along with Buffer Pool


//new struct for LRU
typedef struct LRU_time_t{
    time_t last_t;
    int frameindex;
}LRU_time;
/*
PF_Content* initPF_Content()
{
    PF_Content* pfContent= malloc(sizeof(PF_Content));
    pfContent->dirtyflag=false;
    pfContent->pincount=0;
    pfContent->bmPageHandle= malloc(sizeof(BM_PageHandle));
    pfContent->bmPageHandle->pageNum=NO_PAGE;
    pfContent->bmPageHandle->data= malloc(sizeof(char)*PAGE_SIZE);
    return  pfContent;
}
*/

//this function map the page number to the index which page frame cached
// return -1, if it hasn't been cached.
int foundPageIndex(BM_BufferPool *const bm,PageNumber nP)
{
    const BP_Content* bpContent= (const BP_Content*)(bm->mgmtData);
    const PageNumber* pageIndex=bpContent->pageNumber;
    //const PF_Content** pageframes = (const PF_Content **) bpContent->pageframes;

    // find the page
    int  pagetobefound=nP;
    int numtotalpages=bm->numPages;
    int found=-1;
    for (int i = 0; i < numtotalpages; ++i) {
        if(pagetobefound==bpContent->pageNumber[i]){
            found=i;
            break;
        }
    }
    return found;
}

// Buffer Manager Interface Pool Handling
//creates a new buffer pool with numPages page frames using the page replacement strategy strategy
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData){

    SM_FileHandle* fH= malloc (sizeof(SM_FileHandle));


    RC openRC=openPageFile(pageFileName,fH);// open page file
    if (openRC!=RC_OK){
        free(fH);// open fail, return.
        return openRC;
    }else{
        //open page file successful

            //successful

            bm->numPages = numPages;
            bm->pageFile = pageFileName;
            bm->strategy = strategy;
             BP_Content* bp_content= malloc(sizeof( BP_Content));

             bp_content->queueCurr=0;//FIFO init

             //LRU timestamps init
             bp_content->cachetime= malloc(sizeof(time_t)*numPages);
               //get time now
               time_t nowT=time(NULL);
               bp_content->wallClock=nowT;
             for (int j = 0; j < numPages; ++j) {
                 bp_content->cachetime[j]=nowT;
             }
        //LRU timestamps init complete


            bp_content->pincount=malloc(sizeof(int)*numPages);
            bp_content->dirtyflag=malloc(numPages* sizeof(bool));

             bp_content->pageNumber=malloc(numPages * sizeof(PageNumber));
             bp_content->pageframe=malloc(numPages*sizeof(BM_PageHandle));

            for (int i = 0; i <numPages ; ++i) {
                bp_content->pincount[i]=0;
                bp_content->dirtyflag[i]=false;
                bp_content->pageNumber[i]=NO_PAGE;

                bp_content->pageframe[i]=malloc(sizeof(BM_PageHandle));
                bp_content->pageframe[i]->pageNum=NO_PAGE;
                bp_content->pageframe[i]->data=malloc(sizeof(char)*PAGE_SIZE);
                //bp_content->pageframe[i]->data=NULL;
            }
            bp_content->numReadIO=0;
            bp_content->numWriteIO=0;
            bp_content->fileHandle=fH;
            bm->mgmtData =bp_content;
            return RC_OK;
        }



}

//destroys a buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (bm==NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    forceFlushPool(bm);// write all data back.
    const BP_Content *bpContent = (const BP_Content *) (bm->mgmtData);//as long as bm isn't NULL, it always has
                //bpContent, no need to check
    SM_FileHandle*fH =bpContent->fileHandle;// like above always have a file
    closePageFile(fH);

    free(bpContent->cachetime); // timestamps for LRU =========
    free(bpContent->pageNumber);
    free(bpContent->dirtyflag);
    free(bpContent->pincount);
    int numPages=bm->numPages;
    free(bpContent->fileHandle);
    for (int i = 0; i <numPages ; ++i) {
        free(bpContent->pageframe[i]->data);
        free(bpContent->pageframe[i]);

    }
    free(bpContent->pageframe);
    free(bpContent);
    bm->numPages=-1;
    bm->pageFile=NULL;
    bm->mgmtData=NULL;
    return RC_OK;
}

//causes all dirty pages (with fix count 0) from the buffer pool to be written to disk
RC forceFlushPool(BM_BufferPool *const bm)
{
    const BP_Content *bpContent = (const BP_Content *) (bm->mgmtData);
    bool* dF=bpContent->dirtyflag;
    int numtotalpages=bm->numPages;
    RC rt=RC_OK;
    for (int i = 0; i < numtotalpages; ++i) {

        BM_PageHandle dummy={-1,NULL};
        if(dF[i])
        {
            dummy.pageNum=bpContent->pageNumber[i];
            rt=forcePage(bm,&dummy);
            if(rt!=RC_OK) {
                printf("forceFlushPool: Write Page return ERROR!/n");
                return rt;
            }

        }
    }

    return  rt;
}

// Buffer Manager Interface Access Pages

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {

    const BP_Content *bpContent = (const BP_Content *) (bm->mgmtData);

    int found = foundPageIndex(bm, page->pageNum);
    if (found == -1) {
        //not found
        printf("markDirty:can not find page requested in frame buffer/n");
        return RC_FILE_NOT_FOUND;
    } else {

        bpContent->dirtyflag[found] = true;
        bpContent->pageframe[found]->data=page->data;
        return RC_OK;
    }

}

// unpin a page in frame, note  this will not fore the dirty page frame write back
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{

    const BP_Content *bpContent = (const BP_Content *) (bm->mgmtData);

    int found = foundPageIndex(bm, page->pageNum);
    if (found == -1) {
        //not found
        printf("unpinPage:can not find page requested in frame buffer/n");
        return RC_FILE_NOT_FOUND;
    } else {

        bpContent->pincount[found]--;
        /*
        if(bpContent->pincount[found]==0&&bpContent->dirtyflag[found]){
            //write back;
            forcePage(bm,page);
        }*/
        return RC_OK;
    }

}
// write dirty page back
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{

     BP_Content *bpContent = (const BP_Content *) (bm->mgmtData);

    int found = foundPageIndex(bm, page->pageNum);
    if (found == -1) {
        //not found
        printf("unpinPage:can not find page requested in frame buffer/n");
        return RC_FILE_NOT_FOUND;
    } else {

        BM_PageHandle* bph=  bpContent->pageframe[found];
        bpContent->dirtyflag[found]=false;

        RC writeBlockRC=writeBlock(page->pageNum,bpContent->fileHandle,bph->data);
        if (writeBlockRC!=RC_OK)
        {

            printf("writeBlock failed in forcePage ");
            return writeBlockRC;
        }else{
            bpContent->numWriteIO++;
            return RC_OK;
        }
    }

}

//return a free page frame slot, or -1 if no available
PageNumber FIFO(BM_BufferPool *const bm)
{
     BP_Content *bpContent = (const BP_Content *) (bm->mgmtData);
    const int * pagePinCount=bpContent->pincount;
    int nextfree=bpContent->queueCurr;// we use one head point and the pins array as a circular queue.
    int cpNextFree=nextfree;
    PageNumber rt=-1;
    do {
        if (pagePinCount[nextfree] == 0) {
            // this is a free slot,we will use this.

            rt=nextfree;
            nextfree++;//update
            if (nextfree == bm->numPages)nextfree = 0;// go back to start
            bpContent->queueCurr=nextfree;
            return rt;
        }
        nextfree++;//update
        if (nextfree == bm->numPages)nextfree = 0;// go back to start
    } while(nextfree!=cpNextFree);//we have checked out all slot but all page frames occupied
    printf("No free frame in FIFO");
    return -1;
}

///LRU compare aux func for qsort
int lrutimecomp(const void* a,const void* b)
{
    const LRU_time* at=a;
    const LRU_time* bt=b;
    if(at->last_t==bt->last_t)return at->frameindex>bt->frameindex?1:-1;
    return (at->last_t)>(bt->last_t)?1:-1;
}

//return a free page frame slot with LRU, or -1 if no available
PageNumber LRU(BM_BufferPool *const bm)
{
    BP_Content *bpContent = (const BP_Content *) (bm->mgmtData);
    const int * pagePinCount=bpContent->pincount;
     time_t* timestamps=bpContent->cachetime;
    const PageNumber* pageNumbers=bpContent->pageNumber;
    int numpages=bm->numPages;

    LRU_time* lruTime= malloc(sizeof(LRU_time)*numpages);
    //here we need store the frame index and the corresponding timestamp before sort,
    // so that after sort, we still know which frame is located in lruTime
    for (int i = 0; i <numpages ; ++i) {
        lruTime[i].frameindex=i;
        lruTime[i].last_t=timestamps[i];
    }

    qsort(lruTime,numpages, sizeof(LRU_time),lrutimecomp);// sort time stamp

    /// here's one more thing to do.we now know the oldest time, to prevent time OVERFLOW,
    //here we subtract all the timestamp with the oldest time.
    time_t oldestT=lruTime[0].last_t;
    bpContent->wallClock-=oldestT;
    for (int k = 0; k < numpages; ++k) {
        timestamps[k]-=oldestT;
    }


    PageNumber rt=-1;
    for (int j = 0; j <numpages ; ++j) {
        int freeindex=lruTime[j].frameindex;
        if (pagePinCount[freeindex] == 0) {
            // this is a free slot,we will use this.
            rt=freeindex;
            break;
        }
    }
    // no free frame slot
    free (lruTime);
    return rt;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum) {
    if (bm == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
     BP_Content *bpContent = (const BP_Content *) (bm->mgmtData);
    bpContent->wallClock++;

    int found = foundPageIndex(bm, pageNum);
     PageNumber* pageIndex=bpContent->pageNumber;
     int * pagePinCount=bpContent->pincount;
    BM_PageHandle** pageframs=bpContent->pageframe;
    if (found!=-1)
    {// already pinned
        bpContent->pincount[found]++;

        //bpContent->cachetime[found]=time(NULL);//update time stamp for LRU
        bpContent->cachetime[found]=bpContent->wallClock;//this is a relative time,not real time

        page->pageNum=pageframs[found]->pageNum;
        page->data=pageframs[found]->data;
        return RC_OK;
    }else{
        // find a free slot, than read the block into that slot
        PageNumber freeIndex=-1;
        switch (bm->strategy) {

            case RS_FIFO:
                freeIndex = FIFO(bm);
                break;
            case RS_LRU:
                freeIndex = LRU(bm);
                break;
            case RS_CLOCK:
                break;
            case RS_LFU:
                break;
            case RS_LRU_K:
                break;
        }

        if(freeIndex==-1)
        {
            printf("no free pageframe slot in pinPage/n");
            return RC_FILE_NOT_FOUND;
        }else{


            RC makesurespaceRC=ensureCapacity(pageNum+1,bpContent->fileHandle);//ensure Capacity
            if(makesurespaceRC!=RC_OK){
                printf("ensureCapacity failed in pinPage/n");
                return makesurespaceRC;
            }
            // first check if the old content need to  write back
            if(bpContent->dirtyflag[freeIndex]){
                RC rtForce=forcePage(bm,pageframs[freeIndex]);
            }


            //read data to pageframe first.
            pageframs[freeIndex]->pageNum=pageNum;
            RC readRC=readBlock(pageNum,bpContent->fileHandle,pageframs[freeIndex]->data);//read data to cache
            if(readRC!=RC_OK){
                printf("readBlock failed in pinPage/n");
                return readRC;
            }else{
                //successful
                bpContent->numReadIO++;// update read count




                //update handle
                page->pageNum=pageframs[freeIndex]->pageNum;
                page->data=pageframs[freeIndex]->data;
                //update bm
                bpContent->pincount[freeIndex]++;// Pin it
                bpContent->pageNumber[freeIndex]=pageNum;

                //bpContent->cachetime[freeIndex]=time(NULL);//update time stamp for LRU
                bpContent->cachetime[freeIndex]=bpContent->wallClock;//this is a relative time,not real time
            }
            return RC_OK;
        }
    }
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{

    /*int numofPages=bm->numPages;
    int* rt=malloc(sizeof(bool)*numofPages);*/
    const BP_Content* bpContent= (const BP_Content*)(bm->mgmtData);
    //const PF_Content** pageframes = (const PF_Content **) bpContent->pageframes;
/*
    for (int i = 0; i < numofPages; ++i) {
        rt[i]=bpContent->pageNumber[i];
    }*/

    return bpContent->pageNumber;

}
bool *getDirtyFlags (BM_BufferPool *const bm)
{
    /*int numofPages=bm->numPages;
    bool* rt=malloc(sizeof(bool)*numofPages);*/
    const BP_Content* bpContent= (const BP_Content*)(bm->mgmtData);
/*
    for (int i = 0; i < numofPages; ++i) {
        rt[i]=bpContent->dirtyflag[i];
    }*/

    return bpContent->dirtyflag;
}
int *getFixCounts (BM_BufferPool *const bm)
{

    //int numofPages=bm->numPages;
    //int* rt=malloc(sizeof(bool)*numofPages);
    const BP_Content* bpContent= (const BP_Content*)(bm->mgmtData);
/*
 *  memory leak
    for (int i = 0; i < numofPages; ++i) {
        rt[i]=bpContent->pincount[i];
    }*/

    return bpContent->pincount;
}
int getNumReadIO (BM_BufferPool *const bm)
{
    const BP_Content* bpContent= (const BP_Content*)(bm->mgmtData);
    int rt=bpContent->numReadIO;
    return rt;

}
int getNumWriteIO (BM_BufferPool *const bm)
{

    const BP_Content* bpContent= (const BP_Content*)(bm->mgmtData);
    int rt=bpContent->numWriteIO;
    return  rt;
}
