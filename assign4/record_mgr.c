//
// Created by eddy on 3/20/20.
//
#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include <stdlib.h>
#include <string.h>

//RecordManager* rm;//global RecordManager variable
static int numPagesTotall=(PAGE_SIZE/2)/ (sizeof(bool)+ sizeof(int));
static int numSlotsTotall=0;
// this struct is used to tell us the info of blocks in page file
// We use first page block 0, to store schema in the first half of its 4K
// And in the second half we store PageIndex for every block from block 1~end
// We have 2k to store those info, which means we can have about 400 page block to store the records
typedef struct PageIndex_t
{
    bool  allocated;//1B
    int  page_number;//4B,
    bool freeslots;//4B

} PageIndex;

//Because one block is 4k, we need at most 4096 bits to indicates every Bytes,
//So we use first 4kB x 1/8=512B=4096 bits to store slots info, if the fist slot is free, the the first bit of first char is 1.
//i.e 1000 0000=0x80  0b10000000
typedef struct SlotIndex_t
{
    unsigned char* slotindex;//
    bool* slotsBool;
    int slotsNum;
}SlotIndex;



// table and manager

#define  TABLESPACE 10

typedef struct RecordManager_t//RecordManager Object
{
    //set a array of tables, since we won't store many tables, array is a simple and good way.
    int numTables;// indicate the tables array spaces
    RM_TableData** tables;
    ////////////////////////////

} RecordManager;



//extra content for struct RM_TableData
typedef struct Table_aux_content_s{
    char* name;//use name to find the table in the record manager
    int numRecords;
    BM_BufferPool* bufferPool;// one page file and one bufferpool for one table
    BM_PageHandle* pageHandle;
    /////////////////////
    bool* pagesF;//indicates is the page is FULL
    int* numSlotsinPage;
    //////////////////////////
} TableContent;

int findAFreeSlot(SlotIndex* si){

    unsigned char* flags=si->slotindex;
    int ichar=0;
    for ( ichar = 0; ichar < PAGE_SIZE/8; ++ichar) {
        if((flags[ichar]!=0B11111111))
            break;
    }
    if(ichar == PAGE_SIZE/8) {
        printf("find A FreeSlot \n");
        return -1;
    }
    unsigned char index=flags[ichar];
    int freeslot=ichar*8;

    unsigned char mask=0B00000001;

    if((flags[ichar]&0B00000001)==0)
    {
        freeslot+=0;
    } else if ((flags[ichar]&0B00000010)==0)
    {
        freeslot+=1;
    } else if ((flags[ichar]&0B00000100)==0) {
        freeslot += 2;
    }else if((flags[ichar]&0B00001000)==0){
        freeslot+=3;
    } else if ((flags[ichar]&0B00010000)==0){
        freeslot+=4;
    } else if((flags[ichar]&0B00100000)==0){
        freeslot+=5;
    } else if((flags[ichar]&0B01000000)==0){
        freeslot+=6;
    } else if ((flags[ichar]&0B10000000)==0){
        freeslot+=7;
    }


    return freeslot;
}

int Bits2Bools(SlotIndex* si){
    int numarray=si->slotsNum;
    /*int offset=numarray%8;
    int base=numarray/8;*/
    unsigned char* flags=si->slotindex;
    int ichar=0;int index=0;
    /*for (int i = 0; i <numarray ; ++i) {

        ichar=(i+1)/8;
        si->slotsBool[i]=false;
        if((i)%8==0) {
            index = flags[ichar];
        }
        index=index>>i;
        if(index&0B00000001)
        {
            si->slotsBool[i]=true;
        }

    }*/
    int base = 0;
    for ( ichar = 0; ichar < numarray % 8+1; ichar++)
    {
        if ((flags[ichar] & 0B00000001) != 0)
        {
            si->slotsBool[base+0]=true;
        }
        else {
            si->slotsBool[base + 0] = false;
        }
        index++;
        if (index > numarray - 1)break;
        if ((flags[ichar] & 0B00000010) != 0)
        {
            si->slotsBool[base+1] = true;

        }
        else {
            si->slotsBool[base + 1] = false;
        }
        index++;
        if (index > numarray - 1)break;
        if ((flags[ichar] & 0B00000100) != 0) {
            si->slotsBool[base + 2] = true;
        }
        else {
            si->slotsBool[base + 2] = false;
        }
        index++;
        if (index > numarray - 1)break;
        if ((flags[ichar] & 0B00001000) != 0) {
            si->slotsBool[base + 3] = true;
        }
        else {
            si->slotsBool[base + 3] = false;
        }
        index++;
        if (index > numarray - 1)break;
        if ((flags[ichar] & 0B00010000) != 0) {
            si->slotsBool[base + 4] = true;
        }
        else {
            si->slotsBool[base + 4] = false;
        }
        index++;
        if (index > numarray - 1)break;
        if ((flags[ichar] & 0B00100000) != 0) {
            si->slotsBool[base + 5] = true;
        }
        else {
            si->slotsBool[base + 5] = false;
        }
        index++;
        if (index > numarray - 1)break;
        if ((flags[ichar] & 0B01000000) != 0) {
            si->slotsBool[base + 6] = true;
        }
        else {
            si->slotsBool[base + 6] = false;
        }
        index++;
        if (index > numarray - 1)break;
        if ((flags[ichar] & 0B10000000) != 0) {
            si->slotsBool[base + 7] = true;
        }
        else {
            si->slotsBool[base + 7] = false;
        }
        index++;
        if (index > numarray - 1)break;

        base +=8;
    }

    return 1;
}

bool TestslotFlag(int slotid,SlotIndex* si){
    unsigned char* flags=si->slotindex;

    int offset=slotid%8;
    int base=slotid/8;
    unsigned char mask;
    switch(offset)
    {
        case 0:
            mask=0B00000001;
            break;
        case 1:
            mask=0B00000010;
            break;
        case 2:
            mask=0B00000100;
            break;
        case 3:
            mask=0B00001000;
            break;
        case 4:
            mask=0B00010000;
            break;
        case 5:
            mask=0B00100000;
            break;
        case 6:
            mask=0B01000000;
            break;
        case 7:
            mask=0B10000000;
            break;
        default:
            printf("TestslotFlag failed\n");
            return -1;
    }
    if((flags[base]&mask)==0)
        return false;
    return true;
}

int MarkslotFlag(int slotid,SlotIndex* si,char* pagedata)
{
    unsigned char* flags=si->slotindex;

    int offset=slotid%8;
    int base=slotid/8;

    unsigned char mask;
    switch(offset)
    {
        case 0:
            mask=0B00000001;
            break;
        case 1:
            mask=0B00000010;
            break;
        case 2:
            mask=0B00000100;
            break;
        case 3:
            mask=0B00001000;
            break;
        case 4:
            mask=0B00010000;
            break;
        case 5:
            mask=0B00100000;
            break;
        case 6:
            mask=0B01000000;
            break;
        case 7:
            mask=0B10000000;
            break;
        default:
            printf("MarkslotFlag failed\n");
            return -1;
    }
    flags[base]=flags[base]|mask;
    memcpy(pagedata,flags, sizeof(unsigned char)*PAGE_SIZE/8);//update pagedata
    return mask;
}

int FreeSlotFlag(int slotid,SlotIndex* si,char* pagedata)
{

    unsigned char* flags=si->slotindex;

    int offset=slotid%8;
    int base=slotid/8;

    unsigned char mask;
    switch(offset)
    {
        case 0:
            mask=0B11111110;
            break;
        case 1:
            mask=0B11111101;
            break;
        case 2:
            mask=0B11111011;
            break;
        case 3:
            mask=0B11110111;
            break;
        case 4:
            mask=0B11101111;
            break;
        case 5:
            mask=0B11011111;
            break;
        case 6:
            mask=0B10111111;
            break;
        case 7:
            mask=0B01111111;
            break;
        default:
            printf("MarkslotFlag failed\n");
            return -1;
    }
    flags[base]=flags[base]&mask;
    memcpy(pagedata,flags, sizeof(unsigned char)*PAGE_SIZE/8);//update pagedata
    return mask;

}

unsigned char* GetSlotIndex(const char* pagedata)
{
    unsigned char* flags= malloc(sizeof(unsigned char)*PAGE_SIZE/8);
    /* for ( int ichar = 0; ichar < PAGE_SIZE/8; ++ichar) {
         memcpy(flags,pagedata, sizeof(unsigned char));
     }*/
    memcpy(flags,pagedata, sizeof(unsigned char)*PAGE_SIZE/8);//update pagedata
    return flags;

}
int findFreePage(RM_TableData* tb)
{// Find a free Page
    TableContent* tbcontent=tb->mgmtData;
    bool* pageF=tbcontent->pagesF;

    int i=-1;
    for ( i = 1; i < numPagesTotall; ++i) {//start from 1, the page 0 is reserved for table info
        if(!pageF[i]){
            //not FULL
            return i;
        }
    }

    printf("Cannot find a free page, all page FULL\n");
    return -1;
}

//initialize a record manager
RC initRecordManager (void *mgmtData)
{
    /*
    rm=malloc(sizeof(RecordManager));

    /////////////// allocate space for tables
    rm->numTables=TABLESPACE;
    rm->tables=malloc(rm->numTables * sizeof(RM_TableData*));
    for (int i = 0; i < rm->numTables; ++i) {
        rm->tables[i]=NULL;
    }
    ////////////////////////////

    return RC_OK;
     */
    return RC_OK;
}

//shutdown a record manager
RC shutdownRecordManager ()
{
/*
    if(rm==NULL){
        printf("no initiated RecordManager \n");
        return RC_FILE_NOT_FOUND;
    }
    else{
        rm->numTables=0;
        free(rm->tables);// delete table array first
        free(rm);// delete record manager
        return RC_OK;
    }
*/
    return RC_OK;
}

// Creating a table
RC createTable (char *name, Schema *schema)
{
    ////check input first
    if(name==NULL)
    {
        printf("name pointer is NULL in createTable\n");
        return RC_FILE_NOT_FOUND;
    }
    if(schema==NULL)
    {
        printf("schema pointer is NULL in creatTable\n");
        return RC_RM_UNKOWN_DATATYPE;
    }
    // /////////////////////////////////

    BM_BufferPool* bp=MAKE_POOL();

//
///initialize page file
    initStorageManager();
    int rtRC=createPageFile(name);
    if(rtRC!=RC_OK){
        printf("creatPageFile Failed in creatTable\n");
        return RC_OK;
    }

    rtRC=initBufferPool(bp,name,5,RS_FIFO,NULL);
    if(rtRC!=RC_OK){
        printf("initBufferPool Failed in creatTable\n");
        return RC_OK;
    }
    BM_PageHandle* ph=MAKE_PAGE_HANDLE();


    //Store schema and other info in first page
    pinPage(bp,ph,0);

    char* pagedata=ph->data;
    int offset=0;// write offset of page
    //write schema to the first page
/*
 * typedef struct Schema
{
	int numAttr;
	char **attrNames;
	DataType *dataTypes;
	int *typeLength;
	int *keyAttrs;
	int keySize;
} Schema;*/
    memcpy(pagedata+offset,&(schema->numAttr), sizeof(int));// copy schema->numAttr
    offset+= sizeof(int);
    //write Typelength
    memcpy(pagedata+offset,schema->typeLength, sizeof(int)*schema->numAttr);
    offset+=sizeof(int)*schema->numAttr;
    /////write DataTypes,dataType is at least 1 byte i.e. 1 char, a liitle  trick here.
    memcpy(pagedata+offset,schema->dataTypes, sizeof(DataType)*schema->numAttr);
    offset+=sizeof(DataType) * schema->numAttr;
    ////////////////////////////////////now write attrNames;
    for (int i = 0; i < schema->numAttr; ++i) {
        switch (schema->dataTypes[i])
        {
            case DT_STRING:
                memcpy(pagedata+offset,schema->attrNames[i],schema->typeLength[i]* sizeof(char));
                offset += schema->typeLength[i]* sizeof(char);// string attr length is the number of chars
                break;
            case DT_INT:
                memcpy(pagedata+offset,schema->attrNames[i],sizeof(int));
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                memcpy(pagedata+offset,schema->attrNames[i],sizeof(float));
                offset += sizeof(float);
                break;
            case DT_BOOL:
                memcpy(pagedata+offset,schema->attrNames[i],sizeof(bool));
                offset += sizeof(bool);
                break;
            default:
                printf("undefined dataType\n");
                return RC_FILE_NOT_FOUND;
        }

    }
////////////////////////////////////////////////////////////////////////////////////

    /////Write keySize
    memcpy(pagedata+offset,&(schema->keySize), sizeof(int));// copy schema->keySize
    offset+= sizeof(int);
    /////////////////////////////////////////////////////////////////
    //write keyAttrs
    memcpy(pagedata+offset,schema->keyAttrs, sizeof(int)*schema->keySize);
    offset+=sizeof(int) * schema->keySize;
    //all finished,schema written on the frist page.
    //write numrecords;
    int numrecords=0;
    offset+= sizeof(int);
    if(offset>PAGE_SIZE/2-1)
    {
        printf("Schema too big!");//Schema used up half pag
        return RC_WRITE_FAILED;
    }
    offset=PAGE_SIZE/2-1;
    //numPagesTotall=(PAGE_SIZE/2)/ (sizeof(bool)+ sizeof(int));
    memcpy(pagedata+offset-sizeof(int),&numrecords, sizeof(int));
    for (int j = 0; j < numPagesTotall; ++j) {
        *(pagedata+offset)=false;
        offset+=sizeof(bool);

    }
    for (int j = 0; j < numPagesTotall; ++j) {
        *(pagedata+offset)=0;
        offset+=sizeof(int);
    }
    markDirty(bp,ph);//mark dirty
    unpinPage(bp,ph);//unpin that page
    forceFlushPool(bp);//write back to disk
    shutdownBufferPool(bp);
    free(ph);
    free(bp);
    return RC_OK;
}

//All operations on a table such as scanning or inserting records
// require the table to be opened first
//precondition rel is allocated memory
RC openTable (RM_TableData *rel, char *name)
{


    ////check input first
    if(rel==NULL){
        printf("no initiated RM_TableData \n");
        return RC_FILE_NOT_FOUND;
    }
    if(name==NULL)
    {
        printf("name pointer is NULL in openTable\n");
        return RC_FILE_NOT_FOUND;
    }
    ////first step, find the table in the array in RecordManager
    //set up aux data for table
    TableContent* tbcontent=malloc(sizeof(TableContent));
    BM_BufferPool* bp=MAKE_POOL();
    BM_PageHandle* ph=MAKE_PAGE_HANDLE();
    tbcontent->name=name;
    tbcontent->numRecords=0;
    tbcontent->bufferPool=bp;
    tbcontent->pageHandle=ph;
    rel->mgmtData=tbcontent;
//
///initialize page file
    initStorageManager();
    int rtRC=initBufferPool(bp,name,5,RS_FIFO,NULL);
    if(rtRC!=RC_OK){
        printf("initBufferPool Failed in creatTable\n");
        return RC_OK;
    }



    //Read schema and other info from first page
    pinPage(bp,ph,0);

    char* pagedata=ph->data;
    int offset=0;// write offset of page
    //write schema to the first page
/*
 * typedef struct Schema
{
	int numAttr;
	char **attrNames;
	DataType *dataTypes;
	int *typeLength;
	int *keyAttrs;
	int keySize;
} Schema;*/
    Schema* schema=malloc(sizeof(Schema));
    memcpy(&(schema->numAttr),pagedata+offset, sizeof(int));// copy schema->numAttr
    offset+= sizeof(int);
    //read Typelength
    schema->typeLength=malloc(sizeof(int)*schema->numAttr);
    memcpy(schema->typeLength, pagedata+offset,sizeof(int)*schema->numAttr);
    offset+=sizeof(int)*(schema->numAttr);

    /////read DataTypes,dataType is at least 1 byte i.e. 1 char, a liitle  trick here.
    schema->dataTypes= malloc(sizeof(DataType)*schema->numAttr);//fix allocate memory first
    memcpy(schema->dataTypes, pagedata+offset,sizeof(DataType)*schema->numAttr);
    offset+=sizeof(DataType) * schema->numAttr;
    ////////////////////////////////////now write attrNames;
    schema->attrNames=malloc(sizeof(char*)*schema->numAttr);
    for (int i = 0; i < schema->numAttr; ++i) {
        switch (schema->dataTypes[i])
        {
            case DT_STRING:
                schema->attrNames[i]=malloc(schema->typeLength[i]* sizeof(char));
                memcpy(schema->attrNames[i],pagedata+offset,schema->typeLength[i]* sizeof(char));
                offset += schema->typeLength[i]* sizeof(char);// string attr length is the number of chars
                break;
            case DT_INT:
                schema->attrNames[i]=malloc(sizeof(int));
                memcpy(schema->attrNames[i],pagedata+offset,sizeof(int));
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                schema->attrNames[i]=malloc(sizeof(float));
                memcpy(schema->attrNames[i],pagedata+offset,sizeof(float));
                offset += sizeof(float);
                break;
            case DT_BOOL:
                schema->attrNames[i]=malloc(sizeof(bool));
                memcpy(schema->attrNames[i],pagedata+offset,sizeof(bool));
                offset += sizeof(bool);
                break;
            default:
                printf("undefined dataType\n");
                return RC_FILE_NOT_FOUND;
        }

    }
////////////////////////////////////////////////////////////////////////////////////

    /////read keySize
    memcpy(&(schema->keySize), pagedata+offset,sizeof(int));// copy schema->keySize
    offset+= sizeof(int);
    /////////////////////////////////////////////////////////////////
    //read keyAttrs
    schema->keyAttrs=malloc(sizeof(int)*schema->keySize);
    memcpy(schema->keyAttrs, pagedata+offset,sizeof(int)*schema->keySize);
    offset+=sizeof(int) * schema->keySize;
    //all finished,schema is well constructed.
    offset=PAGE_SIZE/2-1;
    //int numPagesTotall=(PAGE_SIZE/2)/ sizeof(bool);
    memcpy(&(tbcontent->numRecords),pagedata+offset-sizeof(int), sizeof(int));
    bool* pageF= malloc(sizeof(bool)*numPagesTotall);
    int*  numSlotsPage=malloc(sizeof(int)*numPagesTotall);

    memcpy(pageF,pagedata+offset, sizeof(bool)*numPagesTotall);
    offset+=sizeof(bool)*numPagesTotall;
    memcpy(numSlotsPage,pagedata+offset,sizeof(int)*numPagesTotall);
    offset+=sizeof(int)*numPagesTotall;
    /*
    for (int j = 0; j < numPagesTotall; ++j) {
        pageF[j]=*(pagedata+offset);
        offset+= sizeof(bool);
        numSlotsPage[j]=  *((int *) (pagedata + offset));
        offset+= sizeof(int);
    }
*/
    unpinPage(bp,ph);//unpin first page
    tbcontent->numSlotsinPage=numSlotsPage;
    tbcontent->pagesF=pageF;
    rel->schema=schema;
    rel->name=name;
    numSlotsTotall=(PAGE_SIZE-(PAGE_SIZE/8))/getRecordSize(schema);
    return RC_OK;
}

//Closing a table should cause all outstanding changes to
// the table to be written to the page file
RC closeTable (RM_TableData *rel)
{

    if(rel==NULL)//NULL pointer no need to do anything
        return RC_OK;
    TableContent* tbcontent=rel->mgmtData;
    //free tbcontent first


    pinPage(tbcontent->bufferPool,tbcontent->pageHandle,0);

    int offset=PAGE_SIZE/2-1;

    char* pagedata=tbcontent->pageHandle->data;

    memcpy(pagedata+offset-sizeof(int),&(tbcontent->numRecords), sizeof(int));
    bool* pageF=tbcontent->pagesF;
    int temp=sizeof(bool);
    memcpy(pagedata+offset, tbcontent->pagesF,sizeof(bool)*numPagesTotall);
    offset+=sizeof(bool)*numPagesTotall;
    memcpy(pagedata+offset,tbcontent->numSlotsinPage,sizeof(int)*numPagesTotall);
    offset+=sizeof(int)*numPagesTotall;
    markDirty(tbcontent->bufferPool,tbcontent->pageHandle);
    unpinPage(tbcontent->bufferPool,tbcontent->pageHandle);//unpin first page
    forceFlushPool(tbcontent->bufferPool);
    shutdownBufferPool(tbcontent->bufferPool);
    free(tbcontent->bufferPool);
    free(tbcontent->pageHandle);
    free(tbcontent->pagesF);
    free(tbcontent->numSlotsinPage);
    free(tbcontent);

    //free schema
    ////*
    // * typedef struct Schema
    //{
    //	int numAttr;
    //	char **attrNames;
    //	DataType *dataTypes;
    //	int *typeLength;
    //	int *keyAttrs;
    //	int keySize;
    //} Schema;*/

    rel->schema=NULL;
    rel->mgmtData=NULL;
    return RC_OK;
}


RC deleteTable (char *name)
{
    if (name==NULL)
    {
        printf("name in NULL in deleteTable");
        return RC_FILE_NOT_FOUND;
    }
    return  destroyPageFile(name);

}


int getNumTuples (RM_TableData *rel)
{
    if(rel==NULL) {//NULL pointer no need to do anything
        printf("NULL table data in insertRecord \n ");
        return RC_FILE_NOT_FOUND;
    }
    TableContent* tbcontent=rel->mgmtData;
    return tbcontent->numRecords;
}

// handling records in a table




//

//insert a new record
RC insertRecord (RM_TableData *rel, Record *record)
{
    if(rel==NULL) {//NULL pointer no need to do anything
        printf("NULL table data in insertRecord \n ");
        return RC_FILE_NOT_FOUND;
    }
    if(record==NULL)
    {
        printf("Inset NULL record  data in insertRecord \n ");
        return RC_FILE_NOT_FOUND;
    }
    TableContent* tbcontent=rel->mgmtData;
    BM_BufferPool* bp=tbcontent->bufferPool;
    BM_PageHandle* ph=tbcontent->pageHandle;

    int pageid=findFreePage(rel);
    pinPage(bp,ph,pageid);
    char* pagedata=ph->data;

    SlotIndex SI;
    SI.slotindex=GetSlotIndex(pagedata);
    int slotid=findAFreeSlot(&SI);/////////////////////////////////////

    if(pageid<0||slotid<0) {
        printf("No free page or slot\n");
        return RC_WRITE_FAILED;
    }
    record->id.slot=slotid;
    record->id.page=pageid;
    int recordsize=getRecordSize(rel->schema);

    int offset=PAGE_SIZE/8;//The first 1/8 page is used to store slot info,
    offset+=slotid*recordsize;
    //now we can put data into page
    memcpy(pagedata+offset,record->data,recordsize);

    //change the flag in the SlotIndex
    MarkslotFlag(slotid,&SI,pagedata);
    free(SI.slotindex);

    tbcontent->numRecords++;
    tbcontent->numSlotsinPage[pageid]++;
    if(tbcontent->numSlotsinPage[pageid]==numSlotsTotall)
    {
        tbcontent->pagesF[pageid]=TRUE;//This page isFull
    }
    markDirty(bp,ph);
    unpinPage(bp,ph);
    return RC_OK;
}

//delete a record with a certain RID
RC deleteRecord (RM_TableData *rel, RID id)
{
    if(rel==NULL) {//NULL pointer no need to do anything
        printf("NULL table data in deleteRecord \n ");
        return RC_FILE_NOT_FOUND;
    }
    if(id.page<0||id.slot<0)
    {
        printf("RID invalid in deleteRecord \n ");
        return RC_FILE_NOT_FOUND;
    }

    TableContent* tbcontent=rel->mgmtData;
    BM_BufferPool* bp=tbcontent->bufferPool;
    BM_PageHandle* ph=tbcontent->pageHandle;
/*
    if(tbcontent->pagesF[id.page]==false)
    {
        // page id error
        printf("RID.pageid invalid in deleteRecord\n");
        return RC_IM_KEY_NOT_FOUND;
    }*/
    pinPage(bp,ph,id.page);
    SlotIndex SI;
    SI.slotindex=GetSlotIndex(ph->data);
    if(!TestslotFlag(id.slot,&SI))
    {
        // slot id error
        printf("RID.slotid invalid in deleteRecord\n");
        return RC_IM_KEY_NOT_FOUND;
    }
    FreeSlotFlag(id.slot,&SI,ph->data);
    if(tbcontent->pagesF[id.page]==TRUE)
        tbcontent->pagesF[id.page]=FALSE;
    tbcontent->numRecords--;
    tbcontent->numSlotsinPage[id.page]--;
    if(tbcontent->numSlotsinPage[id.page]<numSlotsTotall)
    {
        tbcontent->pagesF[id.page]=TRUE;//This page isFull
    }
    markDirty(bp,ph);
    unpinPage(bp,ph);
    return RC_OK;
}

//update an existing record with new values
RC updateRecord (RM_TableData *rel, Record *record)
{
    if(rel==NULL) {//NULL pointer no need to do anything
        printf("NULL table data in updateRecord \n ");
        return RC_FILE_NOT_FOUND;
    }
    if(record==NULL)
    {
        printf("NULL record  data in updateRecord \n ");
        return RC_FILE_NOT_FOUND;
    }

    TableContent* tbcontent=rel->mgmtData;
    BM_BufferPool* bp=tbcontent->bufferPool;
    BM_PageHandle* ph=tbcontent->pageHandle;

    RID rid=record->id;

    int recordsize=getRecordSize(rel->schema);
    if(tbcontent->pagesF[rid.page]==false)
    {
        // page id error
        printf("RID.pageid invalid in updateRecord\n");
        return RC_IM_KEY_NOT_FOUND;
    }
    pinPage(bp,ph,rid.page);
    SlotIndex SI;
    SI.slotindex=GetSlotIndex(ph->data);
    if(!TestslotFlag(rid.slot,&SI))
    {
        // slot id error
        printf("RID.slotid invalid in updateRecord\n");
        return RC_IM_KEY_NOT_FOUND;
    }
    int offset=PAGE_SIZE/8;//The first 1/8 page is used to store slot info,

    offset+=rid.slot*recordsize;
    //now we can put data into page
    memcpy(ph->data+offset,record->data,recordsize);


    tbcontent->numRecords++;
    tbcontent->numSlotsinPage[rid.page]++;
    markDirty(bp,ph);
    unpinPage(bp,ph);
    return RC_OK;
}

//retrieve a record with a certain RID
RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    if(rel==NULL) {//NULL pointer no need to do anything
        printf("NULL table data in getRecord \n ");
        return RC_FILE_NOT_FOUND;
    }
    if(record==NULL)
    {
        printf("NULL record  data in getRecord \n ");
        return RC_FILE_NOT_FOUND;
    }
    if(id.page<0||id.slot<0)
    {
        printf("RID invalid in getRecord \n ");
        return RC_FILE_NOT_FOUND;
    }

    TableContent* tbcontent=rel->mgmtData;
    BM_BufferPool* bp=tbcontent->bufferPool;
    BM_PageHandle* ph=tbcontent->pageHandle;

    int recordsize=getRecordSize(rel->schema);
    /* if(tbcontent->pagesF[id.page]==false)
     {
         // page id error
         printf("RID.pageid invalid in getRecord\n");
         return RC_IM_KEY_NOT_FOUND;
     }*/
    pinPage(bp,ph,id.page);
    SlotIndex SI;
    SI.slotindex=GetSlotIndex(ph->data);
    if(!TestslotFlag(id.slot,&SI))
    {
        // slot id error
        printf("RID.slotid invalid in getRecord\n");
        return RC_IM_KEY_NOT_FOUND;
    }
    int offset=PAGE_SIZE/8;//The first 1/8 page is used to store slot info,

    offset+=id.slot*recordsize;
    //now we can get data from page
    memcpy(record->data,ph->data+offset,recordsize);
    record->id=id;
    unpinPage(bp,ph);
    return RC_OK;
}

typedef struct ScanHandle_aux_t{

    Expr* cond;
    Record* lastHitrecord;
    int numScanned;
} ScanHandleContent;


// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    if(rel==NULL||scan==NULL||cond==NULL){

        printf("startScan with NULL pointer\n");
        return RC_WRITE_FAILED;
    }
    TableContent* tbcontent=rel->mgmtData;
    BM_BufferPool* bp=tbcontent->bufferPool;
    BM_PageHandle* ph=tbcontent->pageHandle;
    openTable(rel,tbcontent->name);

    scan->rel=rel;

    ScanHandleContent* shc= malloc(sizeof(ScanHandleContent));
    shc->lastHitrecord=NULL;
    shc->cond=cond;
    shc->numScanned=0;
    scan->mgmtData=shc;


    return RC_OK;
}
RC next (RM_ScanHandle *scan, Record *record) {


    RM_TableData *rel = scan->rel;
    ScanHandleContent *shc = scan->mgmtData;

    TableContent *tbcontent = rel->mgmtData;
    BM_BufferPool *bp = tbcontent->bufferPool;
    BM_PageHandle *ph = tbcontent->pageHandle;

    if (tbcontent->numRecords < 1 || tbcontent->numRecords == shc->numScanned) {
        //empty table or Scanned Table
        return RC_RM_NO_MORE_TUPLES;
    }

    Expr *cond = shc->cond;
    Value *evalResult = malloc(sizeof(Value));

    int numRecords = getNumTuples(rel);
    //Record *tempRecord;
    //createRecord(&record, rel->schema);
    RID tempid;
    SlotIndex si;
    si.slotsBool = malloc(PAGE_SIZE / 8);
    si.slotindex = malloc(sizeof(char) * (PAGE_SIZE / 8));
    bool reachEnd = false;


    if (shc->lastHitrecord == NULL) {
        //Page 1 reserved for table info;
        tempid.page = 1;
        tempid.slot = 0;
        si.slotsNum = tbcontent->numSlotsinPage[tempid.page];
        pinPage(bp, ph, tempid.page);
        si.slotindex = GetSlotIndex(ph->data);
        Bits2Bools(&si);
    } else {
        tempid = shc->lastHitrecord->id;
        pinPage(bp, ph, tempid.page);
        si.slotsNum = tbcontent->numSlotsinPage[tempid.page];
        si.slotindex = GetSlotIndex(ph->data);
        Bits2Bools(&si);
        //go scan next record
        tempid.slot++;
        //go scan next record
        while (si.slotsBool[tempid.slot] != TRUE) {// if the record is valid break.
            tempid.slot++;
            if (tempid.slot > si.slotsNum - 1) {// change to next page
                //go next record page
                tempid.slot = 0;
                tempid.page++;
                if (tempid.page >= numPagesTotall) {

                    //we've get the end of the page file
                    reachEnd = true;
                    break;
                }
                unpinPage(bp, ph);
                pinPage(bp, ph, tempid.page);
                si.slotsNum = tbcontent->numSlotsinPage[tempid.page];
                si.slotindex = GetSlotIndex(ph->data);
                Bits2Bools(&si);
            }
        }
    }
    //pinPage(bp, ph, tempid.page);
    while (shc->numScanned <= numRecords) {

        getRecord(rel, tempid, record);
        RC rc = evalExpr(record, rel->schema, cond, &evalResult);
        shc->numScanned++;
        if (rc == RC_OK && evalResult->v.boolV) {

            shc->lastHitrecord = record;
            record->id = tempid;
            /*record->data = tempRecord->data;*/
            break;
        }
        else if (shc->numScanned >= numRecords) {
            reachEnd = true;
            break;

        }else{
            //go scan next record
            tempid.slot++;
            while (si.slotsBool[tempid.slot] != TRUE) {// if the record is valid break.

                tempid.slot++;

                if (tempid.slot > si.slotsNum - 1) {// change to next page
                    //go next record page
                    tempid.slot = 0;
                    tempid.page++;
                    if (tempid.page >= numPagesTotall) {

                        //we've get the end of the page file
                        reachEnd = true;
                        break;
                    }
                    unpinPage(bp, ph);
                    pinPage(bp, ph, tempid.page);
                    si.slotsNum = tbcontent->numSlotsinPage[tempid.page];
                    si.slotindex = GetSlotIndex(ph->data);
                    Bits2Bools(&si);
                }

            }
            if (reachEnd == true)break;
        }


    }
    unpinPage(bp, ph);
    free(si.slotsBool);
    free(si.slotindex);
    //free(tempRecord);
    free(evalResult);
    if (reachEnd == true)return RC_RM_NO_MORE_TUPLES;
    return RC_OK;
}

RC closeScan(RM_ScanHandle *scan) {

    free(scan->mgmtData);
    scan->rel = NULL;
    scan->mgmtData = NULL;
    return RC_OK;
}

// dealing with schemas
//return the size in bytes of records
int getRecordSize(Schema *schema) {

    if (schema == NULL) {
        printf("input NULL pointer in getRecordSize\n");
        return -1;
    }
    int recordsize = 0;
    for (int i = 0; i < schema->numAttr; ++i) {
        switch (schema->dataTypes[i]) {
            case DT_STRING:
                recordsize += schema->typeLength[i] * sizeof(char);
                break;
            case DT_INT:
                recordsize += sizeof(int);
                break;
            case DT_FLOAT:
                recordsize += sizeof(float);
                break;
            case DT_BOOL:
                recordsize += sizeof(bool);
                break;
            default:
                printf("undefined dataType\n");
                return RC_FILE_NOT_FOUND;
        }
    }
    return recordsize;
}

//create a new schema
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {

// first thing is check input validity
    if (numAttr < 0) {
        printf("numAttr less than 0 in createSchema\n");
        return NULL;
    }

    if (attrNames == NULL) {
        printf(" attrNames pointer is zero in createSchema\n ");
        return NULL;
    } else {
        for (int i = 0; i < numAttr; ++i) {
            if (attrNames[i] == NULL) {
                printf(" attrNames pointer is zero in createSchema\n ");
                return NULL;
            }
        }
    }

    if (dataTypes == NULL) {
        printf("dataTypes is NULL in createSchema\n");
        return NULL;
    }

    if (typeLength == NULL) {
        printf("typeLength is NULL in createSchema\n");
        return NULL;
    }
    if (keySize < 0) {
        printf("keySize less than 0 in createSchema\n");
        return NULL;
    }
    if (keys == NULL) {
        printf("keys is NULL in createSchema\n");
        return NULL;
    }
    ///we have to make sure every input is valid before we allocate memory
///////////////////////////////////////////

/*
 * typedef struct Schema
{
	int numAttr;
	char **attrNames;
	DataType *dataTypes;
	int *typeLength;
	int *keyAttrs;
	int keySize;
} Schema;
 * */
    Schema *rt = malloc(sizeof(Schema));
    rt->numAttr = numAttr;
    rt->attrNames = attrNames;
    rt->dataTypes = dataTypes;
    rt->typeLength = typeLength;
    rt->keyAttrs = keys;
    rt->keySize = keySize;

    return rt;
}

//free schema and its content
RC freeSchema(Schema *schema) {
//Note: this fucntion hasn't be called in test_assign3, it may be used insider our code
    if (schema == NULL)
        return RC_OK; //return OK because the code runs without error
    if (schema->numAttr < 1) {// no allocated memory, free the schema itself only
        free(schema);
        return RC_OK;
    }


    if (schema->attrNames != NULL) {
        for (int i = 0; i < schema->numAttr; ++i) {// free arrary content first
            if (schema->attrNames[i] != NULL)
                free(schema->attrNames[i]);
        }

        free(schema->attrNames);// free array itself
    }

    if (schema->dataTypes != NULL) {
        free(schema->dataTypes);
    }

    if (schema->typeLength != NULL) {
        free(schema->typeLength);
    }

    if (schema->keyAttrs != NULL) {
        free(schema->keyAttrs);
    }

    free(schema);//last step free schema itself
    return RC_OK;
}

// dealing with records and attribute values

/*
 typedef struct RID {
	int page;
	int slot;
} RID;

typedef struct Record
{
	RID id;
	char *data;
} Record;
 *
 **/

//create a new record for a given schema, allocate memory
RC createRecord(Record **record, Schema *schema) {
    //check input first
    if (schema == NULL) {
        printf("Schema pointer is NUll in creatRecord \n");
        return RC_FILE_NOT_FOUND;
    }

    Record *rtrecord = malloc(sizeof(Record));//allocate memory for return value of record

    rtrecord->id.page = -1;
    rtrecord->id.slot = -1;

    rtrecord->data = malloc(getRecordSize(schema));
    *record = rtrecord;
    return RC_OK;
}

//free record memory
RC freeRecord(Record *record) {
    if (record != NULL) {
        free(record->data);
        free(record);
    }
    return RC_OK;
}

//get the attribute values of a record, we will allocate memory for value
// the user should free it after use.
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
//first check input validity
    if (record == NULL || schema == NULL) {
        printf("arguments have NULL pointers in setAttr\n");
        return RC_FILE_NOT_FOUND;
    }
    if (attrNum > (schema->numAttr - 1)) {
        printf("arguments have NULL pointers in setAttr\n");
        return RC_FILE_NOT_FOUND;
    }

    //find the position of the attribute to be set in record->data
    //note here string is stored without the end '0'
    //data is stored without any space between two attr
    int offset = 0;
    for (int i = 0; i < attrNum; ++i) {
        switch (schema->dataTypes[i]) {
            case DT_STRING:
                offset += schema->typeLength[i] * sizeof(char);
                break;
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            default:
                printf("undefined dataType\n");
                return RC_FILE_NOT_FOUND;
        }
    }

    char *attr_source = record->data + offset;


    //allocate mem for return value
    Value *rtvalue = malloc(sizeof(Value));
    rtvalue->dt = schema->dataTypes[attrNum];
    ////////////////////////////////////
    //copy attr


    switch (schema->dataTypes[attrNum]) {
        case DT_STRING:
            rtvalue->v.stringV = malloc(schema->typeLength[attrNum] + 1);
            rtvalue->v.stringV[schema->typeLength[attrNum]] = 0;
            memcpy(rtvalue->v.stringV, attr_source, schema->typeLength[attrNum]);
            break;
        case DT_INT:
            memcpy(&(rtvalue->v.intV), attr_source, sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(&(rtvalue->v.floatV), attr_source, sizeof(float));
            break;
        case DT_BOOL:
            memcpy(&(rtvalue->v.boolV), attr_source, sizeof(bool));
            break;
        default:
            printf("undefined dataType\n");
            return RC_FILE_NOT_FOUND;
    }
    *value = rtvalue;//set up return Value
    return RC_OK;
}

//set the attribute values of a record,attrNum start from 0
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    //first check input validity
    if (record == NULL || schema == NULL || value == NULL) {
        printf("arguments have NULL pointers in setAttr\n");
        return RC_FILE_NOT_FOUND;
    }
    if (attrNum > (schema->numAttr - 1)) {
        printf("arguments have NULL pointers in setAttr\n");
        return RC_FILE_NOT_FOUND;
    }
    if (value->dt != schema->dataTypes[attrNum])// if schema is valid, its members too
    {
        printf("dataType inconsistent in setAttr\n");
        return RC_WRITE_FAILED;
    }
    //find the position of the attribute to be set in record->data
    //note here string is stored without the end '0'
    //data is stored without any space between two attr
    int offset = 0;
    for (int i = 0; i < attrNum; ++i) {
        switch (schema->dataTypes[i]) {
            case DT_STRING:
                offset += schema->typeLength[i] * sizeof(char);// string attr length is the number of chars
                break;
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            default:
                printf("undefined dataType\n");
                return RC_FILE_NOT_FOUND;
        }
    }

    char *dest = record->data + offset;
    //copy memory
    switch (schema->dataTypes[attrNum]) {
        case DT_STRING:
            memcpy(dest, value->v.stringV, schema->typeLength[attrNum]);
            break;
        case DT_INT:
            memcpy(dest, &(value->v.intV), sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(dest, &(value->v.floatV), sizeof(float));
            break;
        case DT_BOOL:
            memcpy(dest, &(value->v.boolV), sizeof(bool));
            break;
        default:
            printf("undefined dataType\n");
            return RC_FILE_NOT_FOUND;
    }

    return RC_OK;
}
