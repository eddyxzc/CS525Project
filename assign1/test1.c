//
// Created by eddy on 1/28/20.
//
#include <assert.h>
#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"
/* test output files */
#define TESTPF "testfile.bin"
int main()
{
    SM_FileHandle fh;
    SM_PageHandle ph;
    int i;

    char* testName = "test single page content";

    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    // create a new page file
    TEST_CHECK(createPageFile (TESTPF));
    TEST_CHECK(openPageFile (TESTPF, &fh));
    printf("created and opened file\n");

    // read last page into handle
    TEST_CHECK(readLastBlock (&fh, ph));
    // the page should be empty (zero bytes)
    for (i=0; i < PAGE_SIZE; i++)
        assert (ph[i] == 0);
    printf("readLastBlock test Done\n");

    // try to read 2nd page, which does not exist.
    ASSERT_TRUE((readBlock(1,&fh, ph) != RC_OK), "opening non-existing file should return an error.");
    RC rc=readBlock(1,&fh, ph);

    // change ph to be a string and write that one to disk
    for (i=0; i < PAGE_SIZE; i++)
        ph[i] = (i % 10) + '0';

    // should be five blocks now
    TEST_CHECK(writeBlock (4, &fh, ph));
    printf("writing 5th block with writeBlock, test expand page file \n");


    // read back the page containing the string and check that it is correct
    //using readLastBlock
    TEST_CHECK(readLastBlock (&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        assert(ph[i] == (i % 10) + '0');
    printf("readLastBlock test done\n");

    // read back the page containing the string and check that it is correct
    //using readBlock
    TEST_CHECK(readBlock (4,&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        assert(ph[i] == (i % 10) + '0');
    printf("readLastBlock test done\n");

    // read the expanded page, should be zeros.
    TEST_CHECK(readBlock(2,&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        assert (ph[i] == 0);
    printf("readBlock test done\n");

    // read the expanded page, should be zeros.
    TEST_CHECK(readNextBlock(&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        assert (ph[i] == 0);
    printf("readNextBlock test done\n");


    // change ph to be a string and write that one to disk
    for (i=0; i < PAGE_SIZE; i++)
        ph[i] = (i % 10) + '0';

    // write 4th block with writeCurrentBlock
    TEST_CHECK(writeCurrentBlock(&fh, ph));

    printf("writing 4th block\n");
    printf("test writeCurrentBlock done");

    // read 4th block with readCurrentBlock
    TEST_CHECK(readCurrentBlock(&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        assert(ph[i] == (i % 10) + '0');

    printf("reading 4th block\n");
    printf("test readCurrentBlock done");

    // read 3rd block with readCurrentBlock
    TEST_CHECK(readPreviousBlock(&fh, ph));
    printf("reading 3rd block\n");

    // read 2nd block with readCurrentBlock
    TEST_CHECK(readPreviousBlock(&fh, ph));
    printf("reading 2nd block\n");

    // change ph to be a string and write that one to disk
    for (i=0; i < PAGE_SIZE; i++)
        ph[i] = (i % 10) + '0';
    // write 2nd block with writeCurrentBlock
    TEST_CHECK(writeCurrentBlock(&fh, ph));
    printf("writing 2nd block\n");

    // read 2nd block with readCurrentBlock
    TEST_CHECK(readBlock(1,&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        assert(ph[i] == (i % 10) + '0');
    printf("reading 2nd block\n");

    closePageFile(&fh);
    // destroy new page file
    TEST_CHECK(destroyPageFile (TESTPF));

    TEST_DONE();

    printf("pass all test in test1\n");


    return 0;
}