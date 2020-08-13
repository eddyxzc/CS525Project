//To build:
MakeFile was made in the code directory

or cmake

make cmake both work.

Passed test_assign3_1 and test_expr

// We use first page store schema and table info
// All records are put at pages from the second page
//On each records page, first 4096bits i.e. 512Bytes are reserved for indexing




Function : createTable
    open the buffer pool after creating the page file
    
Function : openTable
    open the buffer pool
    all operations on a table such as scanning or inserting records require the table to be opened first precondition rel is allocated memory

Function : closeTable
    close the buffer pool
    closing a table should cause all outstanding changes to the table to be written to the page file

Function : deleteTable
    destroys the relative page file

Function : getNumTuples
    get the number of tuples

Function : insertRecord
    insert a new record

Function : deleteRecord
    delete a record with a certain RID

Function : updateRecord
    update an existing record with new values

Function: getRecord
    retrieve a record with a certain RID

Function : startScan
    assign values to the scanning manager

Function : next
    assign values to the scanning manager from earlier scanning managers mgmtdata

Function : closeScan
    free expression structure and free scanning manager datastrcture and reciding mgmtdata

Function : getRecordSize
    get the size of a record in a given schema

dealing with schemas
    return the size in bytes of recordsFunction : createSchema
    create a new schema

Function : freeSchema
    free the space occupied by each schema

Function : createRecord
    create a new record for a given schema, allocate memory

Function : freeRecord
    free record memory

Function : getAttr
    get the attribute values of a record, we will allocate memory for value
    the user should free it after use.

Function : setAttr
    set the attribute values of a record,attrNum start from 0