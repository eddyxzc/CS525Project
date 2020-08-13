#include "btree_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>
#include <limits.h>
#include <malloc.h>
#include <assert.h>
//
// Created by eddy on 4/18/20.
//
/*
typedef struct BTreeHandle {
	DataType keyType;
	char* idxId;
	void* mgmtData;
} BTreeHandle;

typedef struct BT_ScanHandle {
	BTreeHandle* tree;
	void* mgmtData;
} BT_ScanHandle;
*/


typedef struct NodeEntry_s
{
	void* points;
	RID* rid;

}NodeEntry;


typedef unsigned int uint;



typedef int key_t; 
#define INVALID_KEY	 INT_MAX
typedef RID value_t;
/* internal nodes' index segment */
struct index_t {
	key_t key;
	uint child; /* child's offset */
};

/***
 * internal node block
 ***/


#define BP_ORDER  12
typedef struct index_t* child_t;
typedef enum NodetType
{
	
  Nonleaf,
  Leaf
}NodeType;

typedef union v_u {
	int child_ptr;
	value_t value;
} nodevalue;
typedef struct filenode_t {

    int self;
	int parent; /* parent node page */
	int next;
	int prev;
	
	key_t key[BP_ORDER+1];
	NodeType type;
	int nchild; /* how many children */
	nodevalue nodeValue[BP_ORDER+1];

}TreeNode;
/*
typedef struct internal_node_t {
	
	uint parent; /* parent node offset */
	//uint next;
	//uint prev;
	//uint n; /* how many children */
	//uint children;// [BP_ORDER] ;
/*}Internal_Node;/**/

/* the final record of value */
/*
typedef struct record_t {
	key_t key;
	value_t value;
}record;
*/

////////////////start OF QUEUE?????????????
#define MAXQUEUESIZE 64
//free block queue, we need this queue to store the block hole after the node is delete from disk
// If  a node is deleted from disk, its page number(block) will be enqueue
// Once we creat a new node, we need get a free block(page number) from this queue, if queue is empty, get a new page number
// allocate a new block in the disk
typedef struct arry_queue
{
	int buf[MAXQUEUESIZE];
	int rear;
	int front;
}Queue;

void queueInit(Queue* q)
{
	q->rear = 0;
	q->front = 0;
}
bool queueIsEmpty(Queue* q)
{
	if (q->rear == q->front)
		return true;
	else
		return false;
}
bool queueIsFull(Queue* q)
{
	if ((q->rear + 1) % MAXQUEUESIZE == q->front)
		return true;
	else
		return false;
}
bool queueEnQueue(Queue* q,int data)
{
	if (queueIsFull(q))
		return false;
	q->buf[q->rear] = data;
	q->rear = (q->rear + 1) % MAXQUEUESIZE;
	return true;
}
bool queueDeQueue(Queue* q,int data)
{
	if (queueIsEmpty(q))
		return false;
	data = q->buf[q->front];
	q->front = (q->front + 1) % MAXQUEUESIZE;
	return true;
}
////////////////END OF QUEUE?????????????



typedef struct Btree_meta_s
{
	int order; /* `order` of B+ tree */
	int numNodes;
	int numEntries;
	//int height;            /* height of tree (exclude leafs) */
	//uint root_offset; /* where is the root of internal nodes */
	//uint leaf_offset; /* where is the first leaf */
	DataType keytype;
	int root;// root page number should be 1 
	Queue freeblock;
}Btree_meta;
typedef struct BplusTree_s
{
	BM_BufferPool* bufferPool;
	BM_PageHandle* pageHandler;
	//char* filename;
	Btree_meta* meta;
	TreeNode* root;
}BplusTree;














RC initIndexManager(void* mgmtData)
{
	return RC_OK;
}

RC shutdownIndexManager()
{
	return RC_OK;
}

RC createBtree(char* idxId, DataType keyType, int n)
{

	//put bplus tree metainfo on first page;
	// root node on 1st page;
	// no leaf node or internal node on 2nd page;

	RC rt;
	int maxnodes1Page = PAGE_SIZE / sizeof(TreeNode);
	if (sizeof(TreeNode)*(n+1)>PAGE_SIZE)
	{

		printf(" n too large in CreatBtree\n");

		return RC_IM_N_TO_LAGE;
	}
		
	int rtRC = createPageFile(idxId);
	if (rtRC != RC_OK) {
		printf("creatPageFile Failed in createBtree\n");
		return RC_FILE_NOT_FOUND;
	}
	BM_BufferPool* bp = ((BM_BufferPool*)malloc(sizeof(BM_BufferPool)));
	rtRC = initBufferPool(bp, idxId, 21, RS_FIFO, NULL);// open 21 buffer for 20 nodes plus one node
	if (rtRC != RC_OK) {
		printf("initBufferPool Failed in createBtree\n");
		return RC_FILE_NOT_FOUND;
	}
	BM_PageHandle* ph = MAKE_PAGE_HANDLE();

	
	Btree_meta meta;
	meta.keytype = keyType;
	meta.numEntries = 0;
	meta.numNodes = 0;
	meta.order = n;
	meta.root = INVALID_KEY;
	queueInit(&(meta.freeblock));//init free block queue
	//write meta on first page
	pinPage(bp, ph, 0);
	assert(sizeof(meta) <= PAGE_SIZE);
	memcpy(ph->data,&meta,sizeof(meta));
	markDirty(bp, ph);
	unpinPage(bp, ph);
	/*
	//write root nodes without childs on first page;
	TreeNode root;
	root.self = 1;
	root.key[0] = INVALID_KEY;// invalid key
	root.nodeValue[0].child_ptr = 2;// dummy child node
	root.nchild = 0;
	root.type = Nonleaf;
	pinPage(bp, ph, 1);
	memcpy(ph->data, &root, sizeof(TreeNode));
	markDirty(bp, ph);
	unpinPage(bp, ph);
*/

	forceFlushPool(bp);
	free(ph);
	free(bp);
	return RC_OK;
}

RC openBtree(BTreeHandle** tree, char* idxId)
{
	
	
	BM_BufferPool* bp = ((BM_BufferPool*)malloc(sizeof(BM_BufferPool)));
	int rtRC = initBufferPool(bp, idxId, 21, RS_FIFO, NULL);// open 21 buffer for 20 nodes plus one node
	if (rtRC != RC_OK) {
		printf("initBufferPool Failed in openBtree\n");
		return RC_FILE_NOT_FOUND;
	}

	*tree = malloc(sizeof(BTreeHandle));
	Btree_meta* meta = malloc(sizeof(Btree_meta));

	BplusTree* bplustree = malloc(sizeof(BplusTree));
	//(*tree)->mgmtData

	//get meta
	BM_PageHandle* ph = MAKE_PAGE_HANDLE();
	pinPage(bp, ph, 0);
	memmove( meta,ph->data, sizeof(Btree_meta));
	unpinPage(bp, ph);

	bplustree->bufferPool = bp;
	bplustree->pageHandler = ph;
	bplustree->meta = meta;
	
	// get root
	BTreeHandle* btreehandle =malloc(sizeof(BTreeHandle)) ;
	btreehandle->idxId = idxId;
	btreehandle->keyType = meta->keytype;
	btreehandle->mgmtData = bplustree;

	*tree = btreehandle;
	return RC_OK;
}

RC closeBtree(BTreeHandle* tree)
{
	BplusTree* bplustree = tree->mgmtData;
	Btree_meta* meta = bplustree->meta;
	BM_BufferPool* bp = bplustree->bufferPool;
	BM_PageHandle* ph = bplustree->pageHandler;
	forceFlushPool(bp);
	free(bp);
	free(ph);
	free(meta);

	free(bplustree);

	free(tree);
	tree = NULL;

	return RC_OK;
}

RC deleteBtree(char* idxId)
{
	RC rt = destroyPageFile(idxId);
	return rt;
}

RC getNumNodes(BTreeHandle* tree, int* result)
{
	BplusTree* bplustree = tree->mgmtData;
	Btree_meta* meta = bplustree->meta;
	*result = meta->numNodes;
	return RC_OK;
}

RC getNumEntries(BTreeHandle* tree, int* result)
{
	BplusTree* bplustree = tree->mgmtData;
	Btree_meta* meta = bplustree->meta;
	*result = meta->numEntries;
	return RC_OK;
}

RC getKeyType(BTreeHandle* tree, DataType* result)
{
	BplusTree* bplustree = tree->mgmtData;
	Btree_meta* meta = bplustree->meta;
	*result = meta->keytype;
	return RC_OK;
}

int binarysearch_(key_t* keys, key_t target, int length)
{//easyily for testing
	int low = -1;
	int high = length;

	while (low + 1 < high) {
		int mid = low + (high - low) / 2;
		if (target > keys[mid]) {
			low = mid;
		}
		else {
			high = mid;
		}
	}

	if (high >= length || keys[high] != target) {
		return -high - 1;
	}
	else {
		return high;
	}
}
/* Function to get index of
ceiling of x in arr[low..high]*/
////if x exits in array, than return index of x, or return the first element whose index is bigger
//than x
int ceilSearch(key_t arr[], int low, int high, int x)
{
	int mid;

	/* If x is smaller than
	or equal to the first element,
	then return low */
	if (x <= arr[low])
		return low;

	/* If x is greater than the last element,
	then return high */
	if (x > arr[high])
		return -high-1;// if high =0, -high would be 0 again which is equals to low 0 when the arry has only one element

	/* get the index of middle element of arr[low..high]*/
	mid = (low + high) / 2; /* low + (high - low)/2 */

	/* If x is same as middle element,
	then return mid */
	if (arr[mid] == x)
		return mid;

	/* If x is greater than arr[mid],
	then either arr[mid + 1] is ceiling of x
	or ceiling lies in arr[mid+1...high] */
	else if (arr[mid] < x)
	{
		if (mid + 1 <= high && x <= arr[mid + 1])
			return mid + 1;
		else
			return ceilSearch(arr, mid + 1, high, x);
	}

	/* If x is smaller than arr[mid],
	then either arr[mid] is ceiling of x
	or ceiling lies in arr[mid-1...high] */
	else
	{
		if (mid - 1 >= low && x > arr[mid - 1])
			return mid;
		else
			return ceilSearch(arr, low, mid - 1, x);
	}
}
int binarysearch_ceil(TreeNode* node,key_t target)
{
	/*
	typedef struct filenode_t {

    uint self;
	uint parent; 
	uint next;
	uint prev;

	key_t key[BP_ORDER];
	NodeType type;
	int nchild; 
	union v {
		int child_ptr;
		value_t;
	} v[BP_ORDER+1];

}TreeNode;
	*/
	/*
	//Upper bound
	while (count > 0) {
		it = first;
		step = count / 2;
		std::advance(it, step);
		if (!comp(value, *it)) {
			first = ++it;
			count -= step + 1;
		}
		else
			count = step;
	}
	return first;
	
	*/
	int length;
	if (node->type == Leaf)
	{
		length = node->nchild;
	}
	else {
		length = node->nchild - 1;
	}
	key_t* arr =node->key;
	return ceilSearch(arr,0, length-1,target);
}



bool binaryFind(TreeNode* node, key_t target,int index) {
	int length;
	if (node->type == Leaf)
	{
		length = node->nchild;
	}
	else {
		length = node->nchild - 1;
	}
	key_t* arr = node->key;

	if (index<length && index>=0&&arr[index]==target)
	{
		return true;
	}
	else {
		return false;
	}
}
bool binarysearch(TreeNode* node, key_t target) {

	int rt = binarysearch_ceil(node, target);
	return binaryFind(node, target, rt);
}

//////////////////////////////////some function used to allocate node in file and in memory
//////// nodt, take care of the node in file and node reflection in memory///////
TreeNode* creatNewNodeinMem(NodeType t) {
	TreeNode* newnode = malloc(sizeof(TreeNode));
	newnode->self = INVALID_KEY;// page number in file
	newnode->prev = INVALID_KEY;
	newnode->next = INVALID_KEY;
	newnode->parent = INVALID_KEY;

	newnode->type = t;
	newnode->nchild = 0;
	//newnode->key[0] = key->v.intV;
	//newnode->nodeValue[0].value = rid;
	return newnode;
}
TreeNode* fetchSubNode(BplusTree* tree,TreeNode* node, uint keyindex)
{
	TreeNode* subtreeroot = creatNewNodeinMem(Leaf);
	int pagenumber = node->nodeValue[keyindex].child_ptr;
	pinPage(tree->bufferPool, tree->pageHandler, pagenumber);
	memcpy(subtreeroot, tree->pageHandler->data, sizeof(TreeNode));
	unpinPage(tree->bufferPool, tree->pageHandler);
	return subtreeroot;
}

TreeNode* fetchNodeFromDisk(BplusTree* tree, int pagenumber)
{
	assert(pagenumber > 0);
	if (pagenumber == INVALID_KEY)return NULL;
	TreeNode* subtreeroot = malloc(sizeof(TreeNode));
	pinPage(tree->bufferPool, tree->pageHandler, pagenumber);
	memcpy(subtreeroot, tree->pageHandler->data, sizeof(TreeNode));
	unpinPage(tree->bufferPool, tree->pageHandler);
	return subtreeroot;
}


void freeNodeinMem(TreeNode* node) {
	if (node != NULL) {
		free(node);
		node = NULL;
	}
}

int allocNodeinDisk(BplusTree* tree) {
	// return a free block number where we can store the node in disk
	Btree_meta* meta = tree->meta;
	if (queueIsEmpty(&(meta->freeblock))){
		meta->numNodes++;
		return meta->numNodes;// return a page number
	}
	else {
		int rt = -1;
		 queueDeQueue(&(meta->freeblock), rt);
		 return rt;
	}
}

void deallocNodeinDisk(BplusTree* tree, int pagenumber) {
	// set the block number to free, where the node stored there is removed
	// add the free block number to queue
	Btree_meta* meta = tree->meta;
	bool rt=queueEnQueue(&(meta->freeblock), pagenumber);
	if (!rt)// rear case
	{
		printf("full queue!\n");
	}
}
void pushNode2disk(BplusTree* tree, TreeNode* node)// push an already in mem node to disk, need the node already has a page number
{
	if (node == NULL)
	{
		return;
	};
	int pagenumber = node->self;
	pinPage(tree->bufferPool, tree->pageHandler, pagenumber);
	memcpy(tree->pageHandler->data, node, sizeof(TreeNode));
	markDirty(tree->bufferPool, tree->pageHandler);
	unpinPage(tree->bufferPool, tree->pageHandler);
}
void deleteNodefromTree(BplusTree* bplustree, TreeNode* deletenode, TreeNode* leftsib, TreeNode* rightsib)
{
	if (leftsib != NULL) {
		if (rightsib != NULL) {
			leftsib->next = rightsib->self;
			rightsib->prev = leftsib->self;
			pushNode2disk(bplustree, rightsib);
			
		}
		else {
			leftsib->next = INVALID_KEY;
		}
		pushNode2disk(bplustree, leftsib);
	
	}
	else {
		if (rightsib != NULL) {
			rightsib->prev = INVALID_KEY;
			pushNode2disk(bplustree, rightsib);
		}
		
	}
	bplustree->meta->numNodes--;
	deallocNodeinDisk(bplustree, deletenode->self);
}

///////////////////////////////////////////////////////////

RC findKey(BTreeHandle* tree, Value* key, RID* result)
{
	int rt = -1;

	BplusTree* bplustree =(BplusTree *) tree->mgmtData;
	
	if (bplustree->meta <= 0)// empty tree
	{
		return RC_IM_KEY_NOT_FOUND;
	}
    TreeNode* root = fetchNodeFromDisk(bplustree,bplustree->meta->root);
	int insertpos = binarysearch(root, key->v.intV);

	TreeNode* node= root;
	while (node != NULL) {
		int i = binarysearch_ceil(node, key->v.intV);
		if (node->type==Leaf) {// search to leaf node
			//rt = i >= 0 ? data(node)[i] : -1;

			if (binaryFind(node,key->v.intV,i))//found double cheack if target is inside node
			{
				result->page=node->nodeValue[i].value.page;
				result->slot = node->nodeValue[i].value.slot;
				rt = i;
			}
			else {
				rt = -1;
			}
			break;
		}
		else {// no leaf node
			if (i >= 0) {
				TreeNode* subnode;//equal go to right
				if (binaryFind(node, key->v.intV, i)) {
					subnode = fetchSubNode(bplustree, node, i + 1);
				}
				else {//less go to left
					subnode = fetchSubNode(bplustree, node, i);
				}
				freeNodeinMem(node);
				node = subnode;
				//node = node_seek(tree, sub(node)[i + 1]);
			}
			else {
				TreeNode* subnode = fetchSubNode(bplustree,node, node->nchild-1);
				freeNodeinMem(node);
				node = subnode;
				//node = node_seek(tree, sub(node)[i]);
			}
			
		}
	}
    freeNodeinMem(node);
	if (rt==-1)
	{
		return RC_IM_KEY_NOT_FOUND;
	}

	return RC_OK;
}


void addLeafnoSplit(TreeNode* leafNode, Value* key, RID rid, int insert) {
	// we need to put the new entry just before insert
	key_t* keyarr = leafNode->key;
	nodevalue* values = leafNode->nodeValue;
	int numkey=leafNode->nchild;

	if (insert==0)// insert the entry to the beginning
	{
		
		for (int i = numkey; i > 0; i--)
		{
			 keyarr[i]= keyarr[i-1];
			 values[i]= values[i-1];
		}
		keyarr[0] = key->v.intV;
		values[0].value = rid;
	}
	else if (insert<0) {// insert entry to the end
		
		keyarr[numkey] = key->v.intV;
		values[numkey].value = rid;
	}
	else
	{
		for (int i = numkey; i > insert; i--)
		{
			keyarr[i] = keyarr[i - 1];
			values[i] = values[i - 1];
		}
		keyarr[insert] = key->v.intV;
		values[insert].value = rid;
	}
	
	leafNode->nchild++;

}

void addNonLeafnoSplit(TreeNode* UPparent, TreeNode* left, TreeNode* right, key_t insertkey,int insert) {
	// we need to put the new entry just before insert
	key_t* keyarr = UPparent->key;
	nodevalue* values = UPparent->nodeValue;
	int numkey = UPparent->nchild;


	///[key0, key1, key2]
	///[less0,less1,less2, great2] -------->need update two index
	///[key0, key1, insert key 2]
	///[less0,less1,left,  right greter_than_2]
	 if (insert < 0) {// insert entry to the end
		 //need update two ptr
		keyarr[numkey-1] = insertkey;
		values[numkey-1].child_ptr= left->self;
		values[numkey].child_ptr = right->self;
	}
	else
	{
		for (int i = numkey; i > insert; i--)
		{
			keyarr[i] = keyarr[i - 1];
			values[i+1] = values[i];
		}
		keyarr[insert] = insertkey;
		values[insert].child_ptr = left->self;
		values[insert+1].child_ptr = right->self;
	}

	UPparent->nchild++;
	//
	left->parent = UPparent->self;
	right->parent = UPparent->self;
}


// creat a parent node after split. either insert the new parent to up level parent or split the uplevel node again
void CreatnewParentnode(BplusTree* bplustree, TreeNode* left, TreeNode* right, key_t  insertkey) {

	Btree_meta* meta = bplustree->meta;
	// simple case, just creat a new parent node, 
	if (left->parent == INVALID_KEY && right->parent == INVALID_KEY)
	{// this node is root node, we have to creat a new root node
		//meta->numNodes++;

		TreeNode* newroot = creatNewNodeinMem(Nonleaf);

		newroot->self = allocNodeinDisk(bplustree);
		newroot->nchild = 2;
		newroot->key[0] = insertkey;
		newroot->nodeValue[0].child_ptr = left->self;//load new child ptr
		newroot->nodeValue[1].child_ptr = right->self;

		meta->root = newroot->self;
		left->parent = newroot->self;
		right->parent = newroot->self;

		pushNode2disk(bplustree, newroot);
		freeNodeinMem(newroot);
		pushNode2disk(bplustree, left);
		pushNode2disk(bplustree, right);
	}
	else {
		TreeNode* UPparent;// uplevel
		if (right->parent == INVALID_KEY)
		{
			UPparent = fetchNodeFromDisk(bplustree, left->parent);

		}
		 else {
			UPparent = fetchNodeFromDisk(bplustree, right->parent);;//
	    }
		
		int insert = binarysearch_ceil(UPparent, insertkey);// get insert position
		if (insert < 0)insert = -insert;
		if (UPparent->nchild-1==meta->order)
		{// parent need split

			int split = (UPparent->nchild) / 2;
			TreeNode* newnonleafnode = creatNewNodeinMem(Nonleaf);
			newnonleafnode->self = allocNodeinDisk(bplustree);
			TreeNode* newright; TreeNode* newleft;
			key_t popUpkey = 0;// need to insert to parent node
			if (insert<split)
			{//add the new entry to left subtree

				
				newright = UPparent;
				newleft = newnonleafnode;
				//creat a new left tree
				int prevptr = UPparent->prev;
				//updateLink here
				if (prevptr == INVALID_KEY) {
					newleft->prev = INVALID_KEY;
					newleft->next = UPparent->self;
					UPparent->prev = newleft->self;
				}
				else {

					TreeNode* prevnode = fetchNodeFromDisk(bplustree, prevptr);
					prevnode->next = newleft->self;
					newleft->prev = prevnode->self;
					newleft->next = UPparent->self;
					UPparent->prev = newleft->self;
					pushNode2disk(bplustree, prevnode);
					freeNodeinMem(prevnode);
				}
				/////////////copy and shift nodes
				int pivot = insert;
				newleft->nchild = split+1;
				UPparent->nchild = meta->order- split+1;

				for (int i = 0; i < pivot; i++)
				{
					newleft->key[i] = UPparent->key[0];
					newleft->nodeValue[i].child_ptr = UPparent->nodeValue[i].child_ptr;
				}

				for (int i = pivot+1; i < split - pivot; i++)
				{
					newleft->key[i] = UPparent->key[i-1];
					newleft->nodeValue[i].child_ptr = UPparent->nodeValue[i-1].child_ptr;
				}

				/* flush sub-nodes of the new splitted left node */
				for (int i = 0; i < left->nchild; i++) {
					if (i != pivot && i != pivot + 1) {
						TreeNode* subnode = fetchSubNode(bplustree, newleft, newleft->nodeValue[i].child_ptr);
						pushNode2disk(bplustree, subnode);
						freeNodeinMem(subnode);
					}
				}

				newleft->key[pivot] = insertkey;
				if (pivot == split ) {
				
					newleft->nodeValue[pivot].child_ptr = left->self;
					left->parent = newleft->self;
					UPparent->nodeValue[0].child_ptr = right->self;
					right->parent = newleft->self;
					popUpkey = insertkey;
				}
				else {

					newleft->nodeValue[pivot].child_ptr = left->self;
					left->parent = newleft->self;

					newleft->nodeValue[pivot+1].child_ptr = right->self;
					right->parent = newleft->self;
					popUpkey = UPparent->key[split - 1];
					UPparent->key[0] = UPparent->key[split];
					UPparent->nodeValue[0] = UPparent->nodeValue[split];
				}

				//right node shift

				for (int i = 1;i < UPparent->nchild; i++)
				{
					UPparent->key[i] = UPparent->key[split+i];
				}
				for (int i = 1; i < UPparent->nchild+1; i++)
				{
					UPparent->nodeValue[i].child_ptr = UPparent->nodeValue[split + i].child_ptr;
				}
			}
			else if (insert ==split) {


				newright = newnonleafnode;
				newleft = UPparent;
				//connect right subtree with left

				//////////update link
				int nextptr = newleft->next;
				if (nextptr == INVALID_KEY)
				{
					newnonleafnode->next = nextptr;
					newnonleafnode->prev = UPparent->self;
					UPparent->next = newnonleafnode->self;
				}
				else {
					TreeNode* nextnode = fetchNodeFromDisk(bplustree, nextptr);
					newnonleafnode->next = nextptr;
					nextnode->prev = newnonleafnode->next;
					newnonleafnode->prev = UPparent->self;
					UPparent->next = newnonleafnode->self;
					pushNode2disk(bplustree, nextnode);
					freeNodeinMem(nextnode);
				}
				/////////////////////////////////////////
				

				popUpkey =UPparent->key [split - 1];

				int pivot = 0;
				UPparent->nchild = split;
				newright->nchild = meta->order+1 - split;

				//insert new key
				newright->key[0] = insertkey;

				newright->nodeValue[pivot].child_ptr = left->self;
				left->parent = newright->self;
				newright->nodeValue[pivot+1].child_ptr = right->self;
				right->parent = newright->self;



				for (int i = 0; i < newright->nchild-2; i++)
				{
					newright->key[pivot + 1 + i] = UPparent->key[split + i];
					newright->nodeValue[pivot + 2 + i] = UPparent->nodeValue[split + 1 + i];
				}
				/* flush sub-nodesto disk*/
				for (int i = pivot + 2; i < right->nchild; i++) {
					TreeNode* subnode = fetchSubNode(bplustree, newright, newright->nodeValue[i].child_ptr);
					pushNode2disk(bplustree, subnode);
					freeNodeinMem(subnode);
				}
			}
			else {

			 newright = newnonleafnode;
			 newleft = UPparent;
			//connect right subtree with left

			//////////update link
			int nextptr = newleft->next;
			if (nextptr == INVALID_KEY)
			{
				newnonleafnode->next = nextptr;
				newnonleafnode->prev = UPparent->self;
				UPparent->next = newnonleafnode->self;
			}
			else {
				TreeNode* nextnode = fetchNodeFromDisk(bplustree, nextptr);
				newnonleafnode->next = nextptr;
				nextnode->prev = newnonleafnode->next;
				newnonleafnode->prev = UPparent->self;
				UPparent->next = newnonleafnode->self;
				pushNode2disk(bplustree, nextnode);
				freeNodeinMem(nextnode);
			}
			/////////////////////////////////////////


			    popUpkey = UPparent->key[split];

				/* calculate split nodes' children (sum as (order + 1))*/
				int pivot = insert - split - 1;
				UPparent->nchild = split + 1;
				newright->nchild = meta->order - split+1;

				for (int i = 0; i < pivot; i++)
				{
					newright->key[i] = UPparent->key[split + i+i];
					newright->nodeValue[i] = UPparent->nodeValue[split + 1 + i];
				}
				//insert new key
				newright->key[pivot] = insertkey;

				newright->nodeValue[pivot].child_ptr = left->self;
				left->parent = newright->self;
				newright->nodeValue[pivot + 1].child_ptr = right->self;
				right->parent = newright->self;
	
				for (int i = 0; i < meta->order-insert-1; i++)
				{
					newright->key[pivot + 1 + i] = UPparent->key[insert + i];
					newright->nodeValue[pivot + 2 + i] = UPparent->nodeValue[insert + 1 + i];
				}
				/* flush sub-nodesto disk*/
				for (int i = 0; i < right->nchild; i++) {
					if (i != pivot && i != pivot + 1) {
						TreeNode* subnode = fetchSubNode(bplustree, newright, newright->nodeValue[i].child_ptr);
						pushNode2disk(bplustree, subnode);
						freeNodeinMem(subnode);
					}
				}
			}

			///////////////find parent again
			
			CreatnewParentnode(bplustree, newleft, newright, popUpkey);
			freeNodeinMem(newnonleafnode);
		}
		else {
			//no need split
			addNonLeafnoSplit(UPparent, left, right, insertkey, insert);

		}
		pushNode2disk(bplustree, UPparent);
		pushNode2disk(bplustree, left);
		pushNode2disk(bplustree, right);
		freeNodeinMem(UPparent);
	}

	




}
int addNewEntry2Leaf(BplusTree* bplustree, TreeNode* leafnode,Value* key, RID rid,int insert) {

	Btree_meta* meta = bplustree->meta;
	if (binaryFind(leafnode, key->v.intV, insert))//found double cheack if target is inside node
	{
		return  -1;// already exit
	}
	else {
		//try to add to leaf tree
		key_t keyUpToParent = -1;
		if (leafnode->nchild >= meta->order)// need split
		{

			int split = (meta->order+1)/2+1;// split start index  ot the right tree;
			if (split % 2 != 0)split = (meta->order + 1) / 2;

			TreeNode* newLeaf = creatNewNodeinMem(Leaf);

			newLeaf->self = allocNodeinDisk(bplustree);// get a location on Disk


			TreeNode* left, * right;
			if (insert<0)
			{
				insert = -insert;
			}
			if (insert < split) {
				//add the new entry to left subtree

				
				right = leafnode;
				left = newLeaf;
				//creat a new left tree
				int prevptr = leafnode->prev;
				//updateLink here
				if (prevptr == INVALID_KEY) {
                    left->prev = INVALID_KEY;
				    left->next = leafnode->self;
				    leafnode->prev = left->self;
				}
				else {

					TreeNode* prevnode = fetchNodeFromDisk(bplustree, prevptr);
					prevnode->next = left->self;
					left->prev = prevnode->self;
					left->next = leafnode->self;
					leafnode->prev = left->self;
					pushNode2disk(bplustree, prevnode);
					freeNodeinMem(prevnode);
				}
				//////// copy and insert data now////////////
				int pivot = insert;// this is index start from 0
				left->nchild = split; /*Left tree[0, split - 1]*/
				leafnode->nchild= meta->order + 1 - split; /* Right tree[split, end] */
                 //careful when copy data;
				 for (int i = 0; i < pivot; i++)
				{
					left->nodeValue[i].value = leafnode->nodeValue[i].value;
					left->key[i] = leafnode->key[i];

				}
				 // insert data
				 left->key[pivot] = key->v.intV;
				 left->nodeValue[pivot].value = rid;
				 // Still data to copy
				 for (int i = pivot+1; i < split; i++)
				 {
					 left->nodeValue[i].value = leafnode->nodeValue[i-1].value;
					 left->key[i] = leafnode->key[i-1];

				 }
				 // Last step, shift the right tree node

				 for (int i = 0; i < leafnode->nchild; i++)
				 {
					 leafnode->nodeValue[i].value = leafnode->nodeValue[split - i-1].value;
					 leafnode->key[i] = leafnode->key[split - i - 1];

				 }
				///////////////////////////////////////////
				 keyUpToParent = leafnode->key[0];
			}
			else {
				// add the new entry to right subtree
				left = leafnode;
				right = newLeaf;
				//connect right subtree with left

				//////////update link
				int nextptr = leafnode->next;
				if (nextptr==INVALID_KEY)
				{
					newLeaf->next = nextptr;
					newLeaf->prev = leafnode->self;
					leafnode->next = newLeaf->self;
				}
				else {
					TreeNode* nextnode = fetchNodeFromDisk(bplustree, nextptr);
					newLeaf->next = nextptr;
					nextnode->prev = newLeaf->self;
					newLeaf->prev = leafnode->self;
					leafnode->next = newLeaf->self;
					pushNode2disk(bplustree, nextnode);
					freeNodeinMem(nextnode);
				}
				/////////////////////////////////////////
/*
			Left tree[0,split-1]
			Right tree[split,end]*/
				int pivot = insert - split;
				leafnode->nchild = split;//split index is the start of right tree
				newLeaf->nchild = meta->order + 1 - split;
				for (int i = 0; i < pivot; i++)
				{
					newLeaf->nodeValue[i].value = leafnode->nodeValue[split + i].value;
					newLeaf->key[i] = leafnode->key[split + i];

				}

				newLeaf->key[pivot] = key->v.intV;
				newLeaf->nodeValue[pivot].value = rid;

				for (int i=pivot+1; i < newLeaf->nchild; i++)
				{
					newLeaf->nodeValue[i].value = leafnode->nodeValue[insert + i].value;
					newLeaf->key[i] = leafnode->key[insert + i];

				}
				//finish move value to new right  subtree
				 keyUpToParent = newLeaf->key[0];
				
			}
//build new parent node
			

            CreatnewParentnode(bplustree, left, right, keyUpToParent);

			
			// then, free the node mirror in RAM
			//pushNode2disk(bplustree, newLeaf); // no need to parent fucntion will push the childs to disk
			//pushNode2disk(bplustree, leafnode);
			freeNodeinMem(newLeaf);
		}
		else {// No split happens,just add
			addLeafnoSplit(leafnode, key, rid, insert);
			pushNode2disk(bplustree, leafnode);// flush the node back to disk
		}

		return 1;
	}
}
RC insertKey(BTreeHandle* tree, Value* key, RID rid)
{
	int rt = -1;

	BplusTree* bplustree = (BplusTree*)tree->mgmtData;
	Btree_meta* meta = bplustree->meta;
	TreeNode* root;
	//root->key = 0;
	if (meta->numNodes== 0)// empty tree
	{//Creat a new leaf node as root
		root = creatNewNodeinMem(Leaf);// creat root in mem
		

		root->self = allocNodeinDisk(bplustree);// get root a disk block

		root->nchild = 1;
		root->key[0] =key->v.intV;
		root->nodeValue[0].value = rid;


		
		meta->numEntries = 1;
		meta->root = root->self;
		// flush root back to file
		pushNode2disk(bplustree, root);
		freeNodeinMem(root);
		return RC_OK;

	}
	else {
		root = fetchNodeFromDisk(bplustree, meta->root);// fetch root from page 1
		TreeNode* node = root;
		while (node != NULL) {
			int i = binarysearch_ceil(node, key->v.intV);
			if (node->type == Leaf) {// search to leaf node
				//rt = i >= 0 ? data(node)[i] : -1;

				rt = addNewEntry2Leaf(bplustree, node, key, rid, i);
				//freeNodeinMem(node);
				break;
			}
			else {// no leaf node, keep searching
				if (i >= 0) {
					TreeNode* subnode;//equal go to right
					if (binaryFind(node, key->v.intV, i)) {
						subnode = fetchSubNode(bplustree, node, i + 1);
					}
					else {//less go to left
						subnode = fetchSubNode(bplustree, node, i);
					}
					freeNodeinMem(node);
					node = subnode;
					//node = node_seek(tree, sub(node)[i + 1]);
				}
				else {
					TreeNode* subnode = fetchSubNode(bplustree, node, node->nchild - 1);
					freeNodeinMem(node);
					node = subnode;
					//node = node_seek(tree, sub(node)[i]);
				}
			}
		}
		freeNodeinMem(node);
		meta->numEntries++;
	}
	
		if (rt ==-1)
		{
			return RC_IM_KEY_ALREADY_EXISTS;
		}
	return RC_OK;
}

//just remove a key in a leaf, simple , no other opration
void rmLeafNoUnderflow(BplusTree* bplustree, TreeNode* leafnode, int deleteindex) {
	Btree_meta* meta = bplustree->meta;
	for (int i = deleteindex; i < leafnode->nchild-1; i++)
	{
		leafnode->key[i] = leafnode->key[i + 1];
		leafnode->nodeValue[i].value = leafnode->nodeValue[i + 1].value;
	}
	leafnode->nchild--;
}
void rmNonLeafNoUnderflow(BplusTree* bplustree, TreeNode* leafnode, int deleteindex) 
{// this version is different from above be careful!
	Btree_meta* meta = bplustree->meta;
	for (int i = deleteindex; i < leafnode->nchild - 1; i++)
	{
		leafnode->key[i] = leafnode->key[i + 1];
		leafnode->nodeValue[i+1].value = leafnode->nodeValue[i + 2].value;
	}
	leafnode->nchild--;
}



void Borrow1leafnodefromLeft(BplusTree* bplustree, TreeNode* parent, TreeNode* leafnode, TreeNode* leftsib, int leafptrIndex, int deleteIndex) {

	//shift leafnode make a new space
	for (int i = 0; i < deleteIndex; i++)
	{
		leafnode->key[i + 1] = leafnode->key[i];
		leafnode->nodeValue[i + 1] = leafnode->nodeValue[i];
	}
	//borrow node from left
	leafnode->key[0] = leftsib->key[leftsib->nchild - 1];
	leafnode->nodeValue[0] = leftsib->nodeValue[leftsib->nchild - 1];
	//leftsibling less one node
	leftsib->nchild--;
	//update parent key, no need to update childptr
	parent->key[leafptrIndex] = leafnode->key[0];
}

void Borrow1NonLeafnodefromLeft(BplusTree* bplustree, TreeNode* parent, TreeNode* nonleafnode, TreeNode* leftsib, int nodeptrIndex, int deleteIndex) {
///////////////different version///careful
	//shift leafnode make a new space
	for (int i = 0; i < deleteIndex; i++)
	{
		nonleafnode->key[i + 1] = nonleafnode->key[i];
		
	}
	for (int i = 0; i < deleteIndex+1; i++)// one more
	{
		nonleafnode->nodeValue[i + 1] = nonleafnode->nodeValue[i];
	}
	
	nonleafnode->key[0] = parent->key[nodeptrIndex];
	parent->key[nodeptrIndex] = leftsib->key[leftsib->nchild - 2];
	//leftsibling less one node
	leftsib->nchild--;
	 //borrow node from left
	nonleafnode->nodeValue[0] = leftsib->nodeValue[leftsib->nchild - 1];
	int subptr = nonleafnode->nodeValue[0].child_ptr;
	TreeNode* subnode = fetchSubNode(bplustree, nonleafnode, subptr);
	subnode->parent = nonleafnode->self;
	pushNode2disk(bplustree, subnode);
	freeNodeinMem(subnode);
}

void Borrow1leafnodefromRight(BplusTree* bplustree, TreeNode* parent, TreeNode* rightsib, TreeNode* leafnode, int leafptrIndex, int deleteIndex) {

	//take the first element of right sibling
	leafnode->key[leafnode->nchild] = rightsib->key[0];
	leafnode->nodeValue[leafnode->nchild] = rightsib->nodeValue[0];
	//shift right sibling

	for (int i = 0; i < rightsib->nchild-1; i++)
	{
		rightsib->key[i] = rightsib->key[i + 1];
		rightsib->nodeValue[i] = rightsib->nodeValue[i + 1];
	}
	leafnode->nchild++;
	rightsib->nchild--;
	//update parent key, no need to update childptr
	parent->key[leafptrIndex] = rightsib->key[0];

}
void Borrow1NonLeafnodefromRight(BplusTree* bplustree, TreeNode* parent, TreeNode* rightsib, TreeNode* nonleafnode, int nodeptrIndex, int deleteIndex) {
	/////////////////different version for nonleafnode
	
	nonleafnode->key[nonleafnode->nchild-1] = parent->key[nodeptrIndex];
	//update parent key, no need to update childptr
	parent->key[nodeptrIndex] = rightsib->key[0];
	//take the first element of right sibling
	nonleafnode->nodeValue[nonleafnode->nchild] = rightsib->nodeValue[0];
	int subptr = nonleafnode->nodeValue[nonleafnode->nchild].child_ptr;
	TreeNode* subnode = fetchSubNode(bplustree, nonleafnode, subptr);
	subnode->parent = nonleafnode->self;
	pushNode2disk(bplustree, subnode);
	freeNodeinMem(subnode);
	//shift right sibling

	for (int i = 0; i < rightsib->nchild - 2; i++)
	{
		rightsib->key[i] = rightsib->key[i + 1];
	}
	for (int i = 0; i < rightsib->nchild - 1; i++)// one more
	{
		rightsib->key[i] = rightsib->key[i + 1];
		rightsib->nodeValue[i] = rightsib->nodeValue[i + 1];
	}
	nonleafnode->nchild++;
	rightsib->nchild--;
	

}
void leafnodeMerge2LeftSibling(BplusTree* bplustree, TreeNode* parent, TreeNode* leafnode, TreeNode* leftsib, int leafptrIndex, int deleteIndex) {
	for (int i = 0; i < deleteIndex; i++)
	{
		leftsib->key[leftsib->nchild + i] = leafnode->key[i];
		leftsib->nodeValue[leftsib->nchild + i] = leafnode->nodeValue[i];
	}

	for (int i = 0; i < deleteIndex; i++)
	{
		leftsib->key[leftsib->nchild + deleteIndex+i] = leafnode->key[deleteIndex + i+1];
		leftsib->nodeValue[leftsib->nchild + deleteIndex + i] = leafnode->nodeValue[deleteIndex + i + 1];
	}
	leftsib->nchild += leafnode->nchild - 1;
}

void NonLeafnodeMerge2LeftSibling(BplusTree* bplustree, TreeNode* parent, TreeNode* nonleafnode, TreeNode* leftsib, int nodeptrIndex, int deleteIndex) {
	/////////////////different version for nonleafnode
	//copy parent key first
	leftsib->key[leftsib->nchild - 1]= parent->key[nodeptrIndex];

	for (int i = 0; i < deleteIndex; i++)
	{
		leftsib->key[leftsib->nchild + i] = nonleafnode->key[i];
	}
	for (int i = 0; i < deleteIndex+1; i++)
	{
		leftsib->nodeValue[leftsib->nchild + i] = nonleafnode->nodeValue[i];
	}

	for (int i = 0; i < nonleafnode->nchild-deleteIndex-2; i++)
	{
		leftsib->key[leftsib->nchild + deleteIndex + i] = nonleafnode->key[deleteIndex + i + 1];
		leftsib->nodeValue[leftsib->nchild + deleteIndex + 1+i] = nonleafnode->nodeValue[deleteIndex + i + 2];
	}
	//change subnode's parent
	int i = leftsib->nchild;
	
	for (int j = 0; j < nonleafnode->nchild-1; j++)
	{
		int subptr = leftsib->nodeValue[i].child_ptr;
		TreeNode* subnode = fetchSubNode(bplustree, nonleafnode, subptr);
		subnode->parent = leftsib->self;
		pushNode2disk(bplustree, subnode);
		freeNodeinMem(subnode);
		i++;
	}
	leftsib->nchild += nonleafnode->nchild - 1;
}

void RightSiblingMerge2Leafnode(BplusTree* bplustree, TreeNode* parent, TreeNode* rightsib, TreeNode* leafnode, int leafptrIndex, int deleteIndex) {
	for (int i = 0; i < rightsib->nchild; i++)
	{
		leafnode->key[leafnode->nchild+i] = rightsib->key[i];
		leafnode->nodeValue[leafnode->nchild+i] = rightsib->nodeValue[i];
	}
	leafnode->nchild += rightsib->nchild;
}

void RightSiblingMerge2NonLeafnode(BplusTree* bplustree, TreeNode* parent, TreeNode* rightsib, TreeNode* nonleafnode, int nodeptrIndex, int deleteIndex) {
	nonleafnode->key[nonleafnode->nchild - 1] = parent->key[nodeptrIndex];
	nonleafnode->nchild++;
	for (int i = 0; i < rightsib->nchild-1; i++)
	{
		nonleafnode->key[nonleafnode->nchild - 1 + i] = rightsib->key[i];
	}
	for (int i = 0; i < rightsib->nchild; i++)
	{
		nonleafnode->nodeValue[nonleafnode->nchild-1 + i] = rightsib->nodeValue[i];
	}

	
	//change subnode's parent
	int i = nonleafnode->nchild-1;

	for (int j = 0; j < nonleafnode->nchild - 1; j++)
	{
		int subptr = nonleafnode->nodeValue[i].child_ptr;
		TreeNode* subnode = fetchSubNode(bplustree, nonleafnode, subptr);
		subnode->parent = nonleafnode->self;
		pushNode2disk(bplustree, subnode);
		freeNodeinMem(subnode);
		i++;
	}
	nonleafnode->nchild += rightsib->nchild - 1;
}
void rmNonLeafFromNonleaf(BplusTree* bplustree, TreeNode* nonleafnode, int deleteindex)
{
	Btree_meta* meta = bplustree->meta;
	if (nonleafnode->parent == INVALID_KEY)
	{
		//this node is root;
		if (nonleafnode->nchild == 2)
		{
			TreeNode* newroot = fetchNodeFromDisk(bplustree, nonleafnode->nodeValue[0].child_ptr);
			newroot->parent = INVALID_KEY;
			meta->root = newroot->self;
			deleteNodefromTree(bplustree, nonleafnode, NULL, NULL);// delete the old node
			pushNode2disk(bplustree, newroot);
			freeNodeinMem(newroot);
		}
		else {
			//simple delete no change of tree structure
			rmNonLeafNoUnderflow(bplustree, nonleafnode, deleteindex);
			pushNode2disk(bplustree, nonleafnode);
		}
	}
	else if(nonleafnode->nchild <= (meta->order + 1) / 2){
		// noleafnode underflow
		TreeNode* leftsib = fetchNodeFromDisk(bplustree, nonleafnode->prev);
		TreeNode* rightsib = fetchNodeFromDisk(bplustree, nonleafnode->next);
		TreeNode* parent = fetchNodeFromDisk(bplustree, nonleafnode->parent);

		//find the nonleafnode ptr index in parent

		int keyindex = binarysearch_ceil(parent, nonleafnode->key[0]);

		//After delete , we have to borrow or merge,
		//first try borrow
		// if we can borrow values from both the left and right sibling, 
		//you should borrow from the left one.
		bool borrowLeft = true;
		if (keyindex < 0)
		{
			keyindex = parent->nchild;
			//no right sibling,chhose left
			if (leftsib->nchild > (meta->order + 1) / 2)
			{// do borrow
				Borrow1NonLeafnodefromLeft(bplustree, parent, nonleafnode, leftsib, keyindex, deleteindex);
				pushNode2disk(bplustree, nonleafnode);
				pushNode2disk(bplustree, leftsib);
				pushNode2disk(bplustree, rightsib);
				pushNode2disk(bplustree, parent);
			}
			else {
				// do merge
				NonLeafnodeMerge2LeftSibling(bplustree, parent, leftsib, rightsib, keyindex, deleteindex);

				deleteNodefromTree(bplustree, nonleafnode, leftsib, rightsib);//remove from disk
				// call again if up level parent need modify
				rmNonLeafFromNonleaf(bplustree, parent, keyindex);
			}
		}
		else if (keyindex == 0)
		{//no left sibling
			borrowLeft = false;
			rmNonLeafNoUnderflow(bplustree, nonleafnode, deleteindex);
			if (rightsib->nchild > (meta->order + 1) / 2)
			{// do borrow
				

				Borrow1NonLeafnodefromRight(bplustree, parent, rightsib, nonleafnode, keyindex, deleteindex);
				pushNode2disk(bplustree, nonleafnode);
				pushNode2disk(bplustree, leftsib);
				pushNode2disk(bplustree, rightsib);
				pushNode2disk(bplustree, parent);
			}
			else {
				// do merge
				RightSiblingMerge2NonLeafnode(bplustree, parent, rightsib, nonleafnode, keyindex, deleteindex);
				TreeNode* rightrightsib = fetchNodeFromDisk(bplustree, rightsib->next);
				deleteNodefromTree(bplustree, rightsib, nonleafnode, rightrightsib);
				pushNode2disk(bplustree, leftsib);
				// call again if up level parent need modify
				rmNonLeafFromNonleaf(bplustree, parent, keyindex+1);
			}
		}
		else {
			//have two sibling
			//choose the left one
			borrowLeft = true;
			// try left
			if (leftsib->nchild > (meta->order + 1) / 2)
			{// do borrow
				Borrow1NonLeafnodefromLeft(bplustree, parent, nonleafnode, leftsib, keyindex, deleteindex);
				pushNode2disk(bplustree, nonleafnode);
				pushNode2disk(bplustree, leftsib);
				pushNode2disk(bplustree, rightsib);
				pushNode2disk(bplustree, parent);
			}
			
			else if (rightsib->nchild > (meta->order + 1) / 2) {
				// try right
			rmNonLeafNoUnderflow(bplustree, nonleafnode, deleteindex);
			Borrow1NonLeafnodefromRight(bplustree, parent, rightsib, nonleafnode, keyindex, deleteindex);
			pushNode2disk(bplustree, nonleafnode);
			pushNode2disk(bplustree, leftsib);
			pushNode2disk(bplustree, rightsib);
			pushNode2disk(bplustree, parent);
			}
			else {
				//do left merge
				// do merge
				NonLeafnodeMerge2LeftSibling(bplustree, parent, leftsib, rightsib, keyindex, deleteindex);

				deleteNodefromTree(bplustree, nonleafnode, leftsib, rightsib);//remove from disk
				// call again if up level parent need modify
				rmNonLeafFromNonleaf(bplustree, parent, keyindex);
			}
		}

	}
	else {
		// just delete, no underflow
		//simple delete no change of tree structure
		rmNonLeafNoUnderflow(bplustree, nonleafnode, deleteindex);
		pushNode2disk(bplustree, nonleafnode);
	}
}
int rmEntryFromLeaf(BplusTree* bplustree, TreeNode* leafnode, key_t deletkey, int deleteindex)
{
	Btree_meta* meta = bplustree->meta;
	if (!binaryFind(leafnode, deletkey, deleteindex))//found double cheack if target is inside node
	{
		return  -1;// no such key
	}
	if (leafnode->parent == INVALID_KEY)
	{
		//root node is the last leafnode, just delete
		//rmLeafNoUnderflow(bplustree, leafnode, deleteindex);
		//after delete we still need check if the root now is empty
		if (leafnode->nchild==1)
		{
			meta->root = INVALID_KEY;
			deleteNodefromTree(bplustree,leafnode, NULL, NULL);
		}
		else {
			rmLeafNoUnderflow(bplustree, leafnode, deleteindex);
			pushNode2disk(bplustree, leafnode);
		}
	}
	else if(leafnode->nchild < (meta->order + 1) / 2+1)
	{//Underflow will happen

		TreeNode* leftsib = fetchNodeFromDisk(bplustree, leafnode->prev);
		TreeNode* rightsib= fetchNodeFromDisk(bplustree, leafnode->next);
		TreeNode* parent = fetchNodeFromDisk(bplustree, leafnode->parent);

		//find the leafnode ptr index in parent

		int keyindex = binarysearch_ceil(parent, leafnode->key[0]);
		
		//After delete , we have to borrow or merge,
		//first try borrow
		// if we can borrow values from both the left and right sibling, 
		//you should borrow from the left one.
		bool borrowLeft = true;
		if (keyindex < 0)
		{
			if(keyindex<0)keyindex = parent->nchild-2;
			//no right sibling,chhose left
			if (leftsib->nchild > (meta->order + 1) / 2)
			{// do borrow
				Borrow1leafnodefromLeft(bplustree, parent, leafnode, leftsib, keyindex, deleteindex);
				pushNode2disk(bplustree, leafnode);
				pushNode2disk(bplustree, leftsib);
				pushNode2disk(bplustree, rightsib);
				pushNode2disk(bplustree, parent);
			}
			else {
				// do merge
				leafnodeMerge2LeftSibling(bplustree, parent, leftsib, leafnode, keyindex, deleteindex);

				deleteNodefromTree(bplustree, leafnode, leftsib, rightsib);//remove from disk
				// call again if up level parent need modify
				rmNonLeafFromNonleaf(bplustree, parent, keyindex);
			}
		}
		else if (keyindex == 0&&deletkey<parent->key[0])
		{//no left sibling
			borrowLeft = false;
			rmLeafNoUnderflow(bplustree, leafnode, deleteindex);
			if (rightsib->nchild > (meta->order + 1) / 2)
			{// do borrow


				Borrow1leafnodefromRight(bplustree, parent, rightsib, leafnode, keyindex, deleteindex);
				pushNode2disk(bplustree, leafnode);
				pushNode2disk(bplustree, leftsib);
				pushNode2disk(bplustree, rightsib);
				pushNode2disk(bplustree, parent);
			}
			else {
				// do merge
				RightSiblingMerge2Leafnode(bplustree, parent, rightsib, leafnode, keyindex, deleteindex);
				TreeNode* rightrightsib = fetchNodeFromDisk(bplustree, rightsib->next);
				deleteNodefromTree(bplustree, rightsib, leafnode, rightrightsib);
				pushNode2disk(bplustree, leftsib);
				// call again if up level parent need modify
				rmNonLeafFromNonleaf(bplustree, parent, keyindex);
			}
		}
		else {
			//have two sibling
			//choose the left one
			borrowLeft = true;
			// try left
			if (leftsib->nchild > (meta->order + 1) / 2)
			{// do borrow
				if (!binaryFind(parent, leafnode->key[0], keyindex))keyindex--;
				Borrow1leafnodefromLeft(bplustree, parent, leafnode, leftsib, keyindex, deleteindex);
				pushNode2disk(bplustree, leafnode);
				pushNode2disk(bplustree, leftsib);
				pushNode2disk(bplustree, rightsib);
				pushNode2disk(bplustree, parent);
			}
			else if (rightsib->nchild > (meta->order + 1) / 2) {
				// try right
				rmLeafNoUnderflow(bplustree, leafnode, deleteindex);
				if (keyindex== 0)keyindex++;
				Borrow1leafnodefromRight(bplustree, parent, rightsib, leafnode, keyindex, deleteindex);
				pushNode2disk(bplustree, leafnode);
				pushNode2disk(bplustree, leftsib);
				pushNode2disk(bplustree, rightsib);
				pushNode2disk(bplustree, parent);
			}
			else {
				//do merge
				// do Leftmerge
				leafnodeMerge2LeftSibling(bplustree, parent, leftsib, rightsib, keyindex, deleteindex);

				deleteNodefromTree(bplustree, leafnode, leftsib, rightsib);//remove from disk
				// call again if up level parent need modify
				rmNonLeafFromNonleaf(bplustree, parent, keyindex);
			}
		}

		

	}
	else {
		// leaf node not the root, and no merge or borrow, just delete
		rmLeafNoUnderflow(bplustree, leafnode, deleteindex);
		pushNode2disk(bplustree, leafnode);
	}

	meta->numEntries--;
	return 1;
}


RC deleteKey(BTreeHandle* tree, Value* key)
{
	int rt = -1;

	BplusTree* bplustree = (BplusTree*)tree->mgmtData;
	Btree_meta* meta = bplustree->meta;
	TreeNode* root;
	//root->key = 0;
	if (meta->numNodes == 0)// empty tree
	{
		return RC_IM_KEY_NOT_FOUND;

	}
	else {
		root = fetchNodeFromDisk(bplustree, meta->root);// fetch root from page 1
		TreeNode* node = root;
		while (node != NULL) {
			int i = binarysearch_ceil(node, key->v.intV);
			if (node->type == Leaf) {// search to leaf node
				//rt = i >= 0 ? data(node)[i] : -1;

				rt = rmEntryFromLeaf(bplustree, node, key->v.intV, i);
				//freeNodeinMem(node);
				break;
			}
			else {// no leaf node, keep searching
				if (i >= 0) {
					TreeNode* subnode;//equal go to right
					if (binaryFind(node, key->v.intV, i)) {
						subnode = fetchSubNode(bplustree, node, i + 1);
					}
					else {//less go to left
						subnode = fetchSubNode(bplustree, node, i);
					}
					freeNodeinMem(node);
					node = subnode;
					//node = node_seek(tree, sub(node)[i + 1]);
				}
				else {
					TreeNode* subnode = fetchSubNode(bplustree, node, node->nchild - 1);
					freeNodeinMem(node);
					node = subnode;
					//node = node_seek(tree, sub(node)[i]);
				}
			}
		}
		//pushNode2disk(bplustree, node);
		freeNodeinMem(node);
	}

	if (rt == -1)
	{
		return RC_IM_KEY_NOT_FOUND;
	}
	else {
		return RC_OK;
	}
}

/*
typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;
*/

typedef struct BT_ScanContent_s
{
	int totalentrys;
	int scannedentry;
	int currentleafnode;
	int currindexinnode;
	bool endofnode;
}BTScanInfo;
RC openTreeScan(BTreeHandle* tree, BT_ScanHandle** handle)
{
	BT_ScanHandle* bscanH = malloc(sizeof(BT_ScanHandle));
	BplusTree* bplustree = (BplusTree*)tree->mgmtData;
	Btree_meta* meta = bplustree->meta;
	BTScanInfo* bscan = malloc(sizeof(BTScanInfo));
	bscan->totalentrys = meta->numEntries;
	bscan->scannedentry = 0;
	bscan->currentleafnode = -1;
	bscan->currindexinnode = -1;
	bscan->endofnode = true;
	bscanH->tree = tree;
	bscanH->mgmtData = bscan;
	*handle = bscanH;
	return RC_OK;
}

RC nextEntry(BT_ScanHandle* handle, RID* result)
{
	BTScanInfo* bscan = handle->mgmtData;
	BTreeHandle* tree = handle->tree;
	BplusTree* bplustree = tree->mgmtData;
	if (bscan->totalentrys <= 0)
	{
		printf("empty tree in nextEntry\n");
		return RC_IM_NO_MORE_ENTRIES;
	}
	if (bscan->scannedentry >= bscan->totalentrys) {
		return RC_IM_NO_MORE_ENTRIES;
	}
	TreeNode* node;
	if (bscan->currentleafnode == -1) {
		//first scan go to first node
		node= fetchNodeFromDisk(bplustree, bplustree->meta->root);

		while (node != NULL) {
			if (node->type == Leaf) {// search to leaf node
			//rt = i >= 0 ? data(node)[i] : -1;
				RID foundR = node->nodeValue[0].value;
				result->page = foundR.page;
				result->slot = foundR.slot;
				
				bscan->currentleafnode = node->self;
				bscan->scannedentry++;
				bscan->currindexinnode = 0;
				if (node->nchild==1)
				{
					bscan->endofnode = true;
				}
				break;
			}
			else {// no leaf node
				
			    TreeNode* subnode = fetchSubNode(bplustree, node, 0);
                freeNodeinMem(node);
				node = subnode;
			}
		}
		freeNodeinMem(node);
	}
	else {
		node = fetchNodeFromDisk(bplustree, bscan->currentleafnode);
		if(bscan->currindexinnode<node->nchild-1){
            bscan->currindexinnode++;
			RID foundR = node->nodeValue[bscan->currindexinnode].value;
			result->page = foundR.page;
			result->slot = foundR.slot;

			bscan->currentleafnode = node->self;
			
			bscan->scannedentry++;
		}
		else if (node->next!=INVALID_KEY)
		{
			TreeNode* subnode = fetchNodeFromDisk(bplustree, node->next);
			freeNodeinMem(node);
			node = subnode;
			RID foundR = node->nodeValue[0].value;
			result->page = foundR.page;
			result->slot = foundR.slot;

			bscan->currentleafnode = node->self;
			bscan->currindexinnode = 0;
			bscan->scannedentry++;
			freeNodeinMem(node);
		}
		else {
			return RC_IM_NO_MORE_ENTRIES;
		}

		
	}
	
	return RC_OK;
}

RC closeTreeScan(BT_ScanHandle* handle)
{
	assert(handle != NULL);
	BTScanInfo* bscan = handle->mgmtData;
	free(bscan);
	free(handle);
	return RC_OK;
}

char *printTree(BTreeHandle *tree) {
    char *buffer = malloc(sizeof(char) * 4096);
    int offset = 0;
    BplusTree *bplustree = (BplusTree *) tree->mgmtData;
    Btree_meta *meta = bplustree->meta;
    if (meta->numEntries <= 0) return NULL;
    TreeNode *node;
    node = fetchNodeFromDisk(bplustree, bplustree->meta->root);
    int level = 0;
    int index = 0;
    buffer[offset] = '(';
    offset++;
    buffer[offset] = level + '0';
    offset++;
    buffer[offset] = ')';
    offset++;
    buffer[offset] = ' ';
    offset++;

    while (node != NULL) {
        if (node->type == Leaf) {// search to leaf node
            while (node->next != INVALID_KEY) {
                node = fetchNodeFromDisk(bplustree, node->next);
                buffer[offset] = '[';
                offset++;

                RID foundR = node->nodeValue[0].value;
                for (int i = 0; i < node->nchild; ++i) {
                    buffer[offset] = node->nodeValue->value.page + '0';
                    offset++;
                    buffer[offset] = '.';
                    offset++;
                    buffer[offset] = node->nodeValue->value.slot + '0';
                    offset++;
                    buffer[offset] = ' ';
                    offset++;
                }
                buffer[offset] = ']';
                offset++;
            }
            break;
        } else {// no leaf node

            while (node->next != INVALID_KEY) {
                node = fetchNodeFromDisk(bplustree, node->next);
                buffer[offset] = '[';
                offset++;

                RID foundR = node->nodeValue[0].value;
                for (int i = 0; i < node->nchild; ++i) {
                    buffer[offset] = node->nodeValue[i].child_ptr + '0';
                    offset++;
                    buffer[offset] = ',';
                    offset++;
                    buffer[offset] = node->key[i] + '0' + '0';
                    offset++;
                    buffer[offset] = ' ';
                    offset++;
                }
                buffer[offset] = ']';
                offset++;
            }

            TreeNode *subnode = fetchSubNode(bplustree, node, 0);
            freeNodeinMem(node);
            level++;
            buffer[offset] = '(';
            offset++;
            buffer[offset] = level + '0';
            offset++;
            buffer[offset] = ')';
            offset++;
            buffer[offset] = ' ';
            offset++;


        }
    }
    freeNodeinMem(node);
    buffer[offset] = '(';
    offset++;
    buffer[offset] = level + '0';
    offset++;
    buffer[offset] = ')';
    offset++;
    buffer[offset] = ' ';
    offset++;
    return buffer;
}

