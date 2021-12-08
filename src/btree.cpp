/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertNodeLeaf
// -----------------------------------------------------------------------------

void BTreeIndex::insertNodeLeaf(LeafNodeInt *node, RIDKeyPair<int> entryInsertPair) 
{
	// for leaf node pages that are empty
	if (node->ridArray[0].page_number == 0) {
		node->ridArray[0] = entryInsertPair.rid;
		node->keyArray[0] = entryInsertPair.key;
	} else {
		// for leaf node pages that are not empty
		int idx = 0;
		for (int i = leafOccupancy - 1; i >= 0; --i) {
			if (node->ridArray[i].page_number != 0) {
				idx = i;
				break;
			}
		}
		// copy and move the record id and key arrays
		for (int i = idx; i >= 0; --i) {
			if (node->keyArray[i] <= entryInsertPair.key) {
				idx = i;
				break;
			} else {
				node->ridArray[i + 1] = node->ridArray[i];
				node->keyArray[i + 1] = node->keyArray[i];
				idx = i;
			}
		}
		// finally add the entry pair to be inserted
		node->ridArray[idx + 1] = entryInsertPair.rid;
		node->keyArray[idx + 1] = entryInsertPair.key;
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertNodeNonLeaf
// -----------------------------------------------------------------------------

void BTreeIndex::insertNodeNonLeaf(NonLeafNodeInt *node, PageKeyPair<int> *entryInsertPair)
{
	int idx = 0;
	for (int i = nodeOccupancy; i>= 0; --i) {
		if (node->pageNoArray[i] != 0) {
			idx = i;
			break;
		}
	}
	for (int i = idx; i > 0; --i) {
		if (node->keyArray[i - 1] <= entryInsertPair->key) {
			idx = i;
			break;
		} else {
			node->keyArray[i] = node->keyArray[i - 1];
			node->pageNoArray[i] = node->pageNoArray[i - 1];
			idx = i;
		}
	}
	// finally add the entry pair to be inserted
	node->keyArray[idx] = entryInsertPair->key;
	node->pageNoArray[idx + 1] = entryInsertPair->pageNo;
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertEntryHelper
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntryHelper(RIDKeyPair<int> entryInsertPair, PageKeyPair<int> *&entryPropPair, 
	Page *currPage, PageId currPageNum, bool isLeafNode)
{
	// when the current code is a non-leaf node
	if (!isLeafNode) {
		NonLeafNodeInt *node = (NonLeafNodeInt*) (currPage);
		int insertIndex = 0;
		int keysSize = nodeOccupancy;
		// search from the right to find the insert position
		for (int i = keysSize; i >= 0; --i) {
			if (node->pageNoArray[i] != 0) {
				insertIndex = i;
				break;
			}
		}
		for (int i = insertIndex; i >= 1; --i) {
			if (node->keyArray[i - 1] < entryInsertPair.key) {
				insertIndex = i;
				break;
			}
		}

		// recusively call the helper on the child node
		PageId childPageNo = node->pageNoArray[insertIndex];
		Page *childPage = nullptr;
		bufMgr->readPage(file, childPageNo, childPage);
		bool isChildLeafNode = node->level != 0;
		insertEntryHelper(entryInsertPair, entryPropPair, childPage, childPageNo, isChildLeafNode);

		// when the node should be splitted and the entry should be pushed up
		bool isDirty = false;
		if (entryPropPair) {
			// when the current node is not full
			if (node->pageNoArray[nodeOccupancy] == 0) {
				insertNodeNonLeaf(node, entryPropPair);
				isDirty = true;
				entryPropPair = nullptr;
				bufMgr->unPinPage(file, currPageNum, isDirty);
			} else {
				splitNodeNonLeaf(node, currPageNum, entryPropPair);
			}
		} else {
			bufMgr->unPinPage(file, currPageNum, isDirty);
		}
	} else {
		// when the current code is a leaf node
		LeafNodeInt *node = (LeafNodeInt*) (currPage);
		// insert the node directly when the page is not full
		if (node->ridArray[leafOccupancy - 1].page_number == 0) {
			insertNodeLeaf(node, entryInsertPair);
			entryPropPair = nullptr;
			bufMgr->unPinPage(file, currPageNum, true);
		} else {
			splitNodeLeaf(node, currPageNum, entryPropPair, entryInsertPair);
		}
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// for making changes to leaf node pages
	RIDKeyPair<int> entryInsertPair;
	int keyInt = *(int*) key;
	entryInsertPair.set(rid, keyInt);
	// for making changes to non-leaf node pages
	PageKeyPair<int> *entryPropPair = nullptr;
	bool isLeafNode = false;
	// read the root page node
	Page *root = nullptr;
	bufMgr->readPage(file, rootPageNum, root);
	bufMgr->unPinPage(file, rootPageNum, false);

	if (rootPageNum != 0) {
		insertEntryHelper(entryInsertPair, entryPropPair, root, rootPageNum, isLeafNode);
	} else {
		insertEntryHelper(entryInsertPair, entryPropPair, root, rootPageNum, !isLeafNode);
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
