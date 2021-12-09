/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

//#define DEBUG

namespace badgerdb {

/**
 * Constructor for BTreeIndex
 * First check if the specified index file exists
 * If not, constructed by concatenating the relational name with the offset of
 * the attribute over which the index is built.
 *
 * @param relationName The name of the relation on which to build the index.
 * @param outIndexName The name of the index file
 * @param bufMgrIn The instance of the global buffer manager.
 * @param attrByteOffset The byte offset of the attribute in the tuple on which
 * to build the index.
 * @param attrType The data type of the attribute we are indexing.
 * @throws  BadIndexInfoException     If the index file already exists for the
 * corresponding attribute, but values in metapage(relationName, attribute
 * byte offset, attribute type etc.) do not match with values received through
 * constructor parameters.
 */
BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName, BufMgr *bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {
  // construct index name
  std::ostringstream idxStr;
  idxStr << relationName << '.' << attrByteOffset;
  outIndexName = idxStr.str();

  bool exist = false;
  // Check if already exists
  if (File::exists(outIndexName)) {
    exist = true;
  }
  this->nodeOccupancy = INTARRAYNONLEAFSIZE;
  this->leafOccupancy = INTARRAYLEAFSIZE;
  this->bufMgr = bufMgrIn;
  this->scanExecuting = false;

  Page *headerPage;
  if (exist) {  // if exist

    // Read the file to construct the Btree
    this->file = new BlobFile(outIndexName, false);
    this->headerPageNum = this->file->getFirstPageNo();
    bufMgr->readPage(this->file, this->headerPageNum, headerPage);
    IndexMetaInfo *metaInfo = (IndexMetaInfo *)headerPage;
    this->rootPageNum = metaInfo->rootPageNo;

    // values in metapage not match
    if (relationName != metaInfo->relationName ||
        attrByteOffset != metaInfo->attrByteOffset ||
        attrType != metaInfo->attrType) {
      throw BadIndexInfoException(outIndexName);
    }
    // Unpin page as soon as possible
    bufMgr->unPinPage(this->file, this->headerPageNum, false);

  } else {  // not exist

    // Create new file
    this->file = new BlobFile(outIndexName, true);
    bufMgr->allocPage(this->file, this->headerPageNum, headerPage);
    Page *rootPage;
    bufMgr->allocPage(this->file, this->rootPageNum, rootPage);
    IndexMetaInfo *metaInfo = (IndexMetaInfo *)headerPage;

    // Read in metaInfo for this file
    strncpy(metaInfo->relationName, relationName.c_str(), 20);
    metaInfo->attrType = attrType;
    metaInfo->attrByteOffset = attrByteOffset;
    this->initial = rootPageNum;

    // Initialize root
    LeafNodeInt *root = (LeafNodeInt *)rootPage;
    root->rightSibPageNo = 0;
    metaInfo->rootPageNo = this->rootPageNum;

    // Unpin as soon as possible
    bufMgr->unPinPage(this->file, this->headerPageNum, true);
    bufMgr->unPinPage(this->file, this->rootPageNum, true);

    // Scan in and fill new file
    FileScan fileScan(relationName, bufMgr);
    RecordId rid;
    try {
      while (true) {
        fileScan.scanNext(rid);
        std::string record = fileScan.getRecord();
        insertEntry(record.c_str() + attrByteOffset, rid);
      }
    } catch (EndOfFileException e) {
      // Save to disk
      bufMgr->flushFile(file);
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

/**
 * The destructor
 * Flush and delete the index file
 */
BTreeIndex::~BTreeIndex() {
  this->bufMgr->flushFile(BTreeIndex::file);
  this->scanExecuting = false;
  delete file;
  this->file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertNodeLeaf
// -----------------------------------------------------------------------------

void BTreeIndex::insertNodeLeaf(LeafNodeInt *node,
                                RIDKeyPair<int> entryInsertPair) {
  // for leaf node pages that are not empty
  if (node->ridArray[0].page_number != 0) {
    int i = leafOccupancy - 1;
    for (; i >= 0 && node->ridArray[i].page_number == 0; --i) {}
    // copy and move the record id and key arrays
    for (; i >= 0 && node->keyArray[i] > entryInsertPair.key; --i) {
      node->ridArray[i + 1] = node->ridArray[i];
      node->keyArray[i + 1] = node->keyArray[i];
    }
    // finally add the entry pair to be inserted
    node->ridArray[i + 1] = entryInsertPair.rid;
    node->keyArray[i + 1] = entryInsertPair.key;
  } else {
    // simple case: for leaf node pages that are empty
    node->ridArray[0] = entryInsertPair.rid;
    node->keyArray[0] = entryInsertPair.key;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertNodeNonLeaf
// -----------------------------------------------------------------------------

void BTreeIndex::insertNodeNonLeaf(NonLeafNodeInt *node,
                                   PageKeyPair<int> *entryInsertPair) {
  // similar approach to inserting a leaf node
  int i = nodeOccupancy;
  for (; i >= 0 && node->pageNoArray[i] == 0; --i) {}
  for (; i > 0 && node->keyArray[i - 1] > entryInsertPair->key; --i) {
    node->keyArray[i] = node->keyArray[i - 1];
    node->pageNoArray[i + 1] = node->pageNoArray[i];
  }
  // finally add the entry pair to be inserted
  node->keyArray[i] = entryInsertPair->key;
  node->pageNoArray[i + 1] = entryInsertPair->pageNo;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntryHelper
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntryHelper(RIDKeyPair<int> entryInsertPair,
                                   PageKeyPair<int> *&entryPropPair,
                                   Page *currPage, PageId currPageNum,
                                   bool isLeafNode) {
  // when the current code is a non-leaf node
  if (!isLeafNode) {
    NonLeafNodeInt *node = (NonLeafNodeInt *)(currPage);
    // find the index of the next child to insert
    int i = nodeOccupancy;
    for (; i >= 0 && node->pageNoArray[i] == 0; --i) {}
    for (; i > 0 && node->keyArray[i - 1] >= entryInsertPair.key; --i) {}

    // recusively call the helper function on the child node
    PageId childPageNo = node->pageNoArray[i];
    Page *childPage = nullptr;
    bufMgr->readPage(file, childPageNo, childPage);
    bool isChildLeafNode = node->level != 0;
    insertEntryHelper(entryInsertPair, entryPropPair, childPage, childPageNo,
                      isChildLeafNode);

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
        // simple case: if full, directly split the node
        splitNonLeafNode(node, currPageNum, entryPropPair);
      }
    } else {
      bufMgr->unPinPage(file, currPageNum, isDirty);
    }
  } else {
    // when the current code is a leaf node
    LeafNodeInt *node = (LeafNodeInt *)(currPage);
    // insert the node directly when the page is not full
    if (node->ridArray[leafOccupancy - 1].page_number == 0) {
      insertNodeLeaf(node, entryInsertPair);
      entryPropPair = nullptr;
      bufMgr->unPinPage(file, currPageNum, true);
    } else {
      // simple case: if full, directly split the node
      splitLeafNode(node, currPageNum, entryPropPair, entryInsertPair);
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  // for making changes to leaf node pages
  RIDKeyPair<int> entryInsertPair;
  int keyInt = *(int *)key;
  entryInsertPair.set(rid, keyInt);
  // for making changes to non-leaf node pages
  PageKeyPair<int> *entryPropPair = nullptr;
  bool isLeafNode = false;
  // read the root page node
  Page *root = nullptr;
  bufMgr->readPage(file, rootPageNum, root);

  if (rootPageNum != initial) {
    // for the non leaf nodes
    insertEntryHelper(entryInsertPair, entryPropPair, root, rootPageNum,
                      isLeafNode);
  } else {
    // for the leaf nodes
    insertEntryHelper(entryInsertPair, entryPropPair, root, rootPageNum,
                      !isLeafNode);
  }
}

void BTreeIndex::splitLeafNode(LeafNodeInt *oldNode, PageId oldPageID,
                               PageKeyPair<int> *&pushUpPage,
                               RIDKeyPair<int> insertRecord) {
  // allocate space for a new leaf node
  Page *newPage;
  PageId newPageID;
  bufMgr->allocPage(file, newPageID, newPage);
  LeafNodeInt *newNode = (LeafNodeInt *)newPage;

  // split the old node into 2 parts from index [0, mid] and [mid + 1,
  // leafOccupancy - 1]
  int mid = leafOccupancy / 2 - 1;
  if (leafOccupancy % 2 == 0 && insertRecord.key >= oldNode->keyArray[mid])
    mid++;  // e.g: keyArray={3, 6, 8}, initially mid = 0 which points to 3.
            // insert 9 mid should be 1 to balance
  int index = 0;
  for (int i = mid + 1; i < leafOccupancy; i++) {
    newNode->keyArray[index] = oldNode->keyArray[i];
    oldNode->keyArray[i] = 0;
    newNode->ridArray[index] = oldNode->ridArray[i];
    oldNode->ridArray[i].page_number = 0;
    index++;
  }

  // insert the record to the appropriate part based on the key value
  if (insertRecord.key < oldNode->keyArray[mid]) {
    insertNodeLeaf(oldNode, insertRecord);
  } else {
    insertNodeLeaf(newNode, insertRecord);
  }

  // update sibling relation after inserting
  newNode->rightSibPageNo = oldNode->rightSibPageNo;
  oldNode->rightSibPageNo = newPageID;

  // copy the middle key value to the pushUpPage
  PageKeyPair<int> middleRecord;
  middleRecord.set(newPageID, newNode->keyArray[0]);
  pushUpPage = &middleRecord;

  // update root if the old node is root itself
  if (oldPageID == rootPageNum) {
    updateRoot(rootPageNum, pushUpPage);
  }

  // unpin
  bufMgr->unPinPage(file, oldPageID, true);
  bufMgr->unPinPage(file, newPageID, true);
}

void BTreeIndex::splitNonLeafNode(NonLeafNodeInt *oldNode, PageId oldPageID,
                                  PageKeyPair<int> *&pushUpPage) {
  // allocate space for a new leaf node
  Page *newPage;
  PageId newPageID;
  bufMgr->allocPage(file, newPageID, newPage);
  NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;

  int mid = nodeOccupancy / 2 - 1;
  if (nodeOccupancy % 2 == 0 && pushUpPage->key >= oldNode->keyArray[mid])
    mid++;
  mid++;  // for the non-leaf node, we make the left part larger by 1 because
          // it's easier to delete key from the end of the left part and
          // maintain structure
  int index = 0;
  for (int i = mid + 1; i < nodeOccupancy; i++) {
    newNode->keyArray[index] = oldNode->keyArray[i];
    oldNode->keyArray[i] = 0;
    newNode->pageNoArray[index] = oldNode->pageNoArray[i];
    oldNode->pageNoArray[i] = 0;
    index++;
  }

  // insert the record to the appropriate part based on the key value
  if (pushUpPage->key < newNode->keyArray[0]) {
    insertNodeNonLeaf(oldNode, pushUpPage);
  } else {
    insertNodeNonLeaf(newNode, pushUpPage);
  }

  // update sibling relation after inserting
  newNode->level = oldNode->level;

  // copy the middle key value to the pushUpPage
  PageKeyPair<int> middleRecord;
  middleRecord.set(newPageID, oldNode->keyArray[mid]);
  pushUpPage = &middleRecord;

  // delete the middle key from the end of the old node
  oldNode->keyArray[mid] = 0;
  oldNode->pageNoArray[mid] = 0;

  // update root if the old node is root itself
  if (oldPageID == rootPageNum) {
    updateRoot(rootPageNum, pushUpPage);
  }

  // unpin
  bufMgr->unPinPage(file, oldPageID, true);
  bufMgr->unPinPage(file, newPageID, true);
}

void BTreeIndex::updateRoot(PageId oldRootID, PageKeyPair<int> *pushUpPage) {
  // allocate space for the new root page
  Page *newRoot;
  PageId newRootID;
  bufMgr->allocPage(file, newRootID, newRoot);

  // retrive and update the old meta page
  Page *metaPage;
  bufMgr->readPage(file, headerPageNum, metaPage);
  IndexMetaInfo *metaInfoPage = (IndexMetaInfo *)metaPage;
  metaInfoPage->rootPageNo = newRootID;

  // define the new root node
  NonLeafNodeInt *newRootNode = (NonLeafNodeInt *)newRoot;
  newRootNode->level = initial == rootPageNum ? 1 : 0;
  rootPageNum = newRootID;
  newRootNode->keyArray[0] = pushUpPage->key;
  newRootNode->pageNoArray[0] = oldRootID;
  newRootNode->pageNoArray[1] = pushUpPage->pageNo;

  // unpin
  bufMgr->unPinPage(file, newRootID, true);
  bufMgr->unPinPage(file, headerPageNum, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

/**
 * Begin a filtered scan of the index.  For instance, if the method is called
 * using ("a",GT,"d",LTE) then we should seek all entries with a value
 * greater than "a" and less than or equal to "d".
 * If another scan is already executing, that needs to be ended here.
 * Set up all the variables for scan. Start from root to find out the leaf
 * page that contains the first RecordID that satisfies the scan parameters.
 * Keep that page pinned in the buffer pool.
 * @param lowVal	Low value of range, pointer to integer / double / char
 * string
 * @param lowOp		Low operator (GT/GTE)
 * @param highVal	High value of range, pointer to integer / double / char
 * string
 * @param highOp	High operator (LT/LTE)
 * @throws  BadOpcodesException If lowOp and highOp do not contain one of
 * their their expected values
 * @throws  BadScanrangeException If lowVal > highval
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that
 * satisfies the scan criteria.
 **/
void BTreeIndex::startScan(const void *lowValParm, const Operator lowOpParm,
                           const void *highValParm, const Operator highOpParm) {
  if (this->scanExecuting) {  // if scan in process
    // end here
    endScan();
  }
  this->lowValInt = *((int *)lowValParm);
  this->highValInt = *((int *)highValParm);
  this->lowOp = lowOpParm;
  this->highOp = highOpParm;

  // check for op exception
  if (!((lowOp == GT || lowOp == GTE) && (highOp == LT || highOp == LTE))) {
    throw BadOpcodesException();
  }

  // check for lowVal>highVal
  if (lowValInt > highValInt) {
    throw BadScanrangeException();
  }

  this->currentPageNum = this->rootPageNum;
  bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
  if (this->initial != currentPageNum) {  // root not leaf
    bool foundLeaf = false;
    while (!foundLeaf) {
      NonLeafNodeInt *current =
          reinterpret_cast<NonLeafNodeInt *>(this->currentPageData);
      if (current->level == 1) {  // Leaf in next level
        foundLeaf = true;
      }

      int index = 0;

      // traverse this level of node to find the first satisfactory index
      while (index < nodeOccupancy && current->keyArray[index] <= lowValInt &&
             current->pageNoArray[index + 1] != 0) {
        index++;
      }

      this->bufMgr->unPinPage(this->file, currentPageNum, false);

      // Read current page in and assign to target for future node traversal
      this->currentPageNum = current->pageNoArray[index];
      this->bufMgr->readPage(this->file, this->currentPageNum,
                             this->currentPageData);
    }
  }  // root is leaf
  bool found = false;
  while (!found) {
    LeafNodeInt *current =
        reinterpret_cast<LeafNodeInt *>(this->currentPageData);
    if (current->ridArray[0].page_number == 0) {  // check if entire page null
      bufMgr->unPinPage(file, currentPageNum, false);
      throw NoSuchKeyFoundException();
    }
    bool nextNull = false;
    for (int i = 0; i < leafOccupancy; i++) {
      if (nextNull) {
        break;
      }
      int key = current->keyArray[i];
      // if next entry is null
      if (i < leafOccupancy - 1 and current->ridArray[i + 1].page_number == 0) {
        nextNull = true;
      }
      if ((lowOp == GT && highOp == LTE && key <= highValInt &&
           key > lowValInt) ||
          (lowOp == GTE && highOp == LTE && key <= highValInt &&
           key >= lowValInt) ||
          (lowOp == GTE && highOp == LT && key < highValInt &&
           key >= lowValInt) ||
          (lowOp == GT && highOp == LT && key < highValInt &&
           key > lowValInt)) {
        found = true;
        scanExecuting = true;
        nextEntry = i;
        break;
      } else if ((highOp == LT && key >= highValInt) ||
                 (highOp == LTE && key > highValInt)) {
        // fail to find the key, unpin the page and throw the exception
        bufMgr->unPinPage(file, currentPageNum, false);
        throw NoSuchKeyFoundException();
      }
      if (i == leafOccupancy - 1 || nextNull) {  // no match, go to next leaf
        bufMgr->unPinPage(file, currentPageNum, false);
        if (current->rightSibPageNo == 0) {  // no more leaf
          throw NoSuchKeyFoundException();
        }
        this->currentPageNum = current->rightSibPageNo;
        bufMgr->readPage(file, currentPageNum, currentPageData);
      }
    }
  }
}

/**
 * Fetch the record id of the next index entry that matches the scan.
 * Return the next record from current page being scanned. If current page has
 *been scanned to its entirety, move on to the right sibling of current page,
 *if any exists, to start scanning that page. Make sure to unpin any pages
 *that are no longer required.
 * @param outRid	RecordId of next record found that satisfies the scan
 *criteria returned in this
 * @throws ScanNotInitializedException If no scan has been initialized.
 * @throws IndexScanCompletedException If no more records, satisfying the scan
 *criteria, are left to be scanned.
 **/
void BTreeIndex::scanNext(RecordId &outRid) {
  if (!scanExecuting) {  // not started
    throw ScanNotInitializedException();
  }
  LeafNodeInt *current = reinterpret_cast<LeafNodeInt *>(this->currentPageData);
  if (current->ridArray[nextEntry].page_number == 0 ||
      nextEntry == this->leafOccupancy - 1) {
    // Unpin page and read papge
    bufMgr->unPinPage(file, currentPageNum, false);
    // No more next leaf
    if (current->rightSibPageNo == 0) {
      throw IndexScanCompletedException();
    }

    // go to next leaf
    this->currentPageNum = current->rightSibPageNo;
    bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);

    // get current page
    current = reinterpret_cast<LeafNodeInt *>(this->currentPageData);
    nextEntry = 0;
  }

  if ((lowOp == GT && highOp == LT &&
       current->keyArray[nextEntry] > lowValInt &&
       current->keyArray[nextEntry] < highValInt) ||
      (lowOp == GTE && highOp == LT &&
       current->keyArray[nextEntry] >= lowValInt &&
       current->keyArray[nextEntry] < highValInt) ||
      (lowOp == GT && highOp == LTE &&
       current->keyArray[nextEntry] > lowValInt &&
       current->keyArray[nextEntry] <= highValInt) ||
      (lowOp == GTE && highOp == LTE &&
       current->keyArray[nextEntry] >= lowValInt &&
       current->keyArray[nextEntry] <= highValInt)) {
    outRid = current->ridArray[nextEntry];
    nextEntry++;
  } else {
    throw IndexScanCompletedException();
  }
}
/**
 * Terminate the current scan. Unpin any pinned pages. Reset scan specific
 *variables.
 * @throws ScanNotInitializedException If no scan has been initialized.
 **/
void BTreeIndex::endScan() {
  if (!scanExecuting) {
    throw ScanNotInitializedException();
  }

  currentPageData = nullptr;
  bufMgr->unPinPage(this->file, this->currentPageNum, false);
  this->scanExecuting = false;

  this->currentPageNum = -1;
  nextEntry = -1;
}
}  // namespace badgerdb