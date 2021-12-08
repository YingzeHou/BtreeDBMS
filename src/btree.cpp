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

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

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

  this->attrByteOffset = attrByteOffset;
  this->attributeType = attrType;
  this->nodeOccupancy = INTARRAYNONLEAFSIZE;
  this->leafOccupancy = INTARRAYLEAFSIZE;
  this->bufMgr = bufMgrIn;
  this->scanExecuting = false;
  this->currentPageData = NULL;

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
    IndexMetaInfo *metaInfo = (IndexMetaInfo *)headerPage;

    // Read in metaInfo for this file
    strncpy(metaInfo->relationName, relationName.c_str(), 20);
    metaInfo->attrType = attrType;
    metaInfo->attrByteOffset = attrByteOffset;

    // Initialize root
    Page *rootPage;
    bufMgr->allocPage(this->file, this->rootPageNum, rootPage);
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
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

/**
 * This method inserts a new entry into the index using the pair <key, rid>.
 * @param key A pointer to the value (integer) we want to insert.
 * @param rid The corresponding record id of the tuple in the base relation.
 */
void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  // Scan from the root
  Page *rootPage;
  bufMgr->readPage(file, rootPageNum, rootPage);

  RIDKeyPair<int> ridKeyPair;
  ridKeyPair.set(rid, *(int *)key);
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
    if(this->scanExecuting){ // if scan in process
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
    if(lowValInt>highValInt){
        throw BadScanrangeException();
    }

    this->currentPageNum = this->rootPageNum;
    bufMgr->readPage(this->file,this->currentPageNum, this->currentPageData);
    LeafNodeInt *target;
    if(true){ // root not leaf
        auto *current = (NonLeafNodeInt *)this->currentPageData;
        bool foundLeaf = false;
        while(true){
            if(current->level==1){ // Leaf in next level
                foundLeaf=true;
            }
            int index = 0;

            // traverse this level of node to find the first satisfactory index
            while(index<nodeOccupancy && current->keyArray[index]<=lowValInt && current->keyArray[index]!=0){
                index++;
            }
            this->bufMgr->unPinPage(this->file,currentPageNum, false);

            // Read current page in and assign to target for future node traversal
            this->currentPageNum = current->pageNoArray[index];
            this->bufMgr->readPage(this->file,this->currentPageNum,this->currentPageData);

            if(foundLeaf) { // if current node is one level above leaf, next node is leaf
                target = (LeafNodeInt *)currentPageData;
                break;
            }
            else{ // if current node not one level above leaf, next node is non-leaf, continue to traverse
                current = (NonLeafNodeInt *)this->currentPageData;
            }
        }
    }
    else{ // root is leaf
        target = (LeafNodeInt *)this->currentPageData;
    }
    if (target->ridArray[0].page_number == 0) {
        bufMgr->unPinPage(file, currentPageNum, false);
        throw NoSuchKeyFoundException();
    }
    bool found = false;
    while(true){
        for(int i = 0; i<leafOccupancy; i++){
            int key = target->keyArray[i];
            if ((highOp == LT && key >= highValInt) || (highOp == LTE && key > highValInt)) {
                //fail to find the key, unpin the page and throw the exception
                bufMgr->unPinPage(file, currentPageNum, false);
                throw NoSuchKeyFoundException();
            }
            if (target->keyArray[i] >= lowValInt) {
                //find the entry successfully
                found = true;
                scanExecuting = true;
                break;
            }
        }
        if(found){
            break;
        }

    }


}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId &outRid) {}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {}

}  // namespace badgerdb
