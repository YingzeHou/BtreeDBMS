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
BTreeIndex::BTreeIndex(const std::string& relationName,
                       std::string& outIndexName, BufMgr* bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {

    // construct index name
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    outIndexName = idxStr.str();

    this->nodeOccupancy = INTARRAYNONLEAFSIZE;
    this->leafOccupancy = INTARRAYLEAFSIZE;
    this->bufMgr = bufMgrIn;
    this->scanExecuting = false;

    try{
        // if file exists
        file=new BlobFile(outIndexName, false);

        // read meta in headerPage
        this->headerPageNum = file->getFirstPageNo();
        Page *headerPage;
        bufMgr->readPage(file,headerPageNum,headerPage);
        IndexMetaInfo *metaInfo = (IndexMetaInfo *)headerPage;
        this->rootPageNum = metaInfo->rootPageNo;

        // Check for index match on relationName, attr byte offset, attr type etc.
        if(relationName!=metaInfo->relationName || attrByteOffset!=metaInfo->attrByteOffset || attrType!=metaInfo->attrType){
            throw BadIndexInfoException(outIndexName);
        }
        bufMgr->unPinPage(file,headerPageNum, false);
    }

    // create new if not exist
    catch (FileNotFoundException e){
        file = new BlobFile(outIndexName, true);
        Page *headerPage;
        Page *rootPage;

        bufMgr->allocPage(file,this->headerPageNum,headerPage);
        bufMgr->allocPage(file, this->rootPageNum, rootPage);

        IndexMetaInfo *metaInfo = (IndexMetaInfo *)headerPage;
        metaInfo->attrType = attrType;
        metaInfo->attrByteOffset = attrByteOffset;
        strncpy(metaInfo->relationName, relationName.c_str(), 20);
        metaInfo->rootPageNo = this->rootPageNum;


        // Initialize node
        LeafNodeInt *root = (LeafNodeInt *)rootPage;
        root->rightSibPageNo = -1;

        bufMgr->unPinPage(file, this->headerPageNum, true);
        bufMgr->unPinPage(file, this->rootPageNum, true);

        FileScan fileScan(relationName, bufMgr);
        RecordId rid;
        try{
            while(true){
                fileScan.scanNext(rid);
                std::string record = fileScan.getRecord();
                insertEntry(record.c_str()+attrByteOffset,rid);
            }
        }
        catch(EndOfFileException e){
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
    bufMgr->flushFile(BTreeIndex::file);
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
void BTreeIndex::insertEntry(const void* key, const RecordId rid) {
    // Scan from the root
    Page *rootPage;
    bufMgr->readPage(file,rootPageNum,rootPage);

    RIDKeyPair<int> ridKeyPair;
    ridKeyPair.set(rid, *(int *)key);

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm, const Operator lowOpParm,
                           const void* highValParm, const Operator highOpParm) {

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) {}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {}

}  // namespace badgerdb
