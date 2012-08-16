/**
 * Copyright (c) 2012, Akamai Technologies
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * 
 *   Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * 
 *   Neither the name of the Akamai Technologies nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdexcept>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/make_shared.hpp>
#include "Merger.hh"
#include "HdfsOperator.hh"
#include "ConstantScan.hh"
#include "RecordParser.hh"
#include "RuntimeProcess.hh"

struct SortNodeLess : std::binary_function<SortNode, SortNode, bool>
{
  RecordTypeEquals IQLCompare;
  bool operator() (const SortNode & lhs, const SortNode & rhs) const
  {
    return lhs.KeyPrefix < rhs.KeyPrefix ||
      (lhs.KeyPrefix == rhs.KeyPrefix && IQLCompare(lhs.Value, rhs.Value));
  }    
  SortNodeLess(const class IQLFunctionModule * f = NULL,
	       class InterpreterContext * ctxt = NULL)
    :
    IQLCompare(f,ctxt)
  {
  }
};

SortRun::SortRun(std::size_t memoryAllowed)
  :
  mBegin(NULL),
  mFilled(NULL),
  mEnd(NULL),
  mSortSz(0),
  mMemoryAllowed(memoryAllowed),
  mReallocThreshold(1.1)
{
}

SortRun::~SortRun()
{
  clear();
}

void SortRun::capacity(std::size_t numRecords)
{
  std::ptrdiff_t oldRecords = (mFilled - mBegin);
  SortNode * newBegin = new SortNode [numRecords];
  memcpy(newBegin, mBegin, sizeof(SortNode)*(mFilled - mBegin));
  mFilled = newBegin + (mFilled - mBegin);
  mEnd = newBegin + numRecords;
  std::swap(mBegin, newBegin);
  delete [] newBegin;
  mSortSz += (numRecords - oldRecords)*sizeof(SortNode);
}

bool SortRun::push_back_with_realloc(const SortNode& n, std::size_t dataLen)
{
  // Can we grow to accept the node?  This means allocating
  // space for both the node and the data pointed to by the 
  // node.  We have to balance efficiency in the reallocation
  // (not doing it too frequently) vs. fragmentation (allocating
  // too much for sort notes and not leaving enough for data).
  // We handle this by having two modes.  In mode 1 there is a large
  // amount of memory left and we can get away with a standard
  // "overallocate" by constant factor approach.  In mode 2
  // we use an estimate the average record size to figure out 
  // how many records to accept.
  std::size_t newSz = mSortSz + dataLen;
  std::ptrdiff_t numRecords = (mEnd - mBegin);
  // Why am I using newSz and not mSortSz?  Not really sure...
  if (numRecords==0 || newSz < mMemoryAllowed/2) {
    // We can only play the double buffer card when we know we
    // won't exceed our limit.
    std::size_t numNodes = mMemoryAllowed/(2*sizeof(SortNode));
    numNodes = (std::min)((std::max)(std::size_t(1),numNodes), std::size_t(8));
    capacity(numRecords ? (size_t) (numRecords*1.25) : numNodes);	  
  } else {
    // Assume we've got enough records around to take a stab
    // at the correct number of records.
    std::ptrdiff_t avgRecSz = mSortSz/numRecords;
    std::size_t targetNumberRecords = mMemoryAllowed/avgRecSz;
    // How much memory to play double buffer?
    std::size_t doubleBufferSpace = (mMemoryAllowed-mSortSz)/sizeof(SortNode);
    targetNumberRecords = (std::min)(doubleBufferSpace, targetNumberRecords);
    if (targetNumberRecords <= mReallocThreshold*numRecords) {
      return false;
    }
    capacity(targetNumberRecords);
  }
  // Recurse; we'll hit the simple case.
  return push_back(n, dataLen);
}

void SortRun::clear()
{
  delete [] mBegin;
  mFilled = mBegin = mEnd = 0;
  mSortSz = 0;
}

class SortWriterContext
{
public:
  typedef RecordBuffer port_type;
  typedef int32_t Services;
private:
  const RuntimeOperatorType & mType;
protected:
  void requestRead(std::size_t )
  {
  }
  void read(port_type port, RecordBuffer& buf)
  {
    buf = port;
  }
  void requestWrite(std::size_t )
  {
  }
  void write(port_type& port, RecordBuffer buf, bool flush)
  {
    port = buf;
  }
  const RuntimeOperatorType & getOperatorType() const
  {
    return mType;
  }
public:
  SortWriterContext(Services& , const RuntimeOperatorType& opType)
    :
    mType(opType)
  {
  }
};

template <class _WriterContext>
class RecordWriter : 
  public _WriterContext,
  public AsynchronousFileSystemReadHandler<stdio_file_traits::file_type>
{
public:
  typedef typename _WriterContext::Services services_type;
  typedef typename _WriterContext::port_type port_type;
  typedef AsyncFileTraits<stdio_file_traits> file_traits;
private:
  enum State { START, READ };
  State mState;
  const InternalFileWriteOperatorType &  getWriteType()
  {
    return *reinterpret_cast<const InternalFileWriteOperatorType *>(&_WriterContext::getOperatorType());
  }
  file_traits::filesystem_type mFileSystem;
  file_traits::file_type mFile;
  class Block
  {
  public:
    enum State { COMPLETED, OUTSTANDING } ;
    uint8_t * mBufferStart;
    uint8_t * mBufferPtr;
    uint8_t * mBufferEnd;
    State mState;
    Block()
      :
      mBufferStart(NULL),
      mBufferPtr(NULL),
      mBufferEnd(NULL),
      mState(COMPLETED)
    {
    }
  };
  Block mBlocks[2];
  std::size_t mCurrentBlock;
  ConcurrentBlockingFifo<int32_t> mWriteCompletionQueue;
  // The record buffer I am writing
  RecordBuffer mRecordBuffer;
  RecordBufferIterator mRecordBufferIt;
  bool mIsDone;
  /**
   * We only allow a single outstanding write.
   * So before writing, we make sure there are
   * no outstanding writes.
   */
  void completeWrites()
  {
    Block & b(mBlocks[(mCurrentBlock + 1) % 2]);
    while (b.mState == Block::OUTSTANDING) {
      BOOST_ASSERT(mOperationsOutstanding == 1);
      int32_t bytesWritten = mWriteCompletionQueue.pop();
      // TODO: Handle short write.
      b.mState = Block::COMPLETED;
      mOperationsOutstanding -= 1;
    }
    BOOST_ASSERT(mWriteCompletionQueue.size() == 0);
    BOOST_ASSERT(mOperationsOutstanding == 0);
  }

  void writeBuffer();

  int32_t mOperationsOutstanding;

  // op type
  typedef InternalFileWriteOperatorType operator_type;
  const operator_type& getMyOperatorType()
  {
    return *static_cast<const operator_type *>(&_WriterContext::getOperatorType());
  }
public:
  RecordWriter(services_type& services, const RuntimeOperatorType& opType);
  ~RecordWriter();
  void start();
  void onEvent(port_type port);
  void shutdown();

  void readComplete(int32_t bytesRead)
  {
    BOOST_ASSERT(false);
  }
  void openComplete(file_traits::file_type f)
  {
    mFile = f;
    mWriteCompletionQueue.push(0);
    // TODO: Handle failure here
  }
  void writeComplete(int32_t bytesRead)
  {
    mWriteCompletionQueue.push(bytesRead);
  }
};

template <class _WriterContext>
RecordWriter<_WriterContext>::RecordWriter(services_type& services, 
			   const RuntimeOperatorType& opType)
  :
  _WriterContext(services, opType),
  mState(START),
  mFileSystem(NULL),
  mFile(NULL),
  mCurrentBlock(0),
  mIsDone(false),
  mOperationsOutstanding(0)
{
  // Allocate buffers
  mBlocks[0].mBufferStart = mBlocks[0].mBufferPtr = new uint8_t [1024*1024];
  mBlocks[0].mBufferEnd = mBlocks[0].mBufferStart + 1024*1024;
  mBlocks[1].mBufferStart = mBlocks[1].mBufferPtr = new uint8_t [1024*1024];
  mBlocks[1].mBufferEnd = mBlocks[1].mBufferStart + 1024*1024;
}

template <class _WriterContext>
RecordWriter<_WriterContext>::~RecordWriter()
{
  delete [] mBlocks[0].mBufferStart;
  delete [] mBlocks[1].mBufferStart;
}

template <class _WriterContext>
void RecordWriter<_WriterContext>::start()
{
  BOOST_ASSERT(mOperationsOutstanding == 0);
  mIsDone = false;

  // Reset buffer pointers
  mBlocks[0].mBufferPtr = mBlocks[0].mBufferStart;
  mBlocks[1].mBufferPtr = mBlocks[1].mBufferStart;
  mCurrentBlock = 0;

  // Open file system and file
  mFileSystem = file_traits::openFor(getWriteType().mFile.c_str());
  file_traits::requestOpenForWrite(mFileSystem,
				   getWriteType().mFile.c_str(),
				   *this);
  mOperationsOutstanding += 1;
  BOOST_ASSERT(mOperationsOutstanding == 1);
  mState = START;
  onEvent(NULL);
}

template <class _WriterContext>
void RecordWriter<_WriterContext>::writeBuffer()
{
  // Make sure pending writes are done.
  completeWrites();
  // Issue a new write
  mBlocks[mCurrentBlock].mState = Block::OUTSTANDING;
  file_traits::requestWrite(mFileSystem,
			    mFile,
			    mBlocks[mCurrentBlock].mBufferStart,
			    (int32_t) (mBlocks[mCurrentBlock].mBufferPtr - 
				       mBlocks[mCurrentBlock].mBufferStart),
			    *this);
  mOperationsOutstanding += 1;
  BOOST_ASSERT(mOperationsOutstanding == 1);
  mBlocks[mCurrentBlock].mBufferPtr=mBlocks[mCurrentBlock].mBufferStart;
  mCurrentBlock = (mCurrentBlock + 1) % 2;
  BOOST_ASSERT(mBlocks[mCurrentBlock].mState == Block::COMPLETED);
}

template <class _WriterContext>
void RecordWriter<_WriterContext>::onEvent(port_type port)
{
  switch(mState) {
  case START:
    // Wait for file open to finish
    {
      BOOST_ASSERT(mOperationsOutstanding == 1);
      int32_t ret = mWriteCompletionQueue.pop();
      mOperationsOutstanding -= 1;
      BOOST_ASSERT(mOperationsOutstanding == 0);
      BOOST_ASSERT(mWriteCompletionQueue.size() == 0);
      BOOST_VERIFY(ret == 0);
      if (mFile == NULL) {
	std::string msg((boost::format("Failed opening file %1% for write") %
			 getWriteType().mFile).str());
	throw std::runtime_error(msg);
      }
    }
    while(!mIsDone) {
      // Allocate a buffer into which to write records.
      
      // Read records and serialize to fill up the buffer
      while(mBlocks[mCurrentBlock].mBufferPtr < 
	    mBlocks[mCurrentBlock].mBufferEnd) {
	// Go get a record if we aren't in the middle of one.
	if (mRecordBuffer.Ptr == NULL) {
	  _WriterContext::requestRead(0);
	  mState = READ;
	  return;
	case READ:
	  {
	    _WriterContext::read(port, mRecordBuffer);
	    if (!RecordBuffer::isEOS(mRecordBuffer)) {
	      mRecordBufferIt.init(mRecordBuffer);
	    } else {
	      // Set the EOF flag and break out of this loop.
	      mIsDone = true;
	      break;
	    }
	  }
	}
	// Try to serialize the whole thing.  This can fail if we exhaust the available output
	// buffer.
	if (getMyOperatorType().mSerialize.doit(mBlocks[mCurrentBlock].mBufferPtr, 
						mBlocks[mCurrentBlock].mBufferEnd, 
						mRecordBufferIt, 
						mRecordBuffer)) {
	  // Done with it so free record.
	  getMyOperatorType().mFree.free(mRecordBuffer);
	  mRecordBuffer = RecordBuffer();
	  mRecordBufferIt.clear();
	} else {
	  BOOST_ASSERT(mBlocks[mCurrentBlock].mBufferPtr == 
		       mBlocks[mCurrentBlock].mBufferEnd);
	}
      }

      BOOST_ASSERT (mBlocks[mCurrentBlock].mBufferPtr <= 
		    mBlocks[mCurrentBlock].mBufferEnd);

      if (mBlocks[mCurrentBlock].mBufferPtr != mBlocks[mCurrentBlock].mBufferStart) {
	writeBuffer();
      }
    }

    completeWrites();

    // Close file
    if (mFile != NULL) {
      file_traits::close(mFile);
      mFile = NULL;
    }
  }
}

template <class _WriterContext>
void RecordWriter<_WriterContext>::shutdown()
{
  // We should only have writes to complete
  // in some kind of abnormal termination situation.
  completeWrites();
  BOOST_ASSERT(mOperationsOutstanding == 0);
  BOOST_ASSERT(mWriteCompletionQueue.size() == 0);
  if (mFile != NULL) {
    file_traits::close(mFile);
    mFile = NULL;
  }
  if (mFileSystem != NULL) {
    file_traits::closeFileSystem(mFileSystem);
    mFileSystem = NULL;
  }
}

LogicalFileRead::LogicalFileRead()
  :
  CompileTimeLogicalOperator(false),
  mMode("binary"),
  mBucketed(false),
  mSkipHeader(false),
  mFieldSeparator('\t'),
  mRecordSeparator('\n')
{
}

LogicalFileRead::~LogicalFileRead()
{
}

std::string LogicalFileRead::readFormatFile(const std::string& formatFile)
{
  return FileSystem::readFile(formatFile);
}

void LogicalFileRead::check(PlanCheckContext& ctxt)
{
  const LogicalOperatorParam * formatParam=NULL;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    try {
      if (it->equals("comment")) {
	mCommentLine = getStringValue(ctxt, *it);
      } else if (it->equals("file")) {
	mFile = getStringValue(ctxt, *it);
      } else if (it->equals("fieldseparator")) {
	std::string tmp = getStringValue(ctxt, *it);
	if (tmp.size() == 1) {
	  mFieldSeparator = tmp[0];
	} else if (boost::algorithm::equals(tmp, "\\t")) {
	  mFieldSeparator = '\t';
	} else if (boost::algorithm::equals(tmp, "\\n")) {
	  mFieldSeparator = '\n';
	} else {
	  ctxt.logError(*this, "unsupported field separator");
	}
      } else if (it->equals("recordseparator")) {
	std::string tmp = getStringValue(ctxt, *it);
	if (tmp.size() == 1) {
	  mRecordSeparator = tmp[0];
	} else if (boost::algorithm::equals(tmp, "\\t")) {
	  mRecordSeparator = '\t';
	} else if (boost::algorithm::equals(tmp, "\\n")) {
	  mRecordSeparator = '\n';
	} else {
	  ctxt.logError(*this, "unsupported record separator");
	}
      } else if (it->equals("format")) {
	mStringFormat = getStringValue(ctxt, *it);
	formatParam = &*it;
      } else if (it->equals("formatfile")) {
	std::string filename(getStringValue(ctxt, *it));
	mStringFormat = readFormatFile(filename);
	formatParam = &*it;
      } else if (it->equals("mode")) {
	mMode = getStringValue(ctxt, *it);
      } else if (it->equals("bucketed")) {
	mBucketed = getBooleanValue(ctxt, *it);
      } else if (it->equals("broadcast")) {
	mConstantScan = getBooleanValue(ctxt, *it);
      } else if (it->equals("skipheader")) {
	mSkipHeader = getBooleanValue(ctxt, *it);
      } else {
	checkDefaultParam(*it);
      }
    } catch(std::runtime_error& ex) {
      ctxt.logError(*this, *it, ex.what());
    }
  }

  if (!boost::algorithm::iequals("binary", mMode) &&
      !boost::algorithm::iequals("text", mMode)) {
    ctxt.logError(*this, "mode parameter must be \"text\" or \"binary\"");
  }

  if (boost::algorithm::iequals("binary", mMode) &&
      mCommentLine.size()) {
    ctxt.logError(*this, "comment only supported when mode is \"text\"");
  }

  // We must have format parameter.
  if (NULL == formatParam || 0==mStringFormat.size()) {
    ctxt.logError(*this, "Must specify format argument");
  } else {
    try {
      IQLRecordTypeBuilder bld(ctxt, mStringFormat, false);
      getOutput(0)->setRecordType(bld.getProduct());
    } catch(std::exception& ex) {
      ctxt.logError(*this, *formatParam, ex.what());
    }
  }
}

void LogicalFileRead::internalCreate(class RuntimePlanBuilder& plan)
{
  typedef AsyncFileTraits<stdio_file_traits> file_traits;
  typedef AsyncDoubleBufferStream<file_traits> buffer_type;
  typedef InternalFileParserOperatorType<buffer_type> binary_op_type;

  typedef GenericParserOperatorType<ExplicitChunkStrategy> text_op_type;
  typedef GenericParserOperatorType<SerialChunkStrategy> serial_op_type;

  RuntimeOperatorType * opType = NULL;
  if(boost::algorithm::iequals("binary", mMode)) {
    opType = new binary_op_type(getOutput(0)->getRecordType(),
				mFile);
  } else if (mBucketed) {
    PathPtr p = Path::get(mFile);
    // Default to a local file URI.
    if (p->getUri()->getScheme().size() == 0)
      p = Path::get("file://" + mFile);
    serial_op_type * sot = new serial_op_type(p,
					      mFieldSeparator,
					      mRecordSeparator,
					      getOutput(0)->getRecordType(),
					      NULL,
					      mCommentLine.c_str());
    sot->setSkipHeader(mSkipHeader);
    opType = sot;
  } else {
    text_op_type * tot = new text_op_type(mFile,
					  mFieldSeparator,
					  mRecordSeparator,
					  getOutput(0)->getRecordType(),
					  NULL,
					  mCommentLine.c_str());
    tot->setSkipHeader(mSkipHeader);
    opType = tot;
  }
  plan.addOperatorType(opType);
  plan.mapOutputPort(this, 0, opType, 0);  
}

LogicalFileWrite::LogicalFileWrite()
  :
  mMode("binary"),
  mFileNameExpr(NULL)
{
}

LogicalFileWrite::~LogicalFileWrite()
{
  delete mFileNameExpr;
}

void LogicalFileWrite::buildHeader(bool isFormatHeader)
{
  if (mHeader.size() == 0) {
    // Make a header out of the input field names.
    const RecordType * input = getInput(0)->getRecordType();
    if (isFormatHeader) {
      mHeader = input->dumpTextFormat();
    } else {
      for(RecordType::const_member_iterator member = input->begin_members();
	  member != input->end_members();
	  ++member) {
	if (mHeader.size()) 
	  mHeader += "\t";
	mHeader += member->GetName();
      }
    }
    mHeader += "\n";
  }
}

void LogicalFileWrite::checkPath(PlanCheckContext& ctxt, 
				 const std::string& path)
{
  UriPtr uri = boost::make_shared<URI>(path.c_str());
  if (boost::algorithm::iequals(uri->getScheme(), "hdfs")) {
    FileSystem * fs = FileSystem::get(uri);
    if(fs->exists(Path::get(uri))) {
      ctxt.logError(*this, (boost::format("output path %1% exists") %
			    path).str());
    }
  }
}

void LogicalFileWrite::check(PlanCheckContext& ctxt)
{
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (it->equals("connect")) {
      mConnect = getStringValue(ctxt, *it);
    } else if (it->equals("file")) {
      mFile = getStringValue(ctxt, *it);
    } else if (it->equals("format")) {
      buildHeader(true);
    } else if (it->equals("formatfile")) {
      buildHeader(true);
      mHeaderFile = getStringValue(ctxt, *it);
    } else if (it->equals("header")) {
      buildHeader(false);
    } else if (it->equals("headerfile")) {
      buildHeader(false);
      mHeaderFile = getStringValue(ctxt, *it);
    } else if (it->equals("mode")) {
      mMode = getStringValue(ctxt, *it);
    } else {
      checkDefaultParam(*it);
    }
  }
  if (!boost::algorithm::iequals("binary", mMode) &&
      !boost::algorithm::iequals("text", mMode)) {
    ctxt.logError(*this, "mode parameter must be \"text\" or \"binary\"");
  }

  if (0==mConnect.size()) {
    checkPath(ctxt, mFile);
  } else {
    std::string xfer = mFile + " AS out";
    mFileNameExpr = new RecordTypeTransfer(ctxt, "fileNameExpr", 
					   getInput(0)->getRecordType(), xfer);
  }
  if (mHeaderFile.size()) 
    checkPath(ctxt, mHeaderFile);
}

void LogicalFileWrite::create(class RuntimePlanBuilder& plan)
{
  URI uri(mConnect.size() ? mConnect.c_str() : mFile.c_str());
  RuntimeOperatorType * opType = NULL;
  if (boost::algorithm::iequals(uri.getScheme(), "hdfs")) {
    opType = new RuntimeHdfsWriteOperatorType("write",
					      getInput(0)->getRecordType(),
					      uri.getHost(),
					      uri.getPort(),
					      mConnect.size() ? "" : uri.getPath(),
					      mHeader,
					      mHeaderFile,
					      mFileNameExpr);
  } else if (boost::algorithm::iequals("binary", mMode)) {
    opType = new InternalFileWriteOperatorType("write",
					       getInput(0)->getRecordType(),
					       uri.getPath());
  } else {
    opType = new RuntimeWriteOperatorType("write",
					  getInput(0)->getRecordType(),
					  uri.getPath(),
					  mHeader,
					  mHeaderFile);
  }
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
}

RuntimeOperator * InternalFileWriteOperatorType::create(RuntimeOperator::Services& services) const
{
  // return new InternalFileWriteOperator(services, *this);
  return new RecordWriter<RuntimeOperator>(services, *this);
}

LogicalSortMerge::LogicalSortMerge()
  :
  mKeyPrefix(NULL),
  mKeyEq(NULL)
  
{
}

LogicalSortMerge::~LogicalSortMerge()
{
  delete mKeyPrefix;
  delete mKeyEq;
}

void LogicalSortMerge::check(PlanCheckContext& ctxt)
{
  std::vector<SortKey> sortKeys;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (it->equals("key")) {
      sortKeys.push_back(getSortKeyValue(ctxt, *it));
    } else {
      checkDefaultParam(*it);
    }
  }

  // Process arguments
  if (getInput(0)->getRecordType() == NULL) {
  }
  checkIdenticalInputs(ctxt);
  const RecordType * input = getInput(0)->getRecordType();
  getOutput(0)->setRecordType(input);

  // Validate that the keys exist and are sortable.
  checkFieldsExist(ctxt, sortKeys, 0);
  
  // Build key prefix extraction and less than predicate
  // for sorting.  Make sure less than has proper NULL handling.
  mKeyPrefix = SortKeyPrefixFunction::get(ctxt, input, sortKeys);
  mKeyEq = LessThanFunction::get(ctxt, input, input, sortKeys, true, "sort_merge_less");
}

void LogicalSortMerge::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimeSortMergeOperatorType(mKeyPrefix, 
				     mKeyEq);
  plan.addOperatorType(opType);
  for(std::size_t i = 0; i < size_inputs(); i++) {
    plan.mapInputPort(this, i, opType, i);
  }
  plan.mapOutputPort(this, 0, opType, 0);  
}

SortMerge::SortMerge(DynamicRecordContext& ctxt,
		     const RecordType * input,
		     const std::vector<std::string>& sortKeys)
  :
  mInput(input),
  mKeyPrefix(NULL),
  mKeyEq(NULL)
{
  // Start by supporting a single integer key.
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  std::vector<const RecordType *> tableOnly;
  tableOnly.push_back(mInput);
  tableOnly.push_back(&emptyTy);
  mKeyPrefix = new RecordTypeFunction(ctxt, 
				      "keyPrefix", 
				      tableOnly, 
				      (boost::format("$(%1%)") % sortKeys[0]).str());


  mKeyEq = LessThanFunction::get(ctxt, mInput, sortKeys);
}

SortMerge::~SortMerge()
{
  delete mKeyPrefix;
  delete mKeyEq;
}

RuntimeOperatorType * SortMerge::create() const
{
  return new RuntimeSortMergeOperatorType(mKeyPrefix, mKeyEq);
}

RuntimeOperator * RuntimeSortMergeOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeSortMergeOperator(s, *this);
}

RuntimeSortMergeOperator::RuntimeSortMergeOperator(RuntimeOperator::Services& services, 
						   const RuntimeSortMergeOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeSortMergeOperatorType>(services, opType),
  mState(START),
  mRuntimeContext(new InterpreterContext()),
  mReturnAddress(NULL),
  mReturnPort(0)
{
}

RuntimeSortMergeOperator::~RuntimeSortMergeOperator()
{
}

void RuntimeSortMergeOperator::start()
{
  // Initialize the tournament tree.  
  if (getInputPorts().size() > std::numeric_limits<uint32_t>::max()) {
    // This is never going to happen, but what the heck.
    throw std::runtime_error("Too many inputs to sort merge operator");
  }
  RecordTypeEquals lt(getMyOperatorType().mEqFun,
		      mRuntimeContext);
  mMergeTree.init((uint32_t) getInputPorts().size(),
		  NotPred<RecordTypeEquals>(lt));

  mState=START;
  onEvent(NULL);
}

void RuntimeSortMergeOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(!mMergeTree.empty()) {
      if (!mMergeTree.isHighSentinel()) {
	requestWrite(0);
	mState = WRITE;
	return;
      case WRITE:
	write(port, mMergeTree.getValue(), false);
      }
      
      // Get the next value
      requestRead(mMergeTree.getInput());
      mState = READ;
      return;
    case READ:
      {
	RecordBuffer buf;
	read(port, buf);
	if(RecordBuffer::isEOS(buf)) {
	  mMergeTree.close(mMergeTree.getInput());
	} else {
	  uint32_t keyPrefix = getMyOperatorType().mKeyPrefix->execute(buf, NULL, mRuntimeContext);
	  // Correct key prefix for the fact that LoserTree is a max-priority queue.
	  mMergeTree.update(mMergeTree.getInput(), 
			    0x7fffffff - keyPrefix, 
			    buf);
	}	
      }
    }

    if (mReturnAddress) {
      // Don't write EOS, splice return address back into dataflow.
      // It might be good to encapsulate this behavior into an operator...
      RuntimePort * myOutput = *output_port_begin();
      BOOST_ASSERT(myOutput->getQueueIndex() == RuntimePort::DISABLED);
      BOOST_ASSERT(*mReturnAddress->output_port_begin() == myOutput);
      mReturnAddress->setOutputPort(myOutput, mReturnPort);
      myOutput->setOperator(*mReturnAddress);
      mReturnAddress->onEvent(NULL);
    } else {
      requestWrite(0);
      mState = WRITE_EOS;
      return;
    case WRITE_EOS:
      write(port, RecordBuffer(), true);
    }
  }
}

void RuntimeSortMergeOperator::shutdown()
{
}

void RuntimeSortMergeOperator::setReturnAddress(RuntimeOperator * op, int32_t port)
{
  mReturnAddress = op;
  mReturnPort = port;
}

LogicalSort::LogicalSort()
  :
  mKeyPrefix(NULL),
  mKeyEq(NULL),
  mPresortedKeyEq(NULL),
  mMemory(128*1024*1024)
{
}

LogicalSort::~LogicalSort()
{
  delete mKeyPrefix;
  delete mKeyEq;
  delete mPresortedKeyEq;
}

void LogicalSort::check(PlanCheckContext& ctxt)
{
  std::vector<SortKey> presortedKeys;
  std::vector<SortKey> sortKeys;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (it->equals("key")) {
      sortKeys.push_back(getSortKeyValue(ctxt, *it));
    } else if (it->equals("memory")) {
      int32_t tmp = getInt32Value(ctxt, *it);
      if (tmp < 0) {
	ctxt.logError(*this, "memory argument must be a positive integer");
      } else {
	mMemory = (std::size_t) tmp;
      }
    } else if (it->equals("presorted")) {
      presortedKeys.push_back(getSortKeyValue(ctxt, *it));
    } else if (it->equals("tempdir")) {
      mTempDir = getStringValue(ctxt, *it);
    } else {
      checkDefaultParam(*it);
    }
  }

  // Process arguments
  if (size_inputs() != 1) {
  } 
  if (getInput(0)->getRecordType() == NULL) {
  }
  const RecordType * input = getInput(0)->getRecordType();
  getOutput(0)->setRecordType(input);

  // Validate that the keys exist and are sortable.
  checkFieldsExist(ctxt, sortKeys, 0);
  checkFieldsExist(ctxt, presortedKeys, 0);
  
  // Build key prefix extraction and less than predicate
  // for sorting.
  mKeyPrefix = SortKeyPrefixFunction::get(ctxt, input, sortKeys);
  // Tell LessThan to sort NULLs properly
  mKeyEq = LessThanFunction::get(ctxt, input, input, sortKeys, true, "sort_less");
  if (presortedKeys.size()) {
    mPresortedKeyEq = EqualsFunction::get(ctxt, input, input, presortedKeys, 
					  "presort_eq", true);
  }
}

void LogicalSort::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimeSortOperatorType(getInput(0)->getRecordType(),
				mKeyPrefix, 
				mKeyEq,
				// TODO: Support presorted keys
				mPresortedKeyEq,
				mTempDir,
				mMemory);
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);
  plan.mapOutputPort(this, 0, opType, 0);  
}

#define NOCACHEOPTIMIZATION 1
#ifdef NOCACHEOPTIMIZATION
class RuntimeSortOperator : public RuntimeOperatorBase<RuntimeSortOperatorType>
{
private:
  enum State { START, READ, WRITE, WAIT_FOR_MERGE, WRITE_EOS };
  State mState;
  
  /**
   * Braindead simple sort run building using quicksort.
   * TODO: Look at building sort runs with L1 Cache (e.g. 32KB) sized sort
   * runs that are merged using Loser tree.  This means quicksorting
   * 2048 records at a time (say do 1024 of these) and then merging to form
   * a sort run.  Merging will have terrible L1 cache locality but on a modern
   * processor might look pretty good from an L2 point of view.
   * TODO: Prototype a replacement selection method using LoserTree.
   */
  SortNodeLess mLessFunction;
  SortRun mSortRuns;
  SortRun::iterator mSortRunIt;
  uint64_t mSortTicks;
  /**
   * Current input record.
   */
  RecordBuffer mInput;
  /**
   * Are we done reading
   */
  bool mInputDone;

  // For writing of sort runs to disk
  InternalFileWriteOperatorType * mWriterType;
  RecordWriter<SortWriterContext> * mWriter;  
  std::vector<std::string> mSortFiles;
  /**
   * Add an input record to sort run.
   */
  void addSortRun();
  /**
   * Sort in memory buffers and write to disk.
   */
  void writeSortRun(SortRun & sortRun);
  void writeSortRun()
  {
    writeSortRun(mSortRuns);
  }

  /**
   * For merging we are dynamically generating a
   * dataflow graph (once we know the number of
   * sort runs) and inserting into the current
   * flow.  These are operators of that dynamic 
   * graph.
   */
  std::vector<RuntimeOperatorType *> mMergeTypes;
  std::vector<RuntimeOperator *> mMergeOps;
  std::vector<InProcessFifo *> mChannels;
  /**
   * Build the merge plan for the sort run
   * files we possess.
   */
  void buildMergeGraph();
  void freeMergeGraph();
public:
  RuntimeSortOperator(RuntimeOperator::Services& services, const RuntimeSortOperatorType& opType);
  ~RuntimeSortOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};
#else
class RuntimeSortOperator : public RuntimeOperatorBase<RuntimeSortOperatorType>
{
private:
  enum State { START, READ, WRITE, WRITE_EOS };
  State mState;
  
  /**
   * Braindead simple sort run building using quicksort.
   * TODO: Look at building sort runs with L1 Cache (e.g. 32KB) sized sort
   * runs that are merged using Loser tree.  This means quicksorting
   * 2048 records at a time (say do 1024 of these) and then merging to form
   * a sort run.  Merging will have terrible L1 cache locality but on a modern
   * processor might look pretty good from an L2 point of view.
   * TODO: Prototype a replacement selection method using LoserTree.
   */
  class SortNode
  {
  public:
    // Should we make the prefix bigger since
    // the padding is there or should be look
    // at storing an index instead of a pointer?
    // Probably the former unless we copied the
    // records into a contiguous buffer so
    // that we could arrange a 32-bit address
    // relative to a known base (that could be squirreled
    // away in compare function's state).
    uint32_t KeyPrefix;
    RecordBuffer Value;
    SortNode()
      :
      KeyPrefix(0xffffffff)
    {
    }
    SortNode(uint32_t keyPrefix, RecordBuffer val)
      :
      KeyPrefix(keyPrefix),
      Value(val)
    {
    }
  };
  struct SortNodeLess : std::binary_function<SortNode, SortNode, bool>
  {
    RecordTypeEquals IQLCompare;
    bool operator() (const SortNode & lhs, const SortNode & rhs) const
    {
      return lhs.KeyPrefix < rhs.KeyPrefix ||
	(lhs.KeyPrefix == rhs.KeyPrefix && IQLCompare(lhs.Value, rhs.Value));
    }    
    SortNodeLess(const class IQLFunctionModule * f = NULL,
		 class InterpreterContext * ctxt = NULL)
      :
      IQLCompare(f,ctxt)
    {
    }
  };

  SortNodeLess mLessFunction;
  // The mini run we are building
  SortNode * mCurrentRunStart;
  SortNode * mCurrentRunPtr;
  SortNode * mCurrentRunEnd;
  // The list of mini-runs we have built
  std::vector<std::pair<SortNode*,SortNode*> > mSortRuns;
  // Tournament tree for merging mini-runs
  LoserTree<RecordBuffer,NotPred<RecordTypeEquals> >mMergeTree;
  // Done reading input?
  bool mInputDone;
public:
  RuntimeSortOperator(RuntimeOperator::Services& services, const RuntimeSortOperatorType& opType);
  ~RuntimeSortOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};
#endif

#ifdef NOCACHEOPTIMIZATION
RuntimeSortOperator::RuntimeSortOperator(RuntimeOperator::Services& services, 
					 const RuntimeSortOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeSortOperatorType>(services, opType),
  mState(START),
  mLessFunction(opType.mLessThanFun, new InterpreterContext()),
  mSortTicks(0),
  mSortRuns(opType.mMemoryAllowed),
  mInputDone(false),
  mWriterType(NULL),
  mWriter(NULL)
{
}

RuntimeSortOperator::~RuntimeSortOperator()
{
  if (mWriter) {
    mWriter->shutdown();
  }
  delete mWriterType;
  delete mWriter;
  freeMergeGraph();
}

__inline__ uint64_t rdtsc() {
uint32_t lo, hi;
__asm__ __volatile__ (      // serialize
"xorl %%eax,%%eax \n        cpuid"
::: "%rax", "%rbx", "%rcx", "%rdx");
/* We cannot use "=A", since this would use %rax on x86_64 */
__asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
return (uint64_t)hi << 32 | lo;
}

void RuntimeSortOperator::addSortRun()
{
  std::size_t sz = getMyOperatorType().mSerialize.getRecordLength(mInput);
  uint32_t keyPrefix = 
    getMyOperatorType().mKeyPrefix->execute(mInput, 
					    NULL, 
					    mLessFunction.IQLCompare.Context);
  SortNode n(keyPrefix, mInput);
  // See if we are forced to spill
  if(!mSortRuns.push_back(n, sz)) {
    writeSortRun();
    // Empty sort run; try again.
    if (!mSortRuns.push_back(n, sz)) {
      // To handle the case in which a single record is bigger than
      // allocated memory, we head straight to disk.  We do that by
      // creating a temporary SortRun object with no memory limit and 
      // writing through it.
      SortRun tmp(std::numeric_limits<std::size_t>::max());
      if (!tmp.push_back(n, sz)) {
	throw std::runtime_error("INTERNAL ERROR : "
				 "Insufficient memory to sort");
      }
      writeSortRun(tmp);
    }
  }
  mInput = RecordBuffer();
}

void RuntimeSortOperator::writeSortRun(SortRun & sortRuns)
{
  // Sort in memory data.
  uint64_t tick = rdtsc();
  std::sort(sortRuns.begin(), sortRuns.end(), mLessFunction);
  mSortTicks += (rdtsc()-tick);

  if (mWriterType == NULL) {
    // Write a sort run to disk
    mWriterType = 
      new InternalFileWriteOperatorType("sortInternalWriter",
					getMyOperatorType().mSerialize,
					getMyOperatorType().mFree,			   
					"");

    int32_t dummy=0;
    mWriter = new RecordWriter<SortWriterContext>(dummy,
						  *mWriterType);
  }

  // Create a name for new sort run.
  std::string tmpStr = FileSystem::getTempFileName();
  std::string tmpDir = getMyOperatorType().mTempDir;
  if(tmpDir.size() == 0) {
    tmpDir = (boost::format("/ghostcache/hadoop/temp/%1%") %
	      ::getenv("USER")).str();
    if (!boost::filesystem::exists(tmpDir))
      tmpDir = "/usr/local/akamai/tmp";
  } else {
    if (!boost::filesystem::exists(tmpDir))
      throw std::runtime_error((boost::format("Temp directory doesn't exists: %1%") %
				tmpDir).str());
  }
  mWriterType->mFile = (boost::format("%1%/sort_%2%.bin") %
			tmpDir %
			tmpStr).str();
  mSortFiles.push_back(mWriterType->mFile);
  // std::cout << "Operator " << this << " writing sort run: " << mWriterType->mFile.c_str() <<
  //   "; size: " << sortRuns.size() << "; memory: " << sortRuns.memory() << std::endl;
  mWriter->start();
  // Write out sort run
  for(mSortRunIt = sortRuns.begin(); 
      mSortRunIt != sortRuns.end();
      ++mSortRunIt) {
    mWriter->onEvent(mSortRunIt->Value);
  }
  mWriter->onEvent(RecordBuffer());
  sortRuns.clear();
}

void RuntimeSortOperator::buildMergeGraph()
{
  typedef AsyncFileTraits<stdio_file_traits> file_traits;
  typedef AsyncDoubleBufferStream<file_traits> buffer_type;
  typedef InternalFileParserOperatorType<buffer_type> op_type;
  typedef op_type::chunk_type chunk_type;
  BOOST_ASSERT(mMergeTypes.size() == 0);
  BOOST_ASSERT(mMergeOps.size() == 0);
  BOOST_ASSERT(mChannels.size() == 0);
  const RuntimeSortOperatorType & myType(getMyOperatorType());
  // How much memory do we have for input buffers?
  // TODO: At what point are we better off doing a multi stage
  // merge?  
  // TODO: We're breaking contract by enforcing a minimum page size.  In this
  // case we MUST use a multi-pass merge.
  // Window size for reads.  Make a multiple of page size (or a guess
  // as to page size).
  static const std::size_t pageSz(4096);
  static const std::size_t maxWindowSz(1024*1024);  
  std::size_t windowSz = 
    buffer_type::getWindowSize(myType.mMemoryAllowed/mSortFiles.size());
  windowSz = pageSz*(windowSz / pageSz);
  // if (windowSz == 0) {
  //   std::cout << "Warning: enforcing minimum page size" << std::endl;
  // }
  windowSz = (std::max)(pageSz, (std::min)(windowSz, maxWindowSz));

  // std::cout << "Merging: num files: " << mSortFiles.size() <<
  //   "; window size: " << windowSz << 
  //   "; total memory: " << myType.mMemoryAllowed << std::endl;
  // Configure the graph
  for(std::vector<std::string>::iterator it = mSortFiles.begin();
      it != mSortFiles.end();
      ++it) {
    chunk_type files(getPartition()+1);
    files[getPartition()].push_back(boost::make_shared<FileChunk>(*it,
								  0,
								  std::numeric_limits<uint64_t>::max()));
    op_type * readOpType = new op_type(getMyOperatorType().mDeserialize,
				       getMyOperatorType().mMalloc,
				       files,
				       windowSz,
				       true);
    mMergeTypes.push_back(readOpType);
    mMergeOps.push_back(readOpType->create(getServices()));
  }
  RuntimeSortMergeOperatorType * mergeOpType
    = new RuntimeSortMergeOperatorType (getMyOperatorType().mKeyPrefix,
					getMyOperatorType().mLessThanFun);
  mMergeTypes.push_back(mergeOpType);
  mMergeOps.push_back(mergeOpType->create(getServices()));
  ((RuntimeSortMergeOperator *) mMergeOps.back())->setReturnAddress(this, 0);
  for(std::vector<RuntimeOperator*>::iterator it = mMergeOps.begin();
      (it+1) != mMergeOps.end();
      ++it) {
    // No need for fifo buffers when merging data from disk.
    mChannels.push_back(new InProcessFifo(getServices(), 
					  getServices(),
					  false));
    (*it)->setOutputPort(mChannels.back()->getSource(), 
			 0);
    mChannels.back()->getSource()->setOperator(**it);
    mMergeOps.back()->setInputPort(mChannels.back()->getTarget(), 
				   it - mMergeOps.begin());
    mChannels.back()->getTarget()->setOperator(*mMergeOps.back());    
  }
  
  // Magic to replace the output of this sort operator with the output
  // of the sort merge. Now the dynamically created graph is connected
  // to the graph we are running in.
  RuntimePort * myOutput = *output_port_begin();
  BOOST_ASSERT(myOutput->getQueueIndex() == RuntimePort::DISABLED);
  // Add the operators to the current scheduler and
  // start them.
  getServices().setOperators(mMergeOps);
  // VERY SUBTLE.  We add the operators to the scheduler
  // before remapping because adding the operators will 
  // put all of the ports on these operators into the scheudler
  // queues.  The output of sort merger is already in the scheduler's
  // queue because it was attached the sort operator (in fact
  // as we have asserted above it was in the disabled queue of
  // the scheduler).
  mMergeOps.back()->setOutputPort(myOutput, 0);
  myOutput->setOperator(*mMergeOps.back());
  getServices().start(mMergeOps.begin(), mMergeOps.end());
}

void RuntimeSortOperator::freeMergeGraph()
{
  // if(mMergeOps.size()) {
  //   std::cout << "Completed merge" << std::endl;
  // }
  getServices().shutdownAndRemoveOperators(mMergeOps.begin(), mMergeOps.end());
  getServices().removeChannels(mChannels.begin(), mChannels.end());
  for(std::vector<RuntimeOperatorType *>::iterator it =  mMergeTypes.begin();
      it != mMergeTypes.end();
      ++it) {
    delete *it;
  }
  for(std::vector<RuntimeOperator *>::iterator it = mMergeOps.begin();
      it != mMergeOps.end();
      ++it) {
    delete *it;
  }
  for(std::vector<InProcessFifo *>::iterator it = mChannels.begin();
      it != mChannels.end();
      ++it) {
    delete *it;
  }
  mMergeTypes.clear();
  mMergeOps.clear();
  mChannels.clear();
}

void RuntimeSortOperator::start()
{
  mState=START;
  mInputDone = false;
  onEvent(NULL);
}

void RuntimeSortOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    // Read records
    while(!mInputDone) {
      while(true) {
	requestRead(0);
	mState = READ;
	return;
      case READ:
	{
	  read(port, mInput);
	  // Check for EOS
	  mInputDone=RecordBuffer::isEOS(mInput);
	  if (mInputDone) 
	    break;
	  // If we have presorted keys, see if we have a new presorted
	  // key value.
	  if (mSortRuns.size() &&
	      getMyOperatorType().mPresortedEqualsFun &&	      
	      0 == getMyOperatorType().mPresortedEqualsFun->execute(mInput,
								    mSortRuns.front().Value,
								    mLessFunction.IQLCompare.Context)) 
	    break;
	  // Otherwise we building sort runs.
	  addSortRun();
	}
      }

      if (mSortFiles.size()) {
	// Write any final in memory data.
	if (mSortRuns.size()) {
	  writeSortRun();
	}
	// Done writing sort runs to disk
	// free up memory used for writing.
	if (mWriter) {
	  mWriter->shutdown();
	}
	delete mWriter;
	mWriter = NULL;
	delete mWriterType;
	mWriterType = NULL;
	mSortRuns.clear();
	// Dynamically construct the graph of operators that implements
	// merging of sort runs.  Splice the output into the exiting
	// graph by replacing the output of this operator with the
	// output of the final sort merge.  When the sort merge completes
	// it doesn't write EOS but rather splices us back in and we have to
	// write EOS when appropriate.
	buildMergeGraph();
	mState = WAIT_FOR_MERGE;
	return;
      case WAIT_FOR_MERGE:
	freeMergeGraph();
	mSortFiles.clear();
      } else {
	// We didn't have to spill.
	// Sort in memory data.
	{
	  uint64_t tick = rdtsc();
	  std::sort(mSortRuns.begin(), mSortRuns.end(), mLessFunction);
	  mSortTicks += (rdtsc()-tick);
	}
	// Write out sort run directly from memory.
	for(mSortRunIt = mSortRuns.begin(); 
	    mSortRunIt != mSortRuns.end();
	    ++mSortRunIt) {
	  requestWrite(0);
	  mState = WRITE;
	  return;
	case WRITE:
	  write(port, mSortRunIt->Value, false);
	}
      }
      // Clear all of the state with sort runs
      mSortRuns.clear();
      if (!mInputDone) 
	addSortRun();
    }
    // Wrap up with EOS
    requestWrite(0);
    mState = WRITE_EOS;
    return;
  case WRITE_EOS:
    write(port, RecordBuffer(), true);
  }
}

void RuntimeSortOperator::shutdown()
{
}

#else
RuntimeSortOperator::RuntimeSortOperator(RuntimeOperator::Services& services, 
					 const RuntimeSortOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeSortOperatorType>(services, opType),
  mState(START),
  mLessFunction(opType.mLessThanFun, new InterpreterContext()),
  mCurrentRunStart(NULL),
  mCurrentRunPtr(NULL),
  mCurrentRunEnd(NULL),
  mInputDone(false)
{
}

RuntimeSortOperator::~RuntimeSortOperator()
{
}

void RuntimeSortOperator::start()
{
  mInputDone = false;
  mState=START;  
  onEvent(NULL);
}

void RuntimeSortOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    // Read records
    // while(true) {
    while(!mInputDone) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      {
	RuntimePort::local_buffer_type& localPortBuf(readLocal(port));
	while(!localPortBuf.empty()) {
	RecordBuffer buf;
	// read(port, buf);
	localPortBuf.Pop(buf);
	if (RecordBuffer::isEOS(buf)) {
	  mInputDone = true;
	  break;
	} else {
	  uint32_t keyPrefix = 
	    getMyOperatorType().mKeyPrefix->execute(buf, 
						    NULL, 
						    mLessFunction.IQLCompare.Context);
	  if (mCurrentRunPtr == mCurrentRunEnd) {
	    // Sort previous L1 cache size buffer and save
	    // block for later merging.  
	    if (mCurrentRunStart) {
	      std::sort(mCurrentRunStart, mCurrentRunEnd, mLessFunction);
	      mSortRuns.push_back(std::make_pair(mCurrentRunStart, mCurrentRunStart));
	    }
	    // Allocate a new L1 cache size buffer.
	    static const std::size_t miniRunSize((32*1024*1024)/sizeof(SortNode));
	    // Note the extra node is to make sure we have a sentinel to 
	    // stop iteration when merging (recognized by RecordBuffer::isEOS()).
	    mCurrentRunPtr = mCurrentRunStart = new SortNode [miniRunSize+1];
	    mCurrentRunEnd = mCurrentRunStart + miniRunSize;
	  }
	  mCurrentRunPtr->KeyPrefix = keyPrefix;
	  mCurrentRunPtr->Value = buf;
	  mCurrentRunPtr += 1;
	}
      }
      }
    }

    // In memory sort of last block and save for merging.
    std::sort(mCurrentRunStart, mCurrentRunPtr, mLessFunction);
    mSortRuns.push_back(std::make_pair(mCurrentRunStart, mCurrentRunStart));

    // Merge mini-runs to form final run.
    // Correct for the fact that LoserTree is a 
    // max-priority queue.
    mMergeTree.init((uint32_t) mSortRuns.size(),
		    NotPred<RecordTypeEquals> (mLessFunction.IQLCompare));

    while(!mMergeTree.empty()) {
      if (!mMergeTree.isHighSentinel()) {
	requestWrite(0);
	mState = WRITE;
	return;
      case WRITE:
	write(port, mMergeTree.getValue(), false);
      }
      
      // Get the next value
      {
	SortNode & n(*mSortRuns[mMergeTree.getInput()].second);
	if(RecordBuffer::isEOS(n.Value)) {
	  mMergeTree.close(mMergeTree.getInput());
	  delete [] mSortRuns[mMergeTree.getInput()].first;
	} else {
	  // Correct key prefix for the fact that LoserTree is a max-priority queue.
	  mMergeTree.update(mMergeTree.getInput(), 0x7fffffff - n.KeyPrefix, n.Value);
	  mSortRuns[mMergeTree.getInput()].second += 1;
	}	
      }
    }

    // Wrap up with EOS
    requestWrite(0);
    mState = WRITE_EOS;
    return;
  case WRITE_EOS:
    write(port, RecordBuffer(), true);
  }
}

void RuntimeSortOperator::shutdown()
{
}

#endif

RuntimeOperator * RuntimeSortOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeSortOperator(s, *this);
}

