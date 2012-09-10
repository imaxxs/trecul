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

#ifndef __MERGER_HH__
#define __MERGER_HH__

#include <vector>
#include "LoserTree.hh"
#include "AsynchronousFileSystem.hh"
#include "RuntimeOperator.hh"
#include "CompileTimeLogicalOperator.hh"

template <class _Pred>
struct NotPred : std::binary_function<typename _Pred::first_argument_type,
				      typename _Pred::second_argument_type,
				      bool>
{
  _Pred mPred;
  bool operator() (const typename _Pred::first_argument_type& lhs, 
		   const typename _Pred::second_argument_type& rhs)
  {
    return !mPred(lhs, rhs);
  }
  NotPred()
  {
  }
  NotPred(const _Pred& p)
    :
    mPred(p)
  {
  }
};

/**
 * A block buffer stream that implements a double
 * buffering policy over an async IO provider.
 */
template <class _AsyncFileTraits>
class AsyncDoubleBufferStream : public AsynchronousFileSystemReadHandler<typename _AsyncFileTraits::file_type>
{
public:
  typedef _AsyncFileTraits file_system_type;
  typedef typename _AsyncFileTraits::filesystem_type filesystem_type;
  typedef typename _AsyncFileTraits::file_type file_type;

  // Given an amount of available memory; what window size to
  // request?  Since this is using a double buffering policy
  // the window size is half the memory size.
  static std::size_t getWindowSize(std::size_t memorySize)
  {
    return memorySize / 2;
  }
private:

  /**
   * This class opens a file and reads a first block asynchronously
   */
  class OpenAndReadOneBlock : public AsynchronousFileSystemReadHandler<typename _AsyncFileTraits::file_type>
  {
  public:
    filesystem_type mFileSystem;
    file_type mFile;
    uint8_t * mBuffer;
    int32_t mBufferSize;
    int32_t mBytesRead;
    ConcurrentBlockingFifo<OpenAndReadOneBlock *> mCompletionQueue;

    OpenAndReadOneBlock(filesystem_type fileSystem, 
			const char * file,
			uint64_t beginOffset,
			uint64_t endOffset,
			int32_t toRead)
      :
      mFileSystem(fileSystem),
      mFile(NULL),
      mBuffer(NULL),
      mBufferSize(toRead),
      mBytesRead(0)
    {
      _AsyncFileTraits::requestOpen(mFileSystem, 
				    file, 
				    beginOffset, 
				    endOffset,
				    *this);
    }
    void readComplete(int32_t bytesRead)
    {
      mBytesRead = bytesRead;
      // Signal completion to caller.
      mCompletionQueue.push(this);
    }
    void openComplete(file_type f)
    {
      mFile = f;
      // TODO: Handle failure here
      mBuffer = new uint8_t [mBufferSize];
      _AsyncFileTraits::requestRead(mFileSystem,
				    mFile, 
				    mBuffer, 
				    mBufferSize,
				    *this);      
    }
    void writeComplete(int32_t bytesRead)
    {
      BOOST_ASSERT(false);
    }
  };

  std::size_t mMinimumBlockRequestSize;

  /**
   * A block of data from the underlying file system.
   */
  class Block 
  {
  public:
    enum State { OUTSTANDING, COMPLETED };
    uint8_t * mMark;
    uint8_t * mStart;
    uint8_t * mEnd;
    uint8_t * mPtr;
    std::size_t mCapacity;
    State mState;
    Block()
      :
    mMark(NULL),
    mStart(NULL),
    mEnd(NULL),
    mPtr(NULL),
    mCapacity(0),
    mState(COMPLETED)
    {
    }
  };
  Block mBlocks[2];
  /**
   * Index of the block we are currently reading.
   */
  std::size_t mCurrent;
  /** 
   * Offset of mBlock[mCurrent].mStart in the file.
   */
  uint64_t mBlockOffset;
  /**
   * The offset of the end of the file chunk we are reading.
   * Technically we can read past this in case there is a record
   * that starts before this and ends after this.
   */
  uint64_t mStreamEnd;

  /**
   * The file system from which we are reading.
   */
  typename _AsyncFileTraits::filesystem_type mFileSystem;

  /**
   * The file we are reading from.
   */
  typename _AsyncFileTraits::file_type mFile;

  /**
   * Queue for read completion notifications.
   */
  ConcurrentBlockingFifo<int32_t> mCompletionQueue;

  /**
   * Asynchronous operator to open the file and read ahead one block.
   */
  OpenAndReadOneBlock * mOpenAndReadOne;

  /**
   * Create a contiguous region of size at least windowSize from the
   * current file pointer.
   */
  void openWindow(std::size_t windowSize)
  {
    // See how much unused we have in current buffer.  Unused may take account
    // of a mark as well as the current ptr.  If any then move it to the 
    // front of the buffer and fill up the rest.
    // We may have to grow the buffer if the requested window is bigger than
    // the current buffer of if the amount to read is less than our commitment.
    Block & currentBlock(mBlocks[mCurrent]);

    BOOST_ASSERT(NULL == currentBlock.mMark ||
		 currentBlock.mPtr >= currentBlock.mMark);
    uint8_t * keepStart = currentBlock.mMark ? currentBlock.mMark : currentBlock.mPtr;
    std::size_t ptrOffset = std::size_t(currentBlock.mPtr - keepStart);
    std::size_t bufUse = std::size_t(currentBlock.mEnd - keepStart);
    std::size_t bufAvail = currentBlock.mCapacity - bufUse;
    std::size_t bufWindow = std::size_t(currentBlock.mEnd - currentBlock.mPtr);
    std::size_t amtToRead = windowSize - bufWindow;
    if (bufAvail < amtToRead) {
      currentBlock.mCapacity = std::max(windowSize+ptrOffset, mMinimumBlockRequestSize);
      uint8_t * newBuffer = new uint8_t [currentBlock.mCapacity];
      if(bufUse)
	memcpy(newBuffer, keepStart, bufUse);
      delete [] currentBlock.mStart;
      currentBlock.mStart = newBuffer;
    } else if (keepStart != currentBlock.mStart && bufUse > 0) {
      memmove(currentBlock.mStart, keepStart, bufUse);
    }
    // Reset all pointers
    currentBlock.mMark = currentBlock.mMark ? currentBlock.mStart : NULL;
    currentBlock.mPtr = currentBlock.mStart + ptrOffset;
    currentBlock.mEnd = currentBlock.mStart + bufUse;

    // Complete the read on the other buffer (this may block)
    // If the current block is not empty then move enough of new
    // data into the current data to complete the window.
    if (bufUse) {
      completeRead();
      Block& nextBlock(mBlocks[(mCurrent + 1) % 2]);
      std::size_t toCopy = std::min(amtToRead, std::size_t(nextBlock.mEnd - nextBlock.mPtr));
      memcpy(currentBlock.mEnd, nextBlock.mPtr, toCopy);
      currentBlock.mEnd += toCopy;
      nextBlock.mPtr += toCopy;      
      amtToRead -= toCopy;
      while (amtToRead > 0 &&
	     !_AsyncFileTraits::isEOF(getFile())) {
	// Here we have hit an unfortunate case that won't be too likely in reality.
	// The window that we are tying to open is larger than the read ahead we've
	// configured.  In this case we fall back to synchronous behavior.
	// This synchronous behavior is undesireable but can't be fixed without making
	// our parsers into coroutines or come other async construct (e.g. continuations).
	nextBlock.mState = Block::OUTSTANDING;
	_AsyncFileTraits::requestRead(mFileSystem,
				      getFile(), 
				      nextBlock.mStart, 
				      mMinimumBlockRequestSize,
				      *this);
	completeRead();
	toCopy = std::min(amtToRead, std::size_t(nextBlock.mEnd - nextBlock.mPtr));
	memcpy(currentBlock.mEnd, nextBlock.mPtr, toCopy);
	currentBlock.mEnd += toCopy;
	nextBlock.mPtr += toCopy;      
	amtToRead -= toCopy;
      }
    } else {
      blockFinished();
      // What happens in the unlikely event the next block isn't big enough?  Must
      // read synchronously at this point.  To get that effect we just call this routine
      // recursively.  Note that the next time through bufSize > 0 so the recursion will
      // be at most 1 deep.  I apologize for writing code that is this subtle....
      if (windowSize > std::size_t(mBlocks[mCurrent].mEnd - mBlocks[mCurrent].mPtr)) {
	openWindow(windowSize);
      }
    }
  }

  void completeRead()
  {
    Block& nextBlock = mBlocks[(mCurrent + 1) % 2];
    if (nextBlock.mState == Block::OUTSTANDING) {
      int32_t bytesRead = mCompletionQueue.pop();
      if (bytesRead <= 0) 
	throw std::runtime_error("Error reading file");
      nextBlock.mEnd = nextBlock.mStart + bytesRead;
      nextBlock.mPtr = nextBlock.mStart;
      nextBlock.mMark = NULL;
      nextBlock.mState = Block::COMPLETED;
    }
  }

  void completeOpen()
  {
    if (mOpenAndReadOne != NULL) {
      // Only allow one open in the class lifetime.
      mOpenAndReadOne->mCompletionQueue.pop();
      mFile = mOpenAndReadOne->mFile;
      mBlocks[0].mPtr = mBlocks[0].mStart = mOpenAndReadOne->mBuffer;
      mBlocks[0].mEnd = mBlocks[0].mStart + mOpenAndReadOne->mBytesRead;
      mBlocks[0].mMark = NULL;
      mBlocks[0].mCapacity = mMinimumBlockRequestSize;
      delete mOpenAndReadOne;
      mOpenAndReadOne = NULL;
    }
  }

  void blockFinished()
  {
    // Wait for outstanding read to complete 
    // (blocking if necessary).
    completeRead();
    // Update block offset to point to the start of
    // new buffer (adjusting for data that we just
    // finished consuming).
    mBlockOffset += (mBlocks[mCurrent].mEnd - mBlocks[mCurrent].mStart);
    // Point to the new buffer.
    mCurrent = (mCurrent + 1) % 2;

    // If not at end of file then initiate a new read
    // into the just finished finished buffer.
    if (!_AsyncFileTraits::isEOF(getFile())) {
      Block & nextBlock = mBlocks[(mCurrent + 1) % 2];
      nextBlock.mState = Block::OUTSTANDING;
      _AsyncFileTraits::requestRead(mFileSystem,
				    getFile(), 
				    nextBlock.mStart, 
				    mMinimumBlockRequestSize,
				    *this);
    }
  }

  file_type getFile()
  {
    completeOpen();
    return mFile;
  }

public:
  AsyncDoubleBufferStream(typename _AsyncFileTraits::filesystem_type fs,
			  const char * file, 
			  int32_t targetBlockSize, 
			  uint64_t beginOffset=0,
			  uint64_t endOffset=0xffffffffffffffffULL)
    :
    mMinimumBlockRequestSize(targetBlockSize),
    mCurrent(1),
    mBlockOffset(beginOffset),
    mStreamEnd(endOffset),
    mFileSystem(fs),
    mFile(NULL),
    mOpenAndReadOne(NULL)
  {
    mOpenAndReadOne = new OpenAndReadOneBlock(mFileSystem, 
					      file, 
					      beginOffset, 
					      endOffset,
					      mMinimumBlockRequestSize);
  }

  ~AsyncDoubleBufferStream()
  {
    _AsyncFileTraits::close(getFile());
    delete [] mBlocks[0].mStart;
    delete [] mBlocks[1].mStart;
  }

  uint8_t * open(std::size_t windowSize)
  {
    if (mBlocks[mCurrent].mPtr + windowSize <= mBlocks[mCurrent].mEnd) 
      return mBlocks[mCurrent].mPtr;
    openWindow (windowSize);
    return mBlocks[mCurrent].mPtr + windowSize <= mBlocks[mCurrent].mEnd ? mBlocks[mCurrent].mPtr : NULL;
  }
  /**
   * Try to open a buffer of requested size.  If not possible,
   * return a buffer of max size possible.
   */
  void open(std::size_t& windowSize, uint8_t *& buf)
  {
    if (mBlocks[mCurrent].mPtr + windowSize <= mBlocks[mCurrent].mEnd) {
      buf = mBlocks[mCurrent].mPtr;
      return;
    }
    openWindow (windowSize);
    buf = mBlocks[mCurrent].mPtr;
    windowSize = std::min(windowSize, std::size_t(mBlocks[mCurrent].mEnd - mBlocks[mCurrent].mPtr));
  }
  void consume(std::size_t bytes)
  {
    mBlocks[mCurrent].mPtr += bytes;
  }

  uint8_t * read(std::size_t sz)
  {
    uint8_t * ptr = open(sz);
    if (ptr) consume(sz);
    return ptr;
  }

  uint8_t * getMark()
  {
    return mBlocks[mCurrent].mMark;
  }
  void setMark()
  {
    BOOST_ASSERT(mBlocks[mCurrent].mMark == NULL);
    mBlocks[mCurrent].mMark = mBlocks[mCurrent].mPtr;
  }
  void releaseMark()
  {
    BOOST_ASSERT (mBlocks[mCurrent].mMark != NULL);
    mBlocks[mCurrent].mMark = NULL;
  }
  bool isEOF()
  {
    completeOpen();
    // EOF is when I've read past the end of my assigned file chunk or I've exhausted
    // the last block in the file.
    return 
      mBlockOffset + (mBlocks[mCurrent].mPtr - mBlocks[mCurrent].mStart) >= mStreamEnd || 
      (mBlocks[0].mState==Block::COMPLETED && 
       mBlocks[0].mPtr >= mBlocks[0].mEnd && 
       mBlocks[1].mState==Block::COMPLETED &&
       mBlocks[1].mPtr >= mBlocks[1].mEnd && 
       _AsyncFileTraits::isEOF(getFile()));
  }

  /**
   * Callback to indicate an IO request is completed.
   * May be called from any thread.
   */
  void readComplete(int32_t bytesRead)
  {
    mCompletionQueue.push(bytesRead);
  }
  /**
   * Callback when file is open.  Called from an arbitrary thread.
   */
  void openComplete(file_type f)
  {
    // Should never get here.
    BOOST_ASSERT(false);
  }
  /**
   * Callback when file write is complete. Called from an arbitrary thread.
   */
  void writeComplete(int32_t bytesWritten)
  {
    // Should never get here.
    BOOST_ASSERT(false);
  }
};

/**
 * Reads a file.
 */

class LogicalFileRead : public CompileTimeLogicalOperator
{
private:
  std::string mFile;
  std::string mStringFormat;
  std::string mMode;
  bool mBucketed;
  bool mSkipHeader;
  char mFieldSeparator;
  char mRecordSeparator;
  std::string mCommentLine;
  const RecordType * mFormat;

  void internalCreate(class RuntimePlanBuilder& plan);  
  std::string readFormatFile(const std::string& formatFile);
public:
  LogicalFileRead();
  ~LogicalFileRead();
  void check(PlanCheckContext& log);
};

/**
 * Operator for reading a binary serialized file in internal format.
 */
template <class _InputBuffer>
class InternalFileParserOperatorType : public RuntimeOperatorType
{
  // Don't really know how to do friends between templates.
public:
  typedef std::vector<std::vector<boost::shared_ptr<FileChunk> > > chunk_type;
  // What file(s) am I parsing?
  chunk_type mFile;
  // Importer instructions
  RecordTypeDeserialize mDeserialize;
  // Create new records
  RecordTypeMalloc mMalloc;
  // Input buffer size for reads
  std::size_t mInputBufferSize;
  // Should I delete file on completion (e.g. sort run).
  // This is not fully general and won't work properly
  // if multiple operators share chunks of the same underlying file.
  bool mDeleteOnCompletion;
  
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFile);
    ar & BOOST_SERIALIZATION_NVP(mDeserialize);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mInputBufferSize);
    ar & BOOST_SERIALIZATION_NVP(mDeleteOnCompletion);
  }
  InternalFileParserOperatorType()
    :
    RuntimeOperatorType("InternalFileParserOperatorType"),
    mDeleteOnCompletion(false)
  {
  }  

public:
  InternalFileParserOperatorType(const RecordType * recordType, 
				 const std::string& file)
    :
    RuntimeOperatorType("InternalFileParserOperatorType"),
    mInputBufferSize(128*1024),
    mDeleteOnCompletion(false)
  {
    // Expand file name globbing
    // TODO: This really doesn't handle parallel execution.
    // Not worth fixing since this operator should be patched into
    // the GenericParserOperatorType
    _InputBuffer::file_system_type::expand(file, 1, mFile);

    // At least one match for the file/file pattern
    if (mFile.size() == 0)
      throw std::runtime_error((boost::format("Could not find any file %1%") % file).str());

    mDeserialize = recordType->getDeserialize();
    mMalloc = recordType->getMalloc();
  }

  InternalFileParserOperatorType(const RecordTypeDeserialize& deserialize,
				 const RecordTypeMalloc& mallocFn,
				 const chunk_type& chunks,
				 std::size_t inputBufferSize,
				 bool deleteOnCompletion)
    :
    RuntimeOperatorType("InternalFileParserOperatorType"),
    mFile(chunks),
    mDeserialize(deserialize),
    mMalloc(mallocFn),
    mInputBufferSize(inputBufferSize),
    mDeleteOnCompletion(deleteOnCompletion)
  {
  }

  ~InternalFileParserOperatorType()
  {
  }

  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

template<class _ReaderContext, class _InputBuffer>
class InternalFileParserOperator : public _ReaderContext
{
private:
  typedef typename _ReaderContext::Services services_type;
  typedef typename _ReaderContext::port_type port_type;
  typedef typename _InputBuffer::file_system_type file_traits;
  typedef typename _InputBuffer::file_system_type::filesystem_type filesystem_type;
  typedef InternalFileParserOperatorType<_InputBuffer> operator_type;

  enum State { START, WRITE, WRITE_EOF };
  State mState;

  // Filesystem I am reading from
  filesystem_type mFileSystem;
  
  // Which file am I working on?
  std::vector<boost::shared_ptr<FileChunk> >::const_iterator mFileIt;
  // Input buffer for the file.
  _InputBuffer * mInputBuffer;
  // The current window of the file
  uint8_t * mBuffer;
  uint8_t * mBufferIt;
  uint8_t * mBufferEnd;
  // The record buffer I am importing into
  RecordBuffer mRecordBuffer;
  RecordBufferIterator mRecordBufferIt;
  // Records imported
  uint64_t mRecordsImported;
  // op type
  const operator_type& getMyOperatorType()
  {
    return *static_cast<const operator_type *>(&_ReaderContext::getOperatorType());
  }
  void deleteFiles() 
  {
    if (mFileSystem) {
      std::set<std::string> files;
      for(std::vector<boost::shared_ptr<FileChunk> >::const_iterator
	    fileIt = getMyOperatorType().mFile[_ReaderContext::getPartition()].begin();
	  fileIt != getMyOperatorType().mFile[_ReaderContext::getPartition()].end();
	  ++fileIt) {	
	const std::string& f((*fileIt)->getFilename());
	if (files.end() == files.find(f)) {
	  files.insert(f);
	  file_traits::remove(mFileSystem, f);
	}
      }
    }
  }
public:
  InternalFileParserOperator(services_type& services, 
			     const InternalFileParserOperatorType<_InputBuffer>& opType)
    :
    _ReaderContext(services, *static_cast<const RuntimeOperatorType *>(&opType)),
    mRecordsImported(0)
  {
  }

  ~InternalFileParserOperator()
  {
  }

  /**
   * intialize.
   */
  void start()
  {
    // Attach to the file system in which this file lives.
    // TODO: Make this asynchronous.
    const char * f = getMyOperatorType().mFile[_ReaderContext::getPartition()].front()->getFilename().c_str();
    mFileSystem = file_traits::openFor(f);
    
    mState = START;
    mRecordsImported = 0;
    onEvent(NULL);
  }

  void onEvent(port_type port)
  {
    switch(mState) {
    case START:
      for(mFileIt = getMyOperatorType().mFile[_ReaderContext::getPartition()].begin();
	  mFileIt != getMyOperatorType().mFile[_ReaderContext::getPartition()].end();
	  ++mFileIt) {
	// Allocate a new input buffer for the file in question.
	mInputBuffer = new _InputBuffer(mFileSystem,
					(*mFileIt)->getFilename().c_str(), 
					getMyOperatorType().mInputBufferSize,
					(*mFileIt)->getBegin(),
					(*mFileIt)->getEnd());

	// Read all of the record in the file.
	while(!mInputBuffer->isEOF()) {
	  {
	    // Open the next window
	    {
	      std::size_t windowSize = getMyOperatorType().mInputBufferSize;
	      mInputBuffer->open(windowSize, mBuffer);
	      mBufferIt = mBuffer;
	      mBufferEnd = mBuffer + windowSize;
	    }
	    // Deserialize records from the window, 
	    // write them and return buffer
	    while(mBufferIt < mBufferEnd) {
	      if (mRecordBuffer.Ptr == NULL) {
		mRecordBuffer = getMyOperatorType().mMalloc.malloc();
		mRecordBufferIt.init(mRecordBuffer);
	      }
	      if(getMyOperatorType().mDeserialize.Do(mBufferIt, 
						     mBufferEnd, 
						     mRecordBufferIt, 
						     mRecordBuffer)) {
		_ReaderContext::requestWrite(0);
		mState = WRITE;
		return;
	      case WRITE:
		_ReaderContext::write(port, mRecordBuffer, false);
		mRecordBuffer = RecordBuffer(NULL);
		mRecordBufferIt.clear();
		mRecordsImported += 1;
	      } 
	    }
	    mInputBuffer->consume(std::size_t(mBufferEnd - mBuffer));
	  }
	}
      }
      // Done with the last file so output EOS.
      _ReaderContext::requestWrite(0);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      _ReaderContext::write(port, RecordBuffer::create(), true);
      return;
    }
  }

  void shutdown()
  {
    // std::cout << "RecordsImported=" << mRecordsImported << std::endl;
    if (getMyOperatorType().mDeleteOnCompletion) {
      deleteFiles();
    }
    if (mFileSystem) {
      file_traits::closeFileSystem(mFileSystem);
      mFileSystem = NULL;
    }
  }
};

template <class _InputBuffer>
RuntimeOperator * InternalFileParserOperatorType<_InputBuffer>::create(RuntimeOperator::Services & services) const
{
  return new InternalFileParserOperator<RuntimeOperator, _InputBuffer>(services, *this);
}

class LogicalFileWrite : public LogicalOperator
{
private:
  std::string mConnect;
  std::string mFile;
  std::string mMode;
  std::string mHeader;
  std::string mHeaderFile;
  RecordTypeTransfer * mFileNameExpr;

  void buildHeader(bool isFormatHeader);
  void checkPath(PlanCheckContext& ctxt, const std::string& path);
public:
  LogicalFileWrite();
  ~LogicalFileWrite();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class InternalFileWriteOperatorType : public RuntimeOperatorType
{
public:
  RecordTypeSerialize mSerialize;
  RecordTypeFree mFree;
  std::string mFile;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mSerialize);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mFile);
  }
  InternalFileWriteOperatorType()
  {
  }
public:
  InternalFileWriteOperatorType(const std::string& opName,
				const RecordType * ty, 
				const std::string& file)
    :
    RuntimeOperatorType(opName.c_str()),
    mSerialize(ty->getSerialize()),
    mFree(ty->getFree()),
    mFile(file)
  {
  }
  InternalFileWriteOperatorType(const std::string& opName,
				const RecordTypeSerialize& serialize,
				const RecordTypeFree& freeFn,
				const std::string& file)
    :
    RuntimeOperatorType(opName.c_str()),
    mSerialize(serialize),
    mFree(freeFn),
    mFile(file)
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& services) const;
};

class LogicalSortMerge : public LogicalOperator
{
private:
  RecordTypeFunction * mKeyPrefix;
  RecordTypeFunction * mKeyEq;
public:
  LogicalSortMerge();
  ~LogicalSortMerge();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class SortMerge
{
private:
  const RecordType * mInput;
  RecordTypeFunction * mKeyPrefix;
  RecordTypeFunction * mKeyEq;
public:
  SortMerge(DynamicRecordContext& ctxt,
	    const RecordType * input,
	    const std::vector<std::string>& sortKeys);
  ~SortMerge();
  // Not supporting transfer yet.  Before we
  // do we want to make sure we can identify 
  // identity transfers and optimize them out.
  const RecordType * getOutputType() const 
  {
    return mInput;
  }
  RuntimeOperatorType * create() const;  
};

class RuntimeSortMergeOperatorType : public RuntimeOperatorType
{
public:
  friend class RuntimeSortMergeOperator;
private:
  // Extract a key prefix from record
  IQLFunctionModule * mKeyPrefix;
  // Compare two inputs for equality
  IQLFunctionModule * mEqFun;
  // Does this instance own the modules?
  bool mOwnModules;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mKeyPrefix);
    ar & BOOST_SERIALIZATION_NVP(mEqFun);
    ar & BOOST_SERIALIZATION_NVP(mOwnModules);    
  }
  RuntimeSortMergeOperatorType()
    :
    mKeyPrefix(NULL),
    mEqFun(NULL)
  {
  }  
public:
  RuntimeSortMergeOperatorType(const RecordTypeFunction * keyPrefix,
			       const RecordTypeFunction * eqFun)
    :
    RuntimeOperatorType("RuntimeSortMergeOperatorType"),
    mKeyPrefix(keyPrefix->create()),
    mEqFun(eqFun->create()),
    mOwnModules(true)
  {
  }
  RuntimeSortMergeOperatorType(IQLFunctionModule * keyPrefix,
			       IQLFunctionModule * eqFun)
    :
    RuntimeOperatorType("RuntimeSortMergeOperatorType"),
    mKeyPrefix(keyPrefix),
    mEqFun(eqFun),
    mOwnModules(false)
  {
  }
  ~RuntimeSortMergeOperatorType()
  {
    if (mOwnModules) {
      delete mKeyPrefix;
      delete mEqFun;
    }
  }

  RuntimeOperator * create(RuntimeOperator::Services & s) const;
};

class RuntimeSortMergeOperator : public RuntimeOperatorBase<RuntimeSortMergeOperatorType>
{
private:
  enum State { START, READ, WRITE, WRITE_EOS };
  State mState;
  class InterpreterContext * mRuntimeContext;
  LoserTree<RecordBuffer,NotPred<RecordTypeEquals> > mMergeTree;
  RuntimeOperator * mReturnAddress;
  int32_t mReturnPort;
public:
  RuntimeSortMergeOperator(RuntimeOperator::Services& services, const RuntimeSortMergeOperatorType& opType);
  ~RuntimeSortMergeOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
  void setReturnAddress(RuntimeOperator * op, int32_t port);
};

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
    KeyPrefix(0)
  {
  }

  SortNode(uint32_t keyPrefix, RecordBuffer val)
    :
    KeyPrefix(keyPrefix),
    Value(val)
  {
  }
};

class SortRun
{
public:
  typedef SortNode* iterator;
private:
  // The storage for sort nodes.
  SortNode * mBegin;
  SortNode * mFilled;
  SortNode * mEnd;
  // Statistics 
  uint64_t mSortSz;
  uint64_t mMemoryAllowed;
  double mReallocThreshold;
  void capacity(std::size_t numRecords);
  bool push_back_with_realloc(const SortNode& n, std::size_t dataLen);
public:
  SortRun(std::size_t memoryAllowed);
  ~SortRun();
  bool push_back(const SortNode& n, std::size_t dataLen)
  {
    uint64_t newSz = mSortSz + dataLen;
    if (newSz > mMemoryAllowed) {
      // TODO: Handle bad case in which we way overallocated
      // space for sort nodes.  Give some back and try again.
      // Perhaps we can't really do the give back without
      // using a bunch of additional memory.
      return false;
    }
    if (mFilled != mEnd) {
      *mFilled++ = n;
      mSortSz = newSz;
      return true;
    } else {
      return push_back_with_realloc(n, dataLen);
    }
  }
  void clear();
  iterator begin() 
  {
    return mBegin;
  }
  iterator end()
  {
    return mFilled;
  }
  const SortNode& front() const
  {
    return *mBegin;
  }
  std::size_t memory() const
  {
    return mSortSz;
  }
  std::ptrdiff_t capacity() const
  {
    return mEnd - mBegin;
  }
  std::ptrdiff_t size() const
  {
    return mFilled - mBegin;
  }
};

class LogicalSort : public LogicalOperator
{
private:
  RecordTypeFunction * mKeyPrefix;
  RecordTypeFunction * mKeyEq;
  RecordTypeFunction * mPresortedKeyEq;
  std::string mTempDir;
  std::size_t mMemory;
public:
  LogicalSort();
  ~LogicalSort();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class RuntimeSortOperatorType : public RuntimeOperatorType
{
public:
  friend class RuntimeSortOperator;
private:
  // Extract a key prefix from record
  IQLFunctionModule * mKeyPrefix;
  // Compare two inputs less than
  IQLFunctionModule * mLessThanFun;
  // Compare two inputs less than for presorted keys (if any)
  IQLFunctionModule * mPresortedEqualsFun;
  // Serialize and deserialize for on disk sort runs
  RecordTypeSerialize mSerialize;
  RecordTypeDeserialize mDeserialize;
  // Malloc and free for disk sort runs
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  // Directory for sort runs
  std::string mTempDir;
  // Amount of memory operator can use
  std::size_t mMemoryAllowed;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mKeyPrefix);
    ar & BOOST_SERIALIZATION_NVP(mLessThanFun);
    ar & BOOST_SERIALIZATION_NVP(mPresortedEqualsFun);
    ar & BOOST_SERIALIZATION_NVP(mSerialize);
    ar & BOOST_SERIALIZATION_NVP(mDeserialize);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mTempDir);
    ar & BOOST_SERIALIZATION_NVP(mMemoryAllowed);
  }
  RuntimeSortOperatorType()
    :
    mKeyPrefix(NULL),
    mLessThanFun(NULL),
    mPresortedEqualsFun(NULL),
    mMemoryAllowed(128*1024*1024)
  {
  }  
public:
  RuntimeSortOperatorType(const RecordType * input,
			  const RecordTypeFunction * keyPrefix,
			  const RecordTypeFunction * lessFun,
			  const RecordTypeFunction * presortedEquals,
			  const std::string& tempDir,
			  std::size_t memoryAllowed)
    :
    RuntimeOperatorType("RuntimeSortOperatorType"),
    mKeyPrefix(keyPrefix->create()),
    mLessThanFun(lessFun->create()),
    mPresortedEqualsFun(presortedEquals ? presortedEquals->create() : NULL),
    mSerialize(input->getSerialize()),
    mDeserialize(input->getDeserialize()),
    mMalloc(input->getMalloc()),
    mFree(input->getFree()),
    mTempDir(tempDir),
    mMemoryAllowed(memoryAllowed)
  {
  }
  ~RuntimeSortOperatorType()
  {
    delete mKeyPrefix;
    delete mLessThanFun;
    delete mPresortedEqualsFun;
  }

  RuntimeOperator * create(RuntimeOperator::Services & s) const;
};


#endif
