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

#ifndef __ASYNCHRONOUSFILESYSTEM_HH__
#define __ASYNCHRONOUSFILESYSTEM_HH__

#include "ConcurrentFifo.hh"
#include "FileSystem.hh"

/**
 * Callback for asynchronous reads from a file system.
 */
template <class _FileType>
class AsynchronousFileSystemReadHandler
{
public:
  virtual ~AsynchronousFileSystemReadHandler() {}
  virtual void readComplete(int32_t bytesRead) = 0;
  virtual void writeComplete(int32_t bytesWritten) = 0;
  virtual void openComplete(_FileType f) = 0;
};

/**
 * Create an asynchronous api on top of an underlying file system.
 * Wrapper implements a thread pool of workers that perform underlying
 * synchronous calls.
 * TODO: It might be worth converting this to be implemented as a 
 * dataflow graph (similar in spirit to how I did the MPI interface).
 */
template <class _FileTraits>
class AsynchronousFileSystem
{
public:
  typedef typename _FileTraits::file_type file_type;
  typedef AsynchronousFileSystemReadHandler<file_type> handler_type;
private:
  class Request
  {
  public:
    virtual ~Request() {}
    virtual void invoke() =0;
  };
  class ReadRequest : public Request
  {
  public:
    file_type mFile;
    uint8_t * mBuffer;
    int32_t mBufferSize;
    handler_type& mHandler;
    ReadRequest(file_type file,
		uint8_t * buffer,
		int32_t bufferSize,
		handler_type& handler)
      :
      mFile(file),
      mBuffer(buffer),
      mBufferSize(bufferSize),
      mHandler(handler)
    {
    }
    void invoke()
    {
      int32_t sz = _FileTraits::read(mFile,
				     mBuffer,
				     mBufferSize);
      mHandler.readComplete(sz);
    }
  };
  class WriteRequest : public Request
  {
  public:
    file_type mFile;
    uint8_t * mBuffer;
    int32_t mBufferSize;
    handler_type& mHandler;
    WriteRequest(file_type file,
		 uint8_t * buffer,
		 int32_t bufferSize,
		 handler_type& handler)
      :
      mFile(file),
      mBuffer(buffer),
      mBufferSize(bufferSize),
      mHandler(handler)
    {
    }
    void invoke()
    {
      int32_t sz = _FileTraits::write(mFile,
				      mBuffer,
				      mBufferSize);
      mHandler.writeComplete(sz);
    }
  };
  class OpenRequest : public Request
  {
  public:
    enum Mode { READ,WRITE };
    std::string mFilename;
    uint64_t mBeginOffset;
    uint64_t mEndOffset;
    handler_type & mHandler;
    Mode mMode;
    OpenRequest(const char * filename,
		uint64_t beginOffset,
		uint64_t endOffset,
		handler_type& handler)
      :
      mFilename(filename),
      mBeginOffset(beginOffset),
      mEndOffset(endOffset),
      mHandler(handler),
      mMode(READ)
    {
    }
    OpenRequest(const char * filename,
		handler_type& handler)
      :
      mFilename(filename),
      mHandler(handler),
      mMode(WRITE)
    {
    }
    void invoke()
    {
      file_type f = NULL;
      if (mMode == READ) {
	f = _FileTraits::open_for_read(mFilename.c_str(),
				       mBeginOffset,
				       mEndOffset);
      } else {
	f = _FileTraits::open_for_write(mFilename.c_str());
      }
      mHandler.openComplete(f);
    }
  };

  // Request shutdown
  bool mShutdown;

  // Queue of read requests
  ConcurrentBlockingFifo<Request *> mRequests;

  // Worker threads
  std::vector<boost::shared_ptr<boost::thread> > mWorkers;

  // Worker thread loop
  void run();

  static AsynchronousFileSystem * gFS;
  static int32_t gRefCount;
  static boost::mutex gLock;
public:
  static AsynchronousFileSystem * get();
  static void release(AsynchronousFileSystem* fs);
  AsynchronousFileSystem(int32_t numberOfThreads);
  ~AsynchronousFileSystem();
  void requestRead(file_type f, 
		   uint8_t * buf, 
		   int32_t bufSize,
		   handler_type& callback);
  void requestWrite(file_type f, 
		   uint8_t * buf, 
		   int32_t bufSize,
		   handler_type& callback);
  void requestOpen(const char * filename,
		   uint64_t beginOffset,
		   uint64_t endOffset,
		   handler_type& callback);
  void requestOpenForWrite(const char * filename,
			   handler_type& callback);
};

template<class _FileTraits>
AsynchronousFileSystem<_FileTraits> * AsynchronousFileSystem<_FileTraits>::gFS=NULL;

template<class _FileTraits>
int32_t AsynchronousFileSystem<_FileTraits>::gRefCount=0;

template<class _FileTraits>
boost::mutex AsynchronousFileSystem<_FileTraits>::gLock;

template<class _FileTraits>
AsynchronousFileSystem<_FileTraits> * AsynchronousFileSystem<_FileTraits>::get()
{
  boost::unique_lock<boost::mutex> guard(gLock);
  if (gRefCount == 0) {
    gFS = new AsynchronousFileSystem<_FileTraits>(2);
  }
  gRefCount += 1;
  return gFS;
}

template<class _FileTraits>
void AsynchronousFileSystem<_FileTraits>::release(AsynchronousFileSystem<_FileTraits> *  fs)
{
  boost::unique_lock<boost::mutex> guard(gLock);
  BOOST_ASSERT(fs == gFS);
  if (fs == gFS) {
    gRefCount -= 1;
    if (gRefCount == 0)
      delete gFS;
  }
}

template<class _FileTraits>
AsynchronousFileSystem<_FileTraits>::AsynchronousFileSystem(int32_t numberOfThreads)
  :
  mShutdown(false)
{
  for(int32_t i=0; i<numberOfThreads; i++) {
    boost::thread * t = 
      new boost::thread(boost::bind(&AsynchronousFileSystem::run, 
				    this));
    mWorkers.push_back(boost::shared_ptr<boost::thread>(t));
  }
}

template<class _FileTraits>
AsynchronousFileSystem<_FileTraits>::~AsynchronousFileSystem()
{
  mShutdown = true; 
  for(std::vector<boost::shared_ptr<boost::thread> >::iterator it = mWorkers.begin();
      it != mWorkers.end();
      ++it) {
    mRequests.push(NULL);
  }
  for(std::vector<boost::shared_ptr<boost::thread> >::iterator it = mWorkers.begin();
      it != mWorkers.end();
      ++it) {
    (*it)->join();
    it->reset();
  }
}

template<class _FileTraits>
void AsynchronousFileSystem<_FileTraits>::run()
{
  while(!mShutdown) {
    Request * r = mRequests.pop();
    if (r) { 
      r->invoke();
      delete r;
    }
  }
}

template<class _FileTraits>
void AsynchronousFileSystem<_FileTraits>::requestRead(file_type f, 
						      uint8_t * buf,
						      int32_t bufSize,
						      handler_type& callback)
{
  mRequests.push(new ReadRequest(f, buf, bufSize, callback));
}

template<class _FileTraits>
void AsynchronousFileSystem<_FileTraits>::requestWrite(file_type f, 
						       uint8_t * buf,
						       int32_t bufSize,
						       handler_type& callback)
{
  mRequests.push(new WriteRequest(f, buf, bufSize, callback));
}

template<class _FileTraits>
void AsynchronousFileSystem<_FileTraits>::requestOpen(const char * filename,
					     uint64_t beginOffset,
					     uint64_t endOffset,
					     handler_type& callback)
{
  mRequests.push(new OpenRequest(filename, 
				 beginOffset, 
				 endOffset, 
				 callback));
}

template<class _FileTraits>
void AsynchronousFileSystem<_FileTraits>::requestOpenForWrite(const char * filename,
							      handler_type& callback)
{
  mRequests.push(new OpenRequest(filename, 
				 callback));
}

template <class _FileTraits>
class AsyncFileTraits
{
public: 
  typedef AsynchronousFileSystem<_FileTraits> * filesystem_type;
  typedef typename _FileTraits::file_type file_type;
  typedef typename AsynchronousFileSystem<_FileTraits>::handler_type handler_type;

  static filesystem_type openFor(const char * filename) {
    return AsynchronousFileSystem<_FileTraits>::get();
  }
  static void closeFileSystem(filesystem_type fs) {
    AsynchronousFileSystem<_FileTraits>::release(fs);
  }
  // Split into desired number of partitions.
  static void expand(std::string pattern, 
		     int32_t numPartitions,
		     std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files) {
    _FileTraits::expand(pattern, numPartitions, files);
  }
  static void requestOpen(filesystem_type fs,
			  const char * filename, 
			  uint64_t beginOffset, 
			  uint64_t endOffset,
			  handler_type& callback) {
    fs->requestOpen(filename, beginOffset, endOffset, callback);
  }
  static void requestOpenForWrite(filesystem_type fs,
				  const char * filename, 
				  handler_type& callback) {
    fs->requestOpenForWrite(filename, callback);
  }
  static void close(file_type f) {
    _FileTraits::close(f);
  }
  static void remove(filesystem_type fs, const std::string& file) {
    _FileTraits::remove(file.c_str());
  }
  static void requestRead(filesystem_type fs,
			  file_type f, 
			  uint8_t * buf, 
			  int32_t bufSize,
			  handler_type& callback) {
    fs->requestRead(f, buf, bufSize, callback);
  }
  static void requestWrite(filesystem_type fs,
			   file_type f, 
			   uint8_t * buf, 
			   int32_t bufSize,
			   handler_type& callback) {
    fs->requestWrite(f, buf, bufSize, callback);
  }
  static bool isEOF(file_type f) {
    return _FileTraits::isEOF(f);
  }
};

#endif
