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

#include <sys/types.h>
#include <fcntl.h>
#include <fstream>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include "FileWriteOperator.hh"

ZLibCompress::ZLibCompress()
  :
  mOutputStart(NULL),
  mOutputEnd(NULL)
{
  std::size_t outputBufferSz=64*1024;
  mOutputStart = new uint8_t [outputBufferSz];
  mOutputEnd = mOutputStart + outputBufferSz;
  mStream.zalloc = Z_NULL;
  mStream.zfree = Z_NULL;
  mStream.opaque = Z_NULL;
  // We use deflateInit2 so we can request a gzip header.  Other
  // params are set to defaults.
  int ret = ::deflateInit2(&mStream, 
			   Z_DEFAULT_COMPRESSION, 
			   Z_DEFLATED,
			   31, // Greater than 15 indicates gzip format
			   8,
			   Z_DEFAULT_STRATEGY);
  if (ret != Z_OK)
    throw std::runtime_error("Error initializing compression library");

  // Bind the buffer to the gzip stream
  mStream.avail_out = (mOutputEnd - mOutputStart);
  mStream.next_out = mOutputStart;
}

ZLibCompress::~ZLibCompress()
{
  deflateEnd(&mStream);
  delete [] mOutputStart;
}

void ZLibCompress::put(const uint8_t * bufStart, std::size_t len, bool isEOS)
{
  mFlush = isEOS ? Z_FINISH : Z_NO_FLUSH;
  mStream.avail_in = (int) len;
  mStream.next_in = const_cast<uint8_t *>(bufStart);
}

bool ZLibCompress::run()
{
  // Try to consume the input.
  int ret=Z_OK;
  do {
    ret = ::deflate(&mStream, mFlush);    
  } while(ret == Z_OK);
  return mStream.avail_in == 0;
}

void ZLibCompress::consumeOutput(uint8_t * & output, std::size_t & len)
{
  // How much data in the compressor state?
  output = mOutputStart;
  len = mStream.next_out - mOutputStart;
  // Reset for more output.
  mStream.avail_out = (mOutputEnd - mOutputStart);
  mStream.next_out = mOutputStart;  
}

class RuntimeWriteOperator : public RuntimeOperator
{
public:
  void writeToHdfs(RecordBuffer input, bool isEOS);  

private:
  enum State { START, READ };
  State mState;
  const RuntimeWriteOperatorType &  getWriteType()
  {
    return *reinterpret_cast<const RuntimeWriteOperatorType *>(&getOperatorType());
  }

  int mFile;
  RuntimePrinter mPrinter;
  ZLibCompress * mCompressor;
  AsyncWriter<RuntimeWriteOperator> mWriter;
  boost::thread * mWriterThread;
  int64_t mRead;
public:
  RuntimeWriteOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType);
  ~RuntimeWriteOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

RuntimeWriteOperator::RuntimeWriteOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mFile(-1),
  mPrinter(getWriteType().mPrint),
  mCompressor(NULL),
  mWriter(*this),
  mWriterThread(NULL),
  mRead(0)
{
}

RuntimeWriteOperator::~RuntimeWriteOperator()
{
  delete mCompressor;
}

void RuntimeWriteOperator::start()
{
  // Are we compressing?
  boost::filesystem::path p (getWriteType().mFile);
  if (boost::algorithm::iequals(".gz", boost::filesystem::extension(p))) {
    mCompressor = new ZLibCompress();
  }
  // Create directories if necessary before opening file.
  boost::filesystem::create_directories(p.parent_path());
  mFile = ::open(getWriteType().mFile.c_str(),
		 O_WRONLY|O_CREAT|O_TRUNC,
		 S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP);
  if (mFile == -1) {
    throw std::runtime_error("Couldn't create file");
  }
  // Start a thread that will write
  mWriterThread = new boost::thread(boost::bind(&AsyncWriter<RuntimeWriteOperator>::run, 
						boost::ref(mWriter)));
  mState = START;
  onEvent(NULL);
}

void RuntimeWriteOperator::writeToHdfs(RecordBuffer input, bool isEOS)
{
  if (mRead == 0 && getWriteType().mHeader.size() && 
      getWriteType().mHeaderFile.size() == 0) {
    if (mCompressor) {
      mCompressor->put((const uint8_t *) getWriteType().mHeader.c_str(), 
		       getWriteType().mHeader.size(), 
		       false);
      while(!mCompressor->run()) {
	uint8_t * output;
	std::size_t outputLen;
	mCompressor->consumeOutput(output, outputLen);
	::write(mFile, (const uint8_t *) output, outputLen);
      }
    } else {
      ::write(mFile, (const uint8_t *) getWriteType().mHeader.c_str(), 
	      getWriteType().mHeader.size());
    }
  }
  if (!isEOS) {
    mPrinter.print(input, true);
    getWriteType().mFree.free(input);
    if (mCompressor) {
      mCompressor->put((const uint8_t *) mPrinter.c_str(), mPrinter.size(), 
		       false);
      while(!mCompressor->run()) {
	uint8_t * output;
	std::size_t outputLen;
	mCompressor->consumeOutput(output, outputLen);
	::write(mFile, (const uint8_t *) output, outputLen);
      }
    } else {
      ::write(mFile, (const uint8_t *) mPrinter.c_str(), mPrinter.size());
    }
    mPrinter.clear();
    mRead += 1;
  } else {
    if (mCompressor) {
      // Flush data through the compressor.
      mCompressor->put(NULL, 0, true);
      while(true) {
	mCompressor->run();
	uint8_t * output;
	std::size_t outputLen;
	mCompressor->consumeOutput(output, outputLen);
	if (outputLen > 0) {
	  ::write(mFile, (const uint8_t *) output, outputLen);
	} else {
	  break;
	}
      }    
    }
    // Clean close of file and file system
    ::close(mFile);
    mFile = -1;
  }
}

void RuntimeWriteOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    if(getWriteType().mHeader.size() && 
       getWriteType().mHeaderFile.size()) {
      boost::filesystem::path p (getWriteType().mHeaderFile);
      if (boost::algorithm::iequals(".gz", p.extension())) {
	// TODO: Support compressed header file.
	throw std::runtime_error("Writing compressed header file not supported yet");
      }
      std::ofstream headerFile(getWriteType().mHeaderFile.c_str(),
			       std::ios_base::out);
      headerFile << getWriteType().mHeader.c_str();
      headerFile.flush();
    }
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      {
	RecordBuffer input;
	read(port, input);
	bool isEOS = RecordBuffer::isEOS(input);
	//writeToHdfs(input, isEOS);
	mWriter.enqueue(input);
	if (isEOS) {
	  // Wait for writers to flush; perhaps this should be done is shutdown
	  mWriterThread->join();
	  break;
	}
      }
    }
  }
}

void RuntimeWriteOperator::shutdown()
{
  if (mFile != -1) {
    ::close(mFile);
    mFile = -1;
  }
}

RuntimeOperator * RuntimeWriteOperatorType::create(RuntimeOperator::Services& services) const
{
  return new RuntimeWriteOperator(services, *this);
}


