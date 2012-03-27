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

#ifndef __FILEWRITEOPERATOR_HH__
#define __FILEWRITEOPERATOR_HH__

#include <zlib.h>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/thread.hpp>

#include "RuntimePort.hh"
#include "RuntimePlan.hh"
#include "RuntimeOperator.hh"

// Threading to make async writes to HDFS
template <class _Op>
class AsyncWriter
{
private:
  _Op& mOp;
  RuntimeFifo<RecordBuffer, 14> mRequests;
  boost::mutex mGuard;
  boost::condition_variable mCondVar;
  std::string mError;

  // Used by enqueue to amortize locking overhead
  RuntimeFifo<RecordBuffer, 14> mDequeueBuffer;
  RuntimeFifo<RecordBuffer, 14> mEnqueueBuffer;

public:
  AsyncWriter(_Op& op);
  RecordBuffer dequeue();
  void enqueue(RecordBuffer buf);
  void run();
  void getError(std::string& err);
};

template <class _Op>
AsyncWriter<_Op>::AsyncWriter(_Op& op)
  :
  mOp(op)
{
}

template <class _Op>
RecordBuffer AsyncWriter<_Op>::dequeue()
{
  if (mDequeueBuffer.getSize() == 0) {
    boost::unique_lock<boost::mutex> lock(mGuard);
    while(mRequests.getSize() == 0) {
      mCondVar.wait(lock);
    }
    mRequests.popAndPushSomeTo(mDequeueBuffer);
    mCondVar.notify_one();
  }

  RecordBuffer buf;
  mDequeueBuffer.Pop(buf);
  return buf;
}

template <class _Op>
void AsyncWriter<_Op>::enqueue(RecordBuffer buf)
{
  bool isEOS = RecordBuffer::isEOS(buf);
  mEnqueueBuffer.Push(buf);
  if (isEOS || mEnqueueBuffer.getSize() >= 140) {
    boost::unique_lock<boost::mutex> lock(mGuard);
    while(mError.size()==0 && mRequests.getSize() >= 1400) {
      mCondVar.wait(lock);
    }

    if (mError.size()) {
      std::string err = mError;
      mError = "";
      throw std::runtime_error(err);
    }

    if (isEOS)
      mEnqueueBuffer.popAndPushAllTo(mRequests);
    else
      mEnqueueBuffer.popAndPushSomeTo(mRequests);

    mCondVar.notify_one();
  }
}

template <class _Op>
void AsyncWriter<_Op>::run()
{
  std::string err;
  try {
    bool isEOS = false;
    while(!isEOS) {
      RecordBuffer buf = dequeue();
      isEOS = RecordBuffer::isEOS(buf);
      mOp.writeToHdfs(buf, isEOS);
    }
  } catch(std::exception& ex) {
    err = ex.what();
    std::cerr << "Exception occurred, writer thread exiting: " << ex.what() << std::endl;
  }

  if (err.size()) {
    boost::unique_lock<boost::mutex> lock(mGuard);
    mError = err;
    mCondVar.notify_one();
  }
}

template <class _Op>
void AsyncWriter<_Op>::getError(std::string& err) 
{
  boost::unique_lock<boost::mutex> lock(mGuard);
  err = mError;
}

// TODO: Refactor this into an operator
class ZLibCompress
{
private:
  z_stream mStream;
  uint8_t * mOutputStart;
  uint8_t * mOutputEnd;
  int mFlush;
public:
  ZLibCompress();
  ~ZLibCompress();

  void put(const uint8_t * buf_start, std::size_t len, bool isEOS);
  bool run();
  void consumeOutput(uint8_t * & output, std::size_t & len);
};

/**
 * A runtime printer manages a character buffer and 
 * print format for printing records.  It allows
 * for read access the underlying buffer as well as
 * the ability to clear the buffer (without freeing
 * memory) for subsequent uses.
 */
class RuntimePrinter
{
private:
  std::string mBuffer;
  typedef boost::iostreams::back_insert_device<std::string> string_device;
  typedef boost::iostreams::stream<string_device> string_stream;
  string_stream mStream;
  const RecordTypePrint& mPrint;
public:
  RuntimePrinter(const RecordTypePrint& p)
    :
    mStream(mBuffer),
    mPrint(p)
  {
    mPrint.imbue(mStream);
  }
  void print(RecordBuffer input)
  {
    mPrint.print(input, mStream);
    mStream.flush();
  }
  void print(RecordBuffer input, bool newLine)
  {
    mPrint.print(input, mStream, newLine);
    mStream.flush();
  }
  const std::string& str() const { return mBuffer; }
  const char * c_str() const { return mBuffer.c_str(); }
  std::size_t size() const { return mBuffer.size(); }
  void clear() { mBuffer.clear(); }
};

class RuntimeWriteOperatorType : public RuntimeOperatorType
{
  friend class RuntimeWriteOperator;
private:
  RecordTypePrint mPrint;
  RecordTypeFree mFree;
  std::string mFile;
  std::string mHeader;
  std::string mHeaderFile;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mPrint);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mFile);
    ar & BOOST_SERIALIZATION_NVP(mHeader);
    ar & BOOST_SERIALIZATION_NVP(mHeaderFile);
  }
  RuntimeWriteOperatorType()
  {
  }
public:
  RuntimeWriteOperatorType(const std::string& opName,
			   const RecordType * ty, 
			   const std::string& file,
			   const std::string& header,
			   const std::string& headerFile)
    :
    RuntimeOperatorType(opName.c_str()),
    mPrint(ty->getPrint()),
    mFree(ty->getFree()),
    mFile(file),
    mHeader(header),
    mHeaderFile(headerFile)
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& services) const;
};



#endif
