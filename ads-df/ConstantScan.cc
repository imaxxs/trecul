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

#include "ConstantScan.hh"

class RuntimeConstantScanOperator : public RuntimeOperatorBase<RuntimeConstantScanOperatorType>
{
private:
  enum State { START, WRITE, WRITE_EOF };
  State mState;
  std::vector<std::vector<uint8_t> >::const_iterator mInput;
  uint8_t * mBufferIt;
  uint8_t * mBufferEnd;
  RecordBuffer mRecordBuffer;
  RecordBufferIterator mRecordBufferIt;
public:
  RuntimeConstantScanOperator(RuntimeOperator::Services& services, 
			      const RuntimeConstantScanOperatorType& opType);
  ~RuntimeConstantScanOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

RuntimeConstantScanOperatorType::RuntimeConstantScanOperatorType(const RecordType * input,
								 const std::vector<RecordBuffer>& inputs)
  :
  mDeserialize(input->getDeserialize()),
  mMalloc(input->getMalloc())
{
  const RecordTypeSerialize& serializer(input->getSerialize());

  uint8_t * outputBufferStart = NULL;
  uint8_t * outputBufferEnd = NULL;
  uint8_t * outputBufferIt = NULL;

  for(std::vector<RecordBuffer>::const_iterator buf = inputs.begin();
      buf != inputs.end();
      ++buf) {
    RecordBufferIterator recordBufferIt;
    recordBufferIt.init(*buf);

    // Serialize the record appending new buffers as needed.
    while (!serializer.doit(outputBufferIt, outputBufferEnd, recordBufferIt, *buf)) {
      BOOST_ASSERT(outputBufferIt == outputBufferEnd);
      // If we are working on a buffer, record the length used
      if (outputBufferStart) {
	// This should be a noop but no harm in calling it.
	mBuffers.back().resize((std::size_t) (outputBufferIt - outputBufferStart));
      }
      // Allocate a new output buffer and setup pointers.
      mBuffers.push_back(std::vector<uint8_t> ());
      mBuffers.back().resize(512*1024, 0);
      outputBufferStart = &mBuffers.back()[0];
      outputBufferIt = outputBufferStart;
      outputBufferEnd = outputBufferStart + mBuffers.back().size();
    }
  }

  // Finalize any remaining buffer
  if (outputBufferStart) {
    mBuffers.back().resize((std::size_t) (outputBufferIt - outputBufferStart));
  }  
}

RuntimeConstantScanOperatorType::~RuntimeConstantScanOperatorType()
{
}

RuntimeOperator * RuntimeConstantScanOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeConstantScanOperator(s, *this);
}


RuntimeConstantScanOperator::RuntimeConstantScanOperator(RuntimeOperator::Services& services, 
			      const RuntimeConstantScanOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeConstantScanOperatorType>(services, opType),
  mState(START),
  mBufferIt(NULL),
  mBufferEnd(NULL)
{
}

RuntimeConstantScanOperator::~RuntimeConstantScanOperator()
{
}

void RuntimeConstantScanOperator::start()
{
  mState = START;
  onEvent(NULL);
}

void RuntimeConstantScanOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    for(mInput = getMyOperatorType().mBuffers.begin();
	mInput != getMyOperatorType().mBuffers.end();
	++mInput) {
      // TODO: Fix const correctness of deserialize methods
      mBufferIt = const_cast<uint8_t *>(&mInput->front());
      mBufferEnd = mBufferIt + mInput->size();
      // Deserialize records, write them and return buffer
      while(mBufferIt < mBufferEnd) {
	if (mRecordBuffer == RecordBuffer()) {
	  mRecordBuffer = getMyOperatorType().mMalloc.malloc();
	  mRecordBufferIt.init(mRecordBuffer);
	}
	if(getMyOperatorType().mDeserialize.Do(mBufferIt, mBufferEnd, mRecordBufferIt, mRecordBuffer)) {
	  requestWrite(0);
	  mState = WRITE;
	  return;
	case WRITE:
	  write(port, mRecordBuffer, false);
	  mRecordBuffer = RecordBuffer();
	  mRecordBufferIt.clear();
	} else {
	  BOOST_ASSERT(mBufferIt == mBufferEnd);
	}
      }
    }  
    // Close up
    requestWrite(0);
    mState = WRITE_EOF;
    return;
  case WRITE_EOF:
    write(port, RecordBuffer(), true);
  }
}

void RuntimeConstantScanOperator::shutdown()
{
}


class RuntimeConstantSinkOperator : public RuntimeOperatorBase<RuntimeConstantSinkOperatorType>
{
private:
  enum State { START, READ };
  State mState;
public:
  RuntimeConstantSinkOperator(RuntimeOperator::Services& services, 
			      const RuntimeConstantSinkOperatorType& opType);
  ~RuntimeConstantSinkOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

RuntimeConstantSinkOperator::RuntimeConstantSinkOperator(RuntimeOperator::Services& services, 
							 const RuntimeConstantSinkOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeConstantSinkOperatorType>(services, opType),
  mState(START)
{
}

RuntimeConstantSinkOperator::~RuntimeConstantSinkOperator()
{
}

void RuntimeConstantSinkOperator::start()
{
  mState = START;
  onEvent(NULL);
}

void RuntimeConstantSinkOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      {
	RecordBuffer input;
	read(port, input);
	bool eos = RecordBuffer::isEOS(input);
	// HACK: this is not a completely generic operator
	// hence we break constness
	if (eos) break;
	else {
	  const_cast<RuntimeConstantSinkOperatorType *>(&getMyOperatorType())->mSink.push_back(input);
	}
      }
    }
  }
}

void RuntimeConstantSinkOperator::shutdown()
{
}

RuntimeConstantSinkOperatorType::RuntimeConstantSinkOperatorType(const RecordType * input)
  :
  mFree(input->getFree())
{
}

RuntimeConstantSinkOperatorType::~RuntimeConstantSinkOperatorType()
{
  for(std::vector<RecordBuffer>::iterator buf = mSink.begin();
      buf != mSink.end();
      ++buf) {
    mFree.free(*buf);
  }
  mSink.clear();
}

RuntimeOperator * RuntimeConstantSinkOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeConstantSinkOperator(s, *this);
}

