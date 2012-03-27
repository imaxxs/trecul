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

#ifndef __QUEUEIMPORT_HH__
#define __QUEUEIMPORT_HH__

#include "RecordParser.hh"
#include "ConcurrentFifo.hh"

class LogicalInputQueue : public LogicalOperator
{
private:
  std::string mStringFormat;
  char mFieldSeparator;
public:
  LogicalInputQueue();
  ~LogicalInputQueue();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
  const std::string& getStringFormat() const
  {
    return mStringFormat;
  }
  void setStringFormat(const std::string& stringFormat)
  {
    mStringFormat = stringFormat;
  }
};

class StringDataBlock : public DataBlock
{
protected:
  void openWindow(std::size_t )
  {
    // Nothing to do here!
  }
public:
  StringDataBlock()
  {
  }
  ~StringDataBlock()
  {
  }
  // Bind the block to string data.  The data block
  // does not make a copy.
  void bindString(const std::string& s)
  {
    // Don't allow binding if there is a mark set.
    BOOST_ASSERT(mCurrentBlockMark == NULL);
    mCurrentBlockStart = mCurrentBlockPtr = (uint8_t *) s.c_str();
    mCurrentBlockEnd = mCurrentBlockStart + s.size() + 1;
  }
  bool isEOF()
  {
    return mCurrentBlockPtr >= mCurrentBlockEnd;
  }
};

class NativeInputQueueOperatorType : public RuntimeOperatorType
{
public:
  typedef FieldImporter<StringDataBlock> field_importer_type;
  typedef std::vector<FieldImporter<StringDataBlock> >::const_iterator field_importer_const_iterator;
  typedef StringDataBlock input_buffer_type;

  // Importer instructions
  std::vector<field_importer_type> mImporters;
  // Create new records
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mImporters);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
  }
  NativeInputQueueOperatorType()
  {
  }  
public:
  NativeInputQueueOperatorType(const RecordType * recordType,
			       char fieldSeparator);

  ~NativeInputQueueOperatorType();

  RuntimeOperator * create(RuntimeOperator::Services & services) const;

  field_importer_const_iterator begin_importers() const 
  {
    return mImporters.begin();
  }
  field_importer_const_iterator end_importers() const 
  {
    return mImporters.end();
  }
};

class NonBlockingQueue
{
public:
  std::deque<RecordBuffer> Q;
  RecordBuffer pop() 
  {
    RecordBuffer b = Q.front();
    Q.pop_front();
    return b;
  }
  void push(RecordBuffer b)
  {
    Q.push_back(b);
  }
  bool empty() const
  {
    return Q.empty();
  }
  std::size_t size() const
  {
    return Q.size();
  }
};

class NativeInputQueueOperator : public RuntimeOperator
{
public:
  typedef NonBlockingQueue queue_type;
private:
  enum State { START, READ, WRITE, WRITE_EOF };
  State mState;

  // Queue that operator reads from.
  // ConcurrentBlockingFifo<RecordBuffer> mQueue;
  queue_type mQueue;
  // Records imported
  uint64_t mRecordsImported;

public:
  NativeInputQueueOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
    :
    RuntimeOperator(services, opType),
    mRecordsImported(0)
  {
  }

  ~NativeInputQueueOperator()
  {
  }

  /**
   * intialize.
   */
  void start()
  {
    mState = START;
    mRecordsImported = 0;
    onEvent(NULL);
  }

  void onEvent(RuntimePort * port);

  void shutdown()
  {
  }

  // The operator must expose an API that gives clients
  // access to the queue (since it is an inprocess queue).
  queue_type& getQueue()
  {
    return mQueue;
  }
  // Public access to the op type that carries the importer info.
  const NativeInputQueueOperatorType & getLogParserType()
  {
    return *static_cast<const NativeInputQueueOperatorType *>(&getOperatorType());
  }
};

#endif
