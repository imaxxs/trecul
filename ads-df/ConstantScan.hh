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

#ifndef __CONSTANT_SCAN_HH__
#define __CONSTANT_SCAN_HH__

#include "RuntimeOperator.hh"

class RuntimeConstantScanOperatorType : public RuntimeOperatorType
{
  friend class RuntimeConstantScanOperator;
private:
  RecordTypeDeserialize mDeserialize;
  RecordTypeMalloc mMalloc;
  std::vector<std::vector<uint8_t> > mBuffers;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mDeserialize);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);    
    ar & BOOST_SERIALIZATION_NVP(mBuffers);
  }
  RuntimeConstantScanOperatorType()
  {
  }
public:
  RuntimeConstantScanOperatorType(const RecordType * input,
				  const std::vector<RecordBuffer>& inputs);
  ~RuntimeConstantScanOperatorType();
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
};

/**
 * A sink operator that stores record stream.  Useful for coordination
 * applications in which multiple dataflows need to be executed and we don't
 * want to bother writing results to disk.
 */
class RuntimeConstantSinkOperatorType : public RuntimeOperatorType
{
  friend class RuntimeConstantSinkOperator;
private:
  // This is only safe when run in a single plan!!!!
  std::vector<RecordBuffer> mSink;
  RecordTypeFree mFree;
  // No Serialization on purpose!!!!!
  RuntimeConstantSinkOperatorType()
  {
  }
public:
  RuntimeConstantSinkOperatorType(const RecordType * input);
  ~RuntimeConstantSinkOperatorType();
  RuntimeOperator * create(RuntimeOperator::Services & s) const;
  const std::vector<RecordBuffer>& getSink() const { return mSink; }
};

#endif
