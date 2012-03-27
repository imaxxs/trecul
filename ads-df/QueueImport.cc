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

#include <boost/algorithm/string.hpp>
#include "FileSystem.hh"
#include "QueueImport.hh"

LogicalInputQueue::LogicalInputQueue()
  :
  mFieldSeparator('\t')
{
}

LogicalInputQueue::~LogicalInputQueue()
{
}

void LogicalInputQueue::check(PlanCheckContext& ctxt)
{
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (it->equals("fieldseparator")) {
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
    } else if (boost::algorithm::iequals(it->Name, "format")) {
      mStringFormat = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "formatfile")) {
      mStringFormat = FileSystem::readFile(boost::get<std::string>(it->Value));
    } else {
      checkDefaultParam(*it);
    }
  }

  try {
    IQLRecordTypeBuilder bld(ctxt, mStringFormat, false);
    getOutput(0)->setRecordType(bld.getProduct());
  } catch(std::exception& ex) {
    ctxt.logError(*this, ex.what());
  }
}

void LogicalInputQueue::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType =
    new NativeInputQueueOperatorType(getOutput(0)->getRecordType(),
				     mFieldSeparator);
  plan.addOperatorType(opType);
  plan.mapOutputPort(this, 0, opType, 0);  
}

NativeInputQueueOperatorType::NativeInputQueueOperatorType(const RecordType * recordType,
							   char fieldSeparator)
  :
  RuntimeOperatorType("NativeInputQueueOperatorType")
{
  mMalloc = recordType->getMalloc();
  mFree = recordType->getFree();

  // Records have tab delimited fields and 0 delimited records
  field_importer_type::createDefaultImport(recordType, recordType, fieldSeparator, 
					   (char) 0, mImporters);
}

NativeInputQueueOperatorType::~NativeInputQueueOperatorType()
{
}

RuntimeOperator * NativeInputQueueOperatorType::create(RuntimeOperator::Services & services) const
{
  return new NativeInputQueueOperator(services, *this);
}

void NativeInputQueueOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    // Read all of the record in the file.
    while(true) {
      if (mQueue.empty()) {
	mState = READ;
	return;
      case READ:
	BOOST_ASSERT(!mQueue.empty());
      }
      requestWrite(0);
      mState = WRITE;
      return;
    case WRITE:
      {
	mRecordsImported += 1;
	RecordBuffer rec = mQueue.pop();
	if (RecordBuffer::isEOS(rec)) break;
	write(port, rec, false);
      }
    }
    // Done with the queue
    requestWrite(0);
    mState = WRITE_EOF;
    return;
  case WRITE_EOF:
    write(port, RecordBuffer::create(), true);
    return;
  }
}

