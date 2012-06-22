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

#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>
#include "RuntimeOperator.hh"
#include "IQLInterpreter.hh"
#include "TypeCheckContext.hh"

/**
 * A class to encapsulate some policy decisions that we probably
 * to make configurable in the future.
 */
class QueryOptions
{
public:
  /**
   * Do NULL values sort low or high?
   */
  static bool nullsSortLow() 
  {
    return true;
  }
};

RecordTypeFunction * HashFunction::get(DynamicRecordContext & ctxt,
				       const RecordType * inputType,
				       const std::vector<std::string>& fields,
				       const std::string& name)
{
  std::vector<const RecordType *> inputOnly;
  inputOnly.push_back(inputType);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  inputOnly.push_back(&emptyTy);
  std::string hashargs;
  for(std::size_t i=0; i<fields.size(); i++) {
    if (hashargs.size() > 0) hashargs += ",";
    hashargs += (boost::format("%1%") % fields[i]).str();
  }
  return new RecordTypeFunction(ctxt, 
				name, 
				inputOnly, 
				(boost::format("#(%1%)") % hashargs).str());
}

RecordTypeFunction * EqualsFunction::get(DynamicRecordContext & ctxt,
					 const RecordType * lhs,
					 const RecordType * rhs,
					 const std::vector<std::string>& fields,
					 const std::string& name,
					 bool areNullsEqual)
{
  std::vector<const RecordType *> eqTypes;
  eqTypes.push_back(lhs);
  eqTypes.push_back(rhs);
  std::string eqPred;
  for(std::size_t i=0; i<fields.size(); i++) {
    bool isFieldNullable = lhs->getMember(fields[i]).GetType()->isNullable() ||
      rhs->getMember(fields[i]).GetType()->isNullable();
    if (eqPred.size() > 0) eqPred += " AND ";
    if (areNullsEqual && isFieldNullable) {
      eqPred += (boost::format("((input1.%1% IS NULL AND input0.%1% IS NULL) OR input1.%1% = input0.%1%)") % fields[i]).str();
    } else {
      eqPred += (boost::format("input1.%1% = input0.%1%") % fields[i]).str();
    }
  }
  return new RecordTypeFunction(ctxt, name, 
				eqTypes, 
				eqPred);
}

RecordTypeFunction * EqualsFunction::get(DynamicRecordContext & ctxt,
					 const RecordType * lhs,
					 const RecordType * rhs,
					 const std::vector<SortKey>& fields,
					 const std::string& name, 
					 bool areNullsEqual)
{
  // Equality of sort keys is order independent
  std::vector<std::string> fieldNames;
  for(std::vector<SortKey>::const_iterator f=fields.begin(), e=fields.end();
      f != e; ++f) {
    fieldNames.push_back(f->getName());
  }
  return get(ctxt, lhs, rhs, fieldNames, name, areNullsEqual);
}

RecordTypeFunction * KeyPrefixFunction::get(DynamicRecordContext & ctxt,
					    const RecordType * inputType,
					    const std::vector<std::string>& fields,
					    const std::string& name)
{
  std::vector<const RecordType *> inputOnly;
  inputOnly.push_back(inputType);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  inputOnly.push_back(&emptyTy);
  std::string hashargs;
  for(std::size_t i=0; i<fields.size(); i++) {
    if (hashargs.size() > 0) hashargs += ",";
    hashargs += (boost::format("%1%") % fields[i]).str();
  }
  return new RecordTypeFunction(ctxt, 
				name, 
				inputOnly, 
				(boost::format("$(%1%)") % hashargs).str());
}

RecordTypeFunction * LessThanFunction::get(DynamicRecordContext & ctxt,
					   const RecordType * lhs,
					   const RecordType * rhs,
					   const std::vector<std::string>& fields,
					   const std::string& name)
{
  // Set up sort keys with ASC
  std::vector<SortKey> keys;
  for(std::vector<std::string>::const_iterator f=fields.begin(),e=fields.end();
      f != e; ++f) {
    keys.push_back(SortKey(*f, SortKey::ASC));
  }
  return get(ctxt, lhs, rhs, keys, false, name);
}

RecordTypeFunction * LessThanFunction::get(DynamicRecordContext & ctxt,
					   const RecordType * lhs,
					   const RecordType * rhs,
					   const std::vector<SortKey>& fields,
					   bool sortNulls,
					   const std::string& name)
{
  // When we compare two records to each other,
  // we need to refer to one field from each record.  Since we do this
  // by name, we generate a temporary prefix to disambiguate.
  std::vector<const RecordType *> eqTypes;
  eqTypes.push_back(lhs);
  eqTypes.push_back(rhs);
  std::string eqPred;
  for(std::size_t i=0; i<fields.size(); i++) {
    if (i > 0) { 
      if (sortNulls && (lhs->getMember(fields[i-1].getName()).GetType()->isNullable() ||
			rhs->getMember(fields[i-1].getName()).GetType()->isNullable())) {
	eqPred += (boost::format(" OR (((input0.%1% IS NULL AND input1.%1% IS NULL) OR "
				 "input0.%1% = input1.%1%) AND (") % 
		   fields[i-1].getName()).str();
      } else {
	eqPred += (boost::format(" OR (input0.%1% = input1.%1% AND (") % 
		   fields[i-1].getName()).str();
      }
    } 
    bool handleNulls = sortNulls && 
      (lhs->getMember(fields[i].getName()).GetType()->isNullable() ||
       rhs->getMember(fields[i].getName()).GetType()->isNullable());
    if (handleNulls) {
      bool isLeftNullLess = 
	(fields[i].getOrder()==SortKey::ASC && QueryOptions::nullsSortLow()) ||
	(fields[i].getOrder()==SortKey::DESC && !QueryOptions::nullsSortLow());
      eqPred += (boost::format("(%2%.%1% IS NULL AND %3%.%1% IS NOT NULL OR ") %
		 fields[i].getName() %
		 (isLeftNullLess ? "input0" : "input1") % (isLeftNullLess ? "input1" : "input0")).str();
    }
    eqPred += (boost::format("input0.%1% %2% input1.%1%") % fields[i].getName() %
	       (fields[i].getOrder()==SortKey::ASC ? "<" : ">")).str();
    if (handleNulls) {
      eqPred += ")";
    }
  }
  for(std::size_t i=1; i<fields.size(); i++) {
    eqPred += "))";
  }
  return new RecordTypeFunction(ctxt, name, 
				eqTypes, 
				eqPred);
}

RecordTypeFunction * 
SortKeyPrefixFunction::get(DynamicRecordContext & ctxt,
			   const RecordType * input,
			   const std::vector<SortKey>& fields,
			   const std::string& name)
{
  // TODO: Handle case in which first field doesn't provide a full 31 bits
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  std::vector<const RecordType *> tableOnly;
  tableOnly.push_back(input);
  tableOnly.push_back(&emptyTy);
  std::string expr((boost::format("$(%1%)") % fields[0].getName()).str());
  if (fields[0].getOrder() == SortKey::DESC) {
    // Switch order for descending.  Remember $ returns only 31 bits
    expr = (boost::format("%1% - %2%") % std::numeric_limits<int32_t>::max() %
	    expr).str();
  }
  return new RecordTypeFunction(ctxt, 
				name,
				tableOnly, 
				expr);
  
}

RuntimePlanBuilder::RuntimePlanBuilder()
{
}

std::pair<RuntimeOperatorType *, std::size_t> 
RuntimePlanBuilder::mapInputPort(LogicalOperator* op, std::size_t port)
{
  std::map<LogicalOperator*, std::vector<OpTypePort> >::iterator it =
    mInputPortMap.find(op);

  std::vector<OpTypePort> & p(it->second);

  return std::make_pair(p[port].OpType, p[port].Index);
}

std::pair<RuntimeOperatorType *, std::size_t> 
RuntimePlanBuilder::mapOutputPort(LogicalOperator* op, std::size_t port)
{
  std::map<LogicalOperator*, std::vector<OpTypePort> >::iterator it =
    mOutputPortMap.find(op);

  std::vector<OpTypePort> & p(it->second);

  return std::make_pair(p[port].OpType, p[port].Index);
}



RuntimeOperator::RuntimeOperator(Services& services, const RuntimeOperatorType& opType)
  :
  mOperatorType(opType),
  mServices(services),
  mTicks(0)
{
}

RuntimeOperator::~RuntimeOperator()
{
}

LogicalFilter::LogicalFilter()
  :
  LogicalOperator(1,1,1,1),
  mPredicate(NULL),
  mLimit(std::numeric_limits<int64_t>::max())
{
}

LogicalFilter::~LogicalFilter()
{
  delete mPredicate;
}

void LogicalFilter::check(PlanCheckContext& log)
{
  // Validate the parameters
  std::string predicate;
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "where")) {
      predicate = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "limit")) {
      mLimit = boost::get<int32_t>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }

  // TODO: If no predicate or limit specified, perhaps
  // we should warn.
  if (predicate.size()) {
    std::vector<RecordMember> emptyMembers;
    RecordType emptyTy(emptyMembers);
    std::vector<const RecordType *> inputs;
    inputs.push_back(getInput(0)->getRecordType());
    inputs.push_back(&emptyTy);
    mPredicate = new RecordTypeFunction(log,
					"filter",
					inputs,
					predicate);
  }

  getOutput(0)->setRecordType(getInput(0)->getRecordType());
}

void LogicalFilter::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimeFilterOperatorType(getInput(0)->getRecordType(),
				  mPredicate,
				  mLimit);
  
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  plan.mapOutputPort(this, 0, opType, 0);      
}

RuntimeOperator * RuntimeFilterOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeFilterOperator(s, *this);
}

RuntimeFilterOperator::RuntimeFilterOperator(RuntimeOperator::Services& services, 
					     const RuntimeFilterOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeFilterOperatorType>(services, opType),
  mState(START),
  mNumRecords(0),
  mRuntimeContext(new InterpreterContext())

{
}

RuntimeFilterOperator::~RuntimeFilterOperator()
{
  delete mRuntimeContext;
}

void RuntimeFilterOperator::start()
{
  mNumRecords = 0;
  mState = START;
  onEvent(NULL);
}

void RuntimeFilterOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      read(port, mInput);

      // Done executing the operator.
      if (RecordBuffer::isEOS(mInput)) 
	break;
      
      if ((NULL == getMyOperatorType().mPredicate ||
	   0 != getMyOperatorType().mPredicate->execute(mInput, 
							RecordBuffer(),
							mRuntimeContext)) &&
	  ++mNumRecords <= getMyOperatorType().mLimit) {
	requestWrite(0);
	mState = WRITE;
	return;
      case WRITE:
	write(port, mInput, false);
      } else {
	getMyOperatorType().mFree.free(mInput);
	mInput = RecordBuffer();
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

void RuntimeFilterOperator::shutdown()
{
}


CopyOp::CopyOp()
  :
  LogicalOperator(1,1,1,std::numeric_limits<uint32_t>::max()),
  mOpType(NULL)
{
}

CopyOp::CopyOp(DynamicRecordContext & ctxt,
	       const RecordType * inputType,
	       const std::vector<std::string>& transfers)
  :
  LogicalOperator(1,1,1,(uint32_t) transfers.size()),
  mOpType(NULL)
{
  std::vector<bool> pics;
  pics.resize(transfers.size(), false);
  init(ctxt, inputType, transfers, pics);
}

CopyOp::~CopyOp()
{
  for(std::vector<const RecordTypeTransfer*>::iterator it = mTransfers.begin();
      it != mTransfers.end();
      ++it) {
    delete (*it);
  }
  // Don't delete op type since it should be added to 
  // a plan.
}

void CopyOp::init(DynamicRecordContext & ctxt,
		  const RecordType * inputType,
		  const std::vector<std::string>& transfers,
		  const std::vector<bool>& pics)
{
  for(std::vector<std::string>::const_iterator it = transfers.begin();
      it != transfers.end();
      ++it) {
    RecordTypeTransfer * t = 
      new RecordTypeTransfer(ctxt, 
			     (boost::format("CopyOpXfer%1%") % (it-transfers.begin())).str(), 
			     inputType, 
			     *it);
    mTransfers.push_back(t);
  }
  mOpType = new RuntimeCopyOperatorType(inputType->getFree(),
					mTransfers,
					pics);
}

RuntimeCopyOperatorType & CopyOp::getOpType()
{
  return *mOpType;
}

const RecordType * CopyOp::getOutputType(std::size_t idx) const
{
  return mTransfers[idx]->getTarget();
}

void CopyOp::check(PlanCheckContext& log)
{
  // Validate the parameters
  std::vector<std::string> transfers;
  std::vector<bool> pics;
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "output")) {
      transfers.push_back(boost::get<std::string>(it->Value));
    } else if (boost::algorithm::iequals(it->Name, "pic")) {
      // If no pic setting for some outputs, default them
      // to false.
      if (pics.size() < transfers.size()) {
	pics.resize(transfers.size(), false);
      }

      if (boost::algorithm::iequals(boost::get<std::string>(it->Value), 
				    "true")) {
	pics.back() = true;
      } else {
	pics.back() = false;
      }
    } else {
      checkDefaultParam(*it);
    }
  }

  // Must have one output spec for each output port.
  // TODO: Perhaps we allow defaulting to input.*.
  if (transfers.size() != size_outputs()) {
    log.logError(*this, (boost::format("Operator has %1% output arrows "
				       "and %2% output arguments") %
			 size_outputs() % transfers.size()).str());
  }
  
  // Apply remaining default PIC settings
  if (pics.size() < transfers.size())
    pics.resize(transfers.size(), false);

  init(log, getInput(0)->getRecordType(), transfers, pics);

  for(std::size_t i=0; i<mTransfers.size(); ++i) {
    getOutput(i)->setRecordType(mTransfers[i]->getTarget());
  }
}

void CopyOp::create(class RuntimePlanBuilder& plan)
{
  plan.addOperatorType(mOpType);
  plan.mapInputPort(this, 0, mOpType, 0);  
  for(std::size_t i=0; i<mTransfers.size(); ++i) {
    plan.mapOutputPort(this, i, mOpType, i);      
  }
}

RuntimeCopyOperatorType::RuntimeCopyOperatorType(const RecordTypeFree & freeFunctor,
						 const std::vector<const class RecordTypeTransfer *>& transfers)
  :
  RuntimeOperatorType("RuntimeCopyOperatorType"),
  mFree(freeFunctor)
{
  for(std::vector<const RecordTypeTransfer *>::const_iterator it = transfers.begin();
      it != transfers.end();
      ++it) {
    mTransfers.push_back((*it)->create());
  }
}

RuntimeCopyOperatorType::RuntimeCopyOperatorType(const RecordTypeFree & freeFunctor,
						 const std::vector<const class RecordTypeTransfer *>& transfers,
						 const std::vector<bool>& pics)
  :
  RuntimeOperatorType("RuntimeCopyOperatorType"),
  mFree(freeFunctor)
{
  for(std::vector<const RecordTypeTransfer *>::const_iterator it = transfers.begin();
      it != transfers.end();
      ++it) {
    mTransfers.push_back((*it)->create(pics[it - transfers.begin()]));
  }
}

RuntimeCopyOperatorType::~RuntimeCopyOperatorType()
{
  for(std::vector<IQLTransferModule *>::iterator it = mTransfers.begin();
      it != mTransfers.end(); 
      ++it) {
    delete *it;
  }
}

RuntimeOperator* RuntimeCopyOperatorType::create(RuntimeOperator::Services& s) const
{
  return new RuntimeCopyOperator(s, *this);
}

RuntimeCopyOperator::RuntimeCopyOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mInput(NULL),
  mRuntimeContext(NULL)
{
}

RuntimeCopyOperator::~RuntimeCopyOperator()
{
  delete mRuntimeContext;
}

void RuntimeCopyOperator::start()
{
  mRuntimeContext = new InterpreterContext();
  mState = START;
  onEvent(NULL);
}

void RuntimeCopyOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      read(port, mInput);

      // Done executing the operator.
      if (RecordBuffer::isEOS(mInput)) 
	break;

      // TODO: Organize iteration here so 
      // we can leverage an identity transfer
      // if one exists (if last transfer is
      // identity we can just copy record
      // through unmodified if we schedule
      // it last).
      for(mOutputIt = output_port_begin(); 
	  mOutputIt != output_port_end();
	  ++mOutputIt) {
	requestWrite(**mOutputIt);
	mState = WRITE;
	return;
      case WRITE:
	{
	  RecordBuffer outputBuf;
	  bool last = mOutputIt+1 == output_port_end();
	  // TODO: The move semantics optimization seems buggy at this point.
	  // Fix it and use move semantics when last is true.
	  getCopyType().mTransfers[mOutputIt - output_port_begin()]->execute(mInput, 
									     outputBuf, 
									     mRuntimeContext, 
									     false);
	  write(port, outputBuf, false);
	  if (last) {
	    getCopyType().mFree.free(mInput);
	    mInput = RecordBuffer();
	  }
	}
      }
    }
    // Close up shop on all downstream ports.
    for(mOutputIt = output_port_begin(); 
	mOutputIt != output_port_end();
	++mOutputIt) {
      requestWrite(**mOutputIt);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer(NULL), true);
    }
  }
}

void RuntimeCopyOperator::shutdown()
{
  delete mRuntimeContext;
  mRuntimeContext = NULL;
}

LogicalDevNull::LogicalDevNull()
  :
  LogicalOperator(1,1,0,0)
{
}

LogicalDevNull::~LogicalDevNull()
{
}

void LogicalDevNull::check(PlanCheckContext& log)
{
}

void LogicalDevNull::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = new RuntimeDevNullOperatorType(getInput(0)->getRecordType());
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
}

RuntimeOperator* RuntimeDevNullOperatorType::create(RuntimeOperator::Services& s) const
{
  return new RuntimeDevNullOperator(s, *this);
}

RuntimeDevNullOperator::RuntimeDevNullOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START)
{
}

void RuntimeDevNullOperator::start()
{
  mState = START;
  onEvent(NULL);
}

void RuntimeDevNullOperator::onEvent(RuntimePort * port)
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
	getDevNullType().mFree.free(input);
	if (eos) break;
      }
    }
  }
}

void RuntimeDevNullOperator::shutdown()
{
}

LogicalPrint::LogicalPrint()
  :
  LogicalOperator(1,1,1,1),
  mNumToPrint(5),
  mPrintFrequency(1),
  mPredicate(NULL)
{
}

LogicalPrint::~LogicalPrint()
{
  delete mPredicate;
}

void LogicalPrint::check(PlanCheckContext& log)
{
  std::string predicate;

  if (size_inputs() != 1) {
  } 
  if (getInput(0)->getRecordType() == NULL) {
  }
  getOutput(0)->setRecordType(getInput(0)->getRecordType());

  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "numtoprint") ||
	boost::algorithm::iequals(it->Name, "limit")) {
      mNumToPrint = boost::get<int32_t>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "printfrequency")) {
      mPrintFrequency = boost::get<int32_t>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "where")) {
      predicate = boost::get<std::string>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }

  if (predicate.size()) {
    std::vector<RecordMember> emptyMembers;
    RecordType emptyTy(emptyMembers);
    std::vector<const RecordType *> inputs;
    inputs.push_back(getInput(0)->getRecordType());
    inputs.push_back(&emptyTy);
    mPredicate = new RecordTypeFunction(log,
					"filter",
					inputs,
					predicate);
  }
}

void LogicalPrint::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimePrintOperatorType(getInput(0)->getRecordType(),
				 mNumToPrint,
				 mPrintFrequency,
				 mPredicate);
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);
  plan.mapOutputPort(this, 0, opType, 0);
}

RuntimeOperator* RuntimePrintOperatorType::create(RuntimeOperator::Services& s) const
{
  return new RuntimePrintOperator(s, *this);
}

RuntimePrintOperator::RuntimePrintOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mNumPrinted(0),
  mNumToPrint(getPrintType().mNumToPrint),
  mPrintFrequency(getPrintType().mPrintFrequency),
  mNumProcessed(0),
  mRuntimeContext(new InterpreterContext())
{
}

RuntimePrintOperator::~RuntimePrintOperator()
{
  delete mRuntimeContext;
}

void RuntimePrintOperator::start()
{
  mState = START;
  mNumPrinted = 0;
  mNumProcessed = 0;
  getPrintType().mPrint.imbue(std::cout);
  onEvent(NULL);
}

void RuntimePrintOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      
  	requestWrite(0);
  	mState = WRITE;
  	return;
    case WRITE:
      {
  	bool isEOS = false;
  	RuntimePort::local_buffer_type & in(readLocal(getInputPorts()[0]));
  	RuntimePort::local_buffer_type & out(readLocal(getOutputPorts()[0]));
  	while(!in.empty()) {
  	  in.Pop(mInput);
  	  isEOS = RecordBuffer::isEOS(mInput);

	  // The actual print logic is here.
	  if (mNumPrinted < mNumToPrint && 
	      mNumProcessed % mPrintFrequency == 0 &&
	      !isEOS &&
	      (NULL == getPrintType().mPredicate ||
	       0 != getPrintType().mPredicate->execute(mInput, 
						       RecordBuffer(),
						       mRuntimeContext))) {
	    mNumPrinted += 1;
	    getPrintType().mPrint.print(mInput, std::cout);
	  }
	  mNumProcessed += 1;

  	  out.Push(mInput);
  	  if (isEOS) break;
  	}
  	if (isEOS) {
  	  flushLocal(port);
  	  break;
  	}
      }
    }
  }
}

void RuntimePrintOperator::shutdown()
{
}

LogicalGenerate::LogicalGenerate()
  :
  LogicalOperator(0,0,1,1),
  mNumRecords(10),
  mStateType(NULL),
  mTransfer(NULL)
{
}

LogicalGenerate::~LogicalGenerate()
{
  delete mTransfer;
}

void LogicalGenerate::check(PlanCheckContext& ctxt)
{
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "program") ||
	boost::algorithm::iequals(it->Name, "output")) {
      mProgram = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "numRecords")) {
      mNumRecords = boost::get<int32_t>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }

  // Create the state type here.
  std::vector<RecordMember> members;
  members.push_back(RecordMember("RECORDCOUNT", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("PARTITIONCOUNT", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("PARTITION", Int32Type::Get(ctxt)));
  mStateType = RecordType::get(ctxt, members);
  mTransfer = new RecordTypeTransfer(ctxt, "myGenerate", mStateType, mProgram);

  getOutput(0)->setRecordType(mTransfer->getTarget());
}

void LogicalGenerate::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = new RuntimeGenerateOperatorType("generate",
								 mStateType,
								 mTransfer,
								 mNumRecords);
  plan.addOperatorType(opType);
  plan.mapOutputPort(this, 0, opType, 0);  
}

RuntimeGenerateOperatorType::RuntimeGenerateOperatorType(const std::string& name,
							 const RecordType * stateType,
							 RecordTypeTransfer * transfer,
							 int64_t upperBound)
  :
  RuntimeOperatorType(name.c_str()),
  mLoopUpperBound(upperBound),
  mTransfer(NULL),
  mModule(NULL)
{
  mRecordCount = stateType->getFieldAddress("RECORDCOUNT");
  mPartitionCount = stateType->getFieldAddress("PARTITIONCOUNT");
  mPartition = stateType->getFieldAddress("PARTITION");
  mStateMalloc = stateType->getMalloc();
  mStateFree = stateType->getFree();

  mModule = transfer->create();
}

RuntimeGenerateOperatorType::RuntimeGenerateOperatorType(DynamicRecordContext & ctxt, const std::string & prog, int64_t upperBound)
  :
  RuntimeOperatorType("RuntimeGenerateOperatorType"),
  mLoopUpperBound(upperBound),
  mTransfer(NULL),
  mModule(NULL)
{
  // Create the state type here.
  std::vector<RecordMember> members;
  members.push_back(RecordMember("RECORDCOUNT", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("PARTITIONCOUNT", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("PARTITION", Int32Type::Get(ctxt)));
  RecordType stateType(members);
  mRecordCount = stateType.getFieldAddress("RECORDCOUNT");
  mPartitionCount = stateType.getFieldAddress("PARTITIONCOUNT");
  mPartition = stateType.getFieldAddress("PARTITION");
  mStateMalloc = stateType.getMalloc();
  mStateFree = stateType.getFree();

  mTransfer = new RecordTypeTransfer(ctxt, "myGenerate", &stateType, prog);
  mModule = mTransfer->create();
}

RuntimeGenerateOperatorType::~RuntimeGenerateOperatorType()
{
  delete mModule;
}

const RecordType * RuntimeGenerateOperatorType::getOutputType() const
{ 
  return mTransfer->getTarget(); 
}

RuntimeOperator* RuntimeGenerateOperatorType::create(RuntimeOperator::Services& s) const
{
  return new RuntimeGenerateOperator(s, *this);
}

RuntimeGenerateOperator::RuntimeGenerateOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mStateRecord(NULL),
  mRuntimeContext(NULL)
{
}

RuntimeGenerateOperator::~RuntimeGenerateOperator()
{
  getGenerateType().mStateFree.free(mStateRecord);
  delete mRuntimeContext;
}

void RuntimeGenerateOperator::start()
{
  mStateRecord = getGenerateType().mStateMalloc.malloc();
  getGenerateType().mRecordCount.setInt64(0, mStateRecord);
  getGenerateType().mPartitionCount.setInt32(getNumPartitions(), mStateRecord);
  getGenerateType().mPartition.setInt32(getPartition(), mStateRecord);
  if (mRuntimeContext) {
    delete mRuntimeContext;
  }
  mRuntimeContext = new InterpreterContext();
  mState = START;
  onEvent(NULL);
}

void RuntimeGenerateOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestWrite(0);
      mState = WRITE;
      return;
    case WRITE: 
      {
	int64_t cnt = getGenerateType().mRecordCount.getInt64(mStateRecord);
	if (cnt < getGenerateType().mLoopUpperBound) {
	  RecordBuffer output;
	  getGenerateType().mModule->execute(mStateRecord, output, mRuntimeContext, false);
	  write(port, output, false);
	  getGenerateType().mRecordCount.setInt64(cnt + 1, mStateRecord);
	} else {
	  write(port, RecordBuffer(NULL), true);
	  break;
	}
      }
    }
  }
}

void RuntimeGenerateOperator::shutdown()
{
}

LogicalGroupBy::LogicalGroupBy(LogicalGroupBy::Algorithm a)
  :
  LogicalOperator(1,1,1,1),
  mAggregate(NULL),
  mHash(NULL),
  mHashEq(NULL),
  mSortEq(NULL),
  mAlgorithm(a),
  mIsRunningTotal(false)
{
}

LogicalGroupBy::~LogicalGroupBy()
{
  delete mAggregate;
  delete mHash;
  delete mHashEq;
  delete mSortEq;
}

void LogicalGroupBy::check(PlanCheckContext& log)
{
  if (size_inputs() != 1) {
    log.logError(*this, "input port not connected");
  } 

  if (getInput(0)->getRecordType() == NULL) {
    log.logError(*this, "Input record type not available");
    std::vector<RecordMember> emptyMembers;
    getInput(0)->setRecordType(RecordType::get(log, emptyMembers));
  }

  std::string init;
  std::string update;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "hashkey")) {
      mHashGroupKeys.push_back(boost::get<std::string>(it->Value));
      if (HYBRID != mAlgorithm) {
	log.logError(*this, *it, "Can only use hashKey "
		     "with hybrid group by operator");
      }
    } else if (boost::algorithm::iequals(it->Name, "key")) {
      switch(mAlgorithm) {
      case SORT:
	mSortGroupKeys.push_back(boost::get<std::string>(it->Value));
	break;
      case HASH:
	mHashGroupKeys.push_back(boost::get<std::string>(it->Value));
	break;
      case HYBRID:
      default:
	log.logError(*this, *it, "Must use sortKey and hashKey "
		     "argument with hybrid group by operator");
	break;
      }
    } else if (boost::algorithm::iequals(it->Name, "initialize")) {
      init = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "program") ||
	       boost::algorithm::iequals(it->Name, "output")) {
      mProgram = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "runningtotal")) {
      if (boost::algorithm::iequals(boost::get<std::string>(it->Value), 
				    "true")) {
	mIsRunningTotal = true;
      } else {
	mIsRunningTotal = false;
      }
    } else if (boost::algorithm::iequals(it->Name, "sortkey")) {
      mSortGroupKeys.push_back(boost::get<std::string>(it->Value));
      if (HYBRID != mAlgorithm) {
	log.logError(*this, *it, "Can only use sortKey "
		     "with hybrid group by operator");
      }
    } else if (boost::algorithm::iequals(it->Name, "update")) {
      update = boost::get<std::string>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }

  if (mIsRunningTotal && SORT != mAlgorithm) {
    log.logError(*this, "runningtotal is only supported with sort group by");
  }

  if (init.size() && 0==update.size() ||
      update.size() && 0==init.size()) {
    log.logError(*this, "either specify both initialize and update or output argument");
  }

  std::vector<std::string> allGroupKeys;
  if (mSortGroupKeys.size()) {
    allGroupKeys.insert(allGroupKeys.end(), 
			mSortGroupKeys.begin(),
			mSortGroupKeys.end());
  }
  if (mHashGroupKeys.size()) {
    allGroupKeys.insert(allGroupKeys.end(), 
			mHashGroupKeys.begin(),
			mHashGroupKeys.end());
  }

  if (init.size()) {
    // Prepend group by keys to the init.
    std::string tmp;
    for(std::vector<std::string>::const_iterator it = allGroupKeys.begin();
	it != allGroupKeys.end();
	++it) {
      tmp += *it;
      tmp += ",";
    }
    init = tmp + init;
    mAggregate = new RecordTypeAggregate(log,
					 "hashAgg",
					 getInput(0)->getRecordType(),
					 init,
					 update,
					 allGroupKeys,
					 mIsRunningTotal);
  } else {
    // Must specify output if non init/update
    if (0 == mProgram.size()) {
      log.logError(*this, "Must specify argument 'output'");
    }
  
    mAggregate = new RecordTypeAggregate(log,
					 "hashAgg",
					 getInput(0)->getRecordType(),
					 mProgram,
					 allGroupKeys,
					 mIsRunningTotal);
  }

  if (mSortGroupKeys.size()) {
    mSortEq = EqualsFunction::get(log, 
				  mAggregate->getAggregate(),
				  getInput(0)->getRecordType(),
				  mSortGroupKeys,
				  "group_by_sort_key_equals",
				  true);
  }

  if (mHashGroupKeys.size()) {
    mHash = HashFunction::get(log, 
			      getInput(0)->getRecordType(),
			      mHashGroupKeys);
    mHashEq = EqualsFunction::get(log, 
				  mAggregate->getAggregate(),
				  getInput(0)->getRecordType(),
				  mHashGroupKeys,
				  "group_by_hash_key_equals",
				  true);
  }

  getOutput(0)->setRecordType(mAggregate->getTarget());
}

void LogicalGroupBy::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = NULL;
  if (mIsRunningTotal) {
    opType = 
      new RuntimeSortRunningTotalOperatorType(getInput(0)->getRecordType()->getFree(),
					      mHash,
					      mSortEq,
					      mAggregate);
  } else if (mHashEq == NULL)
    opType = 
      new RuntimeSortGroupByOperatorType(getInput(0)->getRecordType()->getFree(),
					 mHash,
					 mSortEq,
					 mAggregate);
  else
    opType = 
      new RuntimeHashGroupByOperatorType(getInput(0)->getRecordType()->getFree(),
					 mHash,
					 mHashEq,
					 mAggregate,
					 mSortEq);
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  plan.mapOutputPort(this, 0, opType, 0);  
}

// A group by operator that supports a set of sorted group keys
// and a set of hash group keys.
class RuntimeHybridGroupByOperator : public RuntimeOperator
{
private:
  enum State { START, READ, WRITE, WRITE_EOS };
  State mState;
  class InterpreterContext * mRuntimeContext;
  // Input record identifies the currently active
  // set of sort group keys.
  RecordBuffer mCurrentAggregate;
  // Hash table for the current set of sort group keys.
  paged_hash_table mTable;
  paged_hash_table::query_iterator<paged_hash_table::probe_predicate> mSearchIterator;
  paged_hash_table::scan_all_iterator mScanIterator;

  const RuntimeHashGroupByOperatorType & getHashGroupByType() { return *reinterpret_cast<const RuntimeHashGroupByOperatorType *>(&getOperatorType()); }
public:
  RuntimeHybridGroupByOperator(RuntimeOperator::Services& services, const RuntimeHashGroupByOperatorType& opType);
  ~RuntimeHybridGroupByOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

RuntimeHybridGroupByOperator::RuntimeHybridGroupByOperator(RuntimeOperator::Services& services, 
						       const RuntimeHashGroupByOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mRuntimeContext(new InterpreterContext()),
  mTable(false, NULL),
  mSearchIterator(paged_hash_table::probe_predicate(opType.mHashFun,
						    opType.mHashKeyEqFun))
{
}

RuntimeHybridGroupByOperator::~RuntimeHybridGroupByOperator()
{
  delete mRuntimeContext;
}

void RuntimeHybridGroupByOperator::start()
{
  mCurrentAggregate = RecordBuffer(NULL);
  mState = START;
  onEvent(NULL);
}

void RuntimeHybridGroupByOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    // TODO: If this is GROUP ALL then initialize aggregate record here (to
    // make sure we have an output even without inputs).
    while(true) {
      // Read all inputs
      requestRead(0);
      mState = READ;
      return;
    case READ: 
      {
	read(port, mSearchIterator.mQueryPredicate.ProbeThis);

	// TODO: Proper behavior of GROUP ALL on empty
	// input stream.
	if (RecordBuffer::isEOS(mSearchIterator.mQueryPredicate.ProbeThis) || 
	    mCurrentAggregate == RecordBuffer() ||
	    !getHashGroupByType().mSortKeyEqFun->execute(mCurrentAggregate, 
							 mSearchIterator.mQueryPredicate.ProbeThis, 
							 mRuntimeContext)) {
	  // If table is not empty, then we output the accumulated table
	  // TODO: Potentially speed this up by enabling quick check if empty
	  mScanIterator.init(mTable);
	  while(mScanIterator.next(mRuntimeContext)) {
	    // Write the contents of the table
	    requestWrite(0);
	    mState = WRITE;
	    return;
	  case WRITE: 
	    if (getHashGroupByType().mAggregate->getIsTransferIdentity()) {
	      write(port, mScanIterator.value(), false);
	    } else {
	      RecordBuffer out;
	      getHashGroupByType().mAggregate->executeTransfer(mScanIterator.value(),
	  						       out, 
	  						       mRuntimeContext);
	      getHashGroupByType().mAggregateFree.free(mScanIterator.value());
	      write(port, out, false);
	    }
	  }
	  
	  // Clear the table
	  mTable.clear();
	  mCurrentAggregate = RecordBuffer();

	  // All done if EOS, else create a new aggregator and
	  // initialize.
	  if (RecordBuffer::isEOS(mSearchIterator.mQueryPredicate.ProbeThis)) {
	    requestWrite(0);
	    mState = WRITE_EOS;
	    return;
	  case WRITE_EOS:
	    write(port, RecordBuffer(NULL), true);
	    break;
	  }
	}

	// Lookup into current hash table and insert/update
	RecordBuffer agg;
	mTable.find(mSearchIterator, mRuntimeContext);
	if(!mSearchIterator.next(mRuntimeContext)) {
	  // Create a new record and initialize it (using copy semantics).
	  // TODO: It might be possible to use a move here if the group
	  // keys aren't referenced in the aggregate functions
	  getHashGroupByType().mAggregate->executeInit(mSearchIterator.mQueryPredicate.ProbeThis, 
						       agg, 
						       mRuntimeContext);
	  mTable.insert(agg, mSearchIterator);
	  if (mCurrentAggregate == RecordBuffer()) {
	    // Save a reference to first aggregate record
	    // so that we have an easily found copy of the sort keys to 
	    // compare to.
	    mCurrentAggregate = agg;
	  }
	} else {
	  agg = mSearchIterator.value();
	}
	// In place update agg using input.
	getHashGroupByType().mAggregate->executeUpdate(mSearchIterator.mQueryPredicate.ProbeThis, 
						       agg, 
						       mRuntimeContext);
	getHashGroupByType().mFree.free(mSearchIterator.mQueryPredicate.ProbeThis);
	mSearchIterator.mQueryPredicate.ProbeThis = RecordBuffer();
      }
    }
  }
}

void RuntimeHybridGroupByOperator::shutdown()
{
}

RuntimeOperator * RuntimeHashGroupByOperatorType::create(RuntimeOperator::Services & s) const
{
  if (mSortKeyEqFun == NULL) 
    return new RuntimeHashGroupByOperator(s, *this);
  else
    return new RuntimeHybridGroupByOperator(s, *this);
}

RuntimeHashGroupByOperator::RuntimeHashGroupByOperator(RuntimeOperator::Services& services, 
						       const RuntimeHashGroupByOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mRuntimeContext(new InterpreterContext()),
  mTable(false, NULL),
  mSearchIterator(paged_hash_table::probe_predicate(opType.mHashFun,
						    opType.mHashKeyEqFun))
{
}

RuntimeHashGroupByOperator::~RuntimeHashGroupByOperator()
{
  delete mRuntimeContext;
}

void RuntimeHashGroupByOperator::start()
{
  mState = START;
  onEvent(NULL);
}

void RuntimeHashGroupByOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    // TODO: If this is GROUP ALL then initialize aggregate record here (to
    // make sure we have an output even without inputs).
    while(true) {
      // Read all inputs
      requestRead(0);
      mState = READ;
      return;
    case READ: 
      {
	// Lookup
	read(port, mSearchIterator.mQueryPredicate.ProbeThis);
	if (RecordBuffer::isEOS(mSearchIterator.mQueryPredicate.ProbeThis)) break;
	RecordBuffer agg;
	mTable.find(mSearchIterator, mRuntimeContext);
	if(!mSearchIterator.next(mRuntimeContext)) {
	  // Create a new record and initialize it (using copy semantics).
	  // TODO: It might be possible to use a move here if the group
	  // keys aren't referenced in the aggregate functions
	  getHashGroupByType().mAggregate->executeInit(mSearchIterator.mQueryPredicate.ProbeThis, 
						       agg, 
						       mRuntimeContext);
	  mTable.insert(agg, mSearchIterator);
	} else {
	  agg = mSearchIterator.value();
	}
	// In place update agg using input.
	getHashGroupByType().mAggregate->executeUpdate(mSearchIterator.mQueryPredicate.ProbeThis, 
						       agg, 
						       mRuntimeContext);
	getHashGroupByType().mFree.free(mSearchIterator.mQueryPredicate.ProbeThis);
	mSearchIterator.mQueryPredicate.ProbeThis = RecordBuffer();
      }
    }
    mScanIterator.init(mTable);
    while(mScanIterator.next(mRuntimeContext)) {
      // Write the contents of the table
      requestWrite(0);
      mState = WRITE;
      return;
    case WRITE: 
      if (getHashGroupByType().mAggregate->getIsTransferIdentity()) {
	write(port, mScanIterator.value(), false);
      } else {
	RecordBuffer out;
	getHashGroupByType().mAggregate->executeTransfer(mScanIterator.value(),
							 out, 
							 mRuntimeContext);
	getHashGroupByType().mAggregateFree.free(mScanIterator.value());
	write(port, out, false);
      }
    }
    
    requestWrite(0);
    mState = WRITE_EOS;
    return;
  case WRITE_EOS:
    write(port, RecordBuffer(NULL), true);
  }
}

void RuntimeHashGroupByOperator::shutdown()
{
}

RuntimeOperator * RuntimeSortGroupByOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeSortGroupByOperator(s, *this);
}

RuntimeSortGroupByOperator::RuntimeSortGroupByOperator(RuntimeOperator::Services& services, 
						       const RuntimeSortGroupByOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mRuntimeContext(new InterpreterContext())
{
}

RuntimeSortGroupByOperator::~RuntimeSortGroupByOperator()
{
  delete mRuntimeContext;
}

void RuntimeSortGroupByOperator::start()
{
  mCurrentAggregate = RecordBuffer(NULL);
  mState = START;
  onEvent(NULL);
}

void RuntimeSortGroupByOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    // TODO: If this is GROUP ALL then initialize aggregate record here (to
    // make sure we have an output even without inputs).
    while(true) {
      // Read all inputs
      requestRead(0);
      mState = READ;
      return;
    case READ: 
      {
	read(port, mInput);

	// TODO: Proper behavior of GROUP ALL on empty
	// input stream.
	if (RecordBuffer::isEOS(mInput) || 
	    mCurrentAggregate == RecordBuffer() ||
	    !getSortGroupByType().mEqFun->execute(mCurrentAggregate, mInput, mRuntimeContext)) {
	  // If table is not empty, then we output the accumulated record
	  if (mCurrentAggregate != RecordBuffer()) {
	    requestWrite(0);
	    mState = WRITE;
	    return;
	  case WRITE:
	    if (getSortGroupByType().mAggregate->getIsTransferIdentity()) {
	      write(port, mCurrentAggregate, false);
	      mCurrentAggregate=RecordBuffer();
	    } else {
	      RecordBuffer out;
	      getSortGroupByType().mAggregate->executeTransfer(mCurrentAggregate,
							       out, 
							       mRuntimeContext);
	      getSortGroupByType().mAggregateFree.free(mCurrentAggregate);
	      mCurrentAggregate=RecordBuffer();
	      write(port, out, false);
	    }
	  }

	  // All done if EOS, else create a new aggregator and
	  // initialize.
	  if (RecordBuffer::isEOS(mInput)) {
	    requestWrite(0);
	    mState = WRITE_EOS;
	    return;
	  case WRITE_EOS:
	    write(port, RecordBuffer(NULL), true);
	    break;
	  } else {
	    // Create a new record and initialize it (using copy semantics).
	    // TODO: It might be possible to use a move here if the group
	    // keys aren't referenced in the aggregate functions
	    // TODO: We are using a map because we actually want to use
	    // two different sets of hash/equality: one to probe with the
	    // input and one set to insert the aggregate record.
	    mCurrentAggregate = RecordBuffer::create();
	    getSortGroupByType().mAggregate->executeInit(mInput, 
							 mCurrentAggregate, 
							 mRuntimeContext);
	    getSortGroupByType().mAggregate->executeUpdate(mInput, 
							   mCurrentAggregate, 
							   mRuntimeContext);
	  }
	} else {
	  // In place update agg using input.
	  BOOST_ASSERT(mInput != RecordBuffer(NULL));
	  BOOST_ASSERT(mCurrentAggregate != RecordBuffer(NULL));
	  getSortGroupByType().mAggregate->executeUpdate(mInput, 
							 mCurrentAggregate, 
							 mRuntimeContext);
	}
	if (mInput != RecordBuffer()) {
	  getSortGroupByType().mFree.free(mInput);
	  mInput = RecordBuffer();
	}
      }
    }
  }
}

void RuntimeSortGroupByOperator::shutdown()
{
}

RuntimeOperator * RuntimeSortRunningTotalOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeSortRunningTotalOperator(s, *this);
}

RuntimeSortRunningTotalOperator::RuntimeSortRunningTotalOperator(RuntimeOperator::Services& services, 
								 const RuntimeSortRunningTotalOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mRuntimeContext(new InterpreterContext())
{
}

RuntimeSortRunningTotalOperator::~RuntimeSortRunningTotalOperator()
{
  delete mRuntimeContext;
}

void RuntimeSortRunningTotalOperator::start()
{
  mCurrentAggregate = RecordBuffer(NULL);
  mState = START;
  onEvent(NULL);
}

void RuntimeSortRunningTotalOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    // TODO: If this is GROUP ALL then initialize aggregate record here (to
    // make sure we have an output even without inputs).
    while(true) {
      // Read all inputs
      requestRead(0);
      mState = READ;
      return;
    case READ: 
      {
	read(port, mInput);

	// TODO: Proper behavior of GROUP ALL on empty
	// input stream.
	if (RecordBuffer::isEOS(mInput) || 
	    mCurrentAggregate == RecordBuffer() ||
	    !getSortRunningTotalType().mEqFun->execute(mCurrentAggregate, mInput, mRuntimeContext)) {
	  // If table is not empty, then we output the accumulated record
	  if (mCurrentAggregate != RecordBuffer()) {
	      getSortRunningTotalType().mAggregateFree.free(mCurrentAggregate);
	      mCurrentAggregate=RecordBuffer();
	  }

	  // All done if EOS, else create a new aggregator and
	  // initialize.
	  if (RecordBuffer::isEOS(mInput)) {
	    requestWrite(0);
	    mState = WRITE_EOS;
	    return;
	  case WRITE_EOS:
	    write(port, RecordBuffer(NULL), true);
	    break;
	  } else {
	    // Create a new record and initialize it (using copy semantics).
	    // TODO: It might be possible to use a move here if the group
	    // keys aren't referenced in the aggregate functions
	    // TODO: We are using a map because we actually want to use
	    // two different sets of hash/equality: one to probe with the
	    // input and one set to insert the aggregate record.
	    mCurrentAggregate = RecordBuffer::create();
	    getSortRunningTotalType().mAggregate->executeInit(mInput, 
							      mCurrentAggregate, 
							      mRuntimeContext);
	  }
	}

	// In place update agg using input.
	BOOST_ASSERT(mInput != RecordBuffer(NULL));
	BOOST_ASSERT(mCurrentAggregate != RecordBuffer(NULL));
	getSortRunningTotalType().mAggregate->executeUpdate(mInput, 
							    mCurrentAggregate, 
							    mRuntimeContext);
	
	requestWrite(0);
	mState = WRITE;
	return;
      case WRITE:
	{
	  RecordBuffer out;
	  getSortRunningTotalType().mAggregate->executeTransfer(mInput,
								mCurrentAggregate,
								out, 
								mRuntimeContext);
	  write(port, out, false);
	  if (mInput != RecordBuffer()) {
	    getSortRunningTotalType().mFree.free(mInput);
	    mInput = RecordBuffer();
	  }
	}
      }
    }
  }
}

void RuntimeSortRunningTotalOperator::shutdown()
{
}

paged_hash_table::paged_hash_table(bool ownTableData, const IQLFunctionModule * tableHash)
  :
  mOwnTableData(ownTableData),
  mNumBuckets(8),
  mBuckets(NULL),
  mTableHash(tableHash ? tableHash->getRawFunction() : NULL),
  mLoadFactor(1.0*PageEntries),
  mSize(0)
{
  mBuckets = new bucket_page [mNumBuckets];
}

paged_hash_table::~paged_hash_table()
{
  clear();
  delete [] mBuckets;
}

void paged_hash_table::grow(double growthFactor)
{

  if (growthFactor < 1.25) {
    growthFactor = 1.25;
  }
  int32_t newNumBuckets = (int32_t) (mNumBuckets*growthFactor);
  bucket_page * newBuckets = new bucket_page[newNumBuckets];

  // Iterate existing buckets and reinsert.
  // Think about cache sensitivity here (batching
  // inserts somehow).
  int64_t sz=0;
  bucket_page * basePage = mBuckets;
  bucket_page * end = mBuckets + mNumBuckets;
  for(;
      basePage != end;
      ++basePage) {
    // Walk the chain of buckets.  Free pages as necessary.
    for(bucket_page * p = basePage; p != NULL; ) {
      // Walk each page of the bucket list
      uint32_t * hashPtr = &p->Hash[0];
      for(; *hashPtr != 0; ++hashPtr) {
	uint32_t bucket = (*hashPtr) % newNumBuckets;
	bucket_page * newPage = newBuckets + bucket;
	RecordBuffer val = p->Value[hashPtr - &p->Hash[0]];
	insert(newPage, val, *hashPtr);
	sz += 1;
      }
      bucket_page * nxt = hashPtr == &p->Hash[Sentinel] ? p->Next : NULL;
      if (p != basePage) {
	delete p;
      }
      p = nxt;
    } 
  }

  if (sz != mSize) {
    throw std::runtime_error("Internal Error in paged_hash_table::grow");
  }
  
  delete [] mBuckets;
  mNumBuckets = newNumBuckets;
  mBuckets = newBuckets;
}

void paged_hash_table::clear()
{
  bucket_page * p = mBuckets;
  bucket_page * end = mBuckets + mNumBuckets;
  for(; p != end; ++p) {
    // Mark base page as empty
    memset(&p->Hash[0], 0, sizeof(p->Hash));
    // Free chained pages
    for(bucket_page * it = p->Next; 
	it != NULL; ) {
      bucket_page * tmp = it;
      it = it->Next;
      delete tmp;
    }
    // Clear page list
    p->Next = NULL;
  }  
  mSize = 0;
}

void paged_hash_table::insert(bucket_page * p,
			      RecordBuffer tableInput,
			      uint32_t h)
{
  while(p->Next) {
    p = p->Next;
  }
  uint32_t * hashPtr=NULL;
  for(hashPtr = &p->Hash[0]; *hashPtr != 0; ++hashPtr) {
  }
  
  // We are now at the end of the page.  It is either full or not.
  if (hashPtr == &p->Hash[Sentinel]) {
    bucket_page * tmp = new bucket_page(h, tableInput);
    p->Next = tmp;    
  } else {
    *hashPtr = h;
    p->Value[hashPtr - &p->Hash[0]] = tableInput;
  }
}

void paged_hash_table::insert(RecordBuffer tableInput, InterpreterContext * ctxt)
{
  // Do we need to grow?
  if (mSize >= mLoadFactor * mNumBuckets) {
    grow(2.0);
  }
  // Bind our functions to this input
  mTableHash.InsertThis = tableInput;
  // Calculate full and mod hashes.
  uint32_t h = mTableHash.hash(ctxt);
  // Disallow 0 since it serves as sentinel.
  if (h==0) h = 0xffffffff;
  // Locate bucket
  uint32_t bucket = h % mNumBuckets;
  // Scan to the end of the last bucket.
  // TODO: Better to have a circular list 
  // to make this constant time?
  // Note that this would mean an extra
  // page read when probing (based on my
  // current understanding) so is probably
  // not worthwhile.
  bucket_page * p = &mBuckets[bucket];
  insert(p, tableInput, h);
  mSize += 1;
}

void paged_hash_table::insert(RecordBuffer tableInput, 
			      paged_hash_table::query_iterator<paged_hash_table::probe_predicate> & it)
{
  uint32_t h = it.mQueryHashValue;
  
  // We are at the end of a full page or partial page.
  // The iterator actually lags by 1 so if we are at the
  // end of a partial page then it.next_page() actually points
  // to the last bucket page in the bucket.  If we wound up
  // at the end of a full page, it.next_page() will be NULL.
  if (it.next_page() == NULL) {
    bucket_page * newPage = new bucket_page(h, tableInput);
    it.link(newPage);
  } else {
    it.set(h, tableInput);
  }
  mSize += 1;

  // Do we need to grow?
  if (mSize > mLoadFactor * mNumBuckets) {
    grow(2.0);
  }
}

void paged_hash_table::find(paged_hash_table::query_iterator<paged_hash_table::probe_predicate> & it,
			    InterpreterContext * ctxt)
{
  uint32_t h = it.mQueryPredicate.hash(ctxt);
  // Disallow 0 since it serves as sentinel.
  if (h==0) h = 0xffffffff;
  // Locate bucket
  uint32_t bucket = h % mNumBuckets;
  // Initialize the iterator to the first bucket
  it.init(mBuckets+bucket, h);
}

HashJoin::HashJoin(HashJoin::JoinType joinType)
  :
  LogicalOperator(2,2,1,1),
  mTableInput(NULL),
  mProbeInput(NULL),
  mTableHash(NULL),
  mProbeHash(NULL),
  mEq(NULL),
  mTransfer(NULL),
  mSemiJoinTransfer(NULL),
  mProbeMakeNullableTransfer(NULL),
  mTableMakeNullableTransfer(NULL),  
  mJoinType(joinType),
  mJoinOne(false)
{
}

void HashJoin::check(PlanCheckContext& ctxt)
{
  std::vector<std::string> probeKeys;
  std::vector<std::string> tableKeys;
  std::string residual;
  std::string transfer;

  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "probeKey")) {
      probeKeys.push_back(boost::get<std::string>(it->Value));
    } else if (boost::algorithm::iequals(it->Name, "tableKey")) {
      tableKeys.push_back(boost::get<std::string>(it->Value));
    } else if (boost::algorithm::iequals(it->Name, "residual") ||
	       boost::algorithm::iequals(it->Name, "where")) {
      residual = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "output")) {
      transfer = boost::get<std::string>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }
  mTableInput = getInput(TABLE_PORT)->getRecordType();
  mProbeInput = getInput(PROBE_PORT)->getRecordType();

  init(ctxt, tableKeys, probeKeys, residual, transfer);

  getOutput(0)->setRecordType(mTransfer ? 
			      mTransfer->getTarget() :
			      mSemiJoinTransfer->getTarget());
}

void HashJoin::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = create();
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  plan.mapInputPort(this, 1, opType, 1);  
  plan.mapOutputPort(this, 0, opType, 0);  
}

HashJoin::HashJoin(DynamicRecordContext & ctxt,
		   const RecordType * tableInput,
		   const RecordType * probeInput,
		   const std::string& tableKey,
		   const std::string& probeKey,
		   const std::string& residual,
		   const std::string& transfer,
		   bool joinOne
		   )
  :
  mTableInput(tableInput),
  mProbeInput(probeInput),
  mTableHash(NULL),
  mProbeHash(NULL),
  mEq(NULL),
  mTransfer(NULL),
  mSemiJoinTransfer(NULL),
  mProbeMakeNullableTransfer(NULL),
  mTableMakeNullableTransfer(NULL),  
  mJoinType(INNER),
  mJoinOne(joinOne)
{
  std::vector<std::string> tableKeys;
  std::vector<std::string> probeKeys;
  if (tableKey.size()) tableKeys.push_back(tableKey);
  if (probeKey.size()) probeKeys.push_back(probeKey);
  init(ctxt, tableKeys, probeKeys, residual, transfer);
}

HashJoin::HashJoin(DynamicRecordContext & ctxt,
		   const RecordType * tableInput,
		   const RecordType * probeInput,
		   const std::vector<std::string>& tableKeys,
		   const std::vector<std::string>& probeKeys,
		   const std::string& residual,
		   const std::string& transfer,
		   bool joinOne
		   )
  :
  mTableInput(tableInput),
  mProbeInput(probeInput),
  mTableHash(NULL),
  mProbeHash(NULL),
  mEq(NULL),
  mTransfer(NULL),
  mSemiJoinTransfer(NULL),
  mProbeMakeNullableTransfer(NULL),
  mTableMakeNullableTransfer(NULL),  
  mJoinType(INNER),
  mJoinOne(joinOne)
{
  init(ctxt, tableKeys, probeKeys, residual, transfer);
}

HashJoin::~HashJoin()
{
  delete mTableHash;
  delete mProbeHash;
  delete mEq;
  delete mTransfer;
  delete mSemiJoinTransfer;
  delete mProbeMakeNullableTransfer;
  delete mTableMakeNullableTransfer; 
}

void HashJoin::init(DynamicRecordContext & ctxt,
		    const std::vector<std::string>& tableKeys,
		    const std::vector<std::string>& probeKeys,
		    const std::string& residual,
		    const std::string& transfer)
{
  TypeCheckContext typeCheckCtxt(ctxt);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  std::vector<const RecordType *> tableOnly;
  tableOnly.push_back(mTableInput);
  tableOnly.push_back(&emptyTy);
  std::vector<const RecordType *> probeOnly;
  probeOnly.push_back(mProbeInput);
  probeOnly.push_back(&emptyTy);
  std::vector<AliasedRecordType> tableAndProbe;
  tableAndProbe.push_back(AliasedRecordType("table", mTableInput));
  tableAndProbe.push_back(AliasedRecordType("probe", mProbeInput));
  // Handle case of a cross join
  if (tableKeys.size()) {
    if(tableKeys.size() != probeKeys.size()) {
      throw std::runtime_error("Number of table keys and probe keys in hash join must be same");
    }
    std::string eq;
    std::string tableKey;
    std::string probeKey;
    for(std::size_t i=0; i<tableKeys.size(); i++) {
      if (i>0) {
	tableKey += ",";
	probeKey += ",";
	eq += " AND ";
      }
      // Value of hash functions are type dependent so we must
      // be cognizant of implicit type conversions.
      const FieldType * probeType = mProbeInput->getMember(probeKeys[i]).GetType();
      const FieldType * tableType = mTableInput->getMember(tableKeys[i]).GetType();
      const FieldType * targetType = 
	typeCheckCtxt.leastCommonTypeNullable(probeType, tableType);
      if (targetType->GetEnum() != tableType->GetEnum()) {
	tableKey += (boost::format("CAST(%1% AS %2%)") % tableKeys[i] %
		     targetType->toString()).str();
      } else {
	tableKey += tableKeys[i];
      }
      if (targetType->GetEnum() != probeType->GetEnum()) {
	probeKey += (boost::format("CAST(%1% AS %2%)") % probeKeys[i] %
		     targetType->toString()).str();
      } else {
	probeKey += probeKeys[i];
      }
      eq += (boost::format("table.%1% = probe.%2%") % tableKeys[i] % 
	     probeKeys[i]).str();
    }
    mTableHash = new RecordTypeFunction(ctxt, "tableHash", tableOnly, (boost::format("#(%1%)") % tableKey).str());
    mProbeHash = new RecordTypeFunction(ctxt, "probeHash", probeOnly, (boost::format("#(%1%)") % probeKey).str());

    std::string copy(residual);
    if (residual.size()) {
      eq = residual + " AND " + eq;
    }
    //Table is first argument in equals! 
    //So the input rec fmts are table then probe.
    mEq = new RecordTypeFunction(ctxt, "eq", tableAndProbe, eq);
  } else if(residual.size()) {
    // OK to have a cross join with residual.
    mEq = new RecordTypeFunction(ctxt, "eq", tableAndProbe, residual);
  }

  if (mJoinType == FULL_OUTER ||
      mJoinType == LEFT_OUTER ||
      mJoinType==RIGHT_OUTER ||
      mJoinType==INNER) {
    mTableMakeNullableTransfer = SortMergeJoin::makeNullableTransfer(ctxt, mTableInput);
    mProbeMakeNullableTransfer = SortMergeJoin::makeNullableTransfer(ctxt, mProbeInput);
    std::vector<AliasedRecordType> types;
    types.push_back(AliasedRecordType("table", (mJoinType==FULL_OUTER ||
						mJoinType==RIGHT_OUTER) ?
				      mTableMakeNullableTransfer->getTarget() :
				      mTableInput));
    types.push_back(AliasedRecordType("probe", (mJoinType==FULL_OUTER || 
						mJoinType==LEFT_OUTER) ?
				      mProbeMakeNullableTransfer->getTarget() :
				      mProbeInput));
    mTransfer = new RecordTypeTransfer2(ctxt, "makeoutput", types, transfer.size() ? transfer : "table.*, probe.*");
  } else {
    mSemiJoinTransfer = new RecordTypeTransfer(ctxt, "makeoutput", 
					       mProbeInput, 
					       transfer.size() ? transfer : "input.*");
  }
}

RuntimeOperatorType * HashJoin::create() const
{
  if(mTableHash) {
    if (mJoinType == INNER) {
      return new RuntimeHashJoinOperatorType(mTableInput->getFree(),
					     mProbeInput->getFree(),
					     mTableHash,
					     mProbeHash,
					     mEq,
					     mTransfer,
					     mJoinOne);
    } else if (mJoinType == FULL_OUTER || mJoinType == LEFT_OUTER ||
	       mJoinType == RIGHT_OUTER) {
      return new RuntimeHashJoinOperatorType(mJoinType,
					     mTableInput->getFree(),
					     mProbeInput->getFree(),
					     mTableHash,
					     mProbeHash,
					     mEq,
					     mTransfer,
					     mTableMakeNullableTransfer,
					     mProbeMakeNullableTransfer);
    } else {
      return new RuntimeHashJoinOperatorType(mTableInput->getFree(),
					     mProbeInput->getFree(),
					     mTableHash,
					     mProbeHash,
					     mEq,
					     mSemiJoinTransfer,
					     mJoinType);
    }
  } else {
    return new RuntimeCrossJoinOperatorType(mTableInput->getFree(),
					    mProbeInput->getFree(),
					    mEq,
					    mTransfer);
  }
}

RuntimeHashJoinOperatorType::~RuntimeHashJoinOperatorType()
{
  delete mTableHashFun;
  delete mProbeHashFun;
  delete mEqFun;
  delete mTransferModule;
  delete mSemiJoinTransferModule;
  delete mProbeMakeNullableTransferModule;
  delete mTableMakeNullableTransferModule;
}

RuntimeOperator * RuntimeHashJoinOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeHashJoinOperator(s, *this);
}

RuntimeHashJoinOperator::RuntimeHashJoinOperator(RuntimeOperator::Services& services, 
						       const RuntimeHashJoinOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mRuntimeContext(new InterpreterContext()),
  mTable(true, getHashJoinType().mTableHashFun),
  mSearchIterator(paged_hash_table::probe_predicate(getHashJoinType().mProbeHashFun->getRawFunction(),
						    getHashJoinType().mEqFun->getRawFunction()))
{
}

RuntimeHashJoinOperator::~RuntimeHashJoinOperator()
{
  if ((getHashJoinType().mJoinType == HashJoin::FULL_OUTER ||
       getHashJoinType().mJoinType == HashJoin::RIGHT_OUTER) &&
      mNullTableRecord != RecordBuffer()) {
    getHashJoinType().mTableNullFree.free(mNullTableRecord);
  }
  if ((getHashJoinType().mJoinType == HashJoin::FULL_OUTER ||
       getHashJoinType().mJoinType == HashJoin::LEFT_OUTER) &&
      mNullProbeRecord != RecordBuffer()) {
    getHashJoinType().mProbeNullFree.free(mNullProbeRecord);
  }
  delete mRuntimeContext;
}

void RuntimeHashJoinOperator::start()
{
  mState = START;

  if ((getHashJoinType().mJoinType == HashJoin::FULL_OUTER ||
       getHashJoinType().mJoinType == HashJoin::RIGHT_OUTER) &&
      mNullTableRecord == RecordBuffer()) {
    mNullTableRecord = getHashJoinType().mTableNullMalloc.malloc();
  }
  if ((getHashJoinType().mJoinType == HashJoin::FULL_OUTER ||
       getHashJoinType().mJoinType == HashJoin::LEFT_OUTER) &&
      mNullProbeRecord == RecordBuffer()) {
    mNullProbeRecord = getHashJoinType().mProbeNullMalloc.malloc();
  }
  onEvent(NULL);
}

RecordBuffer RuntimeHashJoinOperator::onInner()
{
  RecordBuffer output;
  // Must put this in tmp variable because signature of transfer
  // wants a non-const reference (to support move semantics).
  RecordBuffer tmp = mSearchIterator.value();
  getHashJoinType().mTransferModule->execute(tmp,
					     mSearchIterator.mQueryPredicate.ProbeThis,
					     output,
					     mRuntimeContext,
					     false,
					     false);
  return output;
}

RecordBuffer RuntimeHashJoinOperator::onSemi()
{
  RecordBuffer output;
  if (getHashJoinType().mSemiJoinTransferModule) {
    getHashJoinType().mSemiJoinTransferModule->execute(mSearchIterator.mQueryPredicate.ProbeThis,
						       output,
						       mRuntimeContext,
						       false);
  } else {
    output = mSearchIterator.mQueryPredicate.ProbeThis;
    mSearchIterator.mQueryPredicate.ProbeThis = RecordBuffer();
  }
  return output;
}

RecordBuffer RuntimeHashJoinOperator::onRightOuterMatch()
{
  // std::cout << "RuntimeHashJoinOperator::onRightOuterMatch" << std::endl;
  RecordBuffer output;
  // Must put this in tmp variable because signature of transfer
  // wants a non-const reference (to support move semantics).
  switch (getHashJoinType().mJoinType) {
  case HashJoin::RIGHT_OUTER:
    {
      // Convert to nullable properties on the table.
      RecordBuffer tmp1 = mSearchIterator.value();
      RecordBuffer tmp2;
      getHashJoinType().mTableMakeNullableTransferModule->execute(tmp1,
								  tmp2,
								  mRuntimeContext,
								  false);
  
      getHashJoinType().mTransferModule->execute(tmp2,
						 mSearchIterator.mQueryPredicate.ProbeThis,
						 output,
						 mRuntimeContext,
						 false,
						 false);
      getHashJoinType().mTableNullFree.free(tmp2);
      break;
    }
  case HashJoin::FULL_OUTER:
    {
      // Convert to nullable properties on the table.
      RecordBuffer tmp = mSearchIterator.value();
      RecordBuffer tmp1;
      RecordBuffer tmp2;
      getHashJoinType().mTableMakeNullableTransferModule->execute(tmp,
								  tmp1,
								  mRuntimeContext,
								  false);
      tmp = mSearchIterator.mQueryPredicate.ProbeThis;
      getHashJoinType().mProbeMakeNullableTransferModule->execute(tmp,
								  tmp2,
								  mRuntimeContext,
								  false);
  
      getHashJoinType().mTransferModule->execute(tmp1,
						 tmp2,
						 output,
						 mRuntimeContext,
						 false,
						 false);
      getHashJoinType().mTableNullFree.free(tmp1);
      getHashJoinType().mProbeNullFree.free(tmp2);
      break;
    }
  case HashJoin::LEFT_OUTER:
    {
      // Convert to nullable properties on the probe.
      RecordBuffer tmp = mSearchIterator.mQueryPredicate.ProbeThis;
      RecordBuffer tmp1 = mSearchIterator.value();
      RecordBuffer tmp2;
      getHashJoinType().mProbeMakeNullableTransferModule->execute(tmp,
								  tmp2,
								  mRuntimeContext,
								  false);
  
      getHashJoinType().mTransferModule->execute(tmp1,
						 tmp2,
						 output,
						 mRuntimeContext,
						 false,
						 false);
      getHashJoinType().mProbeNullFree.free(tmp2);
      break;
    }
  default:
    throw std::runtime_error("INTERNAL ERROR : Invalid join type");
  }
  return output;
}

RecordBuffer RuntimeHashJoinOperator::onTableNonMatch(RecordBuffer tableBuf)
{
  // std::cout << "RuntimeHashJoinOperator::onTableNonMatch" << std::endl;
  RecordBuffer output;
  // Must put this in tmp variable because signature of transfer
  // wants a non-const reference (to support move semantics).
  switch (getHashJoinType().mJoinType) {
  case HashJoin::FULL_OUTER:
    {
      // Convert to nullable properties on the table.
      RecordBuffer tmp1;
      getHashJoinType().mTableMakeNullableTransferModule->execute(tableBuf,
								  tmp1,
								  mRuntimeContext,
								  false);
      getHashJoinType().mTransferModule->execute(tmp1,
						 mNullProbeRecord,
						 output,
						 mRuntimeContext,
						 false,
						 false);
      getHashJoinType().mTableNullFree.free(tmp1);
      break;
    }
  case HashJoin::LEFT_OUTER:
    {
      getHashJoinType().mTransferModule->execute(tableBuf,
						 mNullProbeRecord,
						 output,
						 mRuntimeContext,
						 false,
						 false);
      break;
    }
  default:
    throw std::runtime_error("INTERNAL ERROR : Invalid join type");
  }
  return output;
}

RecordBuffer RuntimeHashJoinOperator::onRightOuterNonMatch()
{
  // std::cout << "RuntimeHashJoinOperator::onRightOuterNonMatch" << std::endl;
  RecordBuffer output;
  switch (getHashJoinType().mJoinType) {
  case HashJoin::RIGHT_OUTER:
    {
      getHashJoinType().mTransferModule->execute(mNullTableRecord,
						 mSearchIterator.mQueryPredicate.ProbeThis,
						 output,
						 mRuntimeContext,
						 false,
						 false);
      break;
    }
  case HashJoin::FULL_OUTER:
    {
      // Convert to nullable properties on the probe.
      RecordBuffer tmp = mSearchIterator.mQueryPredicate.ProbeThis;
      RecordBuffer tmp1;
      getHashJoinType().mProbeMakeNullableTransferModule->execute(tmp,
								  tmp1,
								  mRuntimeContext,
								  false);
      getHashJoinType().mTransferModule->execute(mNullTableRecord,
						 tmp1,
						 output,
						 mRuntimeContext,
						 false,
						 false);
      break;
    }
  default:
    throw std::runtime_error("INTERNAL ERROR : Invalid join type");
  }

  return output;
}


void RuntimeHashJoinOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      // Read the entire table input
      requestRead(0);
      mState = READ_TABLE;
      return;
    case READ_TABLE: 
      {
	RecordBuffer input;
	read(port, input);

	// If EOS then move on to output
	// TODO: Proper behavior of GROUP ALL on empty
	// input stream.
	if (RecordBuffer::isEOS(input)) break;
	mTable.insert(input, mRuntimeContext);
      }
    }

    while(true) {
      // Read the probe and lookup
      requestRead(1);
      mState = READ_PROBE;
      return;
    case READ_PROBE: 
      {
	read(port, mSearchIterator.mQueryPredicate.ProbeThis);
	if (RecordBuffer::isEOS(mSearchIterator.mQueryPredicate.ProbeThis)) break;
	// Lookup
	mTable.find(mSearchIterator, mRuntimeContext);
	if (mSearchIterator.next(mRuntimeContext)) {
	  if (getHashJoinType().mJoinType != HashJoin::RIGHT_ANTI_SEMI) {
	    do {
	      requestWrite(0);
	      mState = WRITE;
	      return;
	    case WRITE:
	      if (getHashJoinType().mJoinType == HashJoin::INNER) {
		RecordBuffer output = onInner();
		write(port, output, false);
	      } else if (getHashJoinType().mJoinType == HashJoin::FULL_OUTER ||
			 getHashJoinType().mJoinType == HashJoin::LEFT_OUTER ||
			 getHashJoinType().mJoinType == HashJoin::RIGHT_OUTER) {
		RecordBuffer output = onRightOuterMatch();
		write(port, output, false);
	      } else {
		RecordBuffer output = onSemi();
		write(port, output, false);
		break;
	      }
	      // If we are hard coded to only return 1 hit (e.g. semantic constraint) 
	      // break outta here
	      if (getHashJoinType().mJoinOne) break;
	    } while(mSearchIterator.next(mRuntimeContext));
	  }
	} else if (getHashJoinType().mJoinType == HashJoin::RIGHT_ANTI_SEMI ||
		   getHashJoinType().mJoinType == HashJoin::FULL_OUTER ||
		   getHashJoinType().mJoinType == HashJoin::RIGHT_OUTER) {
	  requestWrite(0);
	  mState = WRITE_UNMATCHED;
	  return;
	case WRITE_UNMATCHED:
	  if (getHashJoinType().mJoinType == HashJoin::RIGHT_ANTI_SEMI) {
	    RecordBuffer output = onSemi();
	    write(port, output, false);
	  } else {
	    RecordBuffer output = onRightOuterNonMatch();
	    write(port, output, false);
	  }
	} 
	if (mSearchIterator.mQueryPredicate.ProbeThis != RecordBuffer())
	  getHashJoinType().mProbeFree.free(mSearchIterator.mQueryPredicate.ProbeThis);
      }
    }

    if (getHashJoinType().mJoinType == HashJoin::FULL_OUTER ||
	getHashJoinType().mJoinType == HashJoin::LEFT_OUTER) {
      // Find any non matchers in the table and output.
      mScanIterator.init(mTable);
      while(mScanIterator.next(mRuntimeContext)) {
	// Write the contents of the table
	requestWrite(0);
	mState = WRITE_TABLE_UNMATCHED;
	return;
      case WRITE_TABLE_UNMATCHED: 
	{
	  RecordBuffer out = onTableNonMatch(mScanIterator.value());
	  write(port, out, false);
	}
      }
    }

    requestWrite(0);
    mState = WRITE_EOS;
    return;
  case WRITE_EOS:
    write(port, RecordBuffer(NULL), true);
  }
}

void RuntimeHashJoinOperator::shutdown()
{
}


RuntimeCrossJoinOperatorType::~RuntimeCrossJoinOperatorType()
{
  delete mEqFun;
  delete mTransferModule;
}

RuntimeOperator * RuntimeCrossJoinOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeCrossJoinOperator(s, *this);
}

RuntimeCrossJoinOperator::RuntimeCrossJoinOperator(RuntimeOperator::Services& services, 
						       const RuntimeCrossJoinOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mRuntimeContext(new InterpreterContext())
{
}

RuntimeCrossJoinOperator::~RuntimeCrossJoinOperator()
{
  delete mRuntimeContext;
}

void RuntimeCrossJoinOperator::start()
{
  mState = START;
  onEvent(NULL);
}

void RuntimeCrossJoinOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      // Read the entire table input
      requestRead(0);
      mState = READ_TABLE;
      return;
    case READ_TABLE: 
      {
	RecordBuffer input;
	read(port, input);

	if (RecordBuffer::isEOS(input)) break;
	mTable.push_back(input);
      }
    }

    while(true) {
      // Read the probe and lookup
      requestRead(1);
      mState = READ_PROBE;
      return;
    case READ_PROBE: 
      {
	read(port, mProbe);
	if (RecordBuffer::isEOS(mProbe)) break;
	for(mScanIterator = mTable.begin();
	    mScanIterator != mTable.end();
	    ++mScanIterator) {
	  if (getCrossJoinType().mEqFun == NULL ||
	      getCrossJoinType().mEqFun->execute(*mScanIterator, mProbe, mRuntimeContext)) {
	    requestWrite(0);
	    mState = WRITE;
	    return;
	  case WRITE:
	    {
	      // Must put this in tmp variable because signature of transfer
	      // wants a non-const reference (to support move semantics).
	      RecordBuffer output;
	      getCrossJoinType().mTransferModule->execute(*mScanIterator,
							  mProbe,
							  output,
							  mRuntimeContext,
							  false,
							  false);
	      write(port, output, false);
	    }
	  }
	}
	getCrossJoinType().mProbeFree.free(mProbe);
      }
    }
    
    requestWrite(0);
    mState = WRITE_EOS;
    return;
  case WRITE_EOS:
    write(port, RecordBuffer(NULL), true);
    {
      const RecordTypeFree & f(getCrossJoinType().mTableFree);
      // Free records in table
      for(mScanIterator = mTable.begin(); 
	  mScanIterator != mTable.end();
	  ++mScanIterator) {
	f.free(*mScanIterator);
      }
    }
  }
}

void RuntimeCrossJoinOperator::shutdown()
{
}


RuntimeHashPartitionerOperatorType::~RuntimeHashPartitionerOperatorType()
{
  delete mHashFun;
}
  
RuntimeOperator * RuntimeHashPartitionerOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeHashPartitionerOperator(s, *this);
}

RuntimeHashPartitionerOperator::RuntimeHashPartitionerOperator(RuntimeOperator::Services& services, 
						       const RuntimeHashPartitionerOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeHashPartitionerOperatorType>(services, opType),
  mState(START),
  mRuntimeContext(new InterpreterContext())
{
}

RuntimeHashPartitionerOperator::~RuntimeHashPartitionerOperator()
{
  delete mRuntimeContext;
}

void RuntimeHashPartitionerOperator::start()
{
  mState = START;
  onEvent(NULL);
}

void RuntimeHashPartitionerOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ: 
      read(port, mBuffer);
      if (RecordBuffer::isEOS(mBuffer)) break;
      {
	int32_t h = getMyOperatorType().mHashFun->execute(mBuffer, NULL, mRuntimeContext);
	requestWrite(h % getOutputPorts().size());
      }
      mState = WRITE;
      return;
    case WRITE:
      write(port, mBuffer, false);
      mBuffer = NULL;
    }
    for(mOutputIt = output_port_begin();
	mOutputIt != output_port_end();
	++mOutputIt) {
      requestWrite(mOutputIt - output_port_begin());
      mState = WRITE_EOS;
      return;
    case WRITE_EOS:
      write(port, RecordBuffer(NULL), true);
    }
  }
}

void RuntimeHashPartitionerOperator::shutdown()
{
}

RuntimeBroadcastPartitionerOperatorType::~RuntimeBroadcastPartitionerOperatorType()
{
  delete mTransfer;
}
  
RuntimeOperator * RuntimeBroadcastPartitionerOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeBroadcastPartitionerOperator(s, *this);
}

RuntimeBroadcastPartitionerOperator::RuntimeBroadcastPartitionerOperator(RuntimeOperator::Services& services, 
						       const RuntimeBroadcastPartitionerOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeBroadcastPartitionerOperatorType>(services, opType),
  mState(START),
  mRuntimeContext(new InterpreterContext())
{
}

RuntimeBroadcastPartitionerOperator::~RuntimeBroadcastPartitionerOperator()
{
  delete mRuntimeContext;
}

void RuntimeBroadcastPartitionerOperator::start()
{
  mState = START;
  onEvent(NULL);
}

void RuntimeBroadcastPartitionerOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      read(port, mInput);
      if (RecordBuffer::isEOS(mInput))
	break;
      for(mOutputIt = output_port_begin(); 
	  mOutputIt != output_port_end();
	  ++mOutputIt) {
	requestWrite(**mOutputIt);
	mState = WRITE;
	return;
      case WRITE:
	if (mOutputIt+1 != output_port_end()) {
	  RecordBuffer output;
	  getMyOperatorType().mTransfer->execute(mInput, 
						 output, 
						 mRuntimeContext, 
						 false);
	  write(port, output, false);
	} else {
	  write(port, mInput, false);
	  mInput = RecordBuffer();
	}
      }
    }
    // Close up shop on all downstream ports.
    for(mOutputIt = output_port_begin(); 
	mOutputIt != output_port_end();
	++mOutputIt) {
      requestWrite(**mOutputIt);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer(NULL), true);
    }
  }
}

void RuntimeBroadcastPartitionerOperator::shutdown()
{
}

template <class OpType>
class RuntimeConcatenationOperator : public RuntimeOperator
{
private:
  enum State { START, READ, WRITE, WRITE_EOS };
  State mState;
  RecordBuffer mBuffer;
  input_port_iterator mActivePort;
public:
  RuntimeConcatenationOperator(RuntimeOperator::Services& services, const OpType& opType);
  ~RuntimeConcatenationOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

template <class OpType>
RuntimeConcatenationOperator<OpType>::RuntimeConcatenationOperator(RuntimeOperator::Services& services, 
										   const OpType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START)
{
}

template <class OpType>
RuntimeConcatenationOperator<OpType>::~RuntimeConcatenationOperator()
{
}

template <class OpType>
void RuntimeConcatenationOperator<OpType>::start()
{
  mState = START;
  onEvent(NULL);
}

template <class OpType>
void RuntimeConcatenationOperator<OpType>::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    for(mActivePort = input_port_begin();
	mActivePort != input_port_end();
	++mActivePort) {
      while(true) {
	requestRead(**mActivePort);
	mState = READ;
	return;
      case READ: 
	read(port, mBuffer);
	if (RecordBuffer::isEOS(mBuffer)) {
	  // Done with this port.
	  break;
	} else {
	  requestWrite(0);
	  mState = WRITE;
	  return;
	case WRITE:
	  write(port, mBuffer, false);
	  mBuffer = NULL;
	}
      }
    }
    requestWrite(0);
    mState = WRITE_EOS;
    return;
  case WRITE_EOS:
    write(port, RecordBuffer(NULL), true);
  }
}

template <class OpType>
void RuntimeConcatenationOperator<OpType>::shutdown()
{
}

template <class OpType>
class RuntimeNondeterministicCollectorOperator : public RuntimeOperator
{
private:
  enum State { START, READ, WRITE, WRITE_EOS };
  State mState;
  RecordBuffer mBuffer;
  RuntimePort * mActivePorts;
public:
  RuntimeNondeterministicCollectorOperator(RuntimeOperator::Services& services, const OpType& opType);
  ~RuntimeNondeterministicCollectorOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

template <class OpType>
RuntimeNondeterministicCollectorOperator<OpType>::RuntimeNondeterministicCollectorOperator(RuntimeOperator::Services& services, 
										   const OpType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mActivePorts(NULL)
{
}

template <class OpType>
RuntimeNondeterministicCollectorOperator<OpType>::~RuntimeNondeterministicCollectorOperator()
{
}

template <class OpType>
void RuntimeNondeterministicCollectorOperator<OpType>::start()
{
  // TODO: A little more attention to the read policy here.
  // Link all ports into the active list.
  mActivePorts = getInputPorts()[0];
  for(input_port_iterator it = input_port_begin();
      it != input_port_end();
      ++it) {
    mActivePorts->request_link_after(**it);
  }

  mState = START;
  onEvent(NULL);
}

template <class OpType>
void RuntimeNondeterministicCollectorOperator<OpType>::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(mActivePorts) {
      requestRead(*mActivePorts);
      mState = READ;
      return;
    case READ: 
      read(port, mBuffer);
      if (RecordBuffer::isEOS(mBuffer)) {
	// Remove from out list of active ports.
	mActivePorts = port->request_unlink();
      } else {
	requestWrite(0);
	mState = WRITE;
	return;
      case WRITE:
	write(port, mBuffer, false);
	mBuffer = NULL;
      }
    }
    requestWrite(0);
    mState = WRITE_EOS;
    return;
  case WRITE_EOS:
    write(port, RecordBuffer(NULL), true);
  }
}

template <class OpType>
void RuntimeNondeterministicCollectorOperator<OpType>::shutdown()
{
}

RuntimeNondeterministicCollectorOperatorType::~RuntimeNondeterministicCollectorOperatorType()
{
}
  
RuntimeOperator * RuntimeNondeterministicCollectorOperatorType::create(RuntimeOperator::Services & s) const
{
  typedef RuntimeNondeterministicCollectorOperator<RuntimeNondeterministicCollectorOperatorType> op_type;
  return new op_type(s, *this);
}

RecordTypeFunction * SortMergeJoin::createCompareFunction(DynamicRecordContext& ctxt,
							  const std::vector<std::string>& leftFields,
							  const RecordType * leftInputType,
							  const std::vector<std::string>& rightFields,
							  const RecordType * rightInputType,
							  const std::string& op)
{
  std::vector<const RecordType *> eqTypes;
  eqTypes.push_back(leftInputType);
  eqTypes.push_back(rightInputType);
  std::string eqPred;
  for(std::size_t i=0; i<leftFields.size(); i++) {
    if (eqPred.size() > 0) eqPred += " AND ";
    eqPred += (boost::format("input0.%1% %3% input1.%2%") % leftFields[i] % rightFields[i] % op).str();
  }

  return new RecordTypeFunction(ctxt, "xfer5eq", 
				eqTypes, 
				eqPred);
}

RecordTypeFunction * SortMergeJoin::createMemcmpFunction(DynamicRecordContext& ctxt,
							 const std::vector<std::string>& leftFields,
							 const RecordType * leftInputType,
							 const std::vector<std::string>& rightFields,
							 const RecordType * rightInputType)
{
  std::vector<const RecordType *> eqTypes;
  eqTypes.push_back(leftInputType);
  eqTypes.push_back(rightInputType);
  // This is kinda hacky.  When we compare two records to each other,
  // we need to refer to one field from each record.  Since we do this
  // by name, we generate a temporary prefix.
  std::string eqPred;
  for(std::size_t i=0; i<leftFields.size(); i++) {
    const RecordMember& leftMember(leftInputType->getMember(leftFields[i]));
    const RecordMember& rightMember(rightInputType->getMember(rightFields[i]));
    std::string leftNullability = leftMember.GetType()->isNullable() ?
      (boost::format("input0.%1% IS NULL OR ") % leftFields[i]).str() : "";
    std::string rightNullability = rightMember.GetType()->isNullable() ?
      (boost::format("input1.%1% IS NULL OR ") % rightFields[i]).str() : "";
    eqPred += (boost::format("CASE WHEN %3%input0.%1% < input1.%2% THEN -1 ELSE "
			     "CASE WHEN %4%input0.%1% > input1.%2% THEN 1 ELSE ") % 
	       leftFields[i] % rightFields[i] % 
	       (QueryOptions::nullsSortLow() ? leftNullability : rightNullability) % 
	       (QueryOptions::nullsSortLow() ? rightNullability : leftNullability)).str();
  }
  // Terminate with a 0 : this is equals
  eqPred += "0 END END";
  for(std::size_t i=1; i<leftFields.size(); i++) {
    eqPred += " END END";
  }

  return new RecordTypeFunction(ctxt, "xfer5eq", 
				eqTypes, 
				eqPred);
}

RecordTypeTransfer * 
SortMergeJoin::makeNullableTransfer(DynamicRecordContext& ctxt,
				    const RecordType * input)
{
  std::string makeNullableXfer;
  for(RecordType::const_member_iterator m = input->begin_members(),
	e = input->end_members(); m != e; ++m) {
    if (makeNullableXfer.size()) {
      makeNullableXfer += ",";
    }
    if (m->GetType()->isNullable()) {
      makeNullableXfer += m->GetName();
    } else {
      makeNullableXfer += (boost::format("CASE WHEN 1=1 THEN %1% END AS %1%") %
			   m->GetName()).str();
    }
  }
  return new RecordTypeTransfer(ctxt, 
				"makenullable", 
				input,
				makeNullableXfer);
}

bool SortMergeJoin::isInnerOrOuter(JoinType joinType)
{
  return joinType==FULL_OUTER ||
    joinType==LEFT_OUTER ||
    joinType==RIGHT_OUTER ||
    joinType==INNER;
}

SortMergeJoin::SortMergeJoin(SortMergeJoin::JoinType joinType)
  :
  LogicalOperator(2,2,1,1),
  mJoinType(joinType),
  mLeftInput(NULL),
  mRightInput(NULL),
  mLeftKeyCompare(NULL),
  mRightKeyCompare(NULL),
  mLeftRightKeyCompare(NULL),
  mResidual(NULL),
  mMatchTransfer(NULL),
  mLeftMakeNullableTransfer(NULL),
  mRightMakeNullableTransfer(NULL)
{
}

SortMergeJoin::SortMergeJoin(DynamicRecordContext & ctxt,
			     SortMergeJoin::JoinType joinType,
			     const RecordType * leftInput,
			     const RecordType * rightInput,
			     const std::vector<std::string>& leftKeys,
			     const std::vector<std::string>& rightKeys,
			     const std::string& residual,
			     const std::string& matchTransfer)
  :
  LogicalOperator(2,2,1,1),
  mJoinType(joinType),
  mLeftInput(leftInput),
  mRightInput(rightInput),
  mLeftKeyCompare(NULL),
  mRightKeyCompare(NULL),
  mLeftRightKeyCompare(NULL),
  mResidual(NULL),
  mMatchTransfer(NULL),
  mLeftMakeNullableTransfer(NULL),
  mRightMakeNullableTransfer(NULL)
{
  init(ctxt, leftKeys, rightKeys, residual, matchTransfer);
}

void SortMergeJoin::init(DynamicRecordContext & ctxt,
			 const std::vector<std::string>& leftKeys,
			 const std::vector<std::string>& rightKeys,
			 const std::string& residual,
			 const std::string& matchTransfer)
{
  // In the right outer case the left inputs
  // may need to be coerced to be nullable.
  // The current approach is rather inefficient
  // and makes an extra copy of the left to make
  // it nullable and the does the final transfer
  // from the temporary.
  // TODO: Figure out a way of composing multiple
  // transfers within a single LLVM Module (e.g.
  // stack allocate the temporary record type). 
  // Then let the LLVM inliner elimate unnecessary
  // data movement????
  if (isInnerOrOuter(mJoinType)) {
    mLeftMakeNullableTransfer = makeNullableTransfer(ctxt, mLeftInput);
    mRightMakeNullableTransfer = makeNullableTransfer(ctxt, mRightInput);
  }

  // Create functions for testing equality of left and right keys.
  mLeftKeyCompare = createEqualityFunction(ctxt, leftKeys, mLeftInput);
  mRightKeyCompare = createEqualityFunction(ctxt, rightKeys, mRightInput);
  mLeftRightKeyCompare = createMemcmpFunction(ctxt, 
					      leftKeys, mLeftInput, 
					      rightKeys, mRightInput);
  // Create the residual predicate
  if (residual.size()) {
    std::vector<AliasedRecordType> residualTypes;
    residualTypes.push_back(AliasedRecordType("l", mLeftInput));
    residualTypes.push_back(AliasedRecordType("r", mRightInput));
    mResidual = new RecordTypeFunction(ctxt, "eqpred", residualTypes, residual);
  }

  if (isInnerOrOuter(mJoinType)) {
    std::vector<AliasedRecordType> aliasedTypes;
    aliasedTypes.push_back(AliasedRecordType("l", (mJoinType==FULL_OUTER || 
						   mJoinType==RIGHT_OUTER) ?
					     mLeftMakeNullableTransfer->getTarget() :
					     mLeftInput));
    aliasedTypes.push_back(AliasedRecordType("r", (mJoinType==FULL_OUTER || 
						   mJoinType==LEFT_OUTER) ?
					     mRightMakeNullableTransfer->getTarget() :
					     mRightInput));
    mMatchTransfer = new RecordTypeTransfer2(ctxt, "onmatch", 
					     aliasedTypes, 
					     matchTransfer.size() ?
					     matchTransfer :
					     "l.*, r.*");
  } else {
    mLeftMakeNullableTransfer = new RecordTypeTransfer(ctxt, "onmatch",
						   mRightInput,
						   matchTransfer.size() ?
						   matchTransfer :
						   "input.*");
    // Not used at runtime but necessary for creating the runtime operator type.
    mRightMakeNullableTransfer = makeNullableTransfer(ctxt, mRightInput);
  }
}

SortMergeJoin::~SortMergeJoin()
{
  delete mLeftKeyCompare;
  delete mRightKeyCompare;
  delete mLeftRightKeyCompare;
  delete mResidual;
  delete mMatchTransfer;
  delete mLeftMakeNullableTransfer;
  delete mRightMakeNullableTransfer;
}

const RecordType * SortMergeJoin::getOutputType() const
{
  // Need to make sure that match and non match transfer specs are schema compatible.
  return mMatchTransfer->getTarget();
}

void SortMergeJoin::check(PlanCheckContext& ctxt)
{
  std::vector<std::string> leftKeys;
  std::vector<std::string> rightKeys;
  std::string residual;
  std::string transfer;

  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "leftKey")) {
      leftKeys.push_back(boost::get<std::string>(it->Value));
    } else if (boost::algorithm::iequals(it->Name, "rightKey")) {
      rightKeys.push_back(boost::get<std::string>(it->Value));
    } else if (boost::algorithm::iequals(it->Name, "residual") ||
	       boost::algorithm::iequals(it->Name, "where")) {
      residual = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "output")) {
      transfer = boost::get<std::string>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }
  mLeftInput = getInput(0)->getRecordType();
  mRightInput = getInput(1)->getRecordType();

  init(ctxt, leftKeys, rightKeys, residual, transfer);

  getOutput(0)->setRecordType(isInnerOrOuter(mJoinType) ?
			      mMatchTransfer->getTarget() :
			      mLeftMakeNullableTransfer->getTarget());
}

void SortMergeJoin::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = create();
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  plan.mapInputPort(this, 1, opType, 1);  
  plan.mapOutputPort(this, 0, opType, 0);  
}

RuntimeOperatorType * SortMergeJoin::create() const
{
  return new RuntimeSortMergeJoinOperatorType(mJoinType,
					      mLeftInput,
					      mRightInput,
					      mLeftKeyCompare,
					      mRightKeyCompare,
					      mLeftRightKeyCompare,
					      mResidual,
					      mMatchTransfer,
					      mLeftMakeNullableTransfer,
					      mRightMakeNullableTransfer);
}

RuntimeSortMergeJoinOperatorType::~RuntimeSortMergeJoinOperatorType()
{
  delete mLeftKeyCompareFun;
  delete mRightKeyCompareFun;
  delete mLeftRightKeyCompareFun;
  delete mEqFun;
  delete mMatchTransfer;
  delete mLeftMakeNullableTransfer;
  delete mRightMakeNullableTransfer;
}
  
RuntimeOperator * RuntimeSortMergeJoinOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeSortMergeJoinOperator(s, *this);
}

RuntimeSortMergeJoinOperator::RuntimeSortMergeJoinOperator(RuntimeOperator::Services& services, 
										   const RuntimeSortMergeJoinOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeSortMergeJoinOperatorType>(services, opType),
  mState(START),
  mRuntimeContext(NULL),
  mMatchFound(false)
{
  mRuntimeContext = new InterpreterContext();
  if (getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
      getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_OUTER) {
    mLeftNulls = getMyOperatorType().mLeftNullMalloc.malloc();
  }
  if (getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
      getMyOperatorType().mJoinType == SortMergeJoin::LEFT_OUTER) {
    mRightNulls = getMyOperatorType().mRightNullMalloc.malloc();
  }
}

RuntimeSortMergeJoinOperator::~RuntimeSortMergeJoinOperator()
{
  delete mRuntimeContext;
  if (getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
      getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_OUTER) {
    getMyOperatorType().mLeftNullFree.free(mLeftNulls);
  }
  if (getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
      getMyOperatorType().mJoinType == SortMergeJoin::LEFT_OUTER) {
    getMyOperatorType().mRightNullFree.free(mRightNulls);
  }
}

void RuntimeSortMergeJoinOperator::start()
{
  mState = START;
  onEvent(NULL);
}

void RuntimeSortMergeJoinOperator::onRightNonMatch(RuntimePort * port)
{
  switch(getMyOperatorType().mJoinType) {
  case SortMergeJoin::FULL_OUTER:
    {
      RecordBuffer out;
      RecordBuffer tmp;
      getMyOperatorType().mRightMakeNullableTransfer->execute(mRightInput, tmp, mRuntimeContext, false);
      // TODO: Add move optimizations.
      getMyOperatorType().mMatchTransfer->execute(mLeftNulls, tmp, out, mRuntimeContext, false, false);
      write(port, out, false);
      getMyOperatorType().mRightNullFree.free(tmp);
      break;
    }
  case SortMergeJoin::RIGHT_OUTER:
    {
      RecordBuffer out;
      // TODO: Add move optimizations.
      getMyOperatorType().mMatchTransfer->execute(mLeftNulls, mRightInput, out, mRuntimeContext, false, false);
      write(port, out, false);
      break;
    }
  case SortMergeJoin::RIGHT_ANTI_SEMI:
    {
      RecordBuffer out;
      if (NULL == getMyOperatorType().mLeftMakeNullableTransfer) {
	write(port, mRightInput, false);
	mRightInput = NULL;
      } else {
	getMyOperatorType().mLeftMakeNullableTransfer->execute(mRightInput, out, mRuntimeContext, false);
	write(port, out, false);
      }
      break;
    }
  default:
    throw std::runtime_error("Invalid join type for non match processing");
  }
}

void RuntimeSortMergeJoinOperator::onLeftNonMatch(RuntimePort * port, RecordBuffer buf)
{
  switch(getMyOperatorType().mJoinType) {
  case SortMergeJoin::FULL_OUTER:
    {
      RecordBuffer out;
      RecordBuffer tmp;
      getMyOperatorType().mLeftMakeNullableTransfer->execute(buf, tmp, mRuntimeContext, false);
      getMyOperatorType().mMatchTransfer->execute(tmp, mRightNulls, out, mRuntimeContext, false, false);
      write(port, out, false);
      getMyOperatorType().mLeftNullFree.free(tmp);
      break;
    }
  case SortMergeJoin::LEFT_OUTER:
    {
      RecordBuffer out;
      // TODO: Add move optimizations.
      getMyOperatorType().mMatchTransfer->execute(buf, mRightNulls, out, mRuntimeContext, false, false);
      write(port, out, false);
      break;
    }
  default:
    throw std::runtime_error("Invalid join type for non match processing");
  }
}

void RuntimeSortMergeJoinOperator::onMatch(RuntimePort * port)
{
  switch(getMyOperatorType().mJoinType) {
  case SortMergeJoin::INNER:
    {
      RecordBuffer out;
      getMyOperatorType().mMatchTransfer->execute(mLastMatch, mRightInput, out, mRuntimeContext, false, false);		    
      write(port, out, false);
      break;
    }
  case SortMergeJoin::RIGHT_OUTER:
    {
      RecordBuffer out;
      RecordBuffer tmp;
      getMyOperatorType().mLeftMakeNullableTransfer->execute(mLastMatch, tmp, mRuntimeContext, false);
      getMyOperatorType().mMatchTransfer->execute(tmp, mRightInput, out, mRuntimeContext, false, false);		    
      getMyOperatorType().mLeftNullFree.free(tmp);
      write(port, out, false);
      break;
    }
  case SortMergeJoin::LEFT_OUTER:
    {
      RecordBuffer out;
      RecordBuffer tmp;
      getMyOperatorType().mRightMakeNullableTransfer->execute(mRightInput, tmp, mRuntimeContext, false);
      getMyOperatorType().mMatchTransfer->execute(mLastMatch, tmp, out, mRuntimeContext, false, false);		    
      getMyOperatorType().mRightNullFree.free(tmp);
      write(port, out, false);
      break;
    }
  case SortMergeJoin::FULL_OUTER:
    {
      RecordBuffer out;
      RecordBuffer tmp1;
      RecordBuffer tmp2;
      getMyOperatorType().mLeftMakeNullableTransfer->execute(mLastMatch, tmp1, mRuntimeContext, false);
      getMyOperatorType().mRightMakeNullableTransfer->execute(mRightInput, tmp2, mRuntimeContext, false);
      getMyOperatorType().mMatchTransfer->execute(tmp1, tmp2, out, mRuntimeContext, false, false);		    
      getMyOperatorType().mLeftNullFree.free(tmp1);
      getMyOperatorType().mRightNullFree.free(tmp2);
      write(port, out, false);
      break;
    }
  case SortMergeJoin::RIGHT_SEMI:
    {
      if (NULL == getMyOperatorType().mLeftMakeNullableTransfer) {
	write(port, mRightInput, false);
	mRightInput = NULL;
      } else {
	RecordBuffer out;
	getMyOperatorType().mLeftMakeNullableTransfer->execute(mRightInput, out, mRuntimeContext, false);
	write(port, out, false);
      }
      break;
    }
  default:
    throw std::runtime_error("Invalid join type for match processing");
  }
}

void RuntimeSortMergeJoinOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    requestRead(RuntimeSortMergeJoinOperatorType::RIGHT_PORT);
    mState = READ_RIGHT_INIT;
    return;
  case READ_RIGHT_INIT:
    read(port, mRightInput);

    requestRead(RuntimeSortMergeJoinOperatorType::LEFT_PORT);
    mState = READ_LEFT_INIT;
    return;
  case READ_LEFT_INIT:
    read(port, mLeftInput);
      
    while(!RecordBuffer::isEOS(mLeftInput) &&
	  !RecordBuffer::isEOS(mRightInput)) {
      // Advance lagging left until catches up with right
      while (0 > getMyOperatorType().mLeftRightKeyCompareFun->execute(mLeftInput,
								      mRightInput,
								      mRuntimeContext)) {
	if (getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
	    getMyOperatorType().mJoinType == SortMergeJoin::LEFT_OUTER) {
	  requestWrite(0);
	  mState = WRITE_LEFT_NON_MATCH_LT;
	  return;
	case WRITE_LEFT_NON_MATCH_LT:
	  onLeftNonMatch(port, mLeftInput);
	}
	if (mLeftInput != RecordBuffer(NULL))
	  getMyOperatorType().mLeftFree.free(mLeftInput);

	requestRead(RuntimeSortMergeJoinOperatorType::LEFT_PORT);
	mState = READ_LEFT_LT;
	return;
      case READ_LEFT_LT:
	read(port, mLeftInput);

	if (RecordBuffer::isEOS(mLeftInput)) 
	  break;
      }      

      // Compare left and write if we have matches.
      if(!RecordBuffer::isEOS(mLeftInput) &&
	    !RecordBuffer::isEOS(mRightInput) &&
	    0==getMyOperatorType().mLeftRightKeyCompareFun->execute(mLeftInput,
								    mRightInput,
								    mRuntimeContext)) { // On Equality
	// Read the entire key run from left.
	do {
	  mLeftBuffer.push_back(mLeftInput);
	  requestRead(RuntimeSortMergeJoinOperatorType::LEFT_PORT);
	  mState = READ_LEFT_EQ;
	  return;
	case READ_LEFT_EQ:
	  read(port, mLeftInput);
	} while (!RecordBuffer::isEOS(mLeftInput) &&  
		 0==getMyOperatorType().mLeftRightKeyCompareFun->execute(mLeftInput,
									 mRightInput,
									 mRuntimeContext));

	while(!RecordBuffer::isEOS(mRightInput) &&
	      0==getMyOperatorType().mLeftRightKeyCompareFun->execute(mLeftBuffer.back().Buffer,
								      mRightInput,
								      mRuntimeContext)){ // Scan for matches
	  mMatchFound = false;
	  mLastMatch = NULL;

	  // Calculate predicates and output if match
	  for(mIt = mLeftBuffer.begin();
	      mIt != mLeftBuffer.end();
	      ++mIt) {
	    if (NULL==getMyOperatorType().mEqFun ||
		getMyOperatorType().mEqFun->execute(mIt->Buffer,
						    mRightInput,
						    mRuntimeContext)) {
	      mMatchFound = mIt->Matched = true;
	      if (getMyOperatorType().mJoinType == SortMergeJoin::INNER ||
		  getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
		  getMyOperatorType().mJoinType == SortMergeJoin::LEFT_OUTER ||
		  getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_OUTER) {
		if (mLastMatch) {
		  requestWrite(0);
		  mState = WRITE_MATCH;
		  return;
		case WRITE_MATCH:
		  {
		    onMatch(port);
		  }
		}
		mLastMatch = mIt->Buffer;
	      } else if (getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_SEMI) {
		// Only processing a single match, so set last match and break from loop
		mLastMatch = mIt->Buffer;
		break;
	      }
	    }
	  }
	  if (mLastMatch && (getMyOperatorType().mJoinType == SortMergeJoin::INNER ||
			     getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
			     getMyOperatorType().mJoinType == SortMergeJoin::LEFT_OUTER ||
			     getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_OUTER ||
			     getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_SEMI)) {
	    requestWrite(0);
	    mState = WRITE_LAST_MATCH;
	    return;
	  case WRITE_LAST_MATCH:
	    {
	      onMatch(port);
	    }
	  } else if (!mMatchFound && (getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
				      getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_OUTER ||
				      getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_ANTI_SEMI)) {
	    requestWrite(0);
	    mState = WRITE_RIGHT_NON_MATCH;
	    return;
	  case WRITE_RIGHT_NON_MATCH:
	    onRightNonMatch(port);
	  } 

	  // Free right input unless output as part of semi join processing
	  if (mRightInput != RecordBuffer(NULL)) {
	    getMyOperatorType().mRightFree.free(mRightInput);
	    mRightInput = RecordBuffer(NULL);
	  }
	  // Get a new right input and scan for matches.
	  requestRead(RuntimeSortMergeJoinOperatorType::RIGHT_PORT);
	  mState = READ_RIGHT_EQ;
	  return;
	case READ_RIGHT_EQ:
	  read(port, mRightInput);
	}

	// Free left buffers since we don't need them.
	for(mIt = mLeftBuffer.begin();
	    mIt != mLeftBuffer.end();
	    ++mIt) {
	  if((getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
	      getMyOperatorType().mJoinType == SortMergeJoin::LEFT_OUTER) &&
	     !mIt->Matched) { 
	    requestWrite(0);
	    mState = WRITE_LEFT_NON_MATCH;
	    return;
	  case WRITE_LEFT_NON_MATCH:
	    onLeftNonMatch(port, mIt->Buffer);	    
	  } 
	  getMyOperatorType().mLeftFree.free(mIt->Buffer);
	}      
	mLeftBuffer.clear();
      }

      // Advance lagging right if needed (won't have done any equality processing if we hit this).
      while(!RecordBuffer::isEOS(mLeftInput) &&
	    !RecordBuffer::isEOS(mRightInput) &&
	    0 < getMyOperatorType().mLeftRightKeyCompareFun->execute(mLeftInput,
								     mRightInput,
								     mRuntimeContext)) {
	if (getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_ANTI_SEMI ||
	    getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
	    getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_OUTER) {
	  requestWrite(0);
	  mState = WRITE_RIGHT_NON_MATCH_LT;
	  return;
	case WRITE_RIGHT_NON_MATCH_LT:
	  onRightNonMatch(port);
	}
	if (mRightInput != RecordBuffer(NULL))
	  getMyOperatorType().mRightFree.free(mRightInput);

	requestRead(RuntimeSortMergeJoinOperatorType::RIGHT_PORT);
	mState = READ_RIGHT_LT;
	return;
      case READ_RIGHT_LT:
	read(port, mRightInput);
	if (RecordBuffer::isEOS(mRightInput))
	  break;
      }
    }

    // One of the two inputs is EOS.  Drain the other.
    while(!RecordBuffer::isEOS(mLeftInput)) {
      if (getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
	  getMyOperatorType().mJoinType == SortMergeJoin::LEFT_OUTER) {
	requestWrite(0);
	mState = WRITE_LEFT_NON_MATCH_DRAIN;
	return;
      case WRITE_LEFT_NON_MATCH_DRAIN:
	onLeftNonMatch(port, mLeftInput);
      } 
      if (mLeftInput != RecordBuffer(NULL))
	getMyOperatorType().mLeftFree.free(mLeftInput);

      requestRead(RuntimeSortMergeJoinOperatorType::LEFT_PORT);
      mState = READ_LEFT_DRAIN;
      return;
    case READ_LEFT_DRAIN:
      read(port, mLeftInput);
    } 

    while(!RecordBuffer::isEOS(mRightInput)) {
      if (getMyOperatorType().mJoinType == SortMergeJoin::FULL_OUTER ||
	  getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_OUTER ||
	  getMyOperatorType().mJoinType == SortMergeJoin::RIGHT_ANTI_SEMI) {
	requestWrite(0);
	mState = WRITE_RIGHT_NON_MATCH_DRAIN;
	return;
      case WRITE_RIGHT_NON_MATCH_DRAIN:
	onRightNonMatch(port);
	// TODO: Is this correct for RIGHT_OUTER or
	// is it a memory leak????
	mRightInput = RecordBuffer(NULL);
      } 
      if (mRightInput != RecordBuffer(NULL))
	getMyOperatorType().mRightFree.free(mRightInput);

      requestRead(RuntimeSortMergeJoinOperatorType::RIGHT_PORT);
      mState = READ_RIGHT_DRAIN;
      return;
    case READ_RIGHT_DRAIN:
      read(port, mRightInput);
    } 

    requestWrite(0);
    mState = WRITE_EOS;
    return;
  case WRITE_EOS:
    write(port, RecordBuffer(NULL), true);
  }
}

void RuntimeSortMergeJoinOperator::shutdown()
{
}


LogicalUnionAll::LogicalUnionAll()
  :
  LogicalOperator(1,std::numeric_limits<uint32_t>::max(),1,1),
  mConcat(false)
{
}

LogicalUnionAll::~LogicalUnionAll()
{
}

void LogicalUnionAll::check(PlanCheckContext& log)
{
  // Validate the parameters
  std::string predicate;
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "concatenate") ||
	boost::algorithm::iequals(it->Name, "concat")) {
      if (boost::algorithm::iequals(boost::get<std::string>(it->Value), 
				    "true")) {
	mConcat = true;
      } else {
	mConcat = false;
      }
    } else {
      checkDefaultParam(*it);
    }
  }

  if (getInput(0)->getRecordType() == NULL) {
    log.logError(*this, "Type check error: input 0 has no computed type");
  }

  checkIdenticalInputs(log);

  // TODO: Add an output argument option.
  getOutput(0)->setRecordType(getInput(0)->getRecordType());
}

void LogicalUnionAll::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimeUnionAllOperatorType();
  
  plan.addOperatorType(opType);
  for(std::size_t i = 0; i<size_inputs(); ++i) {
    plan.mapInputPort(this, i, opType, i);  
  }
  plan.mapOutputPort(this, 0, opType, 0);      
}

RuntimeUnionAllOperatorType::~RuntimeUnionAllOperatorType()
{
}
  
RuntimeOperator * RuntimeUnionAllOperatorType::create(RuntimeOperator::Services & s) const
{
  typedef RuntimeNondeterministicCollectorOperator<RuntimeUnionAllOperatorType> op_type;
  typedef RuntimeConcatenationOperator<RuntimeUnionAllOperatorType> concat_type;
  if (mConcat) 
    return new concat_type(s, *this);
  else
    return new op_type(s, *this);
}

LogicalUnpivot::LogicalUnpivot()
  :
  LogicalOperator(1,1,1,1),
  mStateType(NULL),
  mConstantScan(NULL),
  mTransfer(NULL),
  mNumPivotColumns(0LL)
{
}

LogicalUnpivot::~LogicalUnpivot()
{
  delete mConstantScan;
  delete mTransfer;
}

void LogicalUnpivot::check(PlanCheckContext& ctxt)
{
  if (size_inputs() != 1) {
  } 
  if (getInput(0)->getRecordType() == NULL) {
  }

  // Pattern specifying the columns to unpivot
  std::string columnPattern;
  // Name(s) of the output column containing the unpivoted column names.
  std::vector<std::string> pivotColumns;
  // Name of the output column containing the data from the unpivoted
  // columns.
  std::string valueColumn;

  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "columnPattern")) {
      columnPattern = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "pivotcolumn")) {
      pivotColumns.push_back(boost::get<std::string>(it->Value));
    } else if (boost::algorithm::iequals(it->Name, "valuecolumn")) {
      valueColumn = boost::get<std::string>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }

  // Extract columns
  boost::regex e(columnPattern);
  std::vector<RecordMember> columnMembers;
  std::vector<std::string> generateExpr(pivotColumns.size());
  std::string valueExpr;
  // Comma delimited list of non-pivot columns
  std::string nonPivotColumns;
  // Number of different values we are unpivoting
  mNumPivotColumns=0;
  // Iterate through the columns and match.
  // If there are subpatterns, use these as the
  // pivot columns.  The names for the pivot
  // columns comes from the pivotColumn(s) argument.
  const RecordType * input = getInput(0)->getRecordType();
  for(RecordType::const_member_iterator member = input->begin_members();
      member != input->end_members();
      ++member) {
    boost::cmatch m;
    if (boost::regex_match(member->GetName().c_str(), m, e)) {
      if (m.size() > 1) {
	if (m.size()-1 != pivotColumns.size()) {
	  ctxt.logError(*this, "Must specify a pivot column for each "
			"subexpression in the column pattern.");
	}
	// Subexpressions.  Use them as the values.
	for(std::size_t i=1; i<m.size(); ++i) {	  
	  std::string str(m[i].first, m[i].second);
	  generateExpr[i-1] +=(boost::format(" WHEN RECORDCOUNT=%1% THEN '%2%' ") %
			       mNumPivotColumns % str).str();
	}
      } else {
	// No  subexpressions.  Use the whole match as the value.
	if (pivotColumns.size() != 1) {
	  ctxt.logError(*this, "Must have exactly one pivot column for a "
			"column pattern with no subexpressions.");
	}
	std::string str(m[0].first, m[0].second);
	generateExpr[0] +=(boost::format(" WHEN RECORDCOUNT=%1% THEN '%2%' ") %
			   mNumPivotColumns % str).str();
      }
      boost::format valueClauseFmt(" WHEN _tableIdx = %1% THEN %2%");
      valueExpr += (valueClauseFmt % mNumPivotColumns % 
		    member->GetName()).str();
      mNumPivotColumns += 1;
    } else {
      nonPivotColumns += ",";
      nonPivotColumns += member->GetName();
    }
  }

  // Create a generator operator that creates all of the column name
  // expressions.  This is then cross joined to the input.  Also output
  // RECORDCOUNT for the join output CASE statement.
  std::string generateOutput;
  for(std::size_t i=0; i<generateExpr.size(); ++i) {
    if (i > 0) {
      generateOutput += ", ";
    }
    generateOutput += "CASE ";
    generateOutput += generateExpr[i];
    generateOutput += " ELSE 'bogus' END AS ";
    generateOutput += pivotColumns[i];
  }
  generateOutput += ", RECORDCOUNT AS _tableIdx";
  valueExpr = ", CASE " + valueExpr + " END AS " + valueColumn;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("RECORDCOUNT", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("PARTITIONCOUNT", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("PARTITION", Int32Type::Get(ctxt)));
  mStateType = RecordType::get(ctxt, members);
  mConstantScan = new RecordTypeTransfer(ctxt, "unpivotConstantScan", 
					 mStateType, generateOutput);
  // In the cross join, output the pivot columns from the table, the
  // pivoted value column and the non-pivoted input columns.
  std::vector<AliasedRecordType> types;
  types.push_back(AliasedRecordType("table", mConstantScan->getTarget()));
  types.push_back(AliasedRecordType("probe", getInput(0)->getRecordType()));
  std::string joinTransfer;
  for(std::size_t i=0; i<pivotColumns.size(); ++i) {
    if (i>0) {
      joinTransfer += ",";
    }
    joinTransfer += pivotColumns[i];
  }
  joinTransfer += valueExpr + nonPivotColumns;
  mTransfer = new RecordTypeTransfer2(ctxt, "makeoutput", types, joinTransfer);

  getOutput(0)->setRecordType(mTransfer->getTarget());
}

void LogicalUnpivot::create(class RuntimePlanBuilder& plan)
{
  // Make generator and cross join with input.
  RuntimeOperatorType * constantScan = 
    new RuntimeGenerateOperatorType("constantScan",
				    mStateType,
				    mConstantScan,
				    mNumPivotColumns);
  plan.addOperatorType(constantScan);
  RuntimeOperatorType * xjoin = 
    new RuntimeCrossJoinOperatorType(mConstantScan->getTarget()->getFree(),
				     getInput(0)->getRecordType()->getFree(),
				     NULL,
				     mTransfer);
  plan.addOperatorType(xjoin);
  plan.connect(constantScan, 0, xjoin, 0);
  plan.mapInputPort(this, 0, xjoin, 1);  
  plan.mapOutputPort(this, 0, xjoin, 0);  
}

LogicalSwitch::LogicalSwitch()
  :
  LogicalOperator(1,1,1,std::numeric_limits<uint32_t>::max()),
  mSwitcher(NULL)
{
}

LogicalSwitch::~LogicalSwitch()
{
  delete mSwitcher;
}

void LogicalSwitch::check(PlanCheckContext& log)
{
  // Validate the parameters
  std::string predicate;
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "on")) {
      predicate = boost::get<std::string>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }

  // TODO: If no predicate or limit specified, perhaps
  // we should warn.
  if (!predicate.size()) {
    log.logError(*this, "Must set 'on' argument");
  } else {
    std::vector<RecordMember> emptyMembers;
    RecordType emptyTy(emptyMembers);
    std::vector<const RecordType *> inputs;
    inputs.push_back(getInput(0)->getRecordType());
    inputs.push_back(&emptyTy);
    mSwitcher = new RecordTypeFunction(log,
				       "switcher",
				       inputs,
				       predicate);
  }

  for(std::size_t i=0; i<size_outputs(); ++i) {
    getOutput(i)->setRecordType(getInput(0)->getRecordType());
  }
}

void LogicalSwitch::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimeSwitchOperatorType(getInput(0)->getRecordType(),
				  mSwitcher);
  
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  for(std::size_t i=0; i<size_outputs(); ++i) {
    plan.mapOutputPort(this, i, opType, i);      
  }
}

RuntimeOperator * RuntimeSwitchOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeSwitchOperator(s, *this);
}

RuntimeSwitchOperator::RuntimeSwitchOperator(RuntimeOperator::Services& services, 
					     const RuntimeSwitchOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeSwitchOperatorType>(services, opType),
  mState(START),
  mNumOutputs(0),
  mRuntimeContext(new InterpreterContext())

{
}

RuntimeSwitchOperator::~RuntimeSwitchOperator()
{
  delete mRuntimeContext;
}

void RuntimeSwitchOperator::start()
{
  mNumOutputs = getNumOutputs();
  mState = START;
  onEvent(NULL);
}

void RuntimeSwitchOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      read(port, mInput);

      // Done executing the operator.
      if (RecordBuffer::isEOS(mInput)) 
	break;
      
      {
	uint32_t output =  getMyOperatorType().mSwitcher->execute(mInput, 
								  RecordBuffer(),
								  mRuntimeContext);
      
	requestWrite(output % mNumOutputs);
      }
      mState = WRITE;
      return;
    case WRITE:
      write(port, mInput, false);
    }
    
    for(mNumOutputs = 0; mNumOutputs < getNumOutputs(); ++mNumOutputs) {
      // Close up
      requestWrite(mNumOutputs);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer(), true);
    }
  }
}

void RuntimeSwitchOperator::shutdown()
{
}


