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

#include <cstring>
#include <boost/format.hpp>
#include "RuntimePlan.hh"

RuntimePartitionConstraint::RuntimePartitionConstraint()
{
}

RuntimePartitionConstraint::~RuntimePartitionConstraint()
{
}

AssignedOperatorType::AssignedOperatorType()
  :
  Operator(NULL)
{
}

AssignedOperatorType::AssignedOperatorType(const RuntimeOperatorType& opType)
  :
  Operator(&opType)
{
}

bool AssignedOperatorType::test(int32_t partition) const
{
  return true;
}

std::size_t AssignedOperatorType::getPartitionCount(int32_t numPartitions) const
{
  return numPartitions;
}

void AssignedOperatorType::getPartitions(int32_t numPartitions,
					 boost::dynamic_bitset<>& result) const
{
  result.resize(numPartitions, true);
}

void AssignedOperatorType::getPartitions(int32_t partitionStart, 
					 int32_t partitionEnd,
					 std::vector<int32_t>& result) const
{
  for(int32_t i=partitionStart; i<=partitionEnd; i++) {
    result.push_back(i);
  }
}

int32_t AssignedOperatorType::getPartitionPosition(int32_t partition) const
{
  return partition;
}

ConstrainedOperatorType::ConstrainedOperatorType()
{
}

ConstrainedOperatorType::ConstrainedOperatorType(const RuntimeOperatorType& opType,
						 int32_t numPartitions)
  :
  AssignedOperatorType(opType)
{
  if (numPartitions <= 0) {
    // TODO: encapsulate this logic
    throw std::runtime_error("May not use operator partition constraints "
			     "under map reduce");
  }
  if (opType.getPartitionConstraint().isDefault()) {
    Partitions.resize(numPartitions, true);
  } else {
    Partitions.resize(numPartitions, false);
    for(RuntimePartitionConstraint::const_iterator it = 
	  opType.getPartitionConstraint().partition_begin();
	it != opType.getPartitionConstraint().partition_end();
	++it) {
      Partitions.set(*it, true);
    }
  }
  calculatePositionIndex();
}

void ConstrainedOperatorType::calculatePositionIndex()
{
  int32_t pos=0;
  for(std::size_t i=Partitions.find_first();
      i != boost::dynamic_bitset<>::npos;
      i = Partitions.find_next(i)) {
    mPartitionToPosition[(int32_t) i] = pos++;
  } 
}

bool ConstrainedOperatorType::test(int32_t partition) const
{
  return Partitions.test(partition);
}

std::size_t ConstrainedOperatorType::getPartitionCount(int32_t numPartitions) const
{
  return Partitions.count();
}

void ConstrainedOperatorType::getPartitions(int32_t numPartitions,
					    boost::dynamic_bitset<>& result) const
{
  result = Partitions;
}

void ConstrainedOperatorType::getPartitions(int32_t partitionStart, 
					    int32_t partitionEnd,
					    std::vector<int32_t>& result) const
{
  boost::dynamic_bitset<> tmp;
  tmp.resize(Partitions.size(), false);
  for(int32_t i=partitionStart; i<=partitionEnd; i++) {
    tmp.set(i);
  }
  tmp = Partitions & tmp;
  for(std::size_t i=tmp.find_first();
      i != boost::dynamic_bitset<>::npos;
      i = tmp.find_next(i)) {
    result.push_back(i);
  }  
}

int32_t ConstrainedOperatorType::getPartitionPosition(int32_t partition) const
{
  std::map<int32_t, int32_t>::const_iterator it = mPartitionToPosition.find(partition);
  return it->second;
}

RuntimeOperatorPlan::RuntimeOperatorPlan(int32_t numPartitions, bool isSingleProcess)
  :
  mPartitions(numPartitions),
  mCurrentTag(0)
{
}

RuntimeOperatorPlan::~RuntimeOperatorPlan()
{
  for(std::vector<boost::shared_ptr<AssignedOperatorType> >::iterator 
	it = mOperators.begin();
      it != mOperators.end();
      ++it) {
    delete (*it)->Operator;
  }
}

void RuntimeOperatorPlan::addOperatorType(RuntimeOperatorType * op)
{
  if (mOperatorIndex.find(op) != mOperatorIndex.end())
    throw std::runtime_error("Cannot add operator to plan multiple times");

  boost::shared_ptr<AssignedOperatorType> ty(
    op->getPartitionConstraint().isDefault() ?
    new AssignedOperatorType(*op) :
    new ConstrainedOperatorType(*op, mPartitions));
  mOperatorIndex[op] = mOperators.size();
  mOperators.push_back(ty);
}

void RuntimeOperatorPlan::connectStraight(RuntimeOperatorType * source, int32_t sourcePort, 
					  RuntimeOperatorType * target, int32_t targetPort,
					  bool buffered, bool locallyBuffered)
{
  std::map<RuntimeOperatorType *, std::size_t>::const_iterator sIt = mOperatorIndex.find(source);
  std::map<RuntimeOperatorType *, std::size_t>::const_iterator tIt = mOperatorIndex.find(target);
  if (sIt == mOperatorIndex.end()) throw std::runtime_error("Attempted to connect operator prior to adding to plan");
  if (tIt == mOperatorIndex.end()) throw std::runtime_error("Attempted to connect operator prior to adding to plan");
  std::size_t sourceIdx = sIt->second;
  std::size_t targetIdx = tIt->second;
  AssignedOperatorType * assignedSource = mOperators[sourceIdx].get();
  AssignedOperatorType * assignedTarget = mOperators[targetIdx].get();
  mStraightLineConnections.push_back(IntraProcessFifoSpec(assignedSource,
							  sourcePort,
							  assignedTarget,
							  targetPort,
							  buffered,
							  locallyBuffered));
}

void RuntimeOperatorPlan::connectCrossbar(RuntimeOperatorType * source, RuntimeOperatorType * target, const RecordType * ty,
					  bool buffered, bool locallyBuffered)
{
  if (mPartitions <= 0) {
    // The behavior of partitioners needs to be abstracted out.
    // When running under a map reduce runtime, we do not set up 
    // the channels as is done here (or as we would when running
    // multithreaded on an SMP or with MPI).
    // TODO: encapsulate this logic
    throw std::runtime_error("May not use partitioners "
			     "under map reduce");
  }

  std::map<RuntimeOperatorType *, std::size_t>::const_iterator sIt = mOperatorIndex.find(source);
  std::map<RuntimeOperatorType *, std::size_t>::const_iterator tIt = mOperatorIndex.find(target);
  if (sIt == mOperatorIndex.end()) throw std::runtime_error("Attempted to connect operator prior to adding to plan");
  if (tIt == mOperatorIndex.end()) throw std::runtime_error("Attempted to connect operator prior to adding to plan");

  if (!source->isPartitioner() || !target->isCollector()) 
    throw std::runtime_error("Can only make crossbar connections between partitioners and collectors");

  mCrossbarConnections.push_back(InterProcessFifoSpec(mOperators[sIt->second].get(),
						      0,
						      mOperators[tIt->second].get(),
						      0,
						      buffered,
						      locallyBuffered,
						      mCurrentTag,
						      ty));

  // For a crossbar we are allocating a quadratic number of fifos.
  std::size_t numFifos = mOperators[sIt->second]->getPartitionCount(mPartitions) * 
    mOperators[sIt->second]->getPartitionCount(mPartitions);
  if (numFifos > std::size_t(std::numeric_limits<int32_t>::max() - mCurrentTag)) {
    throw std::runtime_error((boost::format("InternalError: Can only create %1% interprocess channels") % std::numeric_limits<int32_t>::max()).str());
  }
  mCurrentTag += (int32_t) numFifos;
}

void RuntimeOperatorPlan::connectBroadcast(RuntimeOperatorType * source, RuntimeOperatorType * target, int32_t targetPort,
					   bool buffered, bool locallyBuffered)
{
  throw std::runtime_error("TODO: RuntimeOperatorPlan::connectBroadcast not yet implemented");
}

void RuntimeOperatorPlan::connectCollect(RuntimeOperatorType * source, int32_t sourcePort, RuntimeOperatorType * target,
					 bool buffered, bool locallyBuffered)
{
  throw std::runtime_error("TODO: RuntimeOperatorPlan::connectCollect not yet implemented");
}
