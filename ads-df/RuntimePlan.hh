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

#ifndef __RUNTIMEPLAN_HH
#define __RUNTIMEPLAN_HH

// RuntimeOperatorTypes must be serializable in order for
// us to send plans to slave machines.
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/map.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/iterator/filter_iterator.hpp>

#include "RecordType.hh"

namespace boost {
  namespace serialization {

template <class Archive>
inline void load(Archive& ar,
		 boost::dynamic_bitset<>& t,
		 const unsigned int file_version)
{
    typedef boost::dynamic_bitset<>::block_type B;
    typedef boost::dynamic_bitset<>::allocator_type A;
    typedef std::vector<B> blocks_type;
    std::size_t bits;
    blocks_type blocks;
    ar & BOOST_SERIALIZATION_NVP(bits);
    ar & BOOST_SERIALIZATION_NVP(blocks);
    t.resize(bits);  
    boost::from_block_range<blocks_type::iterator,B,A>(blocks.begin(), blocks.end(), t);
}

template <class Archive>
inline void save(Archive& ar,
		 const boost::dynamic_bitset<>& t,
		 const unsigned int file_version)
{
    typedef boost::dynamic_bitset<>::block_type B;
    typedef boost::dynamic_bitset<>::allocator_type A;
    typedef std::vector<B> blocks_type;
    std::size_t bits = t.size();
    blocks_type blocks(t.num_blocks());
    boost::to_block_range<B,A,blocks_type::iterator>(t, blocks.begin());
    ar & BOOST_SERIALIZATION_NVP(bits);
    ar & BOOST_SERIALIZATION_NVP(blocks);  
}

template<class Archive>
inline void serialize(
    Archive & ar,
    boost::dynamic_bitset<> & t,
    const unsigned int file_version
){
    boost::serialization::split_free(ar, t, file_version);
}
  }
}
class RuntimePartitionConstraint
{
private:
  std::vector<int32_t> mPartitions;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mPartitions);
  }
public:
  RuntimePartitionConstraint();
  ~RuntimePartitionConstraint();
  
  bool isDefault() const { return mPartitions.size() == 0; }
  
  typedef std::vector<int32_t>::const_iterator const_iterator;
  const_iterator partition_begin() const { return mPartitions.begin(); }
  const_iterator partition_end() const { return mPartitions.end(); }
};

class RuntimeOperatorType {
protected:
  std::string mName;
  RuntimePartitionConstraint mConstraint;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mName);
    ar & BOOST_SERIALIZATION_NVP(mConstraint);
  }

  RuntimeOperatorType(const char * name="")
    :
    mName(name)
  {
  }
public:
  virtual ~RuntimeOperatorType () {}
  virtual class RuntimeOperator * create(class DataflowScheduler& s) const =0;
  virtual bool isPartitioner() const { return false; }
  virtual bool isCollector() const { return false; }
  const RuntimePartitionConstraint& getPartitionConstraint() const 
  {
    return mConstraint;
  }
  const std::string& getName() const
  {
    return mName;
  }
};

/**
 * An operator type together that is assigned to all 
 * available partitions.  
 */
class AssignedOperatorType
{
public:
  const RuntimeOperatorType * Operator;

  AssignedOperatorType();

  AssignedOperatorType(const RuntimeOperatorType& opType);

  virtual ~AssignedOperatorType()
  {
  }

  /**
   * Check whether a partition is assigned to an operator.
   */
  virtual bool test(int32_t partition) const;

  /**
   * Get the total number of partitions assigned to an operator.
   */
  virtual std::size_t getPartitionCount(int32_t numPartitions) const;

  /**
   * Get the partitions assigned to this operator.
   */
  virtual void getPartitions(int32_t numPartitions,
			     boost::dynamic_bitset<>& result) const;

  /**
   * Get the partitions assigned within the interval [partitionStart, partitionEnd].
   */
  virtual void getPartitions(int32_t partitionStart, int32_t partitionEnd,
			     std::vector<int32_t>& result) const;

  /**
   * Get the position of the partition within the order list of partitions
   * this operator is assigned to.
   */
  virtual int32_t getPartitionPosition(int32_t partition) const;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    RuntimeOperatorType* constHack = const_cast<RuntimeOperatorType*>(Operator);
    ar & BOOST_SERIALIZATION_NVP(constHack);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    RuntimeOperatorType* constHack;
    ar & BOOST_SERIALIZATION_NVP(constHack);
    Operator = constHack;
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
};

/**
 * An operator type together with an assignment of the operator
 * to different partitions.
 */
class ConstrainedOperatorType : public AssignedOperatorType
{
  boost::dynamic_bitset<> Partitions;
  std::map<int32_t, int32_t> mPartitionToPosition;

  void calculatePositionIndex();
public:
  ConstrainedOperatorType();

  ConstrainedOperatorType(const RuntimeOperatorType& opType,
			  int32_t numPartitions);

  ~ConstrainedOperatorType()
  {
  }

  /**
   * Check whether a partition is assigned to an operator.
   */
  bool test(int32_t partition) const;

  /**
   * Get the total number of partitions assigned to an operator.
   */
  std::size_t getPartitionCount(int32_t numPartitions) const;

  /**
   * Get the partitions assigned to this operator.
   */
  void getPartitions(int32_t numPartitions,
		     boost::dynamic_bitset<>& result) const;

  /**
   * Get the partitions assigned within the interval [partitionStart, partitionEnd].
   */
  void getPartitions(int32_t partitionStart, int32_t partitionEnd,
		     std::vector<int32_t>& result) const;

  /**
   * Get the position of the partition within the order list of partitions
   * this operator is assigned to.
   */
  int32_t getPartitionPosition(int32_t partition) const;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    // Don't bother to serialize if we have default
    // partition mask.
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(AssignedOperatorType);
    ar & BOOST_SERIALIZATION_NVP(Partitions);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(AssignedOperatorType);
    ar & BOOST_SERIALIZATION_NVP(Partitions);
    calculatePositionIndex();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
};

/**
 * A connection between ports of operators.
 */
class IntraProcessFifoSpec
{
protected:
  AssignedOperatorType * mSourceOperator;
  int32_t mSourcePort;
  AssignedOperatorType * mTargetOperator;
  int32_t mTargetPort;
  bool mLocallyBuffered;
  bool mBuffered;
  
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mSourceOperator);
    ar & BOOST_SERIALIZATION_NVP(mSourcePort);
    ar & BOOST_SERIALIZATION_NVP(mTargetOperator);
    ar & BOOST_SERIALIZATION_NVP(mTargetPort);
    ar & BOOST_SERIALIZATION_NVP(mLocallyBuffered);
    ar & BOOST_SERIALIZATION_NVP(mBuffered);
  }

  IntraProcessFifoSpec()
    :
    mSourceOperator(NULL),
    mSourcePort(0),
    mTargetOperator(NULL),
    mTargetPort(0),
    mLocallyBuffered(true),
    mBuffered(true)
  {
  }
public:
  IntraProcessFifoSpec(AssignedOperatorType * sourceOperator, int32_t sourcePort,
		       AssignedOperatorType * targetOperator, int32_t targetPort,
		       bool buffered, bool locallyBuffered)
    :
    mSourceOperator(sourceOperator),
    mSourcePort(sourcePort),
    mTargetOperator(targetOperator),
    mTargetPort(targetPort),
    mLocallyBuffered(locallyBuffered),
    mBuffered(buffered)
  {
  }
  ~IntraProcessFifoSpec()
  {
  }
  AssignedOperatorType * getSourceOperator() const { return mSourceOperator; }
  int32_t getSourcePort() const { return mSourcePort; }
  AssignedOperatorType * getTargetOperator() const { return mTargetOperator; }
  int32_t getTargetPort() const { return mTargetPort; }
  bool getLocallyBuffered() const { return mLocallyBuffered; }
  bool getBuffered() const { return mBuffered; }
};

class InterProcessFifoSpec : public IntraProcessFifoSpec
{
protected:
  int32_t mTag;
  RecordTypeDeserialize mDeserialize;
  RecordTypeSerialize mSerialize;
  RecordTypeFree mFree;
  RecordTypeMalloc mMalloc;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(IntraProcessFifoSpec);
    ar & BOOST_SERIALIZATION_NVP(mTag);
    ar & BOOST_SERIALIZATION_NVP(mDeserialize);
    ar & BOOST_SERIALIZATION_NVP(mSerialize);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
  }

  InterProcessFifoSpec()
    :
    mTag(0)
  {
  }
public:
  InterProcessFifoSpec(AssignedOperatorType * sourceOperator, int32_t sourcePort,
		       AssignedOperatorType * targetOperator, int32_t targetPort,
		       bool buffered, bool locallyBuffered, int32_t tag, 
		       const RecordType * ty)
    :
    IntraProcessFifoSpec(sourceOperator, sourcePort, targetOperator, targetPort, locallyBuffered, buffered),
    mTag(tag),
    mDeserialize(ty->getDeserialize()),
    mSerialize(ty->getSerialize()),
    mFree(ty->getFree()),
    mMalloc(ty->getMalloc())
  {
  }
  ~InterProcessFifoSpec()
  {
  }

  int32_t getTag() const { return mTag; }
  const RecordTypeDeserialize& getDeserialize() const { return mDeserialize; }
  const RecordTypeSerialize& getSerialize() const { return mSerialize; }
  const RecordTypeFree& getFree() const { return mFree; }
  const RecordTypeMalloc& getMalloc() const { return mMalloc; }
};

/**
 * A plan represents a graph of runtime operators assigned to different
 * partitions.  We want a single space efficient representation of the 
 * plan that allows each partition to easily create its assigned subgraph.
 * Part of the reason that we want a single representation is that we don't
 * have to serialize a different object for each target partition; this just
 * doesn't scale well.  The only thing in the representation below that is
 * O(# partitions) is the bitset that assigns operators to partitions.
 * For large plans running in parallel over thousands of machines this can
 * probably become a problem.  A simple run length encoding scheme would 
 * fix that for most cases of interest.
 */
class RuntimeOperatorPlan
{
private:
  // The number of partitions.
  int32_t mPartitions;
  // The operators in the graph.
  std::vector<boost::shared_ptr<AssignedOperatorType> > mOperators;
  std::map<RuntimeOperatorType *, std::size_t> mOperatorIndex;
  
  // Different connections between operators
  std::vector<IntraProcessFifoSpec> mStraightLineConnections;
  std::vector<InterProcessFifoSpec> mBroadcastConnections;
  std::vector<InterProcessFifoSpec> mCollectConnections;
  std::vector<InterProcessFifoSpec> mCrossbarConnections;

  // The current count of channels created that aren't straight line.
  int32_t mCurrentTag;

  // Serialization.  Not sure there is a point in having
  // these be template functions.  We'll only use binary archives.
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mPartitions);
    ar & BOOST_SERIALIZATION_NVP(mOperators);
    ar & BOOST_SERIALIZATION_NVP(mStraightLineConnections);
    ar & BOOST_SERIALIZATION_NVP(mBroadcastConnections);
    ar & BOOST_SERIALIZATION_NVP(mCollectConnections);
    ar & BOOST_SERIALIZATION_NVP(mCrossbarConnections);
    ar & BOOST_SERIALIZATION_NVP(mCurrentTag);
  }

  RuntimeOperatorPlan()
    :
    mPartitions(0),
    mCurrentTag(0)
  {
  }

  // Predicate for extracting operators associated with a given partition
  typedef boost::shared_ptr<AssignedOperatorType> op_type_ptr;
  struct OperatorTypeInPartition 
  {
    int32_t Partition;
    bool operator()(op_type_ptr & op)
    {
      return op->test(Partition);
    }
  };

  typedef boost::filter_iterator<OperatorTypeInPartition, 
				 std::vector<op_type_ptr>::const_iterator> 
  operators_const_iterator;
public:
  /**
   * For the moment, only support two models of assigning processes and partitions.
   * In the first model there is one process with all partitions and in the second
   * model there is one process per partition.
   */
  RuntimeOperatorPlan(int32_t numPartitions, bool isSingleProcess);
  ~RuntimeOperatorPlan();

  /**
   * Add an operator to the plan.
   */
  void addOperatorType(RuntimeOperatorType * op);
  /**
   * Connect two operators without repartitioning.  Requires that the operators
   * are mapped to exactly the same partitions.
   */
  void connectStraight(RuntimeOperatorType * source, int32_t sourcePort, 
		       RuntimeOperatorType * target, int32_t targetPort,
		       bool buffered, bool locallyBuffered);
  /**
   * Connect two operators with repartitioning.  Assumes that the source operator is
   * a partitioner and the target is a collector.
   */
  void connectCrossbar(RuntimeOperatorType * source, RuntimeOperatorType * target, const RecordType * ty,
		       bool buffered, bool locallyBuffered);
  /**
   * Connect a partitioner running on a single partition with an operator running
   * on 1 or more partitions.
   */
  void connectBroadcast(RuntimeOperatorType * source, RuntimeOperatorType * target, int32_t targetPort,
			bool buffered, bool locallyBuffered);
  /**
   * Connect a partitioner an operator running on 1 or more partitions with a collector
   * running on a single partition.
   */
  void connectCollect(RuntimeOperatorType * source, int32_t sourcePort, RuntimeOperatorType * target,
		      bool buffered, bool locallyBuffered);

  typedef std::vector<IntraProcessFifoSpec>::const_iterator intraprocess_fifo_const_iterator;
  typedef std::vector<InterProcessFifoSpec>::const_iterator interprocess_fifo_const_iterator;
  typedef std::vector<op_type_ptr>::const_iterator operator_const_iterator;
  intraprocess_fifo_const_iterator straight_line_begin() const { return mStraightLineConnections.begin(); }
  intraprocess_fifo_const_iterator straight_line_end() const { return mStraightLineConnections.end(); }
  interprocess_fifo_const_iterator broadcast_begin() const { return mBroadcastConnections.begin(); }
  interprocess_fifo_const_iterator broadcast_end() const { return mBroadcastConnections.end(); }
  interprocess_fifo_const_iterator collect_begin() const { return mCollectConnections.begin(); }
  interprocess_fifo_const_iterator collect_end() const { return mCollectConnections.end(); }
  interprocess_fifo_const_iterator crossbar_begin() const { return mCrossbarConnections.begin(); }
  interprocess_fifo_const_iterator crossbar_end() const { return mCrossbarConnections.end(); }
  operator_const_iterator operator_begin() const { return mOperators.begin(); }
  operator_const_iterator operator_end() const { return mOperators.end(); }

  /**
   * Get all operators of a particular type.
   */
  template <class _OpType>
  void getOperatorOfType(std::vector<const _OpType*>& ret) {
    for(operator_const_iterator it = mOperators.begin();
	it != mOperators.end();
	++it) {
      const _OpType * tmp = dynamic_cast<const _OpType *>((*it)->Operator);
      if (tmp != NULL)
	ret.push_back(tmp);
    }
  }
};

#endif
