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

#if !defined(__DATAFLOW_RUNTIME_HH)
#define __DATAFLOW_RUNTIME_HH

#include <vector>
#include <list>

#include <boost/intrusive/list.hpp>
#include <boost/thread/mutex.hpp>

#include "RuntimePort.hh"
#include "RuntimePlan.hh"

class RuntimeOperatorType;
class RuntimePort;
class RuntimeOperator;
class InProcessFifo;
class DataflowScheduler;

/**
 * A runtime operator process contains a collection of partitions that
 * are executed by one or more threads.
 */
class RuntimeOperatorProcess
{
private:
  int32_t mPartitionStart;
  int32_t mPartitionEnd;

public:
  RuntimeOperatorProcess(int32_t partitionStart, 
			 int32_t partitionEnd,
			 const RuntimeOperatorPlan& plan);
  ~RuntimeOperatorProcess();
};

/**
 * A partition represents two things (perhaps best to split them apart): 
 * 1) a physical
 * subset of the data being processed and the set of operators that works on that 
 * subset of data
 * 2) The thread and the scheduling of those operators.
 * The reason that it may be best to split them up is because we may want
 * to support modes in which either there are multiple threads working on
 * a single partition (e.g. pipeline parallelism) or a single thread (pool)
 * handles multiple partitions.
 * At the end of the day, this thing boils down to a bunch of operators that
 * are being scheduled.
 * For the moment, I am inclined to do without the abstraction...
 */
// class RuntimeOperatorPartition
// {
// private:
//   std::vector<InProcessFifo *> mInProcessFifos;
//   std::vector<RuntimeOperator *> mOperators;
// public:
// };

template <class _Fifo>
class InProcessPort : public RuntimePort
{
private:
  _Fifo & mChannel;

public:
  InProcessPort(_Fifo& channel)
    :
    mChannel(channel)
  {
  }

  ~InProcessPort()
  {
  }

  /**
   * Transfer between local endpoint and the channel fifo.
   */
  void sync() { mChannel.sync(*this); }
  
  /**
   * Return the size of the attached channel.
   */
  uint64_t getSize() const 
  {
    return mChannel.getSize(); 
  }

  /**
   * The local buffer on this port.
   */
  RuntimePort::local_buffer_type & getLocalBuffer() 
  { 
    return mLocalBuffer; 
  }
};

class InProcessFifo {
private:
  /**
   * Lock that protects mQueue.  This lock must be taken before taking any locks
   * on scheduler queues.
   */
  boost::mutex mLock;
  /**
   * The actual fifo itself.
   */
  RuntimeFifo<RecordBuffer, 14> mQueue;
  // Total number of records that have entered this fifo (including those that
  // have left).
  uint64_t mRecordsRead;
  // Two ends of the fifo for folks to talk to.
  // Source is the input into this.
  // Target is the output of this.
  InProcessPort<InProcessFifo> * mSource;
  InProcessPort<InProcessFifo> * mTarget;
  // We need to be able to talk to the operator scheduler
  // to tell it about changes to my state.
  DataflowScheduler & mSourceScheduler;
  DataflowScheduler & mTargetScheduler;
  /**
   * Buffering state.  In general buffers are necessary when a dataflow
   * has any cycles (undirected or directed) in it.  On the other hand,
   * too much buffering in a dataflow can cause memory bloat and limits
   * scalability with respect to operator complexity so we want to be
   * able to disable (or severely limit) buffers when not needed.
   */
  bool mBuffered;
public:
  InProcessFifo(DataflowScheduler & sourceScheduler, 
		DataflowScheduler & targetScheduler,
		bool buffered=true);
  ~InProcessFifo();

  /**
   * The source for the fifo.
   */
  InProcessPort<InProcessFifo> * getSource() 
  {
    return mSource; 
  }
  
  /** 
   * The target for the fifo.
   */
  InProcessPort<InProcessFifo> * getTarget()
  {
    return mTarget;
  }
  
  /**
   * Implement sync on behalf of the ports.
   */
  void sync(InProcessPort<InProcessFifo> & port);

  /**
   * Move a chunk of data from the channel queue into the target port 
   * local cache.   Reach into the scheduler(s) associated with the ports and
   * notify them about the event.
   */
  void writeSomeToPort();
  /**
   * Move all the data in the local cache of the source into the channel
   * queue.  Reach into the scheduler(s) associated with the ports and
   * notify them about the event.
   */
  void readAllFromPort();

  /**
   * The number of elements currently in the channel.
   * Does not account for the number of elements in
   * read endpoint or write endpoints.  Note that it 
   * could safely account for the size of the target cache.
   * In fact that might be convenient when thinking of other
   * types of channels in which the size of the target cache
   * might be much larger (here I am thinking of a port
   * that serves as a proxy for a very large buffer in a different
   * thread or machine).
   * If the channel is not buffered then size is either 0 or
   * "infinity".
   */
  uint64_t getSize() 
  {
    return mBuffered || 0==mQueue.getSize() ? mQueue.getSize() : std::numeric_limits<int32_t>::max();
  }

  void setBuffered(bool buffered) 
  {
    mBuffered = buffered;
  }
};

/**
 * A special channel for receiving data from a remote channel.
 * It tracks the number of channel capacity as a number that is
 * separate from the actual number of records in this process.
 * 
 * The channel has an ordinary InProcessPort on the target and
 * has two special endpoints on the source.
 * One of the source ports provides a stream of updates to the
 * channel capacity.  This channel capacity is provided to the target
 * as the channel size for scheduling purposes.
 */
class RemoteReceiveFifo
{
private:
  /**
   * Lock that protects mQueue.  This lock must be taken before taking any locks
   * on scheduler queues.
   */
  boost::mutex mLock;
  /**
   * The actual fifo itself.
   */
  RuntimeFifo<RecordBuffer, 14> mQueue;
  // Total number of records that have entered this fifo (including those that
  // have left).
  uint64_t mRecordsRead;
  // Number of records that are available on the remote channel.  Most of
  // these records are buffered up on the sender side.
  uint64_t mDataAvailable;
  // Two ends of the fifo for folks to talk to.
  // AvailableSource is the input for updates to remote channel capacity.
  class DataAvailablePort * mAvailableSource;
  // DataSource is the input for data arriving from the remote channel.
  class ReceiveDataPort * mDataSource;
  // Target is the output of this.
  InProcessPort<RemoteReceiveFifo> * mTarget;
  // We need to be able to talk to the operator scheduler
  // to tell it about changes to my state.
  DataflowScheduler & mSourceScheduler;
  DataflowScheduler & mTargetScheduler;
public:
  RemoteReceiveFifo(DataflowScheduler & sourceScheduler, DataflowScheduler & targetScheduler);
  ~RemoteReceiveFifo();

  /**
   * The data available end of the source
   */
  DataAvailablePort * getAvailableSource()
  {
    return mAvailableSource;
  }

  /**
   * The actual data source
   */
  ReceiveDataPort * getDataSource()
  {
    return mDataSource;
  }
  
  /** 
   * The target for the fifo.
   */
  InProcessPort<RemoteReceiveFifo> * getTarget()
  {
    return mTarget;
  }
  
  /**
   * Implement sync on behalf of the ports.
   */
  void sync(class ReceiveDataPort & port);
  void sync(InProcessPort<RemoteReceiveFifo> & port);
  void sync(uint64_t available);

  /**
   * Move a chunk of data from the channel queue into the target port 
   * local cache.   Reach into the scheduler(s) associated with the ports and
   * notify them about the event.
   */
  void writeSomeToPort();
  /**
   * Move all the data in the local cache of the data source into the channel
   * queue.  Reach into the scheduler(s) associated with the ports and
   * notify them about the event.
   */
  void readAllFromPort();

  /**
   * The number of elements currently in the channel.
   * Does not account for the number of elements in
   * read endpoint or write endpoints.  Note that it 
   * could safely account for the size of the target cache.
   * In fact that might be convenient when thinking of other
   * types of channels in which the size of the target cache
   * might be much larger (here I am thinking of a port
   * that serves as a proxy for a very large buffer in a different
   * thread or machine).
   */
  uint64_t getSize() 
  {
    // Can't report non-zero size if we really don't have data to provide.
    // Obviously things aren't in good shape if mDataAvailable but mQueue.getSize()
    // is zero.  
    // TODO: Report on when we stall here.
    return mQueue.getSize() ? mDataAvailable : 0;
  }
  uint64_t getSourceSize() 
  {
    // TODO: Should we implement finite channel capacity here?
    return mQueue.getSize(); 
  }
};

class DataAvailablePort : public RuntimePort
{
  // We are tightly coupled to the fifo.
  friend class RemoteReceiveFifo;
private:
  RemoteReceiveFifo & mChannel;

  RuntimePort::local_buffer_type & getLocalBuffer() { return mLocalBuffer; }

public:
  DataAvailablePort(RemoteReceiveFifo& channel)
    :
    mChannel(channel)
  {
  }

  ~DataAvailablePort()
  {
  }

  /**
   * Transfer between local endpoint and the channel fifo.
   */
  void sync() 
  { 
    // Aggregate the values in the local buffer
    // and then update the channel.
    uint64_t avail=0;
    while(!mLocalBuffer.empty()) {
      RecordBuffer tmp;
      mLocalBuffer.Pop(tmp);
      if(!RecordBuffer::isEOS(tmp)) {
	avail += *reinterpret_cast<int32_t *>(tmp.Ptr);
	delete [] tmp.Ptr;
      } else {
	// EOS counts as a record!
	avail += 1;
      }
    }
    // Always call sync because this is what clears the write request.
    mChannel.sync(avail);
  }
  
  /**
   * Return the size of the attached channel.
   * Here we return 0 since DataAvailable message should never
   * block.
   */
  uint64_t getSize() const 
  {
    return 0;
  }
};

class ReceiveDataPort : public RuntimePort
{
  // We are tightly coupled to the fifo.
  friend class RemoteReceiveFifo;
private:
  RemoteReceiveFifo & mChannel;

  RuntimePort::local_buffer_type & getLocalBuffer() { return mLocalBuffer; }

public:
  ReceiveDataPort(RemoteReceiveFifo& channel)
    :
    mChannel(channel)
  {
  }

  ~ReceiveDataPort()
  {
  }

  /**
   * Transfer between local endpoint and the channel fifo.
   */
  void sync() { mChannel.sync(*this); }
  
  /**
   * Return the size of the attached channel.  We get this
   * from the special size that refers to the actual contents
   * of the fifo and not the getSize method which is 
   * retrieving the total of the amounts reported available
   * by the sender side of the remote channel.
   */
  uint64_t getSize() const 
  {
    return mChannel.getSourceSize(); 
  }
};

/**
 * An IntraSchedulerFifo connects RuntimePorts of two operators within a scheduler (hence within
 * an OS thread).  This means that the fifo requires no synchronization.
 * TODO: Figure out whether it is worth implementing this.
 */

/**
 * An operator scheduler manages a collection of runtime operators
 * and decides when they run.  It does this by carefully managing the
 * state of the operator's ports and watching how full the corresponding
 * channels are.
 */
class DataflowScheduler {
  friend class TwoDataflowSchedulerScopedLock;
private:

  /**
   * Lock that protects the internal scheduler queues. Using Boost threads/pthreads for now.
   * Will evaluate performance of this vs. user defined spin locks.
   */
  boost::mutex mLock;

  /**
   * These are the operators I am scheduling.
   */
  std::list<RuntimeOperator*> mOperators;
  
  /**
   * Current state of the scheduler.  Think more carefully about the
   * synchronization aspect of this state if there are multiple worker
   * threads banging against this state (specifically a possible race
   * condition as we transition from running to completing).
   */
  enum SchedulerState { INITIALIZED, STARTING, RUNNING, COMPLETING, COMPLETED };
  SchedulerState mState;

  /**
   * Read queues for different priorities.
   * Bitmask inidicating whether a queue in mReadQueues is empty or not.
   */
  uint32_t mReadMask;
  RuntimePort::SchedulerQueue mReadQueues[32];

  /**
   * Write queues for different priorities.
   * Bitmask inidicating whether a queue in mWriteQueues is empty or not.
   */
  uint32_t mWriteMask;
  RuntimePort::SchedulerQueue mWriteQueues[32];

  /**
   * Ports that have no pending read or write request
   */
  RuntimePort::SchedulerQueue mDisabled;

  /** 
   * Total number of ports waiting to be scheduled.
   * This is just the total number of ports in all of
   * the read queues and write queues together.
   * When this drops to zero the scheduler exits.
   */
  int32_t mRequestsOutstanding;

  /**
   * Partition this scheduler is assigned to.
   * Not sure this is really meaningful to statically assign this
   * but right now it is simply a mechanism of communicating to
   * operators that care.
   */
  int32_t mPartition;
  int32_t mNumPartitions;

  /** 
   * Put a list of read requests onto the scheduler queues.
   */
  void internalRequestRead(RuntimePort * port);

  /** 
   * Put a list of write requests onto the scheduler queues.
   * Assumes they are on the disabled queue.
   */
  void internalRequestWrite(RuntimePort * port);

  /** 
   * Put a list of write requests onto the scheduler queues.
   * Assumes they are on the flush queue.
   */
  void internalRequestFlush(RuntimePort * port);

  /**
   * Remove a disabled port from the scheduling queue.
   * Presumably for decomissioning a subgraph.
   */
  void internalRemoveDisabled(RuntimePort & port);

  /**
   * The following state is specific to the method for implementing the runOperator
   * method.  At the moment I don't anticipate multiple scheduler variations but if
   * so it might be worthwhile putting this in a subclass or a policy object.
   * Note also that we might want to pull this state out of the scheduler core
   * entirely since we might want to have multiple scheduler threads working on
   * the same set of scheduler queues and we would like each worker thread
   * to have its own private state below.
   */

  /**
   * Ports that need to be flushed (only write ports at this point).
   */
  RuntimePort::SchedulerQueue mFlush;

  /** 
   * The port I am supposed to process next for a given 
   * operator we have selected to run.
   */
  RuntimePort * mCurrentPort;

  /**
   * This number controls the scheduling granularity for operators.
   * It is implemented by putting a cap on the number of records
   * produced.
   */
  uint64_t mMaxWritesBeforeYield;

  /** 
   * Run an operator for a bit of time.
   */
  void runOperator(RuntimePort & port);

  /**
   * Schedule a port for later flush.
   */
  void scheduleFlush(RuntimePort & port);

public:
  DataflowScheduler(int32_t partition=0, int32_t numPartitions=1);
  ~DataflowScheduler();
  // TODO: Not sure we want this here.  It is really here because the 
  // RuntimeOperator needs this at creation time.
  int32_t getPartition() const 
  {
    return mPartition;
  }
  int32_t getNumPartitions() const 
  {
    return mNumPartitions;
  }
  // TODO: Fix this API.
  void setOperator(RuntimeOperator * op);
  template <typename _InputIterator>
  void setOperators(_InputIterator opBegin, _InputIterator opEnd)
  {
    mOperators.insert(mOperators.end(), opBegin, opEnd);
    for(_InputIterator it=opBegin; it != opEnd;	++it) {
      setOperator(*it);
    }
  }
  template <typename _Container>
  void setOperators(const _Container&  opsToSchedule)
  {
    setOperators(opsToSchedule.begin(), opsToSchedule.end());
  }
  template <typename _InputIterator>
  void shutdownAndRemoveOperators(_InputIterator opBegin, _InputIterator opEnd)
  {
    for(_InputIterator it = opBegin; it != opEnd; ++it) {
      (*it)->shutdown();
      mOperators.remove(*it);
    } 
    // TODO: We are leaving the operators ports in the scheduling
    // queues for a subtle reason: some of them may still be needed.
    // In particular, we have modeled the ports as belonging to channels
    // rather than operators.  So in the case in which we have dynamically
    // added a subgraph to the dataflow and want to shut it down, we allow
    // the "boundary" ports of the subgraph to be reconnected to other operators
    // (perhaps of a new dynamic graph or perhaps existing operators in the 
    // "static" graph).  Look at the sort operator merge process for an 
    // example of this.  We don't actually know which of the ports attached
    // to the operators will be reused in this way.  For the moment we
    // deal with this by providing an API (below) for a client to remove channels;
    // this places the burden on the client to track which channels
    // are "internal" to the subgraph being decommissioned.
  }
  template <typename _InputIterator>
  void removeChannels(_InputIterator channelBegin, _InputIterator channelEnd)
  {
    for(_InputIterator it = channelBegin; it != channelEnd; ++it) {
      internalRemoveDisabled(*(*it)->getSource());
      internalRemoveDisabled(*(*it)->getTarget());
    }
  }

  /**
   * Initialize operators in the dataflow.  In particular, give 
   * each the opportunity to make its first read/write request 
   * back to the scheduler.
   */
  void init();
  /**
   * The reasons why runSome can return.
   * MAX_ITERATIONS_REACHED - The configured number of iterations has been 
   * executed and there are outstanding dataflow requests.
   * NO_REQUESTS_OUTSTANDING - The dataflow has no outstanding requests 
   * (usually this means the flow has completed execution).
   * NO_ENABLED_REQUESTS - There are operators with outstanding requests
   * on ports, but all such requests are blocked.  Presumably these are
   * blocked waiting on an event external to the dataflow (e.g. network
   * packets arriving).  The caller is responsible for getting those
   * events into the dataflow (e.g. through a NativeInputQueueOperator).
   */
  enum RunCompletion { MAX_ITERATIONS_REACHED, NO_REQUESTS_OUTSTANDING, NO_ENABLED_REQUESTS };
  RunCompletion runSome(int64_t maxIterations);
  /**
   * Run the dataflow until no more forward progress can be made.
   * Return TRUE if the dataflow has completed execution and
   * FALSE if the dataflow is stalled due to unsatisfied read requests.
   * In the latter case it is incumbent on the caller to modify the
   * state of the dataflow to unblock the read (e.g. send data into
   * a NativeInputQueueOperator).
   */
  bool runSome();
  /**
   * Call shutdown on each operator to free up resources.
   */
  void complete();

  /**
   * Run the entire scheduling lifecycle of the dataflow 
   * (init, runSome, complete).  Useful for executing
   * a dataflow when you do not need it to non-preemptively
   * yield and when you do not need to pass external data
   * into a dataflow (e.g. a dataflow in a Hadoop reducer
   * needs the dataflow to yield back to the caller in order
   * to get more data from Hadoop).  Non networked single
   * process dataflows generally satisfy these constraints.
   */
  void run();

  /**
   * Reset internal state of the scheduler upon completion.
   * TODO: Should this be folded into complete?
   */
  void cleanup();
  // Important special case of requesting a read/write for a single port.
  // In this case it is definitely worth inlining.
  void requestRead(RuntimePort * ports)
  {
    // If any request in the list can be satisfied
    // out of local queue then do it.
    RuntimePort * it = ports;
    do {
      if(it->getLocalSize()) {
	mCurrentPort = &*it;
	return;
      }
      it = it->request_next();
    } while (it != ports);

    // Nothing there!  Enqueue the requests in the scheduler queues.
    internalRequestRead(ports);
  }
  void requestWrite(RuntimePort * ports)
  {
    if (mState != STARTING) {
      // A write is satisfied immediately if the corresponding local queue
      // is small enough.
      RuntimePort * it = ports;
      do {
	if(it->getLocalSize() < mMaxWritesBeforeYield) {
	  mCurrentPort = &*it;
	  return;
	}
	it = it->request_next();
      } while (it != ports);
    }

  // Pass the request on to be queued and we yield.
  internalRequestWrite(ports);
  }
  void read(RuntimePort * port, RecordBuffer& buf)
  {
    port->pop(buf);
  }
  void write(RuntimePort * port, RecordBuffer buf, bool flush)
  {
    port->push(buf);
    if (flush) {
      scheduleFlush(*port);
    }
  }
  /**
   * Experimental interface to test performance impact of
   * batched dispatch.
   */
  RuntimePort::local_buffer_type& readLocal(RuntimePort * port)
  {
    return port->getLocalBuffer();
  }
  void flushLocal(RuntimePort * port)
  {
    scheduleFlush(*port);
  }
  /**
   * For special cases, write through immediately to channel ignoring
   * scheduler.  Use with caution since we may not keep this.
   */
  void writeAndSync(RuntimePort * port, RecordBuffer buf);

  /**
   * Indicate whether a collection of reads would block.
   */
  bool readWouldBlock(RuntimePort & ports);

  void readComplete(RuntimePort & port);
  void writeComplete(RuntimePort & port);
  void reprioritizeReadRequest(RuntimePort & port);
  void reprioritizeWriteRequest(RuntimePort & port);

  /**
   * Calculate the priority of a read on a channel of size sz.
   */
  static int32_t getReadPriority(uint64_t sz)
  {
    uint32_t truncatedPriority = (uint32_t) std::min(sz, (uint64_t) std::numeric_limits<int32_t>::max());
    uint32_t bitOffset;
    uint8_t zeroFlag;
    __asm__ ("bsrl %2, %%eax;\n\t"
	     "movl %%eax, %0;\n\t"
	     "setzb %1;\n\t"
	     : "=r"(bitOffset), "=r"(zeroFlag)
	     : "r"(truncatedPriority)
	     : "%eax");
    return zeroFlag ? 0 : bitOffset+1;
  }

  /**
   * Calculate the priority of a read on a channel of size sz.
   */
  static int32_t getWritePriority(uint64_t sz)
  {
    return 31-getReadPriority(sz);
  }

  /**
   * Start a collection of operators in the scheduler.
   */
  template <class _InputIterator>
  void start(_InputIterator beginOp, _InputIterator endOp)
  {
    SchedulerState prevState = mState;
    mState = STARTING;
    for(_InputIterator it=beginOp; it != endOp; ++it) {
      // uint64_t tick = rdtsc();
      (*it)->start();
      // (*it)->addTicks(rdtsc()-tick);
    }
    // Enter running state.  Note that this may seem redundant
    // given that runSome enters the RUNNING state at its beginning.
    // This is really here to handle the case in which an operator 
    // calls start during its event loop (hence inside of a call to 
    // runSome).  This can and does happen when an
    // operator dynamically modifies the dataflow graph (e.g. sort
    // operator does this when it sets up its merge phase).
    // Clearly this state flag business is an error prone thing
    // and I'd like a better way of handling it.
    if (prevState == RUNNING)
      mState = RUNNING;
  }
};

class TwoDataflowSchedulerScopedLock
{
private:
  DataflowScheduler & mOne;
  DataflowScheduler & mTwo;
public:
  TwoDataflowSchedulerScopedLock(DataflowScheduler & one, DataflowScheduler & two)
    :
    mOne(one),
    mTwo(two)
  {
    if (&mOne == &mTwo)		
      mOne.mLock.lock();
    else if (&mOne < &mTwo) {
      mOne.mLock.lock();
      mTwo.mLock.lock();
    } else {
      mTwo.mLock.lock();
      mOne.mLock.lock();
    }    
  }
  ~TwoDataflowSchedulerScopedLock()
  {
    mOne.mLock.unlock();
    if (&mOne != &mTwo) 
      mTwo.mLock.unlock();
  }
};

#endif
