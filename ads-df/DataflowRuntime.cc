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
#include <boost/assert.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include "DataflowRuntime.hh"
#include "RuntimeOperator.hh"

RuntimeOperatorProcess::RuntimeOperatorProcess(int32_t partitionStart, 
					       int32_t partitionEnd,
					       const RuntimeOperatorPlan& plan)
{
  // Create operators for each subordinate partition.  Bind to the Scheduler for that partition.
  
  for(RuntimeOperatorPlan::intraprocess_fifo_const_iterator it = plan.straight_line_begin();
      it != plan.straight_line_end();
      ++it) {
  }
  for(RuntimeOperatorPlan::interprocess_fifo_const_iterator it = plan.crossbar_begin();
      it != plan.crossbar_end();
      ++it) {
  }
}

RuntimeOperatorProcess::~RuntimeOperatorProcess()
{
}

DataflowScheduler::DataflowScheduler(int32_t partition, int32_t numPartitions)
  :
  mState(INITIALIZED),
  mReadMask(0),
  mWriteMask(0),
  mRequestsOutstanding(0),
  mPartition(partition),
  mNumPartitions(numPartitions),
  mCurrentPort(NULL),
  mMaxWritesBeforeYield(14*10)
{
}

DataflowScheduler::~DataflowScheduler()
{
}

void DataflowScheduler::setOperator(RuntimeOperator * op)
{
      for(RuntimeOperator::input_port_iterator portIt=op->input_port_begin();
	  portIt != op->input_port_end();
	  ++portIt) {
	mDisabled.push_back(**portIt);
	(*portIt)->setDisabled();
      }
      for(RuntimeOperator::output_port_iterator portIt=op->output_port_begin();
	  portIt != op->output_port_end();
	  ++portIt) {
	mDisabled.push_back(**portIt);
	(*portIt)->setDisabled();
      }
}

void DataflowScheduler::cleanup() 
{
  for(int32_t i=0; i<32; i++) {
    mReadQueues[i].clear();
    mWriteQueues[i].clear();
  }
  mDisabled.clear();
}

void DataflowScheduler::runOperator(RuntimePort & port)
{
  // Either there is new data on port to read or there is data on port to flush.
  // This also has the important side effect of clearing the read/write request 
  // that lead to this operator being scheduled.
  port.sync();

  mCurrentPort = &port;
  do {
    RuntimePort * tmp = mCurrentPort;
    mCurrentPort = NULL;
    // During the onEvent call, the op will callback into the scheduler
    // with a read or write request.  Any request that the scheduler determines
    // can be satisfied immediately will result in mCurrentPort being reset.
    // SUBTLE: Note that in a world with dynamic graphs (graph "subroutines"), 
    // the operator we are working on can change while we are in this method.
    // For example consider the case of the sort operator that spawns 
    // dynamic subgraphs to perform merges.  If the sort operator is a refinement
    // sort (uses presorted keys) then a presorted run can spill. 
    // In that scenario, we can enter this method to perform
    // the final writes for merges (a dynamic subgraph operator)
    // which can then return back to the sort 
    // operator that starts to read the next presorted run.
    RuntimeOperator & op(tmp->getOperator());
    op.onEvent(tmp);
  } while (mCurrentPort != NULL);

  while(!mFlush.empty()) {
    // Force a schedule and a sync
    // TODO: We don't handle flushing on non-deterministic write (multiple requests in
    // RequestList).  The question really is whether forcing a flush of one request
    // should force a flush on all requests.
    // This is potentially important if we want to support load balancing partitioning.
    RuntimePort * port = &mFlush.front();
    if (!port->request_unique())
      throw std::runtime_error("Internal Error: Cannot call flush on a port that is in a request list");
    internalRequestFlush(port);
    port->sync();
  }
}

void DataflowScheduler::writeAndSync(RuntimePort * port, RecordBuffer buf)
{
  if (!port->request_unique())
    throw std::runtime_error("Internal Error: Cannot call writeAndSync on a port that is in a request list");
  port->push(buf);
  scheduleFlush(*port);
  internalRequestFlush(port);
  port->sync();
}

void DataflowScheduler::internalRequestRead(RuntimePort * ports)
{
  // TODO: Add assertions
  boost::mutex::scoped_lock sl(mLock);
  RuntimePort * it = ports;
  do {
    RuntimePort &  port (*it);
    BOOST_ASSERT(RuntimePort::DISABLED == port.getQueueIndex());
    mDisabled.erase(mDisabled.iterator_to(port));
    int32_t priority = getReadPriority(port.getSize());
    mReadQueues[priority].push_back(port);
    mReadMask |= (1 << priority);
    port.setQueueIndex(priority);
    mRequestsOutstanding += 1;
    it = it->request_next();
  } while (it != ports);
}

void DataflowScheduler::internalRequestWrite(RuntimePort * ports)
{
  // TODO: Add assertions
  boost::mutex::scoped_lock sl(mLock);
  RuntimePort * it = ports;
  do {
    RuntimePort &  port (*it);
    // If someone has written a bunch of flush records to the output queue,
    // we can wind up here in FLUSH_PENDING.  The common case is that
    // we are in here with DISABLED.
    BOOST_ASSERT(0 > port.getQueueIndex());
    if (RuntimePort::DISABLED == port.getQueueIndex()) {
      mDisabled.erase(mDisabled.iterator_to(port));
    } else {
      mFlush.erase(mFlush.iterator_to(port));
    }
    int32_t priority = getWritePriority(port.getSize());
    mWriteQueues[priority].push_back(port);
    mWriteMask |= (1<<priority);
    port.setQueueIndex(priority);
    mRequestsOutstanding += 1;
    it = it->request_next();
  } while (it != ports);
}

// TODO: Get rid of this method.  It is redundant now that
// internalRequestWrite handles ports in PENDING_FLUSH state.
void DataflowScheduler::internalRequestFlush(RuntimePort * ports)
{
  // TODO: Add assertions
  boost::mutex::scoped_lock sl(mLock);
  RuntimePort * it = ports;
  do {
    RuntimePort &  port (*it);
    BOOST_ASSERT(RuntimePort::PENDING_FLUSH == port.getQueueIndex());
    mFlush.erase(mDisabled.iterator_to(port));
    int32_t priority = getWritePriority(port.getSize());
    mWriteQueues[priority].push_back(port);
    mWriteMask |= (1<<priority);
    port.setQueueIndex(priority);
    mRequestsOutstanding += 1;
    it = it->request_next();
  } while (it != ports);
}

void DataflowScheduler::internalRemoveDisabled(RuntimePort & port)
{
  // No need to lock for access to disabled.
  BOOST_ASSERT(RuntimePort::DISABLED == port.getQueueIndex());
  mDisabled.erase(mDisabled.iterator_to(port));
}

__inline__ uint64_t rdtsc() {
uint32_t lo, hi;
__asm__ __volatile__ (      // serialize
"xorl %%eax,%%eax \n        cpuid"
::: "%rax", "%rbx", "%rcx", "%rdx");
/* We cannot use "=A", since this would use %rax on x86_64 */
__asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
return (uint64_t)hi << 32 | lo;
}

void DataflowScheduler::init()
{
  // Gentlemen, start you're engines...
  start(mOperators.begin(), mOperators.end());
}

bool DataflowScheduler::runSome()
{
  int64_t infinity = std::numeric_limits<int64_t>::max();
  RunCompletion ret = MAX_ITERATIONS_REACHED;
  do {
    ret = runSome(infinity);
  } while(ret == MAX_ITERATIONS_REACHED);
  return ret == NO_REQUESTS_OUTSTANDING;
}

DataflowScheduler::RunCompletion 
DataflowScheduler::runSome(int64_t maxIterations)
{
  // Put myself in RUNNING state while in here.
  // If we exited in STARTING state we need to reenter
  // RUNNING.
  mState = RUNNING;
  while(mRequestsOutstanding && --maxIterations >= 0) {
    mLock.lock();
    uint32_t bitmask = mReadMask;
    uint32_t bitOffset;
    uint8_t zeroFlag;
    __asm__ ("bsrl %2, %%eax;\n\t"
	     "movl %%eax, %0;\n\t"
	     "setzb %1;\n\t"
	     : "=r"(bitOffset), "=r"(zeroFlag)
	     : "r"(bitmask)
	     : "%eax");
    if (!zeroFlag && bitOffset > 0) {
      BOOST_ASSERT(!mReadQueues[bitOffset].empty());
      RuntimePort& port(mReadQueues[bitOffset].front());
      // RuntimeOperator& op(port.getOperator());
      mLock.unlock();
      {
	// uint64_t tick = rdtsc();
	runOperator(port);
	// op.addTicks(rdtsc()-tick);
      }
      continue;
    }

    bitmask = mWriteMask;
    __asm__ ("bsrl %2, %%eax;\n\t"
	     "movl %%eax, %0;\n\t"
	     "setzb %1;\n\t"
	     : "=r"(bitOffset), "=r"(zeroFlag)
	     : "r"(bitmask)
	     : "%eax");
    if(!zeroFlag && bitOffset>0) {
      BOOST_ASSERT(!mWriteQueues[bitOffset].empty());
      RuntimePort& port(mWriteQueues[bitOffset].front());
      // RuntimeOperator& op(port.getOperator());
      mLock.unlock();

      {
	// uint64_t tick = rdtsc();
	runOperator(port);
	// op.addTicks(rdtsc()-tick);
      }
      continue;
    }
    mLock.unlock();

    // Nothing to do right now maybe our controlling thread
    // can do something that will enable forward progress.
    // Return to "STARTING" state so that local queues are disabled
    // and external events can be pumped in.
    mState = STARTING;
    return NO_ENABLED_REQUESTS;
  }
  // Requests have dried up of we've been asked to 
  // yield, we may be done.  Leave state as RUNNING.
  return mRequestsOutstanding == 0 ? NO_REQUESTS_OUTSTANDING : 
    MAX_ITERATIONS_REACHED;
}

void DataflowScheduler::complete()
{
  if (mState == RUNNING) {
    // Really don't want to initiate shutdown if there is anyone
    // still scheduling an operator.
    // Specifically this thread could have seen mNumRequestsOutstanding==0
    // that was set by the last scheduling operation on a different thread,
    // but it is a bad assumption that we can start cleanup before that
    // thread REALLY finished the loop.
    mState = COMPLETING;
    for(std::list<RuntimeOperator*>::const_iterator it=mOperators.begin();
	it != mOperators.end();
	++it) {
      (*it)->shutdown();
    }  
    mState = COMPLETED;
    // uint64_t total=0;
    // std::cout << "Report for partition:" << getPartition() << std::endl;
    // std::cout << "Ticks\tOperator Name\n";
    // for(std::vector<RuntimeOperator*>::iterator it=mOperators.begin();
    // 	it != mOperators.end();
    // 	++it) {
    //   total += (*it)->getTicks();
    //   std::cout << (*it)->getTicks() << "\t" << (*it)->getName().c_str() << "\n";
    // }
    // std::cout << "Total Ticks\n" << total << "\n";
  }
}

void DataflowScheduler::run()
{
  init();
  while(!runSome()) {
    // TODO: Should spin for a bit and then wait in an
    // alertable state.
    // if (0 == --spins) {
    //   std::cout << "Scheduler blocked mRequestsOutstanding=" << mRequestsOutstanding << std::endl;
    //   if (mReadMask & 1) {
    // 	// Iterate through and report back on who is blocked.
    // 	for(RuntimePort::SchedulerQueue::iterator it = mReadQueues[0].begin(); 
    // 	    it != mReadQueues[0].end();
    // 	    ++it) {
    // 	  // Scan to find the blocked port.
    // 	  std::size_t portNum=0;
    // 	  for(RuntimeOperator::input_port_iterator pit=it->getOperator().input_port_begin();
    // 	      pit != it->getOperator().input_port_end();
    // 	      ++pit) {
    // 	    if (*pit == &*it) break;
    // 	    portNum += 1;
    // 	  }
    // 	  std::cout << "Blocked on read: op=" << it->getOperator().getName().c_str() << "; port=" << portNum << std::endl;
    // 	}
    //   }
    //   if (mWriteMask & 1) {
    // 	// Iterate through and report back on who is blocked.
    // 	for(RuntimePort::SchedulerQueue::iterator it = mWriteQueues[0].begin(); 
    // 	    it != mWriteQueues[0].end();
    // 	    ++it) {
    // 	  std::cout << "Blocked on write: " << it->getOperator().getName().c_str() << std::endl;
    // 	}
    //   }
    //   spins = 10000;
    //   boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
    // }
  }
  complete();
}

void DataflowScheduler::scheduleFlush(RuntimePort & port)
{
  if (0 <= port.getQueueIndex()) {
    std::cout << "DataflowScheduler::scheduleFlush port.getQueueIndex()=" << port.getQueueIndex() << std::endl;
  }
  // Port cannot have active request
  BOOST_ASSERT(0 > port.getQueueIndex());
  // No problem writing multiple times with flush requested
  if (RuntimePort::PENDING_FLUSH == port.getQueueIndex()) return;
  // Only other case is disabled.
  BOOST_ASSERT(RuntimePort::DISABLED == port.getQueueIndex());
  mDisabled.erase(mDisabled.iterator_to(port));
  mFlush.push_back(port);
  port.setPendingFlush();
}

bool DataflowScheduler::readWouldBlock(RuntimePort & ports)
{
  RuntimePort * it = &ports;
  do {
    if(it->getLocalSize() > 0 ||
       it->getSize() > 0) {
	return false;
      }
    it = it->request_next();
  } while(it != &ports);
  return true;
}

void DataflowScheduler::readComplete(RuntimePort & ports)
{
  RuntimePort * it = &ports;
  do {
    RuntimePort & port(*it);
    BOOST_ASSERT(port.getQueueIndex()>=0);
    mReadQueues[port.getQueueIndex()].erase(RuntimePort::SchedulerQueue::s_iterator_to(port));
    if (mReadQueues[port.getQueueIndex()].empty())
      mReadMask &= ~(1 << port.getQueueIndex());
    mRequestsOutstanding -= 1;
    mDisabled.push_back(port);
    port.setDisabled();
    it = it->request_next();
  } while(it != &ports);
}

void DataflowScheduler::writeComplete(RuntimePort & ports)
{
  RuntimePort * it = &ports;
  do {
    RuntimePort & port(*it);
    BOOST_ASSERT(port.getQueueIndex()>=0);
    mWriteQueues[port.getQueueIndex()].erase(RuntimePort::SchedulerQueue::s_iterator_to(port));
    if (mWriteQueues[port.getQueueIndex()].empty()) 
      mWriteMask &= ~(1 << port.getQueueIndex());
    mRequestsOutstanding -= 1;
    mDisabled.push_back(port);
    port.setDisabled();
    it = it->request_next();
  } while(it != &ports);
}

void DataflowScheduler::reprioritizeReadRequest(RuntimePort & port)
{
  // Grab the current size of the port and see if it has changed from currently
  // scheduled priority. Do this only if the port has an outstanding request.
  int32_t newPriority = getReadPriority(port.getSize());
  if (port.getQueueIndex() >= 0 && newPriority != port.getQueueIndex()) {
    mReadQueues[port.getQueueIndex()].erase(RuntimePort::SchedulerQueue::s_iterator_to(port));
    if (mReadQueues[port.getQueueIndex()].empty())
      mReadMask &= ~(1 << port.getQueueIndex());
    mReadQueues[newPriority].push_back(port);
    mReadMask |= (1 << newPriority);
    port.setQueueIndex(newPriority);
  }
}

void DataflowScheduler::reprioritizeWriteRequest(RuntimePort & port)
{
  // Grab the current size of the port and see if it has changed from currently
  // scheduled priority. Do this only if the port has an outstanding request.
  int32_t newPriority = getWritePriority(port.getSize());
  if (port.getQueueIndex() >= 0 && newPriority != port.getQueueIndex()) {
    mWriteQueues[port.getQueueIndex()].erase(RuntimePort::SchedulerQueue::s_iterator_to(port));
    if (mWriteQueues[port.getQueueIndex()].empty())
      mWriteMask &= ~(1 << port.getQueueIndex());
    mWriteQueues[newPriority].push_back(port);
    mWriteMask |= (1 << newPriority);
    port.setQueueIndex(newPriority);
  }
}

InProcessFifo::InProcessFifo(DataflowScheduler & sourceScheduler, 
			     DataflowScheduler & targetScheduler,
			     bool buffered)
:
  mRecordsRead(0),
  mSource(NULL),
  mTarget(NULL),
  mSourceScheduler(sourceScheduler),
  mTargetScheduler(targetScheduler),
  mBuffered(buffered)
{
  mSource = new InProcessPort<InProcessFifo>(*this);
  mTarget = new InProcessPort<InProcessFifo>(*this);
}

InProcessFifo::~InProcessFifo()
{
  delete mSource;
  delete mTarget;
}

void InProcessFifo::sync(InProcessPort<InProcessFifo> & port)
{
  if (&port == mSource)
    readAllFromPort();
  else
    writeSomeToPort();
}

void InProcessFifo::writeSomeToPort()
{
  // Move data into the target port.
  // Clear the associated read request in the target port's scheduler
  // Possibly reprioritize a pending request on the source port's scheduler
  // TODO: Update statistics on RecordsRead
  // TODO: Eliminate the call to reprioritze (and the corresponding lock)
  // if the priority hasn't changed.
  boost::mutex::scoped_lock channelGuard(mLock);
  TwoDataflowSchedulerScopedLock schedGuard(mSourceScheduler, mTargetScheduler);
  mQueue.popAndPushSomeTo(mTarget->getLocalBuffer());
  mTargetScheduler.readComplete(*mTarget);
  mSourceScheduler.reprioritizeWriteRequest(*mSource);
}

void InProcessFifo::readAllFromPort()
{
  // Move data from the target port.
  // Clear the associated write request in the source port's scheduler
  // Possibly reprioritize a pending request on the target port's scheduler
  // TODO: Update statistics on RecordsRead
  // TODO: Eliminate the call to reprioritze (and the corresponding lock)
  // if the priority hasn't changed.
  boost::mutex::scoped_lock channelGuard(mLock);
  TwoDataflowSchedulerScopedLock schedGuard(mSourceScheduler, mTargetScheduler);
  mSource->getLocalBuffer().popAndPushAllTo(mQueue);
  mSourceScheduler.writeComplete(*mSource);
  mTargetScheduler.reprioritizeReadRequest(*mTarget); 
}

// class MemcpyStateMachine
// {
// private:
//   // Our goal
//   std::ptrdiff mToMove;

// public:
//   void run(char ** targetBegin, char * targetEnd, 
// 		 const char ** sourceBegin, const char * sourceEnd) {
//     std::ptrdiff targetSz = targetEnd - *targetBegin;
//     std::ptrdiff sourceSz = sourceEnd - *sourceBegin;
//     std::ptrdiff toMove = std::min(std::min(mToMove, sourceSz), targetSz);
//     memcpy(*targetBegin, *sourceBegin, toMove);
//     *targetBegin += toMove;
//     *targetEnd += toMove;
//     mToMove -= toMove;
//   }
// };

RemoteReceiveFifo::RemoteReceiveFifo(DataflowScheduler & sourceScheduler, DataflowScheduler & targetScheduler)
:
  mRecordsRead(0),
  mDataAvailable(0),
  mAvailableSource(NULL),
  mDataSource(NULL),
  mTarget(NULL),
  mSourceScheduler(sourceScheduler),
  mTargetScheduler(targetScheduler)
{
  mAvailableSource = new DataAvailablePort(*this);
  mDataSource = new ReceiveDataPort(*this);
  mTarget = new InProcessPort<RemoteReceiveFifo>(*this);
}

RemoteReceiveFifo::~RemoteReceiveFifo()
{
  delete mAvailableSource;
  delete mDataSource;
  delete mTarget;
}

void RemoteReceiveFifo::sync(ReceiveDataPort & port)
{
  readAllFromPort();
}

void RemoteReceiveFifo::sync(InProcessPort<RemoteReceiveFifo> & port)
{
  writeSomeToPort();
}

void RemoteReceiveFifo::sync(uint64_t available)
{
  boost::mutex::scoped_lock channelGuard(mLock);
  TwoDataflowSchedulerScopedLock schedGuard(mSourceScheduler, mTargetScheduler);
  mDataAvailable += available;
  mSourceScheduler.writeComplete(*mAvailableSource);
  mTargetScheduler.reprioritizeReadRequest(*mTarget);
}

void RemoteReceiveFifo::writeSomeToPort()
{
  // Move data into the target port.
  // Clear the associated read request in the target port's scheduler
  // Possibly reprioritize a pending request on the source port's scheduler
  // TODO: Update statistics on RecordsRead
  // TODO: Eliminate the call to reprioritze (and the corresponding lock)
  // if the priority hasn't changed.
  boost::mutex::scoped_lock channelGuard(mLock);
  TwoDataflowSchedulerScopedLock schedGuard(mSourceScheduler, mTargetScheduler);
  uint64_t before = mQueue.getSize();
  mQueue.popAndPushSomeTo(mTarget->getLocalBuffer());
  mDataAvailable -= (before - mQueue.getSize());
  mTargetScheduler.readComplete(*mTarget);
  mSourceScheduler.reprioritizeWriteRequest(*mDataSource);
}

void RemoteReceiveFifo::readAllFromPort()
{
  // Move data from the target port.
  // Clear the associated write request in the source port's scheduler
  // Possibly reprioritize a pending request on the target port's scheduler
  // TODO: Update statistics on RecordsRead
  // TODO: Eliminate the call to reprioritze (and the corresponding lock)
  // if the priority hasn't changed.
  boost::mutex::scoped_lock channelGuard(mLock);
  TwoDataflowSchedulerScopedLock schedGuard(mSourceScheduler, mTargetScheduler);
  mDataSource->getLocalBuffer().popAndPushAllTo(mQueue);
  mSourceScheduler.writeComplete(*mDataSource);
  mTargetScheduler.reprioritizeReadRequest(*mTarget);
}

