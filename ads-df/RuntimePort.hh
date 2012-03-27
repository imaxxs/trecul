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

#ifndef __RUNTIMEPORT_HH
#define __RUNTIMEPORT_HH

#include <boost/intrusive/list.hpp>
#include "RecordBuffer.hh"

class RuntimeOperator;

template<class _T> 
class IntrusiveFifo
{
private:
  uint64_t mSize;
  _T * mHead;
  _T * mTail;  
public:
  IntrusiveFifo()
    :
    mSize(0),
    mHead(NULL),
    mTail(NULL)
  {
  }
  
  void Push(_T * elmt)
  {
    if (mTail)
      mTail->mNext = elmt;
    else
      mHead = elmt;
    elmt->mNext = NULL;
    mTail = elmt;
    mSize += 1;
  }

  void Pop(_T *& elmt)
  {
    mSize -= 1;
    elmt = mHead;
    mHead = mHead->mNext;
    elmt->mNext = NULL;
    if (!mHead)
      mTail = NULL;
  }

  uint64_t GetSize() const {
    return mSize;
  }
  uint64_t getSize() const {
    return mSize;
  }
};

/**
 * Runtime queue implemented as a intrusive list of circular arrays.
 */

/**
 * The circular arrays are allocated as pages.  These shall
 * be aligned to cache line boundaries.
 */
template <class _T, int N>
class RuntimeFifoPage {
public:
  RuntimeFifoPage * mNext;

  // Disallow copying
  RuntimeFifoPage(const RuntimeFifoPage& ) {}
  RuntimeFifoPage & operator=(const RuntimeFifoPage& ) {}
private:
  // TODO: Proper calculation here.  Account for x86 and x64.
  // TODO: Static assertion that N must be less than 256.
  _T mElements[N];
  uint8_t mStart;
  uint8_t mEnd;
  uint8_t mSize;
public:
  RuntimeFifoPage(RuntimeFifoPage * next)
    :
    mNext(next),
    mStart(0),
    mEnd(0),
    mSize(0)
  {
  }

  void Push(_T elmt) {
    mSize += 1;
    mElements[mEnd] = elmt;
    mEnd = mEnd == N-1 ? 0 : (mEnd + 1);
  }

  void Pop(_T& elmt) {
    mSize -= 1;
    elmt = mElements[mStart];
    mStart = mStart == N-1 ? 0 : mStart+1;
  }

  _T& top() {
    return mElements[mStart];
  }

  _T& bottom() {
    return mElements[mEnd ? mEnd-1 : N-1];
  }

  uint8_t GetSize() const {
    return mSize; 
  }
};

template <class _T, int N>
class RuntimeFifo {
private:
  uint64_t mSize;
  RuntimeFifoPage<_T, N> * mHead;
  RuntimeFifoPage<_T, N> * mTail;

  // Disallow copying
  RuntimeFifo(const RuntimeFifo& ) {}
  RuntimeFifo & operator=(const RuntimeFifo& ) {}

  RuntimeFifoPage<_T, N> * allocPage()
  {
    return new RuntimeFifoPage<_T,N>(NULL);
  }
  void freePage(RuntimeFifoPage<_T, N> * pg)
  {
    delete pg;
  }
public:
  RuntimeFifo()
    :
    mSize(0),
    mHead(NULL),
    mTail(NULL)
  {
    mHead = mTail = allocPage();
  }
  
  ~RuntimeFifo()
  {
    while(mHead) {
      RuntimeFifoPage<_T, N> * tmp = mHead;
      mHead = mHead->mNext;
      freePage(tmp);
    }      
  }

  void Push(_T elmt) {
    if (mTail->GetSize() == N) {
      RuntimeFifoPage<_T, N> * tmp = allocPage();
      mTail->mNext = tmp;
      mTail = tmp;
    }
    mTail->Push(elmt);
    mSize += 1;
  }

  void Pop(_T& elmt) {
    mHead->Pop(elmt);
    mSize -= 1;
    if (mHead->GetSize() == 0 && mHead->mNext != 0) {
      RuntimeFifoPage<_T, N> * tmp = mHead;
      mHead = mHead->mNext;
      freePage(tmp);
    }
  }

  _T& top() {
    return mHead->top();
  }

  _T& bottom() {
    return mTail->bottom();
  }

  void swap(RuntimeFifo& other) {
    std::swap(mSize, other.mSize);
    std::swap(mHead, other.mHead);
    std::swap(mTail, other.mTail);
  }

  void popAndPushAllTo(RuntimeFifo& targetFifo) {
    // Fast common case is that targetFifo.mTail is either
    // empty or full.  Right now we don't handle partially
    // filled pages in the middle of the fifo so we have to
    // compact in the unlikely event that we see a partially
    // filled tail.  Note that for our application tails
    // are almost always full because we have structured our
    // operator scheduler in such a way that we tend to write
    // full pages to fifos.
    if (targetFifo.mTail->GetSize() == N && mHead->GetSize() == N) {
      targetFifo.mTail->mNext = mHead;
      targetFifo.mTail = mTail;
      targetFifo.mSize += mSize;
      mSize = 0;
      mHead = mTail = allocPage();
    } else if (targetFifo.mTail->GetSize() == 0 && mHead->GetSize() == N) {
      // Should only occur when targetFifo.mHead = targetFifo.mTail (i.e. targetFifo.empty())
      // Just swap out the state of these fifos
      swap(targetFifo);
    } else {
      // Slow path can still be optimized I suppose
      while(getSize()) {
	_T tmp;
	Pop(tmp);
	targetFifo.Push(tmp);
      }
    }
  }
  
  /**
   * TODO: Implement proper method with configurable
   * size that moves in page chunks when possible.
   * Right now this moves a single page.
   */
  void popAndPushSomeTo(RuntimeFifo& targetFifo) {
    popAndPushAllTo(targetFifo);
    // if (targetFifo.mTail->GetSize() == N && mHead->GetSize() == N) {
    //   targetFifo.mTail->mNext = mHead;
    //   mHead = mHead->mNext;
    //   targetFifo.mTail->mNext = NULL;
    //   targetFifo.mSize += N;
    //   mSize -= N;
    //   if (mHead == NULL)
    // 	mHead = mTail = allocPage();
    // } else if (targetFifo.mTail->GetSize() == 0 && mHead->GetSize() == N) {
    //   // Should only occur when targetFifo.mHead = targetFifo.mTail (i.e. targetFifo.empty())
    //   if (getSize() == N)
    // 	swap(targetFifo);
    //   else {
    // 	freePage(targetFifo.mHead);
    // 	targetFifo.mHead = targetFifo.mTail = mHead;
    // 	mHead = mHead->mNext;
    // 	targetFifo.mHead->mNext = 0;
    // 	targetFifo.mSize = N;
    // 	mSize -= N;
    //   }
    // } else {
    //   // Slow path can still be optimized I suppose
    //   int32_t cnt=0;
    //   while(getSize() != 0 && cnt++ < N) {
    // 	_T tmp;
    // 	Pop(tmp);
    // 	targetFifo.Push(tmp);
    //   }
    // }
  }
 
  uint64_t GetSize() const {
    return mSize; 
  }
  uint64_t getSize() const {
    return mSize;
  }

  bool empty() const {
    return mHead == mTail && mHead->mNext == 0 && mSize==0 && mHead->GetSize()==0;
  }

  // Testing only!  This is slow.
  uint64_t GetNumPages() const {
    uint64_t num = 0;
    RuntimeFifoPage<_T, N> * tmp = mHead;
    while(tmp) {
      num += 1;
      tmp = tmp->mNext;
    }
    return num;
  }

  // Testing only!  This is slow.
  bool TestPageSizes(const std::vector<uint8_t>& pages) const {
    std::size_t num = 0;
    RuntimeFifoPage<_T, N> * tmp = mHead;
    while(tmp) {
      if (num >= pages.size() || tmp->GetSize() != pages[num++]) return false;
      tmp = tmp->mNext;
    }
    return num==pages.size();
  }
};

/**
 * RuntimePort represents an input or an output of a RuntimeOperator.
 * RuntimePorts can either connect ports that are within a thread, ports
 * between threads or ports that process/machine boundaries.
 * RuntimePorts have local RecordBuffer queues to which they have
 * exclusive access.  These queues may be (and are) used to batch up
 * transfers to channels.
 *
 * The fact that there are different kinds of channels in the world means
 * that RuntimePort is an abstract base class.  A RuntimePort that is 
 * connected to a channel is a concrete class.
 */
class RuntimePort {
public:
  /**
   * Doubly linked list so that port can be managed on scheduler queues.
   */
  typedef boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::normal_link> > link_type;
  link_type mSchedulerQueueHook;

  typedef boost::intrusive::member_hook<RuntimePort, 
					link_type, 
					&RuntimePort::mSchedulerQueueHook> SchedulerQueueOption;
  typedef boost::intrusive::list<RuntimePort, SchedulerQueueOption, boost::intrusive::constant_time_size<false> > SchedulerQueue;

  /**
   * Doubly linked list so that ports can be threaded into lists for scheduler request.
   */
  link_type mRequestHook;

  typedef boost::intrusive::member_hook<RuntimePort, 
					link_type, 
					&RuntimePort::mRequestHook> RequestOption;
  typedef RuntimePort::RequestOption::member_value_traits request_value_traits;
  typedef RuntimePort::RequestOption::member_value_traits::node_ptr request_node_ptr;
  typedef boost::intrusive::circular_list_algorithms<RuntimePort::RequestOption::member_value_traits::node_traits> node_algorithms;
  // typedef boost::intrusive::list<RuntimePort, RequestOption, boost::intrusive::constant_time_size<false> > RequestList;
  void request_init()
  {
    request_node_ptr This = request_value_traits::to_node_ptr(*this);
    node_algorithms::init_header(This);    
  }
  bool request_unique()
  {
    request_node_ptr This = request_value_traits::to_node_ptr(*this);
    return node_algorithms::unique(This);    
  }
  std::size_t request_count()
  {
    request_node_ptr This = request_value_traits::to_node_ptr(*this);
    return node_algorithms::count(This);    
  }
  RuntimePort * request_unlink()
  {
    request_node_ptr This = request_value_traits::to_node_ptr(*this);
    RuntimePort * n = request_value_traits::to_value_ptr(This->next_);
    node_algorithms::unlink(This);    
    node_algorithms::init_header(This);
    return n == this ? NULL : n;
  }
  void request_link_after(RuntimePort & p)
  {
    request_node_ptr This = request_value_traits::to_node_ptr(*this);
    request_node_ptr P = request_value_traits::to_node_ptr(p);
    node_algorithms::link_after(This, P);
  }
  RuntimePort * request_next()
  {
    request_node_ptr n = request_value_traits::to_node_ptr(*this);
    return request_value_traits::to_value_ptr(n->next_);
  }

  static const int32_t DISABLED = -1;
  static const int32_t PENDING_FLUSH = -2;

  typedef RuntimeFifo<RecordBuffer, 14> local_buffer_type;

protected:

  /**
   * Local buffer of records.
   * This port has exclusive access to this hence needn't worry about locking.
   */
  local_buffer_type mLocalBuffer;

private:

  /**
   * RuntimeOperator that this port is connected to.
   */
  RuntimeOperator * mOperator;

  /** 
   * Record where this port is in the scheduler's queues.
   * Special value -1 means that port in in the disabled queue.
   * Special value -2 means that the port is in the flush queue.
   */
  int32_t mQueueIndex;

public:
  RuntimePort() 
    :
    mOperator(NULL),
    mQueueIndex(DISABLED)
  {
    request_init();
  }

  virtual ~RuntimePort()
  {
  }

  /**
   * Connect this port to an operator.
   */
  void setOperator(RuntimeOperator& op) { mOperator = &op; }

  /**
   * Illegal to call this if an operator hasn't been set.
   */
  RuntimeOperator& getOperator() { return *mOperator; }

  /**
   * Get a pointer to the operator attached to the port.
   */
  RuntimeOperator * getOperatorPtr() { return mOperator; }

  int32_t getQueueIndex() const { return mQueueIndex; }
  void setQueueIndex(int32_t val)  { mQueueIndex = val; }
  void setDisabled() { mQueueIndex = DISABLED; }
  void setPendingFlush() { mQueueIndex = PENDING_FLUSH; }

  /**
   * Returns the number of elements in the ports local buffer.
   */
  uint64_t getLocalSize() const 
  {
    return mLocalBuffer.getSize();
  }

  /**
   * Fast path access directly to the local buffer.
   */
  local_buffer_type& getLocalBuffer()
  {
    return mLocalBuffer;
  }

  void push(RecordBuffer buf)
  {
    mLocalBuffer.Push(buf);
  }

  void pop(RecordBuffer& buf)
  {
    mLocalBuffer.Pop(buf);
  }

  /**
   * Depending on which side of a channel I am on, I either pull data from
   * the channel into my local queue or I flush data on my local queue to channel.
   * This method is virtual because there are potentially many different kinds of 
   * channels in the world.
   */
  virtual void sync() =0;
  /**
   * Get the size of the underlying channel.
   */
  virtual uint64_t getSize() const =0;
};

#endif
