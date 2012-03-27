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

#ifndef __CONCURRENT_FIFO_HH__
#define __CONCURRENT_FIFO_HH__

#include <queue>
#include <boost/thread.hpp>

/**
 * This class is not meant to be compatible with
 * STL queue.  The reason is that the STL queue is 
 * sufficiently generic that they had to split pop'ing
 * into two separate operators.  The locking overhead of
 * that is unacceptable for this application, so we restrict
 * ourselves to copyable data (note that pop does NOT return
 * a reference).
 */
template <class _Ty>
class ConcurrentBlockingFifo
{
private:
  // Queue of read requests
  std::queue<_Ty> mRequests;
  boost::mutex mGuard;
  boost::condition_variable mEmptyCondVar;
  boost::condition_variable mFullCondVar;
  std::size_t mMaxSize;
public:
  ConcurrentBlockingFifo(std::size_t maxSize=std::numeric_limits<std::size_t>::max())
    :
    mMaxSize(maxSize)
  {
  }
  ~ConcurrentBlockingFifo()
  {
  }
  _Ty pop() 
  {
    boost::unique_lock<boost::mutex> lock(mGuard);
    while(mRequests.empty()) {
      mEmptyCondVar.wait(lock);
    }
    _Ty r = mRequests.front();
    mRequests.pop();
    mFullCondVar.notify_one();
    return r;
  }
  void push(_Ty t)
  {
    boost::unique_lock<boost::mutex> lock(mGuard);
    while(mRequests.size() >= mMaxSize) {
      mFullCondVar.wait(lock);
    }
    mRequests.push(t);
    mEmptyCondVar.notify_one();
  }
  std::size_t size() 
  {
    boost::unique_lock<boost::mutex> lock(mGuard);
    return mRequests.size();
  }
};

#endif
