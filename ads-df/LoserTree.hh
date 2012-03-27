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

#ifndef __LOSERTREE_HH__
#define __LOSERTREE_HH__

#include <stdint.h>
#include <limits>
#include <functional>
#include <stdexcept>
#include <boost/assert.hpp>
#include <boost/format.hpp>

class Bithack
{
public:
  static uint32_t roundUpPow2(uint32_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return ++v;
  }
  static uint32_t logBase2(uint32_t v) {
    static const int MultiplyDeBruijnBitPosition[32] = 
      {
	0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
	8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
      };
    
    v |= v >> 1; // first round down to one less than a power of 2 
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    
    return MultiplyDeBruijnBitPosition[(uint32_t)(v * 0x07C4ACDDU) >> 27];  
  }
};

/**
 * A Tournament tree of losers.
 * Many of the implementation details here are taken
 * from Graefe "Implementing Sorting in Database Systems".
 */
template <class _Data, class _Compare = std::less<_Data> >
class LoserTree
{
public:
  class Node 
  {
  public:
    /**
     * Player in the tournament that lost here.
     */
    uint32_t InputNumber;
    /**
     * Prefix of normalized key.
     */
    uint32_t KeyPrefix;
    /**
     * The full record in the tree.  Required 
     * to resolve equality when KeyPrefixes are equal.
     */
    _Data Value;
  };

private:
  class SentinelPolicy
  {
  public:
    static const uint32_t MaxPlayers = 8192;
    static const uint32_t MaxPlayersLog2 = 13;
    // We've reserved 2*8192 values for low/high sentinels,
    // this leaves us with 2^32 - 2*8192 for real key
    // values.  For simplicity we take only 2^31 values.
    static const uint32_t KeyPrefixLog2 = 31;
    static uint32_t getMaxPlayers() {
      return MaxPlayers;
    }
    /**
     * Get high sorting sentinel value for player i.
     * Guarantees that sentinels are in increasing order with
     * player index.
     */
    static uint32_t getHighSentinel(uint32_t i) {
      return std::numeric_limits<uint32_t>::max() - MaxPlayers + i;
    }
    static bool isHighSentinel(uint32_t i) {
      return i >=std::numeric_limits<uint32_t>::max() - MaxPlayers;
    }
    /**
     * Get low sorting sentinel value for player i.
     * Guarantees that sentinels are in increasing order with
     * player index.
     */
    static uint32_t getLowSentinel(uint32_t i) {
      return i;
    }
    static bool isLowSentinel(uint32_t i) {
      return i < MaxPlayers;
    }
    /**
     * Apply correction of a user key value so that it may
     * be inserted into a node.
     */
    static uint32_t insertKey(uint32_t i) {
	return i + MaxPlayers;
    }
    /** 
     * Apply correction to a node key so that the user
     * key is recreated.
     */
    static uint32_t removeKey(uint32_t i) {
      return i - MaxPlayers;
    }
  };

  uint32_t mNumPlayers;
  Node * mNodes;
  _Compare mCompare;

  void internalUpdate(uint32_t player, uint32_t keyPrefix, _Data val);

public:
  LoserTree();
  LoserTree(uint32_t numberOfPlayers, const _Compare& eq = _Compare());
  ~LoserTree();

  /**
   * Initialize a loser tournament tree for
   * numberOfPlayers inputs.
   */
  void init(uint32_t numberOfPlayers, const _Compare& eq = _Compare());
  /**
   * Replace the entry at position player with the value val whose
   * key prefix is keyPrefix.
   */
  void update(uint32_t player, uint32_t keyPrefix, _Data val)
  {
    // Apply sentinel correction to prefix
    internalUpdate(player, SentinelPolicy::insertKey(keyPrefix), val);
  }
  /**
   * Return the root node of the tree.
   */
  Node& getRoot()
  {
    return mNodes[0];
  }
  /**
   * Check if high sentinel.
   */
  bool isHighSentinel() const;
  /**
   * Check if tree is empty.
   */
  bool empty() const
  {
    return SentinelPolicy::isLowSentinel(mNodes[0].KeyPrefix);
  }
  /**
   * Close the input to the tree.
   */
  void close(uint32_t input)
  {
    internalUpdate(input, SentinelPolicy::getLowSentinel(input), NULL);
  }
  /**
   * Get the key of the top.
   */
  uint32_t getKeyPrefix() const 
  {
    return SentinelPolicy::removeKey(mNodes[0].KeyPrefix);
  }
  /**
   * The input/player corresponding to the top.
   */
  uint32_t getInput() const
  {
    return mNodes[0].InputNumber;
  }
  /**
   * The value corresponding to the top.
   */
  _Data getValue() const
  {
    return mNodes[0].Value;
  }
};

template <class _Data, class _Compare>
LoserTree<_Data,_Compare>::LoserTree()
  :
  mNumPlayers(0),
  mNodes(NULL),
  mCompare(_Compare())
{
}

template <class _Data, class _Compare>
LoserTree<_Data,_Compare>::LoserTree(uint32_t numberOfPlayers, const _Compare& eq)
  :
  mNumPlayers(0),
  mNodes(NULL),
  mCompare(eq)
{
  init(numberOfPlayers,eq);
}

template <class _Data, class _Compare>
LoserTree<_Data,_Compare>::~LoserTree()
{
  delete [] mNodes;
}

template <class _Data, class _Compare>
void LoserTree<_Data,_Compare>::init(uint32_t numberOfPlayers, 
				    const _Compare& eq)
{
  // Reset state.
  delete [] mNodes;
  mNumPlayers = numberOfPlayers;
  mCompare = eq;

  // There is an easy to compute closed from for initializing
  // a tree that is a power of 2.  I don't know of a closed form
  // for initializing arbitrary tree (though I suspect it isn't
  // too hard to calculate).  For the moment we round up to 
  // nearest power of 2, build tree and then close inputs that
  // we aren't using.  There is a little memory wastage but it
  // shouldn't be macroscopic (nor are there cache implications
  // because we'll never touch the tail of the array after we
  // init).
  numberOfPlayers = Bithack::roundUpPow2(mNumPlayers);
  uint32_t numberOfPlayersLog2 = Bithack::logBase2(numberOfPlayers);
  // Our use of sentinels limits the number of inputs to the tree.
  if (numberOfPlayers > SentinelPolicy::getMaxPlayers()) {
    throw std::runtime_error((boost::format("Cannot merge more than %1% streams") %
			      SentinelPolicy::getMaxPlayers()).str());
  }
  // TODO: Assume number of players is a power of 2 for now.
  // Gotta figure out clean logic for initializing an arbitrary
  // tree.
  // Initialize tree.
  mNodes = new Node [numberOfPlayers];

  for(uint32_t lev=1; lev<=numberOfPlayersLog2; lev++) {
    uint32_t levelBegin = numberOfPlayers >> lev;
    uint32_t numInLevel = levelBegin;
    uint32_t levelInc = (1<<lev);
    for(uint32_t i=0; i<numInLevel; i++) {
      uint32_t tmp = (1 << (lev-1)) - 1 + levelInc*i;
      BOOST_ASSERT(levelBegin+i < numberOfPlayers);
      mNodes[levelBegin + i].InputNumber = tmp;
      mNodes[levelBegin + i].KeyPrefix=SentinelPolicy::getHighSentinel(tmp);
      // We never need this because sentinels never compare equal to anything.
      mNodes[levelBegin + i].Value = NULL;
    }
  }
  mNodes[0].InputNumber = numberOfPlayers-1;
  mNodes[0].KeyPrefix = SentinelPolicy::getHighSentinel(numberOfPlayers - 1);  
  mNodes[0].Value = NULL;

  // Close all excess inputs
  for(uint32_t i=mNumPlayers; i<numberOfPlayers; i++) {
    close(i);
  }
}

template <class _Data, class _Compare>
void LoserTree<_Data,_Compare>::internalUpdate(uint32_t player, uint32_t keyPrefix, _Data val)
{
  // Bottom up tournament.  Start playing in bracket player/2.
  for(std::size_t idx = (Bithack::roundUpPow2(mNumPlayers)>>1) + (player >> 1);
      idx>=1;
      idx >>= 1) {
    Node & n (mNodes[idx]);
    bool cmp = keyPrefix < n.KeyPrefix ||
      (keyPrefix==n.KeyPrefix && mCompare(val, n.Value));

    if(cmp) {
      std::swap(player, n.InputNumber);
      std::swap(keyPrefix, n.KeyPrefix);
      std::swap(val, n.Value);
    } 
  }

  // Update the top of the tree
  mNodes[0].InputNumber = player;
  mNodes[0].KeyPrefix = keyPrefix;
  mNodes[0].Value = val;
}

template <class _Data, class _Compare>
bool LoserTree<_Data,_Compare>::isHighSentinel() const {
  return SentinelPolicy::isHighSentinel(mNodes[0].KeyPrefix);
}


// Extract configured number of bits from
// normalized keys.  Make them into a N-bit
// integer that can be compared using integer
// comparison instructions.
// TODO: Do we assume that we have to build
// the entire normalized key and then extract
// the prefix?
class KeyPrefixBuilder {
private:
  uint32_t mPrefix;
  uint32_t mBitsPos;
  const uint32_t mBitsEnd;

  // Copy the bits at the position of the current bit iterator.
  typedef uint8_t _IntTy;
  bool copyTo(const _IntTy * begin, uint32_t beginBitPos, uint32_t endBitPos) {
    while(mBitsPos<mBitsEnd && beginBitPos != endBitPos) {
      uint32_t beginByte = beginBitPos/(8*sizeof(_IntTy));
      uint32_t beginOffsetWithinByte = beginBitPos - (beginByte*8*sizeof(_IntTy));
      uint32_t bitsToCopy = std::min(mBitsEnd-mBitsPos, 
				     8*((uint32_t) sizeof(_IntTy)) - beginOffsetWithinByte);
      // 2^bitsToCopy-1, then shift into place.  Very tricky thing is that
      // we want to take most significant bits first.
      _IntTy sourceMask = (bitsToCopy ? ~_IntTy(0) << (8*sizeof(_IntTy) - bitsToCopy) : _IntTy(0));
      sourceMask >>= beginOffsetWithinByte;
      // Extract correct bits.
      uint32_t newBits = ((uint32_t) (begin[beginByte] & sourceMask)) << mBitsPos;
      mPrefix |= newBits;
      mBitsPos += bitsToCopy;
      beginBitPos += bitsToCopy;
    }
    return mBitsPos == mBitsEnd;
  }

  static uint32_t byteSwap(uint32_t val)
  {
    return ((((val) & 0xff000000) >> 24) |
	    (((val) & 0x00ff0000) >>  8) |
	    (((val) & 0x0000ff00) <<  8) |
	    (((val) & 0x000000ff) << 24));
  }

public:
  KeyPrefixBuilder(uint32_t bits)
    :
    mPrefix(0),
    mBitsPos(0),
    mBitsEnd(bits)
  {
  }

  bool add(uint32_t v)
  {
    // Byte swap
    uint32_t ret = byteSwap(v);
    // Consume up to the available 32-bits
    return copyTo((const uint8_t *) &ret, 0, 32);   
  }

  bool add(const uint8_t * begin, const uint8_t * end)
  {
    return copyTo(begin, 0, 8*(end-begin));   
  }

  bool add(const char * begin, const char * end)
  {
    return copyTo((const uint8_t *) begin, 0, 8*(end-begin));   
  }

  uint32_t getPrefix() const
  {
    return byteSwap(mPrefix) >> (32-mBitsEnd);
  }

  void clear() 
  {
    mPrefix = 0;
    mBitsPos = 0;
  }
};

#endif
