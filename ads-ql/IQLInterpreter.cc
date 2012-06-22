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
#include <stdexcept>
#include <boost/utility.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/local_time_adjustor.hpp>
#include <antlr3defs.h>

// LLVM Includes
#include "llvm/LLVMContext.h"
#include "llvm/ExecutionEngine/JIT.h"
#include "llvm/ExecutionEngine/JITEventListener.h"
#include "llvm/ExecutionEngine/Interpreter.h"
#include "llvm/ExecutionEngine/GenericValue.h"
#include "llvm/PassManager.h"
#include "llvm/Analysis/Verifier.h"
#include "llvm/Target/TargetData.h"
#include "llvm/Target/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/IRReader.h"
#include "llvm/Bitcode/BitstreamWriter.h"
#include "llvm/Bitcode/ReaderWriter.h"
#include "llvm/CodeGen/MachineRelocation.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/System/DynamicLibrary.h"
#include "llvm/System/Memory.h"

#include "md5.h"
#include "IQLInterpreter.hh"
#include "LLVMGen.h"
#include "CodeGenerationContext.hh"
#include "TypeCheckContext.hh"
#include "GetVariablesPass.h"
#include "IQLLexer.h"
#include "IQLParser.h"
#include "IQLAnalyze.h"
#include "IQLTypeCheck.h"
#include "IQLGetVariables.h"
#include "IQLToLLVM.h"
#include "RecordType.hh"
#include "IQLExpression.hh"

extern "C" {
#include "decNumberLocal.h"
}

InterpreterContext::InterpreterContext() {
  decContextDefault(&mDecimalContext, DEC_INIT_DECIMAL128);
}

InterpreterContext::~InterpreterContext() {
  clear();
}

void * InterpreterContext::malloc(size_t sz) {
  void * tmp = ::malloc(sz);
  mToFree.insert(tmp);
  return tmp;
}

void InterpreterContext::erase(void * ptr) {
  mToFree.erase(ptr);
}

void InterpreterContext::clear() {
  for(std::set<void *>::iterator it = mToFree.begin();
      it != mToFree.end();
      ++it) {
    void * ptr = *it;
    ::free(ptr);
  }
  mToFree.clear();
}

bool InterpreterContext::regex_match(const char* regex_source_c, const char* string) {
  std::string regex_source(regex_source_c);
  regex_cache_type::iterator it = mRegexCache.find(regex_source);
  if (it == mRegexCache.end()) {
    // when it comes time to add a new element to the cache
    if (mRegexCache.size() >= MAX_REGEX_CACHE) {
      // we drop an existing element chosen at random
      // the result is like LRU, but cheaper to implement
      it = mRegexCache.begin();
      std::advance(it, rand() % mRegexCache.size());
      mRegexCache.erase(it);
    }
    mRegexCache[regex_source] = boost::regex(regex_source);
    it = mRegexCache.find(regex_source);
  }
  boost::regex& regex = it->second;
  return boost::regex_match(string, regex);
}

// TODO: Add the Int64 stuff to decNumber.c
/* ------------------------------------------------------------------ */
/* decGetDigits -- count digits in a Units array                      */
/*                                                                    */
/*   uar is the Unit array holding the number (this is often an       */
/*          accumulator of some sort)                                 */
/*   len is the length of the array in units [>=1]                    */
/*                                                                    */
/*   returns the number of (significant) digits in the array          */
/*                                                                    */
/* All leading zeros are excluded, except the last if the array has   */
/* only zero Units.                                                   */
/* ------------------------------------------------------------------ */
// This may be called twice during some operations.
static Int decGetDigits(Unit *uar, Int len) {
  Unit *up=uar+(len-1);            // -> msu
  Int  digits=(len-1)*DECDPUN+1;   // possible digits excluding msu
  #if DECDPUN>4
  uInt const *pow;                 // work
  #endif
                                   // (at least 1 in final msu)
  #if DECCHECK
  if (len<1) printf("decGetDigits called with len<1 [%ld]\n", (LI)len);
  #endif

  for (; up>=uar; up--) {
    if (*up==0) {                  // unit is all 0s
      if (digits==1) break;        // a zero has one digit
      digits-=DECDPUN;             // adjust for 0 unit
      continue;}
    // found the first (most significant) non-zero Unit
    #if DECDPUN>1                  // not done yet
    if (*up<10) break;             // is 1-9
    digits++;
    #if DECDPUN>2                  // not done yet
    if (*up<100) break;            // is 10-99
    digits++;
    #if DECDPUN>3                  // not done yet
    if (*up<1000) break;           // is 100-999
    digits++;
    #if DECDPUN>4                  // count the rest ...
    for (pow=&powers[4]; *up>=*pow; pow++) digits++;
    #endif
    #endif
    #endif
    #endif
    break;
    } // up
  return digits;
  } // decGetDigits

decNumber * decNumberFromUInt64(decNumber *dn, uint64_t uin) {
  Unit *up;                             // work pointer
  decNumberZero(dn);                    // clean
  if (uin==0) return dn;                // [or decGetDigits bad call]
  for (up=dn->lsu; uin>0; up++) {
    *up=(Unit)(uin%(DECDPUNMAX+1));
    uin=uin/(DECDPUNMAX+1);
    }
  dn->digits=decGetDigits(dn->lsu, up-dn->lsu);
  return dn;
  } // decNumberFromUInt64

decNumber * decNumberFromInt64(decNumber *dn, int64_t in) {
  uint64_t unsig;
  if (in>=0) unsig=in;
  else unsig=-in;                    // invert
  // in is now positive
  decNumberFromUInt64(dn, unsig);
  if (in<0) dn->bits=DECNEG;            // sign needed
  return dn;
  } // decNumberFromInt64

std::ostream& operator<<(std::ostream& str, decimal128& val) {
  char buf[DECIMAL128_String];
  decimal128ToString(&val, buf);
  str << buf;
  return str;
}

extern "C" void InternalDecimalAdd(decimal128 * lhs, decimal128 * rhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberAdd(&c, &a, &b, ctxt->getDecimalContext());
  decimal128FromNumber(result, &c, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalSub(decimal128 * lhs, decimal128 * rhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberSubtract(&c, &a, &b, ctxt->getDecimalContext());
  decimal128FromNumber(result, &c, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalMul(decimal128 * lhs, decimal128 * rhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberMultiply(&c, &a, &b, ctxt->getDecimalContext());
  decimal128FromNumber(result, &c, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalDiv(decimal128 * lhs, decimal128 * rhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberDivide(&c, &a, &b, ctxt->getDecimalContext());
  decimal128FromNumber(result, &c, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalNeg(decimal128 * lhs, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a,b;
  decimal128ToNumber(lhs, &a);
  decNumberCopyNegate(&b, &a);
  decimal128FromNumber(result, &b, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalFromInt32(int32_t val, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a;
  decNumberFromInt32(&a, val);
  decimal128FromNumber(result, &a, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalFromInt64(int64_t val, decimal128 * result, InterpreterContext * ctxt) {
  decNumber a;
  decNumberFromInt64(&a, val);
  decimal128FromNumber(result, &a, ctxt->getDecimalContext());
}

extern "C" void InternalDecimalCmp(decimal128 * lhs, decimal128 * rhs, int32_t * result, InterpreterContext * ctxt) {
  decNumber a,b,c;
  decimal128ToNumber(lhs, &a);
  decimal128ToNumber(rhs, &b);
  decNumberCompare(&c, &b, &a, ctxt->getDecimalContext());
  *result = c.lsu[0];
  if (c.bits & DECNEG)
    *result *= -1;
}

extern "C" void InternalVarcharAdd(Varchar* lhs, Varchar* rhs, Varchar* result, InterpreterContext * ctxt) {
  result->Size = lhs->Size + rhs->Size;
  char * buf = (char *) ctxt->malloc(result->Size + 1);
  memcpy(buf, lhs->Ptr, lhs->Size);
  memcpy(buf+lhs->Size, rhs->Ptr, rhs->Size);
  buf[lhs->Size + rhs->Size] = 0;
  result->Ptr = buf;
}

extern "C" void InternalVarcharCopy(Varchar * lhs, Varchar * result, int32_t trackForDelete, InterpreterContext * ctxt) {
  result->Size = lhs->Size;
  char * buf = trackForDelete ? (char *) ctxt->malloc(result->Size + 1) : (char*) ::malloc(result->Size + 1);
  memcpy(buf, lhs->Ptr, lhs->Size);
  buf[lhs->Size] = 0;
  result->Ptr = buf;
}

extern "C" void InternalVarcharErase(Varchar * lhs, InterpreterContext * ctxt) {
  // Remove varchar pointer from internal heap tracking
  ctxt->erase(const_cast<char *>(lhs->Ptr));
}

extern "C" int32_t InternalVarcharEquals(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  return lhs->Size == rhs->Size && 0 == memcmp(lhs->Ptr, rhs->Ptr, lhs->Size);
}

extern "C" int32_t InternalVarcharNE(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  return lhs->Size != rhs->Size || 0 != memcmp(lhs->Ptr, rhs->Ptr, lhs->Size);
}

extern "C" int32_t InternalVarcharLT(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  int cmp = strcmp(lhs->Ptr, rhs->Ptr);
  return cmp < 0;
}

extern "C" int32_t InternalVarcharLE(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  int cmp = strcmp(lhs->Ptr, rhs->Ptr);
  return cmp <= 0;
}

extern "C" int32_t InternalVarcharGT(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  int cmp = strcmp(lhs->Ptr, rhs->Ptr);
  return cmp > 0;
}

extern "C" int32_t InternalVarcharGE(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  int cmp = strcmp(lhs->Ptr, rhs->Ptr);
  return cmp >= 0;
}

extern "C" int32_t InternalVarcharRLike(Varchar* lhs, Varchar* rhs, InterpreterContext * ctxt) {
  return ctxt->regex_match(rhs->Ptr, lhs->Ptr);
}

extern "C" void InternalCharFromVarchar(Varchar* in, char * out, int32_t outSz) {
  if (outSz <= in->Size) {
    memcpy(out, in->Ptr, outSz);
  } else {
    memcpy(out, in->Ptr, in->Size);
    memset(out+in->Size, ' ', outSz-in->Size);
  }
  // Null terminate.
  out[outSz] = 0;
}

extern "C" void substr(Varchar* lhs, 
		       int32_t start, 
		       int32_t len,
		       Varchar * result,
		       InterpreterContext * ctxt) {
  // Size of the result.
  if (len < 0) len = std::numeric_limits<int32_t>::max();
  // If start is too big then return empty string.
  if (lhs->Size <= start) {
    start = lhs->Size;
    len = 0;
  }
  result->Size = std::min(len, lhs->Size - start);
  char * buf = (char *) ctxt->malloc(result->Size + 1);
  memcpy(buf, lhs->Ptr+start, result->Size);
  buf[result->Size] = 0;
  result->Ptr = buf;
}

extern "C" void trim(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  boost::iterator_range<const char*> rng(lhs->Ptr, lhs->Ptr+lhs->Size);
  rng = boost::trim_copy(rng);
  result->Size = (int32_t) std::distance(rng.begin(), rng.end());
  char * buf = (char *) ctxt->malloc(result->Size + 1);
  std::copy(rng.begin(), rng.end(), buf);
  buf[result->Size] = 0;
  result->Ptr = buf;  
}

extern "C" void ltrim(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  boost::iterator_range<const char*> rng(lhs->Ptr, lhs->Ptr+lhs->Size);
  rng = boost::trim_left_copy(rng);
  result->Size = (int32_t) std::distance(rng.begin(), rng.end());
  char * buf = (char *) ctxt->malloc(result->Size + 1);
  std::copy(rng.begin(), rng.end(), buf);
  buf[result->Size] = 0;
  result->Ptr = buf;  
}

extern "C" void rtrim(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  boost::iterator_range<const char*> rng(lhs->Ptr, lhs->Ptr+lhs->Size);
  rng = boost::trim_right_copy(rng);
  result->Size = (int32_t) std::distance(rng.begin(), rng.end());
  char * buf = (char *) ctxt->malloc(result->Size + 1);
  std::copy(rng.begin(), rng.end(), buf);
  buf[result->Size] = 0;
  result->Ptr = buf;  
}

extern "C" void lower(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  result->Size = lhs->Size;
  char * buf = (char *) ctxt->malloc(result->Size + 1);
  boost::iterator_range<const char*> rng(lhs->Ptr, lhs->Ptr+lhs->Size);
  boost::to_lower_copy(buf, rng);
  buf[result->Size] = 0;
  result->Ptr = buf;  
}

extern "C" void upper(Varchar* lhs, 
		     Varchar * result,
		     InterpreterContext * ctxt) {
  result->Size = lhs->Size;
  char * buf = (char *) ctxt->malloc(result->Size + 1);
  boost::iterator_range<const char*> rng(lhs->Ptr, lhs->Ptr+lhs->Size);
  boost::to_upper_copy(buf, rng);
  buf[result->Size] = 0;
  result->Ptr = buf;  
}

extern "C" int32_t length(Varchar* lhs) {
  return lhs->Size;
}

extern "C" void InternalVarcharFromChar(const char * lhs, 
					Varchar * result,
					InterpreterContext * ctxt) {
  int32_t len = ::strlen(lhs);
  boost::iterator_range<const char*> rng(lhs, lhs+len);
  result->Size = len;
  char * buf = (char *) ctxt->malloc(len + 1);
  std::copy(rng.begin(), rng.end(), buf);
  buf[result->Size] = 0;
  result->Ptr = buf;  
}

extern "C" void InternalVarcharFromInt32(int32_t val,
					Varchar * result,
					InterpreterContext * ctxt) 
{
  // 10 digits + trailing EOS + optional - 
  char * buf = (char *) ctxt->malloc(12);
  sprintf(buf, "%d", val);
  result->Size = strlen(buf);
  result->Ptr = buf;  
}

extern "C" void InternalVarcharFromInt64(int64_t val,
					Varchar * result,
					InterpreterContext * ctxt) 
{
  // 20 digits + trailing EOS + optional - 
  char * buf = (char *) ctxt->malloc(22);
  // Cast to hush warnings from gcc
  sprintf(buf, "%lld", (long long int) val);
  result->Size = strlen(buf);
  result->Ptr = buf;  
}

extern "C" void InternalVarcharFromDecimal(decimal128 * val,
					   Varchar * result,
					   InterpreterContext * ctxt) 
{
  char * buf = (char *) ctxt->malloc(DECIMAL128_String + 1);
  ::decimal128ToString(val, buf);
  result->Size = strlen(buf);
  result->Ptr = buf;  
}

extern "C" void InternalVarcharFromDouble(double val,
					  Varchar * result,
					  InterpreterContext * ctxt) 
{
  char * buf = (char *) ctxt->malloc(64);
  sprintf(buf, "%.15g", val);
  result->Size = strlen(buf);
  result->Ptr = buf;  
}

extern "C" void InternalVarcharFromDate(boost::gregorian::date d,
					Varchar * result,
					InterpreterContext * ctxt) 
{
  char * buf = (char *) ctxt->malloc(11);
  boost::gregorian::greg_year_month_day parts = d.year_month_day();
  sprintf(buf, "%04d-%02d-%02d", (int32_t) parts.year, (int32_t) parts.month,
	  (int32_t) parts.day);
  result->Size = 10;
  result->Ptr = buf;  
}

extern "C" void InternalVarcharFromDatetime(boost::posix_time::ptime t,
					    Varchar * result,
					    InterpreterContext * ctxt) 
{
  char * buf = (char *) ctxt->malloc(20);
  boost::gregorian::greg_year_month_day parts = t.date().year_month_day();
  boost::posix_time::time_duration dur = t.time_of_day();
  sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d", (int32_t) parts.year, (int32_t) parts.month,
	  (int32_t) parts.day, dur.hours(), dur.minutes(), dur.seconds());
  result->Size = 19;
  result->Ptr = buf;  
}

extern "C" boost::gregorian::date InternalDateFromVarchar(Varchar* lhs) {
  return boost::gregorian::from_string(lhs->Ptr);
}

extern "C" boost::gregorian::date date(boost::posix_time::ptime t) {
  return t.date();
}
/**
 * ODBC standard dayofweek with Sunday=1
 */
extern "C" int32_t dayofweek(boost::gregorian::date t) {
  return 1+t.day_of_week();
}

extern "C" int32_t dayofmonth(boost::gregorian::date t) {
  return t.day();
}

extern "C" int32_t dayofyear(boost::gregorian::date t) {
  return t.day_of_year();
}

extern "C" int32_t month(boost::gregorian::date t) {
  return t.month();
}

extern "C" int32_t year(boost::gregorian::date t) {
  return t.year();
}

extern "C" boost::gregorian::date last_day(boost::gregorian::date t) {
  return t.end_of_month();
}

extern "C" int32_t datediff(boost::gregorian::date a, boost::gregorian::date b) {
  return (int32_t) (a-b).days();
}

extern "C" int32_t julian_day(boost::gregorian::date a) {
  return (int32_t) a.julian_day();
}

extern "C" boost::posix_time::ptime InternalDatetimeFromVarchar(Varchar* lhs) {
  return lhs->Size == 10 ?
    boost::posix_time::ptime(boost::gregorian::from_string(lhs->Ptr)) :
    boost::posix_time::time_from_string(lhs->Ptr);
}

extern "C" boost::posix_time::ptime InternalDatetimeFromDate(boost::gregorian::date lhs) {
  return boost::posix_time::ptime(lhs, boost::posix_time::time_duration(0,0,0,0));
}

extern "C" boost::posix_time::ptime utc_timestamp() {
  return boost::posix_time::microsec_clock::universal_time();
}

extern "C" int64_t unix_timestamp(boost::posix_time::ptime t)
{
  // TODO: Should make a way of setting the ambient time zone
  typedef boost::date_time::local_adjustor<boost::posix_time::ptime, -5, 
    boost::posix_time::us_dst> us_eastern;
  boost::posix_time::ptime unixZero = boost::posix_time::from_time_t(0); 
  boost::posix_time::ptime utcTime = us_eastern::local_to_utc(t);
  return (utcTime - unixZero).total_seconds();
}

extern "C" boost::posix_time::ptime from_unixtime(int64_t unixTime)
{
  // TODO: Should make a way of setting the ambient time zone
  typedef boost::date_time::local_adjustor<boost::posix_time::ptime, -5, 
    boost::posix_time::us_dst> us_eastern;
  boost::posix_time::ptime utcTime = boost::posix_time::from_time_t(unixTime);
  return us_eastern::utc_to_local(utcTime);
}

extern "C" boost::posix_time::ptime datetime_add_second(boost::posix_time::ptime t,
						    int32_t units) {
  return t + boost::posix_time::seconds(units);
}

extern "C" boost::posix_time::ptime datetime_add_hour(boost::posix_time::ptime t,
						    int32_t units) {
  return t + boost::posix_time::hours(units);
}

extern "C" boost::posix_time::ptime datetime_add_minute(boost::posix_time::ptime t,
						    int32_t units) {
  return t + boost::posix_time::minutes(units);
}

extern "C" boost::posix_time::ptime datetime_add_day(boost::posix_time::ptime t,
						 int32_t units) {
  return t + boost::gregorian::days(units);
}

extern "C" boost::posix_time::ptime datetime_add_month(boost::posix_time::ptime t,
						 int32_t units) {
  return t + boost::gregorian::months(units);
}

extern "C" boost::posix_time::ptime datetime_add_year(boost::posix_time::ptime t,
						 int32_t units) {
  return t + boost::gregorian::years(units);
}

extern "C" boost::posix_time::ptime date_add_second(boost::gregorian::date t,
						    int32_t units) {
  return boost::posix_time::ptime(t, boost::posix_time::seconds(units));
}

extern "C" boost::posix_time::ptime date_add_hour(boost::gregorian::date t,
						  int32_t units) {
  return boost::posix_time::ptime(t, boost::posix_time::hours(units));
}

extern "C" boost::posix_time::ptime date_add_minute(boost::gregorian::date t,
						    int32_t units) {
  return boost::posix_time::ptime(t, boost::posix_time::minutes(units));
}

extern "C" boost::gregorian::date date_add_day(boost::gregorian::date t,
					       int32_t units) {
  return t + boost::gregorian::days(units);
}

extern "C" boost::gregorian::date date_add_month(boost::gregorian::date t,
						 int32_t units) {
  return t + boost::gregorian::months(units);
}

extern "C" boost::gregorian::date date_add_year(boost::gregorian::date t,
						int32_t units) {
  return t + boost::gregorian::years(units);
}

extern "C" void InternalArrayException() {
  throw std::runtime_error("Array Bounds Exception");
}

template <class _T>
class ANTLR3AutoPtr : boost::noncopyable {
private:
  _T * mPtr;

public:
  ANTLR3AutoPtr(_T * ptr)
    :
    mPtr(ptr)
  {
  }

  ~ANTLR3AutoPtr() {
    if (mPtr != NULL)
      mPtr->free(mPtr);
    mPtr = NULL;
  }

  operator bool () const {
    return mPtr != NULL;
  }

  _T * get() {
    return mPtr;
  }

  _T * operator-> () {
    return mPtr;
  }
};

LLVMBase::LLVMBase()
  :
  TheExecutionEngine(NULL),
  mContext(NULL),
  mFPM(NULL)
{
}

LLVMBase::~LLVMBase()
{
  bool hasEE = (TheExecutionEngine != NULL);
  delete mFPM;
  delete TheExecutionEngine;
  if (mContext) {
    if (hasEE) {
      mContext->disownModule();
    }
    delete mContext;
    mContext = NULL;
  }

}

static LLVMValueRef LoadAndValidateExternalFunction(LLVMBase& b, 
						    const char * externalFunctionName,
						    LLVMTypeRef funTy)
{
  return llvm::wrap(b.LoadAndValidateExternalFunction(externalFunctionName, 
						      llvm::unwrap(funTy)));
}

llvm::Value * LLVMBase::LoadAndValidateExternalFunction(const char * externalFunctionName, 
							llvm::Type * funTy)
{
  void * addr;
  if (NULL == (addr=llvm::sys::DynamicLibrary::SearchForAddressOfSymbol(externalFunctionName)))
    throw std::runtime_error((boost::format("Unable to find symbol for external library function: %1%") % externalFunctionName).str());
  mExternalFunctionsIdx[addr] = externalFunctionName;
  return llvm::unwrap(LLVMAddFunction(mContext->LLVMModule, 
				      externalFunctionName, 
				      llvm::wrap(funTy)));
}

void LLVMBase::CreateMemcpyIntrinsic()
{
  llvm::Module * mod = llvm::unwrap(mContext->LLVMModule);

  llvm::PointerType* PointerTy_3 = llvm::PointerType::get(llvm::IntegerType::get(mod->getContext(), 8), 0);
  
  std::vector<const llvm::Type*>FuncTy_7_args;
  FuncTy_7_args.push_back(PointerTy_3);
  FuncTy_7_args.push_back(PointerTy_3);
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 64));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 32));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 1));
  llvm::FunctionType* FuncTy_7 = llvm::FunctionType::get(
							 /*Result=*/llvm::Type::getVoidTy(mod->getContext()),
							 /*Params=*/FuncTy_7_args,
							 /*isVarArg=*/false);

  llvm::Function* func_llvm_memcpy_i64 = llvm::Function::Create(
								/*Type=*/FuncTy_7,
								/*Linkage=*/llvm::GlobalValue::ExternalLinkage,
								/*Name=*/"llvm.memcpy.p0i8.p0i8.i64", mod); // (external, no body)
  func_llvm_memcpy_i64->setCallingConv(llvm::CallingConv::C);
  llvm::AttrListPtr func_llvm_memcpy_i64_PAL;
  {
    llvm::SmallVector<llvm::AttributeWithIndex, 4> Attrs;
    llvm::AttributeWithIndex PAWI;
    PAWI.Index = 1U; PAWI.Attrs = 0  | llvm::Attribute::NoCapture;
    Attrs.push_back(PAWI);
    PAWI.Index = 2U; PAWI.Attrs = 0  | llvm::Attribute::NoCapture;
    Attrs.push_back(PAWI);
    PAWI.Index = 4294967295U; PAWI.Attrs = 0  | llvm::Attribute::NoUnwind;
    Attrs.push_back(PAWI);
    func_llvm_memcpy_i64_PAL = llvm::AttrListPtr::get(Attrs.begin(), Attrs.end());
    
  }
  func_llvm_memcpy_i64->setAttributes(func_llvm_memcpy_i64_PAL);

  mContext->LLVMMemcpyIntrinsic = llvm::wrap(func_llvm_memcpy_i64);
}

void LLVMBase::CreateMemsetIntrinsic()
{
  llvm::Module * mod = llvm::unwrap(mContext->LLVMModule);

  llvm::PointerType* PointerTy_3 = llvm::PointerType::get(llvm::IntegerType::get(mod->getContext(), 8), 0);
  
  std::vector<const llvm::Type*>FuncTy_7_args;
  FuncTy_7_args.push_back(PointerTy_3);
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 8));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 64));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 32));
  FuncTy_7_args.push_back(llvm::IntegerType::get(mod->getContext(), 1));
  llvm::FunctionType* FuncTy_7 = llvm::FunctionType::get(
    /*Result=*/llvm::Type::getVoidTy(mod->getContext()),
    /*Params=*/FuncTy_7_args,
    /*isVarArg=*/false);

  llvm::Function* func_llvm_memset_i64 = llvm::Function::Create(
    /*Type=*/FuncTy_7,
    /*Linkage=*/llvm::GlobalValue::ExternalLinkage,
    /*Name=*/"llvm.memset.p0i8.i64", mod); // (external, no body)
  func_llvm_memset_i64->setCallingConv(llvm::CallingConv::C);
  llvm::AttrListPtr func_llvm_memset_i64_PAL;
  {
    llvm::SmallVector<llvm::AttributeWithIndex, 4> Attrs;
    llvm::AttributeWithIndex PAWI;
    PAWI.Index = 1U; PAWI.Attrs = 0  | llvm::Attribute::NoCapture;
    Attrs.push_back(PAWI);
    PAWI.Index = 4294967295U; PAWI.Attrs = 0  | llvm::Attribute::NoUnwind;
    Attrs.push_back(PAWI);
    func_llvm_memset_i64_PAL = llvm::AttrListPtr::get(Attrs.begin(), Attrs.end());
    
  }
  func_llvm_memset_i64->setAttributes(func_llvm_memset_i64_PAL);

  mContext->LLVMMemsetIntrinsic = llvm::wrap(func_llvm_memset_i64);
}

void LLVMBase::CreateMemcmpIntrinsic()
{
  llvm::Module * mod = llvm::unwrap(mContext->LLVMModule);

  llvm::PointerType* PointerTy_0 = llvm::PointerType::get(llvm::IntegerType::get(mod->getContext(), 8), 0);

  std::vector<const llvm::Type*>FuncTy_12_args;
  FuncTy_12_args.push_back(PointerTy_0);
  FuncTy_12_args.push_back(PointerTy_0);
  FuncTy_12_args.push_back(llvm::IntegerType::get(mod->getContext(), 64));
  llvm::FunctionType* FuncTy_12 = llvm::FunctionType::get(
							  /*Result=*/llvm::IntegerType::get(mod->getContext(), 32),
							  /*Params=*/FuncTy_12_args,
							  /*isVarArg=*/false);

  llvm::Function* func_memcmp = llvm::Function::Create(
    /*Type=*/FuncTy_12,
    /*Linkage=*/llvm::GlobalValue::ExternalLinkage,
    /*Name=*/"memcmp", mod); // (external, no body)
  func_memcmp->setCallingConv(llvm::CallingConv::C);
  llvm::AttrListPtr func_memcmp_PAL;
  {
    llvm::SmallVector<llvm::AttributeWithIndex, 4> Attrs;
    llvm::AttributeWithIndex PAWI;
    PAWI.Index = 4294967295U; PAWI.Attrs = 0  | llvm::Attribute::NoUnwind | llvm::Attribute::ReadOnly;
    Attrs.push_back(PAWI);
    func_memcmp_PAL = llvm::AttrListPtr::get(Attrs.begin(), Attrs.end());
    
  }
  func_memcmp->setAttributes(func_memcmp_PAL);

  // Register symbol for address lookup.
  void * addr;
  if (NULL == (addr=llvm::sys::DynamicLibrary::SearchForAddressOfSymbol("memcmp")))
    throw std::runtime_error("Unable to find symbol for external library function: memcmp");
  mExternalFunctionsIdx[addr] = "memcmp";

  mContext->LLVMMemcmpIntrinsic = llvm::wrap(func_memcmp);
}

void LLVMBase::InitializeLLVM()
{
  // We disable this because we want to avoid it's installation of
  // signal handlers.
  llvm::InitializeNativeTarget();

  // Compile into an LLVM program
  mContext = new CodeGenerationContext();
  mContext->createFunctionContext();

#if DECSUBST
  static unsigned numDecContextMembers(8);
#else
  static unsigned numDecContextMembers(7);
#endif
  // Declare extern functions for decNumber
  LLVMTypeRef decContextMembers[numDecContextMembers];
  decContextMembers[0] = LLVMInt32TypeInContext(mContext->LLVMContext);
  decContextMembers[1] = LLVMInt32TypeInContext(mContext->LLVMContext);
  decContextMembers[2] = LLVMInt32TypeInContext(mContext->LLVMContext);
  // This is actually enum; is this OK?
  decContextMembers[3] = LLVMInt32TypeInContext(mContext->LLVMContext);
  // These two are actually unsigned in decContext
  decContextMembers[4] = LLVMInt32TypeInContext(mContext->LLVMContext);
  decContextMembers[5] = LLVMInt32TypeInContext(mContext->LLVMContext);
  decContextMembers[6] = LLVMInt8TypeInContext(mContext->LLVMContext);
#if DECSUBST
  decContextMembers[7] = LLVMInt8TypeInContext(mContext->LLVMContext);
#endif
  mContext->LLVMDecContextPtrType = LLVMPointerType(LLVMStructTypeInContext(mContext->LLVMContext, &decContextMembers[0], numDecContextMembers, 0), 0);
  // Don't quite understand LLVM behavior of what happens with you pass { [16 x i8] } by value on the call stack.
  // It seems to align each member at 4 bytes and therefore is a gross overestimate of the actually representation.
  //LLVMTypeRef decimal128Member = LLVMArrayType(LLVMInt8TypeInContext(mContext->LLVMContext), DECIMAL128_Bytes);
  LLVMTypeRef decimal128Members[DECIMAL128_Bytes/sizeof(int32_t)];
  for(unsigned int i=0; i<DECIMAL128_Bytes/sizeof(int32_t); ++i)
    decimal128Members[i]= LLVMInt32TypeInContext(mContext->LLVMContext);
  mContext->LLVMDecimal128Type = LLVMStructTypeInContext(mContext->LLVMContext, &decimal128Members[0], DECIMAL128_Bytes/sizeof(int32_t), 1);

  // Set up VARCHAR type
  LLVMTypeRef varcharMembers[2];
  varcharMembers[0] = LLVMInt32TypeInContext(mContext->LLVMContext);
  varcharMembers[1] = LLVMPointerType(LLVMInt8TypeInContext(mContext->LLVMContext), 0);
  mContext->LLVMVarcharType = LLVMStructTypeInContext(mContext->LLVMContext, &varcharMembers[0], 2, 0);
  // DATETIME runtime type
  mContext->LLVMDatetimeType = LLVMInt64TypeInContext(mContext->LLVMContext);

  // Try to register the program as a source of symbols to resolve against.
  llvm::sys::DynamicLibrary::LoadLibraryPermanently(0, NULL);
  // Prototypes for external functions we want to provide access to.
  // TODO:Handle redefinition of function
  LLVMTypeRef argumentTypes[10];
  unsigned numArguments=0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  LLVMTypeRef funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  LLVMValueRef libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDecimalAdd", funTy);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDecimalSub", funTy);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDecimalMul", funTy);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDecimalDiv", funTy);
  
  numArguments=0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDecimalNeg", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDecimalFromInt32", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDecimalFromInt64", funTy);

  numArguments=0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = LLVMPointerType(LLVMInt32TypeInContext(mContext->LLVMContext), 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDecimalCmp", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharAdd", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharCopy", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharErase", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharEquals", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharRLike", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharNE", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharLT", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharLE", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharGT", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharGE", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  funTy = LLVMFunctionType(mContext->LLVMDatetimeType, 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDatetimeFromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(mContext->LLVMDatetimeType, 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDatetimeFromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalDateFromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(LLVMInt8TypeInContext(mContext->LLVMContext), 0);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalCharFromVarchar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "length", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(LLVMInt8TypeInContext(mContext->LLVMContext), 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharFromChar", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharFromInt32", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharFromInt64", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMDecimal128Type, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharFromDecimal", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMDoubleTypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharFromDouble", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharFromDate", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), 
			   &argumentTypes[0], 
			   numArguments, 
			   0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalVarcharFromDatetime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "substr", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "trim", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "ltrim", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "rtrim", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "lower", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = LLVMPointerType(mContext->LLVMVarcharType, 0);
  argumentTypes[numArguments++] = mContext->LLVMDecContextPtrType;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "upper", funTy);

  // Date functions
  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "date", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "dayofweek", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "dayofmonth", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "dayofyear", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "month", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "year", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "last_day", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "datediff", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "julian_day", funTy);

  // Datetime functions
  numArguments = 0;
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "utc_timestamp", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "unix_timestamp", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "from_unixtime", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "datetime_add_day", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "datetime_add_month", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "datetime_add_year", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "datetime_add_second", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "datetime_add_minute", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt64TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "datetime_add_hour", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "date_add_day", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "date_add_month", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "date_add_year", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "date_add_second", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "date_add_minute", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt64TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "date_add_hour", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMPointerType(LLVMInt8TypeInContext(mContext->LLVMContext), 0);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  argumentTypes[numArguments++] = LLVMInt32TypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMInt32TypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "SuperFastHash", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMDoubleTypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMDoubleTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "sqrt", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMDoubleTypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMDoubleTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "log", funTy);

  numArguments = 0;
  argumentTypes[numArguments++] = LLVMDoubleTypeInContext(mContext->LLVMContext);
  funTy = LLVMFunctionType(LLVMDoubleTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "exp", funTy);

  numArguments = 0;
  funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], numArguments, 0);
  libFunVal = ::LoadAndValidateExternalFunction(*this, "InternalArrayException", funTy);

  // LLVM intrinsics we want to use
  CreateMemcpyIntrinsic();
  CreateMemsetIntrinsic();
  CreateMemcmpIntrinsic();

  // Create the JIT.  This takes ownership of the module.
  llvm::EngineBuilder engBuilder(llvm::unwrap(mContext->LLVMModule));
  std::string ErrStr;
  engBuilder.setErrorStr(&ErrStr);
  TheExecutionEngine = engBuilder.create();
  if (!TheExecutionEngine) {
    throw std::runtime_error((boost::format("Could not create ExecutionEngine: %1%\n") % ErrStr).str());
  }

  mFPM = new llvm::FunctionPassManager(llvm::unwrap(mContext->LLVMModule));

  // Set up the optimizer pipeline.  Start with registering info about how the
  // target lays out data structures.
  mFPM->add(new llvm::TargetData(*TheExecutionEngine->getTargetData()));
  // Promote allocas to registers.
  mFPM->add(llvm::createPromoteMemoryToRegisterPass());
  // Do simple "peephole" optimizations and bit-twiddling optzns.
  mFPM->add(llvm::createInstructionCombiningPass());
  // Reassociate expressions.
  mFPM->add(llvm::createReassociatePass());
  // Eliminate Common SubExpressions.
  mFPM->add(llvm::createGVNPass());
  // Simplify the control flow graph (deleting unreachable blocks, etc).
  mFPM->add(llvm::createCFGSimplificationPass());

  mFPM->doInitialization();

}

void LLVMBase::ConstructFunction(const std::string& funName, const std::vector<std::string>& recordArgs)
{
  ConstructFunction(funName, recordArgs, llvm::unwrap(LLVMVoidTypeInContext(mContext->LLVMContext)));
}

void LLVMBase::ConstructFunction(const std::string& funName, 
				 const std::vector<std::string>& recordArgs,
				 llvm::Type * retType)
{
  // Setup LLVM access to our external structure.  Here we just set a char* pointer and we manually
  // create typed pointers to offsets/members.  We could try to achieve the same effect with
  // defining a struct but it isn't exactly clear how alignment rules in the LLVM data layout might
  // make this difficult.
  std::vector<const char *> argumentNames;
  std::vector<LLVMTypeRef> argumentTypes;
  for(std::vector<std::string>::const_iterator it = recordArgs.begin();
      it != recordArgs.end();
      ++it) {
    argumentNames.push_back(it->c_str());
    argumentTypes.push_back(LLVMPointerType(LLVMInt8TypeInContext(mContext->LLVMContext), 0));
  }
  // If we have a non-void ret type then add it as a special out param
  if (retType != llvm::unwrap(LLVMVoidTypeInContext(mContext->LLVMContext))) {
    argumentNames.push_back("__ReturnValue__");
    argumentTypes.push_back(LLVMPointerType(llvm::wrap(retType), 0));
  }
  // Every Function takes the decContext pointer as a final argument
  argumentNames.push_back("__DecimalContext__");
  argumentTypes.push_back(mContext->LLVMDecContextPtrType);
  LLVMTypeRef funTy = LLVMFunctionType(LLVMVoidTypeInContext(mContext->LLVMContext), &argumentTypes[0], argumentTypes.size(), 0);
  mContext->LLVMFunction = LLVMAddFunction(mContext->LLVMModule, funName.c_str(), funTy);
  LLVMBasicBlockRef entryBlock = LLVMAppendBasicBlockInContext(mContext->LLVMContext, mContext->LLVMFunction, "EntryBlock");
  LLVMPositionBuilderAtEnd(mContext->LLVMBuilder, entryBlock);
  for(unsigned int i = 0; i<argumentNames.size(); i++) {
    LLVMValueRef allocAVal = LLVMCreateEntryBlockAlloca(mContext, argumentTypes[i], argumentNames[i]);
    LLVMValueRef arg = LLVMGetParam(mContext->LLVMFunction, i);
    mContext->defineVariable(argumentNames[i], llvm::unwrap(allocAVal),
			     NULL, IQLToLLVMValue::eGlobal);
    // Set names on function arguments
    LLVMSetValueName(arg, argumentNames[i]);
    // Store argument in the alloca 
    LLVMBuildStore(mContext->LLVMBuilder, arg, allocAVal);
  }  
}

void LLVMBase::createTransferFunction(const std::string& funName,
				      const RecordType * input,
				      const RecordType * output)
{
  std::vector<std::string> argumentNames;
  argumentNames.push_back("__BasePointer__");
  argumentNames.push_back("__OutputPointer__");
  // Create the function object with its arguments (these go into the
  // new freshly created symbol table).
  ConstructFunction(funName, argumentNames);
  // Inject the members of the input struct into the symbol table.
  mContext->addInputRecordType("input", "__BasePointer__", input);
  mContext->IQLOutputRecord = wrap(output);
}

void LLVMBase::createTransferFunction(const std::string & funName,
				      const std::vector<const RecordType *>& sources,
				      const std::vector<boost::dynamic_bitset<> >& masks,
				      const RecordType * output)
{
  // Setup LLVM access to our external structure(s).  
  std::vector<std::string> argumentNames;
  for(std::size_t i=0; i<sources.size(); i++)
    argumentNames.push_back((boost::format("__BasePointer%1%__") % i).str());
  if (output != NULL)
    argumentNames.push_back("__OutputPointer__");
  ConstructFunction(funName, argumentNames);

  // Inject the members of the input struct into the symbol table.
  // For the moment just make sure we don't have any ambiguous references
  std::set<std::string> uniqueNames;
  for(std::vector<const RecordType *>::const_iterator it = sources.begin();
      it != sources.end();
      ++it) {
    std::size_t i = (std::size_t) (it - sources.begin());
    const boost::dynamic_bitset<> mask(masks[i]);
    for(RecordType::const_member_iterator mit = (*it)->begin_members();
	mit != (*it)->end_members();
	++mit) {
      if (mask.test(mit-(*it)->begin_members())) {
	if (uniqueNames.end() != uniqueNames.find(mit->GetName()))
	  throw std::runtime_error("Field names must be unique in in place update statements");
	uniqueNames.insert(mit->GetName());
      }
    }
    mContext->addInputRecordType((boost::format("input%1%") % i).str().c_str(), 
				 (boost::format("__BasePointer%1%__") % i).str().c_str(), 			   
				 *it,
				 mask);
  }
  mContext->IQLOutputRecord = wrap(output);
}

void LLVMBase::createUpdate(const std::string & funName,
			    const std::vector<const RecordType *>& sources,
			    const std::vector<boost::dynamic_bitset<> >& masks)
{
  createTransferFunction(funName, sources, masks, NULL);
}

void X86MethodInfo::relocate()
{
  for(std::vector<Relocation>::const_iterator reloc = mRelocations.begin();
      reloc != mRelocations.end();
      ++reloc) {
    if (reloc->Symbol.size() == 0) {
      *((uint8_t **) (mMethodBegin + reloc->MachineCodeOffset)) = 
	mMethodBegin + reloc->ResultPtrOffset;
    } else {
      void * sym = llvm::sys::DynamicLibrary::SearchForAddressOfSymbol(reloc->Symbol.c_str());
      if (sym == NULL) {
	throw std::runtime_error((boost::format("Failed to locate symbol %1%"
						" for relocation in IQL"
						" expression.  Disable native"
						" serialization.") % 
				  reloc->Symbol).str());
      }
      *((void **) (mMethodBegin + reloc->MachineCodeOffset)) = sym;
      // std::cout << "Relocated " << reloc->Symbol.c_str() << " at location 0x" << std::hex << (uint64_t)sym << std::endl;
    }
  }
}

uint8_t * X86MethodInfo::allocateRWX(std::size_t sz, uint8_t alignMod16)
{
  std::string errMsg;
  std::size_t toAlloc=sz+15;
  llvm::sys::MemoryBlock block = llvm::sys::Memory::AllocateRWX(toAlloc, NULL, &errMsg);
  if (block.base() == NULL) {
    throw std::runtime_error(errMsg);
  }
  if (block.size() < toAlloc) {
    throw std::runtime_error("Failed to allocate memory for JIT code");
  }
  uint8_t * alignedBase = (uint8_t *) (16*((((uintptr_t)block.base()) + 15 - alignMod16)/16));
  return alignedBase + alignMod16;
}

void X86MethodInfo::setMethod(void * methodBegin,
			      void * methodEnd,
			      void * codeBegin)
{
  mMethodBegin = reinterpret_cast<uint8_t *>(methodBegin);
  mMethodSize = reinterpret_cast<uint8_t *>(methodEnd) - mMethodBegin;
  mCodeOffset = reinterpret_cast<uint8_t *>(codeBegin) - mMethodBegin;
}

void X86MethodInfo::addRelocation(const llvm::MachineRelocation& reloc,
				  const std::map<void*,std::string>& externalFunctions)
{
  uint8_t * ptr = reinterpret_cast<uint8_t*>(reloc.getResultPointer());
  switch(reloc.getRelocationType()) {
  case reloc_pcrel_word:
  case reloc_picrel_word:
    // Both of these guys are cool in general and don't require
    // any special work from us because they are position
    // independent.
    // In particular we should be seeing these
    // for branches (basic block relocations in LLVM-speak).
    // TODO: Should validate that the address is
    // within the method region.
    break;
  case reloc_absolute_word:
  case reloc_absolute_word_sext:
    // Be pessimistic about these two: assume they can't
    // be fixed up.
    mIsValid = false;
    break;
  case reloc_absolute_dword:
    // The most important case here for our purposes is
    // references into the constant pool.  The most important
    // subcase of the important case is floating point constants.
    if (ptr < mMethodBegin ||
	ptr >= mMethodBegin+mMethodSize) {
      // We are pointing to something outside the method region.
      // TODO: Chances are good that this is an external function.
      // We can cover this case by saving the fn id and reresolving
      // on load (dlsym).
      std::map<void*,std::string>::const_iterator it = externalFunctions.find(ptr);
      if (externalFunctions.end() != it) {
	mRelocations.push_back(Relocation(reloc.getMachineCodeOffset(),
					  it->second));
      } else {
	mIsValid = false;
      }
    } else {
      // Save mMethodBegin relative address so we can fix it 
      // up in another process.
      mRelocations.push_back(Relocation(reloc.getMachineCodeOffset(), 
					ptr - mMethodBegin));
    }
    break;
  default:
    throw std::runtime_error("Unknown X86 Relocation type");
  }
}

class IQLRecordBufferMethodHandle 
{
private:
  llvm::LLVMContext ctxt;
  llvm::Module * mModule;
  llvm::ExecutionEngine * TheExecutionEngine;
  std::map<std::string, llvm::Function *> funVal;
  std::map<std::string, boost::shared_ptr<X86MethodInfo> > funInfo;
  const std::map<void*, std::string> * mExternalFunctions;
  bool mOwnMap;

  void initialize(const std::vector<unsigned char>& bitcode, 
		  const std::vector<std::string>& functionNames);
public:
  IQLRecordBufferMethodHandle(const std::vector<unsigned char>& bitcode, 
			      const std::vector<std::string>& functionNames,
			      const std::map<void*, std::string>& externalFunctions);
  IQLRecordBufferMethodHandle(const std::vector<unsigned char>& bitcode, 
			      const std::vector<std::string>& functionNames);
  ~IQLRecordBufferMethodHandle();
  void * getFunPtr(const std::string& name);
  boost::shared_ptr<X86MethodInfo> getFunInfo(const std::string& name) const;
};

IQLRecordBufferMethodHandle::IQLRecordBufferMethodHandle(const std::vector<unsigned char>& bitcode, 
							 const std::vector<std::string>& functionNames)
  :
  TheExecutionEngine(NULL),
  mExternalFunctions(new std::map<void*, std::string>()),
  mOwnMap(true)
{
  initialize(bitcode, functionNames);
}

IQLRecordBufferMethodHandle::IQLRecordBufferMethodHandle(const std::vector<unsigned char>& bitcode, 
							 const std::vector<std::string>& functionNames,
							 const std::map<void*, std::string>& externalFunctions)
  :
  TheExecutionEngine(NULL),
  mExternalFunctions(&externalFunctions),
  mOwnMap(false)
{
  initialize(bitcode, functionNames);
}

IQLRecordBufferMethodHandle::~IQLRecordBufferMethodHandle()
{
  delete TheExecutionEngine;
  if (mOwnMap)
    delete mExternalFunctions;
}

void IQLRecordBufferMethodHandle::initialize(const std::vector<unsigned char>& bitcode, 
					     const std::vector<std::string>& functionNames)
{  
  llvm::InitializeNativeTarget();
  llvm::StringRef str((char *) &bitcode[0], bitcode.size()-1);
  llvm::MemoryBuffer * mb = llvm::MemoryBuffer::getMemBuffer(str, "MyModule");
  if (mb == NULL) 
    throw std::runtime_error("Failed to create MemoryBuffer");
  llvm::SMDiagnostic err;
  mModule = llvm::ParseIR(mb, err, ctxt);
  if (mModule == NULL)
    throw std::runtime_error("Failed to restore LLVM module");

  // Grab the functions pointers.
  for(std::vector<std::string>::const_iterator it=functionNames.begin();
      it != functionNames.end();
      ++it) {
    llvm::Function * tmp = mModule->getFunction(*it);
    if (tmp == NULL) 
      throw std::runtime_error((boost::format("Failed to retrieve function '%1%' from reconstituted bitcode") % (*it)).str());
    funVal[*it] = tmp;
  }

  llvm::EngineBuilder engBuilder(mModule);
  std::string ErrStr;
  engBuilder.setErrorStr(&ErrStr);
  engBuilder.setOptLevel(llvm::CodeGenOpt::Default);
  TheExecutionEngine = engBuilder.create();
  if (!TheExecutionEngine) {
    throw std::runtime_error((boost::format("Could not create ExecutionEngine: %1%\n") % ErrStr).str());
  }
  // At this point, the execution engine owns the module
  // and we don't need the reference any more.
  mModule = NULL;
}

void * IQLRecordBufferMethodHandle::getFunPtr(const std::string& nm)
{
  std::map<std::string, llvm::Function *>::const_iterator it = funVal.find(nm);
  if (it == funVal.end())
    throw std::runtime_error((boost::format("Failed to retrieve function '%1%' from reconstituted bitcode") % nm).str());
  return TheExecutionEngine->getPointerToFunction(it->second);
}

boost::shared_ptr<X86MethodInfo> IQLRecordBufferMethodHandle::getFunInfo(const std::string& nm) const
{
  std::map<std::string, boost::shared_ptr<X86MethodInfo> >::const_iterator it = funInfo.find(nm);
  if (it == funInfo.end())
    throw std::runtime_error((boost::format("Failed to retrieve function '%1%' from reconstituted bitcode") % nm).str());
  return it->second;
}

IQLRecordBufferMethod::IQLRecordBufferMethod(const std::vector<unsigned char>& bitcode, const std::string& functionName)
  :
  mFunction(NULL),
  mImpl(NULL)
{
  std::vector<std::string> tmp;
  tmp.push_back(functionName);
  mImpl = new IQLRecordBufferMethodHandle(bitcode, tmp);
  mFunction = (LLVMFuncType) mImpl->getFunPtr(functionName);
}

IQLRecordBufferMethod::~IQLRecordBufferMethod()
{
  // Pretty sure that mFunction is owned by the ExecutionEngine in the impl.
  delete mImpl;
}


void Copy(const std::map<std::string, RecordType *> & sources, const std::string& program)
{
  // First calculate the 
}

class IQLParserStuff
{
private:
  pANTLR3_INPUT_STREAM mInput;
  pANTLR3_COMMON_TOKEN_STREAM mStream;
  pIQLLexer mLexer;
  pIQLParser mParser;
  pANTLR3_COMMON_TREE_NODE_STREAM mNodes;
  IQLExpression * mNativeAST;

  void initParse(const std::string& transfer);
  void cleanup();
public:
  IQLParserStuff();
  ~IQLParserStuff();
  void parseFunction(const std::string& transfer);
  void parseTransfer(const std::string& transfer);
  void parseUpdate(const std::string& transfer);
  void getFreeVariables(std::set<std::string>& freeVariables);
  const RecordType * typeCheckTransfer(DynamicRecordContext & recCtxt,
				       const RecordType * source);
  const RecordType * typeCheckTransfer(DynamicRecordContext & recCtxt,
				       const std::vector<AliasedRecordType>& sources,
				       const std::vector<boost::dynamic_bitset<> >& masks);
  void typeCheckUpdate(DynamicRecordContext & recCtxt,
		       const std::vector<const RecordType *>& sources,
		       const std::vector<boost::dynamic_bitset<> >& masks);
  pANTLR3_COMMON_TREE_NODE_STREAM getNodes() { return mNodes; }
  pANTLR3_BASE_TREE getAST() { return mNodes->root; }
  IQLRecordConstructor * generateTransferAST(DynamicRecordContext & recCtxt);
  IQLExpression * generateFunctionAST(DynamicRecordContext & recCtxt);
};

IQLParserStuff::IQLParserStuff()
  :
  mInput(NULL),
  mStream(NULL),
  mLexer(NULL),
  mParser(NULL),
  mNodes(NULL),
  mNativeAST(NULL)
{
}

IQLParserStuff::~IQLParserStuff()
{
  cleanup();
}

void IQLParserStuff::cleanup()
{
  if(mInput)
    mInput->free(mInput);
  if(mLexer)
    mLexer->free(mLexer);
  if(mStream)
    mStream->free(mStream);
  if(mParser)
    mParser->free(mParser);
  if(mNodes)
    mNodes->free(mNodes);
  mInput = NULL;
  mStream = NULL;
  mLexer = NULL;
  mParser = NULL;
  mNodes = NULL;
}
static int streamID=0;

void IQLParserStuff::initParse(const std::string& transfer)
{
  // Erase any state that has accumulated.
  cleanup();
  // Parse the transfer spec and generate the program to perform the operations.
  // Feed from an in place stream
  mInput = antlr3NewAsciiStringInPlaceStream((pANTLR3_UINT8) transfer.c_str(),
					     transfer.size(), 
					     (pANTLR3_UINT8) (boost::format("My Program %1%") % (streamID++)).str().c_str());
  if (!mInput)
    throw std::runtime_error("Antlr out of memory");

  // SQL is case insensitive
  mInput->setUcaseLA(mInput, ANTLR3_TRUE);
  
  mLexer = IQLLexerNew(mInput);
  if (!mLexer)
    throw std::runtime_error("Antlr out of memory");

  mStream = antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, TOKENSOURCE(mLexer));
  mParser = IQLParserNew(mStream);
  if (!mParser)
    throw std::runtime_error("Antlr out of memory");  
}

void IQLParserStuff::parseFunction(const std::string& f)
{  
  initParse(f);
  IQLParser_singleExpression_return parserRet = mParser->singleExpression(mParser);
  if (mParser->pParser->rec->state->errorCount > 0)
    throw std::runtime_error((boost::format("Parse failed: %1%") % f).str());
  mNodes = antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT);
}

void IQLParserStuff::parseTransfer(const std::string& transfer)
{  
  initParse(transfer);
  IQLParser_recordConstructor_return parserRet = mParser->recordConstructor(mParser);
  if (mParser->pParser->rec->state->errorCount > 0)
    throw std::runtime_error((boost::format("Parse failed: %1%") % transfer).str());
  mNodes = antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT);
}

void IQLParserStuff::parseUpdate(const std::string& transfer)
{  
  initParse(transfer);
  IQLParser_statementList_return parserRet = mParser->statementList(mParser);
  if (mParser->pParser->rec->state->errorCount > 0)
    throw std::runtime_error((boost::format("Parse failed: %1%") % transfer).str());
  mNodes = antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT);
}

void IQLParserStuff::getFreeVariables(std::set<std::string>& freeVariables)
{
  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  GetVariablesContext getVariablesContext;

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLGetVariables> alz(IQLGetVariablesNew(mNodes));
  alz->recordConstructor(alz.get(), wrap(&getVariablesContext));
  // We should only get a failure here if there is a bug in our
  // tree grammar.  Not much a user can do about this.
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("INTERNAL ERROR: Failed to get free variables.");

  for(std::set<std::string>::const_iterator 
	it = getVariablesContext.getFreeVariables().begin(), 
	end = getVariablesContext.getFreeVariables().end();
      it != end;
      ++it) {
    if (freeVariables.find(*it) == freeVariables.end()) 
      freeVariables.insert(*it);
  }
}

const RecordType * IQLParserStuff::typeCheckTransfer(DynamicRecordContext & recCtxt,
						     const RecordType * source)
{
  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  std::vector<AliasedRecordType> aliased;
  aliased.push_back(AliasedRecordType("input", source));
  std::vector<boost::dynamic_bitset<> > masks;
  masks.resize(1);
  masks[0].resize(source->size(), true);
  TypeCheckContext typeCheckContext(recCtxt, aliased, masks);

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(mNodes));
  alz->recordConstructor(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");

  // There should be a present for us now...
  if (typeCheckContext.getOutputRecord() == NULL)
    throw std::runtime_error("Failed to create output record");

  return typeCheckContext.getOutputRecord();
}

const RecordType * 
IQLParserStuff::typeCheckTransfer(DynamicRecordContext & recCtxt,
				  const std::vector<AliasedRecordType>& sources,
				  const std::vector<boost::dynamic_bitset<> >& masks)

{
  // Create an appropriate context for type checking.  This requires associating the
  // input records with a name and then inserting all the members of the record type
  // with a symbol table.
  TypeCheckContext typeCheckContext(recCtxt, sources, masks);

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(mNodes));
  alz->recordConstructor(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");

  // There should be a present for us now...
  if (typeCheckContext.getOutputRecord() == NULL)
    throw std::runtime_error("Failed to create output record");

  return typeCheckContext.getOutputRecord();
}

void IQLParserStuff::typeCheckUpdate(DynamicRecordContext & recCtxt,
				     const std::vector<const RecordType *>& sources,
				     const std::vector<boost::dynamic_bitset<> >& masks)
{
  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  std::vector<AliasedRecordType> aliased; 
  for(std::size_t i=0; i<sources.size(); ++i) {
    std::string nm ((boost::format("input%1%") % i).str());
    aliased.push_back(AliasedRecordType(nm, sources[i]));
  } 
  TypeCheckContext typeCheckContext(recCtxt, aliased, masks);

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(mNodes));
  alz->statementBlock(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");
}

IQLRecordConstructor * IQLParserStuff::generateTransferAST(DynamicRecordContext & recCtxt)
{
  // Generate native AST.  We'll eventually move all analysis
  // to this.
  ANTLR3AutoPtr<IQLAnalyze> nativeASTGenerator(IQLAnalyzeNew(mNodes));
  IQLRecordConstructor * nativeAST = unwrap(nativeASTGenerator->recordConstructor(nativeASTGenerator.get(), 
							   wrap(&recCtxt)));
  if (nativeASTGenerator->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("AST generation failed");
  return nativeAST;
}

IQLExpression * IQLParserStuff::generateFunctionAST(DynamicRecordContext & recCtxt)
{
  // Generate native AST.  We'll eventually move all analysis
  // to this.
  ANTLR3AutoPtr<IQLAnalyze> nativeASTGenerator(IQLAnalyzeNew(mNodes));
  IQLExpression * nativeAST = unwrap(nativeASTGenerator->singleExpression(nativeASTGenerator.get(), 
							   wrap(&recCtxt)));
  if (nativeASTGenerator->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("AST generation failed");
  return nativeAST;
}

IQLRecordConstructor * RecordTypeTransfer::getAST(class DynamicRecordContext& recCtxt,
						  const std::string& xfer)
{
  IQLParserStuff p;
  p.parseTransfer(xfer);
  return p.generateTransferAST(recCtxt);
}

void RecordTypeTransfer::getFreeVariables(const std::string& xfer,
					  std::set<std::string>& freeVariables)
{
  IQLParserStuff p;
  p.parseTransfer(xfer);
  p.getFreeVariables(freeVariables);
}

RecordTypeTransfer::RecordTypeTransfer(DynamicRecordContext& recCtxt, const std::string & funName, const RecordType * source, const std::string& transfer)
  :
  mSource(source),
  mTarget(NULL),
  mFunName(funName),
  mTransfer(transfer),
  mCopyFunction(NULL),
  mMoveFunction(NULL),
  mImpl(NULL),
  mIsIdentity(false)
{
  IQLParserStuff p;
  p.parseTransfer(transfer);
  mTarget = p.typeCheckTransfer(recCtxt, source);

  // Create a valid code generation context based on the input and output record formats.
  InitializeLLVM();

  std::vector<std::string> argumentNames;
  argumentNames.push_back("__BasePointer__");
  argumentNames.push_back("__OutputPointer__");
  // Create two variants of the function: one for move and one for copy.
  // They have the same signature.
  std::vector<std::string> funNames;
  for(int i=0; i<2; i++) {
    // Save the name
    funNames.push_back(mFunName + (i ? "&move" : "&copy"));
    // Clean up the symbol table from the last code gen
    mContext->reinitializeForTransfer();
    // Create the function object with its arguments (these go into the
    // new freshly created symbol table).
    ConstructFunction(funNames.back(), argumentNames);
    // Inject the members of the input struct into the symbol table.
    mContext->addInputRecordType("input", "__BasePointer__", mSource);
    // Special context entry for output record required by statement list
    mContext->IQLOutputRecord = wrap(mTarget);
    // Set state about whether we want move or copy semantics
    mContext->IQLMoveSemantics = i;
    // Code generate
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(p.getNodes()));  
    toLLVM->recordConstructor(toLLVM.get(), wrap(mContext));
    LLVMBuildRetVoid(mContext->LLVMBuilder);
    
    // If doing copy find out if this was an identity transfer
    if (0 == i)
      mIsIdentity = 1==mContext->IsIdentity;

    llvm::verifyFunction(*llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
    // llvm::outs() << "We just constructed this LLVM module:\n\n" << *llvm::unwrap(mContext->LLVMModule);
    // // Now run optimizer over the IR
    mFPM->run(*llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
    // llvm::outs() << "We just optimized this LLVM module:\n\n" << *llvm::unwrap(mContext->LLVMModule);
    // llvm::outs() << "\n\nRunning foo: ";
    // llvm::outs().flush();
  }

  // Save the built module as bitcode
  llvm::BitstreamWriter writer(mBitcode);
  llvm::WriteBitcodeToStream(llvm::unwrap(mContext->LLVMModule), writer);
  // Odd thing about MemoryBuffer::getMemBuffer is that it actually wants the buffer to have an
  // extra null byte at the end (but that extra byte is not reported as part of the size
  // just as with strings).  Put that null terminator here.
  mBitcode.push_back(0);

  // TODO: The remaining stuff should be in a separate class because
  // we really want to be able to push the bitcode over the wire.
  mImpl = new IQLRecordBufferMethodHandle(mBitcode, funNames, mExternalFunctionsIdx);
  mCopyFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[0]);
  mMoveFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[1]);
}

RecordTypeTransfer::~RecordTypeTransfer()
{
  delete mImpl;
}

void RecordTypeTransfer::execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt, bool isSourceMove) const
{
  if (isSourceMove) {
    (*mMoveFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);  
  } else {
    (*mCopyFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);  
  }
}

IQLTransferModule * RecordTypeTransfer::create(bool isPIC) const
{
  return new IQLTransferModule(mTarget->getMalloc(), mFunName + "&copy", mFunName + "&move", mBitcode, mExternalFunctionsIdx, isPIC);
}


IQLTransferModule::IQLTransferModule(const RecordTypeMalloc& recordMalloc,
				     const std::string& copyFunName, 
				     const std::string& moveFunName, 
				     const std::vector<unsigned char>& bitcode,
				     const std::map<void*,std::string>& externalFunctions,
				     bool isPIC)
  :
  mMalloc(recordMalloc),
  mCopyFunName(copyFunName),
  mMoveFunName(moveFunName),
  mBitcode(bitcode),
  mCopyFunction(NULL),
  mMoveFunction(NULL),
  mImpl(NULL),
  mInfo(NULL),
  mIsPIC(isPIC)
{
  initImpl(externalFunctions);
}

IQLTransferModule::~IQLTransferModule()
{
  if (mImpl) {
    delete mImpl;
  } else {
    // TODO: Free RWX memory
  }
}

X86MethodInfo * IQLTransferModule::getFunInfo(const std::string& str) const
{
  return mImpl->getFunInfo(str).get();
}


void IQLTransferModule::initImpl(const std::map<void*,std::string>& externalFunctions)
{
  // TODO: The remaining stuff should be in a separate class because
  // we really want to be able to push the bitcode over the wire.
  std::vector<std::string> funNames;
  funNames.push_back(mCopyFunName);
  funNames.push_back(mMoveFunName);
  mImpl = new IQLRecordBufferMethodHandle(mBitcode, funNames, externalFunctions);
  mCopyFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[0]);
  mMoveFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[1]);
}

void IQLTransferModule::execute(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt, bool isSourceMove) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mMalloc.malloc();
  if (isSourceMove) {
    (*mMoveFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);  
  } else {
    (*mCopyFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);  
  }
  ctxt->clear();
}

RecordTypeTransfer2::RecordTypeTransfer2(DynamicRecordContext& recCtxt, 
					 const std::string & funName, 
					 const std::vector<AliasedRecordType>& sources, 
					 const std::string& transfer)
  :
  mSources(sources),
  mTarget(NULL),
  mFunName(funName),
  mTransfer(transfer),
  mCopyFunction(NULL),
  mMoveFunction(NULL),
  mImpl(NULL)
{
  // Parse the transfer spec and generate the program to perform the operations.
  // Feed from an in place stream
  ANTLR3AutoPtr<ANTLR3_INPUT_STREAM> input(antlr3NewAsciiStringInPlaceStream((pANTLR3_UINT8) mTransfer.c_str(),
									     mTransfer.size(), 
									     (pANTLR3_UINT8) "My Program"));
  if (!input)
    throw std::runtime_error("Antlr out of memory");
  
  // SQL is case insensitive
  input->setUcaseLA(input.get(), ANTLR3_TRUE);

  ANTLR3AutoPtr<IQLLexer> lxr(IQLLexerNew(input.get()));
  if (!lxr)
    throw std::runtime_error("Antlr out of memory");

  ANTLR3AutoPtr<ANTLR3_COMMON_TOKEN_STREAM> tstream(antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, TOKENSOURCE(lxr.get())));
  ANTLR3AutoPtr<IQLParser> psr(IQLParserNew(tstream.get()));
  if (!psr)
    throw std::runtime_error("Antlr out of memory");  
  
  IQLParser_recordConstructor_return parserRet = psr->recordConstructor(psr.get());
  if (psr->pParser->rec->state->errorCount > 0)
    throw std::runtime_error((boost::format("Parse failed: %1%") % mTransfer).str());

  // Create an appropriate context for type checking.  This requires associating the
  // input records with a name and then inserting all the members of the record type
  // with a symbol table.
  TypeCheckContext typeCheckContext(recCtxt, mSources);

  // Now pass through the type checker
  ANTLR3AutoPtr<ANTLR3_COMMON_TREE_NODE_STREAM> nodes(antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT));
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(nodes.get()));
  alz->recordConstructor(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");

  // There should be a present for us now...
  if (typeCheckContext.getOutputRecord() == NULL)
    throw std::runtime_error("Failed to create output record");

  mTarget = typeCheckContext.getOutputRecord();

  // Create a valid code generation context based on the input and output record formats.
  InitializeLLVM();

  std::vector<std::string> argumentNames;
  for(std::vector<AliasedRecordType>::const_iterator it = mSources.begin(); 
      it != mSources.end();
      ++it) {
    argumentNames.push_back(mSources.size() == 1 ? 
			    "__BasePointer__" : 
			    (boost::format("__BasePointer%1%__") % (it - mSources.begin())).str().c_str());
  }
  argumentNames.push_back("__OutputPointer__");
  // Create two variants of the function: one for move and one for copy.
  // They have the same signature.
  std::vector<std::string> funNames;
  for(int i=0; i<2; i++) {
    // Save the name
    funNames.push_back(mFunName + (i ? "&move" : "&copy"));
    // Clean up the symbol table from the last code gen
    mContext->reinitializeForTransfer();
    // Create the function object with its arguments (these go into the
    // new freshly created symbol table).
    ConstructFunction(funNames.back(), argumentNames);
    for(std::vector<AliasedRecordType>::const_iterator it = mSources.begin(); 
	it != mSources.end();
	++it) {
      // Inject the members of the input struct into the symbol table.
      mContext->addInputRecordType(it->getAlias().c_str(),
				   mSources.size() == 1 ? 
				   "__BasePointer__" :  
				   (boost::format("__BasePointer%1%__") % (it - mSources.begin())).str().c_str(), 
				   it->getType());
    }
    // Special context entry for output record required by statement list
    mContext->IQLOutputRecord = wrap(mTarget);
    // Set state about whether we want move or copy semantics
    mContext->IQLMoveSemantics = i;
    // Code generate
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(nodes.get()));  
    toLLVM->recordConstructor(toLLVM.get(), wrap(mContext));
    LLVMBuildRetVoid(mContext->LLVMBuilder);

    llvm::verifyFunction(*llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
    // llvm::outs() << "We just constructed this LLVM module:\n\n" << *llvm::unwrap(mContext->LLVMModule);
    // // Now run optimizer over the IR
    mFPM->run(*llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
    // llvm::outs() << "We just optimized this LLVM module:\n\n" << *llvm::unwrap(mContext->LLVMModule);
    // llvm::outs() << "\n\nRunning foo: ";
    // llvm::outs().flush();
  }

  // Save the built module as bitcode
  llvm::BitstreamWriter writer(mBitcode);
  llvm::WriteBitcodeToStream(llvm::unwrap(mContext->LLVMModule), writer);
  // Odd thing about MemoryBuffer::getMemBuffer is that it actually wants the buffer to have an
  // extra null byte at the end (but that extra byte is not reported as part of the size
  // just as with strings).  Put that null terminator here.
  mBitcode.push_back(0);

  // TODO: The remaining stuff should be in a separate class because
  // we really want to be able to push the bitcode over the wire.
  mImpl = new IQLRecordBufferMethodHandle(mBitcode, funNames);
  mCopyFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[0]);
  mMoveFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[1]);
}

RecordTypeTransfer2::~RecordTypeTransfer2()
{
  delete mImpl;
}

void RecordTypeTransfer2::execute(RecordBuffer * sources, 
				  bool * isSourceMove, 
				  int32_t numSources,
				  RecordBuffer target, 
				  class InterpreterContext * ctxt) const
{
  if (numSources != 2) 
    throw std::runtime_error("RecordTypeTransfer2::execute : Number of sources must be 2");
  // TODO: Handle arbitrary number of inputs.
  (*mCopyFunction)((char *) sources[0].Ptr, (char *) sources[1].Ptr, (char *) target.Ptr, ctxt);  
}

IQLTransferModule2 * RecordTypeTransfer2::create() const
{
  return new IQLTransferModule2(mTarget->getMalloc(), mFunName + "&copy", mFunName + "&move", mBitcode);
}

IQLTransferModule2::IQLTransferModule2(const RecordTypeMalloc& recordMalloc,
				       const std::string& copyFunName, 
				       const std::string& moveFunName, 
				       const std::vector<unsigned char>& bitcode)
  :
  mMalloc(recordMalloc),
  mCopyFunName(copyFunName),
  mMoveFunName(moveFunName),
  mBitcode(bitcode),
  mCopyFunction(NULL),
  mMoveFunction(NULL),
  mImpl(NULL)
{
  initImpl();
}

IQLTransferModule2::~IQLTransferModule2()
{
  delete mImpl;
}

void IQLTransferModule2::initImpl()
{
  // TODO: The remaining stuff should be in a separate class because
  // we really want to be able to push the bitcode over the wire.
  std::vector<std::string> funNames;
  funNames.push_back(mCopyFunName);
  funNames.push_back(mMoveFunName);
  mImpl = new IQLRecordBufferMethodHandle(mBitcode, funNames);
  mCopyFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[0]);
  mMoveFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[1]);
}

void IQLTransferModule2::execute(RecordBuffer * sources, 
				  bool * isSourceMove, 
				  int32_t numSources,
				  RecordBuffer & target, 
				  class InterpreterContext * ctxt) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mMalloc.malloc();
  if (numSources != 2) 
    throw std::runtime_error("IQLTransferModule2::execute : Number of sources must be 2");
  // TODO: Handle move semantics
  BOOST_ASSERT(!isSourceMove[0] && !isSourceMove[1]);
  // TODO: Handle arbitrary number of inputs.
  (*mCopyFunction)((char *) sources[0].Ptr, (char *) sources[1].Ptr, (char *) target.Ptr, ctxt);  ctxt->clear();  
}

IQLUpdateModule::IQLUpdateModule(const std::string& funName, 
				 const std::vector<unsigned char>& bitcode)
  :
  mFunName(funName),
  mBitcode(bitcode),
  mFunction(NULL),
  mImpl(NULL)
{
  initImpl();
}

IQLUpdateModule::~IQLUpdateModule()
{
  delete mImpl;
}

void IQLUpdateModule::initImpl()
{
  // TODO: The remaining stuff should be in a separate class because
  // we really want to be able to push the bitcode over the wire.
  std::vector<std::string> funNames;
  funNames.push_back(mFunName);
  mImpl = new IQLRecordBufferMethodHandle(mBitcode, funNames);
  mFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[0]);
}

void IQLUpdateModule::execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt) const
{
    (*mFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);    
}

RecordTypeInPlaceUpdate::RecordTypeInPlaceUpdate(class DynamicRecordContext& recCtxt, 
						 const std::string & funName, 
						 const std::vector<const RecordType *>& sources, 
						 const std::string& statements)
  :
  mImpl(NULL)
{
  // By default include all fields in all sources.
  std::vector<boost::dynamic_bitset<> > masks;
  masks.resize(sources.size());
  for(std::size_t i=0; i<masks.size(); ++i) {
    masks[i].resize(sources[i]->size(), true);
  }
  init(recCtxt, funName, sources, masks, statements);
}

RecordTypeInPlaceUpdate::RecordTypeInPlaceUpdate(class DynamicRecordContext& recCtxt, 
						 const std::string & funName, 
						 const std::vector<const RecordType *>& sources, 
						 const std::vector<boost::dynamic_bitset<> >& masks,
						 const std::string& statements)
  :
  mImpl(NULL)
{
  init(recCtxt, funName, sources, masks, statements);
}

RecordTypeInPlaceUpdate::~RecordTypeInPlaceUpdate()
{
  delete mImpl;
}

void RecordTypeInPlaceUpdate::init(class DynamicRecordContext& recCtxt, 
				   const std::string & funName, 
				   const std::vector<const RecordType *>& sources, 
				   const std::vector<boost::dynamic_bitset<> >& masks,
				   const std::string& statements)
{
  mSources = sources;
  mFunName = funName;
  mStatements = statements;

  IQLParserStuff p;
  p.parseUpdate(statements);
  p.typeCheckUpdate(recCtxt, sources, masks);

  InitializeLLVM();

  // Create the LLVM function and populate variables in
  // symbol table to prepare for code gen.
  createUpdate(mFunName, mSources, masks);

  // Special context entry for output record required by 
  // transfer but not by in place update.
  mContext->IQLMoveSemantics = 0;

  ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(p.getNodes()));  
  toLLVM->statementBlock(toLLVM.get(), wrap(mContext));
  LLVMBuildRetVoid(mContext->LLVMBuilder);

  llvm::verifyFunction(*llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
  // llvm::outs() << "We just constructed this LLVM module:\n\n" << *llvm::unwrap(mContext->LLVMModule);
  // // Now run optimizer over the IR
  mFPM->run(*llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
  // llvm::outs() << "We just optimized this LLVM module:\n\n" << *llvm::unwrap(mContext->LLVMModule);
  // llvm::outs() << "\n\nRunning foo: ";
  // llvm::outs().flush();

  // Save the built module as bitcode
  llvm::BitstreamWriter writer(mBitcode);
  llvm::WriteBitcodeToStream(llvm::unwrap(mContext->LLVMModule), writer);
  // Odd thing about MemoryBuffer::getMemBuffer is that it actually wants the buffer to have an
  // extra null byte at the end (but that extra byte is not reported as part of the size
  // just as with strings).  Put that null terminator here.
  mBitcode.push_back(0);

  // TODO: The remaining stuff should be in a separate class because
  // we really want to be able to push the bitcode over the wire.
  std::vector<std::string> funNames;
  funNames.push_back(mFunName);
  mImpl = new IQLRecordBufferMethodHandle(mBitcode, funNames);
  mUpdateFunction = (LLVMFuncType) mImpl->getFunPtr(mFunName);
}

void RecordTypeInPlaceUpdate::execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt) const
{
    (*mUpdateFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);    
}

IQLUpdateModule * RecordTypeInPlaceUpdate::create() const
{
  return new IQLUpdateModule(mFunName, mBitcode);
}

IQLFunctionModule::IQLFunctionModule(const std::string& funName, 
				     const std::vector<unsigned char>& bitcode)
  :
  mFunName(funName),
  mBitcode(bitcode),
  mFunction(NULL),
  mImpl(NULL)
{
  initImpl();
}

IQLFunctionModule::~IQLFunctionModule()
{
  delete mImpl;
}

void IQLFunctionModule::initImpl()
{
  std::vector<std::string> funNames;
  funNames.push_back(mFunName);
  mImpl = new IQLRecordBufferMethodHandle(mBitcode, funNames);
  mFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[0]);
}

int32_t IQLFunctionModule::execute(RecordBuffer sourceA, RecordBuffer sourceB, class InterpreterContext * ctxt) const
{
  int32_t ret;
  (*mFunction)((char *) sourceA.Ptr, (char *) sourceB.Ptr, &ret, ctxt);    
  ctxt->clear();
  return ret;
}

IQLExpression * RecordTypeFunction::getAST(class DynamicRecordContext& recCtxt,
						  const std::string& f)
{
  IQLParserStuff p;
  p.parseFunction(f);
  return p.generateFunctionAST(recCtxt);
}

RecordTypeFunction::RecordTypeFunction(class DynamicRecordContext& recCtxt, 
				       const std::string & funName, 
				       const std::vector<const RecordType *> sources, 
				       const std::string& statements)
  :
  mFunName(funName),
  mStatements(statements),
  mFunction(NULL),
  mImpl(NULL)
{
  for(std::vector<const RecordType *>::const_iterator rit = sources.begin();
      rit != sources.end();
      ++rit) {
    mSources.push_back(AliasedRecordType(sources.size() == 1 ? 
					"input" : 
					(boost::format("input%1%") % (rit-sources.begin())).str().c_str(), 
					*rit));
  }
  init(recCtxt);
}

RecordTypeFunction::RecordTypeFunction(class DynamicRecordContext& recCtxt, 
				       const std::string & funName, 
				       const std::vector<AliasedRecordType>& sources, 
				       const std::string& statements)
  :
  mSources(sources),
  mFunName(funName),
  mStatements(statements),
  mFunction(NULL),
  mImpl(NULL)
{
  init(recCtxt);
}

RecordTypeFunction::~RecordTypeFunction()
{
  delete mImpl;
}

void RecordTypeFunction::init(DynamicRecordContext& recCtxt)
{
  // Right now we assmue 2 input sources (one may be empty).
  if (mSources.size() != 2)
    throw std::runtime_error("RecordTypeFunction requires 2 source record types (the second may be empty)");

  // Feed from an in place stream
  ANTLR3AutoPtr<ANTLR3_INPUT_STREAM> input(antlr3NewAsciiStringInPlaceStream((pANTLR3_UINT8) mStatements.c_str(),
									     mStatements.size(), 
									     (pANTLR3_UINT8) "My Program"));
  if (!input)
    throw std::runtime_error("Antlr out of memory");
  
  // SQL is case insensitive
  input->setUcaseLA(input.get(), ANTLR3_TRUE);

  ANTLR3AutoPtr<IQLLexer> lxr(IQLLexerNew(input.get()));
  if (!lxr)
    throw std::runtime_error("Antlr out of memory");

  ANTLR3AutoPtr<ANTLR3_COMMON_TOKEN_STREAM> tstream(antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, TOKENSOURCE(lxr.get())));
  ANTLR3AutoPtr<IQLParser> psr(IQLParserNew(tstream.get()));
  if (!psr)
    throw std::runtime_error("Antlr out of memory");  
  
  IQLParser_singleExpression_return parserRet = psr->singleExpression(psr.get());
  if (psr->pParser->rec->state->errorCount > 0)
    throw std::runtime_error((boost::format("Parse failed: %1%") % mStatements).str());

  // std::cout << parserRet.tree->toStringTree(parserRet.tree)->chars << std::endl;

  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  // TODO: check for name ambiguity and resolve.
  TypeCheckContext typeCheckContext(recCtxt, mSources);

  ANTLR3AutoPtr<ANTLR3_COMMON_TREE_NODE_STREAM> nodes(antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT));
  
  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(nodes.get()));
  IQLFieldTypeRef retTy = alz->singleExpression(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");

  // There should be a present for us now...
  if (unwrap(retTy)->clone(true) != Int32Type::Get(recCtxt, true))
    throw std::runtime_error("Only supporting int32_t return type on functions right now");

  InitializeLLVM();

  // Setup LLVM access to our external structure(s).  
  std::vector<std::string> argumentNames;
  for(std::size_t i=0; i<mSources.size(); i++)
    argumentNames.push_back((boost::format("__BasePointer%1%__") % i).str());
  ConstructFunction(mFunName, argumentNames, llvm::unwrap(LLVMInt32TypeInContext(mContext->LLVMContext)));

  // Inject the members of the input struct into the symbol table.
  // For the moment just make sure we don't have any ambiguous references
  for(std::vector<AliasedRecordType>::const_iterator it = mSources.begin();
      it != mSources.end();
      ++it) {
    mContext->addInputRecordType(it->getAlias().c_str(), 
				 (boost::format("__BasePointer%1%__") % (it - mSources.begin())).str().c_str(), 			   
				 it->getType());
  }

  // Special context entry for output record required by 
  // transfer but not by in place update.
  mContext->IQLOutputRecord = NULL;
  mContext->IQLMoveSemantics = 0;

  ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(nodes.get()));  
  toLLVM->singleExpression(toLLVM.get(), wrap(mContext));
  LLVMBuildRetVoid(mContext->LLVMBuilder);

  llvm::verifyFunction(*llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
  // llvm::outs() << "We just constructed this LLVM module:\n\n" << *llvm::unwrap(mContext->LLVMModule);
  // Now run optimizer over the IR
  mFPM->run(*llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
  // llvm::outs() << "We just optimized this LLVM module:\n\n" << *llvm::unwrap(mContext->LLVMModule);
  // llvm::outs() << "\n\nRunning foo: ";
  // llvm::outs().flush();

  // Save the built module as bitcode
  llvm::BitstreamWriter writer(mBitcode);
  llvm::WriteBitcodeToStream(llvm::unwrap(mContext->LLVMModule), writer);
  // Odd thing about MemoryBuffer::getMemBuffer is that it actually wants the buffer to have an
  // extra null byte at the end (but that extra byte is not reported as part of the size
  // just as with strings).  Put that null terminator here.
  mBitcode.push_back(0);

  // TODO: The remaining stuff should be in a separate class because
  // we really want to be able to push the bitcode over the wire.
  std::vector<std::string> funNames;
  funNames.push_back(mFunName);
  mImpl = new IQLRecordBufferMethodHandle(mBitcode, funNames);
  mFunction = (LLVMFuncType) mImpl->getFunPtr(mFunName);
}

int32_t RecordTypeFunction::execute(RecordBuffer source, RecordBuffer target, class InterpreterContext * ctxt) const
{
  int32_t ret;
  (*mFunction)((char *) source.Ptr, (char *) target.Ptr, &ret, ctxt);    
  return ret;
}

IQLFunctionModule * RecordTypeFunction::create() const
{
  return new IQLFunctionModule(mFunName, mBitcode);
}


// TODO: Put this backin GraphBuilder.cc and figure out what is
// goofy with headers that caused the compilation issue that lead
// me to put this in here.
#include "IQLGraphBuilder.hh"

void IQLGraphBuilder::buildGraph(const std::string& graphSpec, bool isFile)
{
  // Parse the transfer spec and generate the program to perform the operations.
  // Feed from an in place stream
  ANTLR3AutoPtr<ANTLR3_INPUT_STREAM> input(isFile ?
					   antlr3AsciiFileStreamNew((pANTLR3_UINT8) graphSpec.c_str()) :
					   antlr3NewAsciiStringInPlaceStream((pANTLR3_UINT8) graphSpec.c_str(),
									     graphSpec.size(), 
									     (pANTLR3_UINT8) "My Program"));
  if (!input)
    throw std::runtime_error("Antlr out of memory");
  
  ANTLR3AutoPtr<IQLLexer> lxr(IQLLexerNew(input.get()));
  if (!lxr)
    throw std::runtime_error("Antlr out of memory");

  ANTLR3AutoPtr<ANTLR3_COMMON_TOKEN_STREAM> tstream(antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, 
										     TOKENSOURCE(lxr.get())));
  ANTLR3AutoPtr<IQLParser> psr(IQLParserNew(tstream.get()));
  if (!psr)
    throw std::runtime_error("Antlr out of memory");  
  
  IQLParser_graph_return parserRet = psr->graph(psr.get());
  if (psr->pParser->rec->state->errorCount > 0)
    throw std::runtime_error("Parse failed");

  // std::cout << parserRet.tree->toStringTree(parserRet.tree)->chars << std::endl;

  // Now pass through the type checker
  IQLGraphContextRef gc = wrap(this);
  ANTLR3AutoPtr<ANTLR3_COMMON_TREE_NODE_STREAM> nodes(antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT));
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(nodes.get()));
  alz->graph(alz.get(), gc);
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");
}

IQLRecordTypeBuilder::IQLRecordTypeBuilder(DynamicRecordContext& ctxt,
					   const std::string& spec, 
					   bool isFile)
  :
  mContext(ctxt)
{
  // Parse the transfer spec and generate the program to perform the operations.
  // Feed from an in place stream
  ANTLR3AutoPtr<ANTLR3_INPUT_STREAM> input(isFile ?
					   antlr3AsciiFileStreamNew((pANTLR3_UINT8) spec.c_str()) :
					   antlr3NewAsciiStringInPlaceStream((pANTLR3_UINT8) spec.c_str(),
									     spec.size(), 
									     (pANTLR3_UINT8) "RecordTypeParser"));
  if (!input)
    throw std::runtime_error("Antlr out of memory");
  
  // SQL is case insensitive
  input->setUcaseLA(input.get(), ANTLR3_TRUE);

  ANTLR3AutoPtr<IQLLexer> lxr(IQLLexerNew(input.get()));
  if (!lxr)
    throw std::runtime_error("Antlr out of memory");

  ANTLR3AutoPtr<ANTLR3_COMMON_TOKEN_STREAM> tstream(antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, 
										     TOKENSOURCE(lxr.get())));
  ANTLR3AutoPtr<IQLParser> psr(IQLParserNew(tstream.get()));
  if (!psr)
    throw std::runtime_error("Antlr out of memory");  
  
  IQLParser_recordFormat_return parserRet = psr->recordFormat(psr.get());
  if (psr->pParser->rec->state->errorCount > 0)
    throw std::runtime_error("Parse failed");

  // std::cout << parserRet.tree->toStringTree(parserRet.tree)->chars << std::endl;

  // Now pass through builder
  TypeCheckContext typeCheckContext(ctxt);
  IQLRecordTypeContextRef gc = wrap(this);
  ANTLR3AutoPtr<ANTLR3_COMMON_TREE_NODE_STREAM> nodes(antlr3CommonTreeNodeStreamNewTree(parserRet.tree, ANTLR3_SIZE_HINT));
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(nodes.get()));
  alz->recordFormat(alz.get(), gc, wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");
}

IQLRecordTypeBuilder::~IQLRecordTypeBuilder()
{
}

RecordTypeAggregate::RecordTypeAggregate(DynamicRecordContext& recCtxt, 
					 const std::string & funName, 
					 const RecordType * source, 
					 const std::string& transfer,
					 const std::vector<std::string>& groupKeys,
					 bool isOlap)
  :
  mSource(source),
  mAggregate(NULL),
  mTarget(NULL),
  mIsIdentity(false)
{
  mInitializeFun = funName + "$init";
  mUpdateFun = funName + "$update";
  mTransferFun = funName + "$transfer";

  IQLParserStuff p;
  p.parseTransfer(transfer);

  // Create an appropriate context for type checking.  This requires associating the
  // input record with a name and then inserting all the members of the record type
  // with a symbol table.
  TypeCheckContext typeCheckContext(recCtxt, mSource, groupKeys, isOlap);

  // Now pass through the type checker
  ANTLR3AutoPtr<IQLTypeCheck> alz(IQLTypeCheckNew(p.getNodes()));
  alz->recordConstructor(alz.get(), wrap(&typeCheckContext));
  if (alz->pTreeParser->rec->state->errorCount > 0)
    throw std::runtime_error("Type check failed");

  // There should be a present for us now...
  if (typeCheckContext.getOutputRecord() == NULL ||
      typeCheckContext.getAggregateRecord() == NULL)
    throw std::runtime_error("Failed to create output record");
  
 mTarget = typeCheckContext.getOutputRecord();
 mAggregate = typeCheckContext.getAggregateRecord();

 // Create a valid code generation context based on the input and output record formats.
 InitializeLLVM();

 // Create update function on top of source
 // and aggregate. Mask out group by keys in
 // aggregate to avoid conflicts.
 createUpdateFunction(groupKeys);

 // Save stuff into Update specific variables.
 mContext->saveAggregateContext(&mContext->Update);

 // Reinitialize and create initializer.
 mContext->createFunctionContext();
 createTransferFunction(mInitializeFun, mSource, mAggregate);

 // Generate code to initialize group by keys (nothing
 // about this exists in the aggregate functions we have).
 mContext->AggFn = 0;
 for(std::vector<std::string>::const_iterator it = groupKeys.begin();
     it != groupKeys.end();
     ++it) {
   LLVMSetField(wrap(mContext),
		&mContext->AggFn, 
		IQLToLLVMBuildVariableRef(wrap(mContext), it->c_str(), NULL));
 }
 // We know that aggregate initialization isn't
 // identity.  Reset the flag so we can find out
 // about the top level transfer.
 mContext->IsIdentity = 1;

 // Save stuff into Initialize specific variables.
 mContext->saveAggregateContext(&mContext->Initialize);

 // Reinitialize and create transfer
 mContext->createFunctionContext();
 if (!isOlap) {
   createTransferFunction(mTransferFun, mAggregate, mTarget);
 } else {
   // In the OLAP case, we have a Transfer2 going on in which
   // we have the source record and the aggregate to transfer 
   // from.  Mask out the group keys from aggregate record to
   // avoid name conflicts.
   // TODO: In the sort running total case we don't need
   // group keys in the aggregate record.  In the hash case we
   // do (so we can put the aggregate record into a hash table).
   std::vector<const RecordType *> updateSources;
   updateSources.push_back(mSource);
   updateSources.push_back(mAggregate);
   std::vector<boost::dynamic_bitset<> > masks(2);
   masks[0].resize(updateSources[0]->size(), true);
   masks[1].resize(updateSources[1]->size(), true);
   for(std::size_t i=0; i<groupKeys.size(); i++) {
     masks[1].set(i, false);
   }
   createTransferFunction(mTransferFun, updateSources, masks, mTarget);
   mContext->IQLMoveSemantics = 0;
 }
 mContext->saveAggregateContext(&mContext->Transfer);

 // Code generate
 ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(p.getNodes()));  
 toLLVM->recordConstructor(toLLVM.get(), wrap(mContext));

 // Complete all builders
 LLVMBuildRetVoid(mContext->Update.Builder);
 LLVMBuildRetVoid(mContext->Initialize.Builder);
 LLVMBuildRetVoid(mContext->Transfer.Builder);
    
 mIsIdentity = 1==mContext->IsIdentity;

 llvm::verifyFunction(*llvm::unwrap<llvm::Function>(mContext->Update.Function));
 llvm::verifyFunction(*llvm::unwrap<llvm::Function>(mContext->Initialize.Function));
 llvm::verifyFunction(*llvm::unwrap<llvm::Function>(mContext->Transfer.Function));

 // // Now run optimizer over the IR
 mFPM->run(*llvm::unwrap<llvm::Function>(mContext->Update.Function));
 mFPM->run(*llvm::unwrap<llvm::Function>(mContext->Initialize.Function));
 mFPM->run(*llvm::unwrap<llvm::Function>(mContext->Transfer.Function));

 // Save the built module as bitcode
 llvm::BitstreamWriter writer(mBitcode);
 llvm::WriteBitcodeToStream(llvm::unwrap(mContext->LLVMModule), writer);
 // Odd thing about MemoryBuffer::getMemBuffer is that it actually wants the buffer to have an
 // extra null byte at the end (but that extra byte is not reported as part of the size
 // just as with strings).  Put that null terminator here.
 mBitcode.push_back(0);
}

RecordTypeAggregate::RecordTypeAggregate(class DynamicRecordContext& recCtxt, 
					 const std::string & funName, 
					 const RecordType * source, 
					 const std::string& initializer,
					 const std::string& update,
					 const std::vector<std::string>& groupKeys,
					 bool isOlap)
  :
  mSource(source),
  mAggregate(NULL),
  mTarget(NULL),
  mIsIdentity(false)
{
  init(recCtxt, funName, source, initializer, update, groupKeys, isOlap);
}

RecordTypeAggregate::~RecordTypeAggregate()
{
}

void RecordTypeAggregate::init(class DynamicRecordContext& recCtxt, 
			       const std::string & funName, 
			       const RecordType * source, 
			       const std::string& initializer,
			       const std::string& update,
			       const std::vector<std::string>& groupKeys,
			       bool isOlap)
{
  mInitializeFun = funName + "$init";
  mUpdateFun = funName + "$update";
  mTransferFun = funName + "$transfer";

  // First parse all into AST
  IQLParserStuff initParser;
  initParser.parseTransfer(initializer);
  mAggregate = initParser.typeCheckTransfer(recCtxt, source);
  // TODO: Add type checking phase to update
  IQLParserStuff updateParser;
  updateParser.parseUpdate(update);
  std::vector<const RecordType *> updateSources;
  updateSources.push_back(mSource);
  updateSources.push_back(mAggregate);
  std::vector<boost::dynamic_bitset<> > masks(2);
  masks[0].resize(updateSources[0]->size(), true);
  masks[1].resize(updateSources[1]->size(), true);
  for(std::size_t i=0; i<groupKeys.size(); i++) {
    masks[1].set(i, false);
  }
  updateParser.typeCheckUpdate(recCtxt, updateSources, masks);
  //
  // Code gen time
  //
  std::vector<llvm::Function *> funs;
  InitializeLLVM();

  //
  // Update function
  //

  // Create the LLVM function and populate variables in
  // symbol table to prepare for code gen.
  createUpdateFunction(groupKeys);
  {
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(updateParser.getNodes()));  
    toLLVM->statementBlock(toLLVM.get(), wrap(mContext));
    LLVMBuildRetVoid(mContext->LLVMBuilder);
    funs.push_back(llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
  }

  // 
  // Init function
  //
  // Reinitialize and create transfer
  mContext->reinitialize();

  createTransferFunction(mInitializeFun, mSource, mAggregate);
  {
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(initParser.getNodes()));  
    toLLVM->recordConstructor(toLLVM.get(), wrap(mContext));
    LLVMBuildRetVoid(mContext->LLVMBuilder);
    funs.push_back(llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
  }
  
  // 
  // Transfer function
  //
  mContext->reinitialize();

  // Subtle point, the string argument to parser
  // needs to be at same scope because we are not
  // copying the string inside of the parser and the
  // constructed trees have pointers into the string.
  std::string defaultTransfer;
  IQLParserStuff transferParser;
  // createTransferFunction(mTransferFun, mAggregate, mTarget);
  if (!isOlap) {
    defaultTransfer = "input.*";
    transferParser.parseTransfer(defaultTransfer);
    mTarget = transferParser.typeCheckTransfer(recCtxt, mAggregate);
    createTransferFunction(mTransferFun, mAggregate, mTarget);
  } else {
    std::stringstream xfer;
    xfer << "input0.*";
    // In the OLAP case, we have a Transfer2 going on in which
    // we have the source record and the aggregate to transfer 
    // from.  Mask out the group keys from aggregate record to
    // avoid name conflicts.
    // TODO: In the sort running total case we don't need
    // group keys in the aggregate record.  In the hash case we
    // do (so we can put the aggregate record into a hash table).
    std::vector<const RecordType *> updateSources;
    updateSources.push_back(mSource);
    updateSources.push_back(mAggregate);
    std::vector<boost::dynamic_bitset<> > masks(2);
    masks[0].resize(updateSources[0]->size(), true);
    masks[1].resize(updateSources[1]->size(), true);
    for(std::size_t i=0; i<groupKeys.size(); i++) {
      masks[1].set(i, false);
    }
    for(std::size_t i=groupKeys.size(); i < mAggregate->size(); i++) {
      xfer << ", " << mAggregate->GetMember(i).GetName().c_str();
    }
    std::vector<AliasedRecordType> types;
    types.push_back(AliasedRecordType("input0", mSource));
    types.push_back(AliasedRecordType("input1", mAggregate));

    defaultTransfer = xfer.str();
    transferParser.parseTransfer(defaultTransfer);
    mTarget = transferParser.typeCheckTransfer(recCtxt, types, masks);
    createTransferFunction(mTransferFun, updateSources, masks, mTarget);
    mContext->IQLMoveSemantics = 0;
  }
  mContext->IsIdentity = 1;
  {
    ANTLR3AutoPtr<IQLToLLVM> toLLVM(IQLToLLVMNew(transferParser.getNodes()));  
    toLLVM->recordConstructor(toLLVM.get(), wrap(mContext));
    LLVMBuildRetVoid(mContext->LLVMBuilder);
    funs.push_back(llvm::unwrap<llvm::Function>(mContext->LLVMFunction));
  }
  mIsIdentity = 1==mContext->IsIdentity;
  BOOST_ASSERT(isOlap || mIsIdentity);

 // Verify and optimize
 for(std::vector<llvm::Function*>::iterator it=funs.begin();
     it != funs.end();
     ++it) {
   llvm::verifyFunction(**it);
   mFPM->run(**it);
 }

 // Save the built module as bitcode
 llvm::BitstreamWriter writer(mBitcode);
 llvm::WriteBitcodeToStream(llvm::unwrap(mContext->LLVMModule), writer);
 // Odd thing about MemoryBuffer::getMemBuffer is that it actually wants the buffer to have an
 // extra null byte at the end (but that extra byte is not reported as part of the size
 // just as with strings).  Put that null terminator here.
 mBitcode.push_back(0);
}

void RecordTypeAggregate::createUpdateFunction(const std::vector<std::string>& groupKeys)
{
  // Create update function on top of source
  // and aggregate. Mask out group by keys in
  // aggregate to avoid conflicts.
  std::vector<const RecordType *> updateSources;
  updateSources.push_back(mSource);
  updateSources.push_back(mAggregate);
  std::vector<boost::dynamic_bitset<> > masks(2);
  masks[0].resize(updateSources[0]->size(), true);
  masks[1].resize(updateSources[1]->size(), true);
  for(std::size_t i=0; i<groupKeys.size(); i++) {
    masks[1].set(i, false);
  }
  createUpdate(mUpdateFun, updateSources, masks);
  mContext->IQLMoveSemantics = 0;
}

IQLAggregateModule * RecordTypeAggregate::create() const
{
  return new IQLAggregateModule(mAggregate->getMalloc(),
				mTarget->getMalloc(),
				mInitializeFun,
				mUpdateFun,
				mTransferFun,
				mBitcode,
				mIsIdentity);
}

IQLAggregateModule::IQLAggregateModule(const RecordTypeMalloc& aggregateMalloc,
				       const RecordTypeMalloc& targetMalloc,
				       const std::string& initName, 
				       const std::string& updateName,
				       const std::string& transferName,
				       const std::vector<unsigned char>& bitcode,
				       bool isTransferIdentity)
  :
  mAggregateMalloc(aggregateMalloc),
  mTransferMalloc(targetMalloc),
  mInitName(initName),
  mUpdateName(updateName),
  mTransferName(transferName),
  mBitcode(bitcode),
  mInitFunction(NULL),
  mUpdateFunction(NULL),
  mTransferFunction(NULL),
  mImpl(NULL),
  mIsTransferIdentity(isTransferIdentity)
{
  initImpl();
}

IQLAggregateModule::~IQLAggregateModule()
{
  delete mImpl;
}

void IQLAggregateModule::initImpl()
{
  // TODO: The remaining stuff should be in a separate class because
  // we really want to be able to push the bitcode over the wire.
  std::vector<std::string> funNames;
  funNames.push_back(mInitName);
  funNames.push_back(mUpdateName);
  funNames.push_back(mTransferName);
  mImpl = new IQLRecordBufferMethodHandle(mBitcode, funNames);
  mInitFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[0]);
  mUpdateFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[1]);
  mTransferFunction = (LLVMFuncType) mImpl->getFunPtr(funNames[2]);
}

void IQLAggregateModule::executeInit(RecordBuffer & source, 
				     RecordBuffer & target, 
				     class InterpreterContext * ctxt) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mAggregateMalloc.malloc();
  (*mInitFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);      
}

void IQLAggregateModule::executeUpdate(RecordBuffer source, RecordBuffer target, class InterpreterContext * ctxt) const
{
  (*mUpdateFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);      
}

void IQLAggregateModule::executeTransfer(RecordBuffer & source, 
					 RecordBuffer & target, 
					 class InterpreterContext * ctxt) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mTransferMalloc.malloc();
  (*mTransferFunction)((char *) source.Ptr, (char *) target.Ptr, ctxt);      
}

void IQLAggregateModule::executeTransfer(RecordBuffer & source1, 
					 RecordBuffer & source2, 
					 RecordBuffer & target, 
					 class InterpreterContext * ctxt) const
{
  BOOST_ASSERT(target == RecordBuffer());
  target = mTransferMalloc.malloc();  
  (*((LLVMFuncType2) mTransferFunction))((char *) source1.Ptr, 
					 (char *) source2.Ptr, 
					 (char *) target.Ptr, ctxt);      
}

