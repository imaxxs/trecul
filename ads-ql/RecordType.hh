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

#ifndef __RECORDTYPE_HH
#define __RECORDTYPE_HH

#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <stdexcept>
#include <boost/format.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/vector.hpp>

namespace llvm {
  class Type;
  class Value;
}

extern "C" {
#include "decimal128.h"
}

#include "RecordBuffer.hh"

class CodeGenerationContext;
class FieldType;
class RecordType;

// Runtime representations of primitive fields.
// We require a C interface for these so LLVM can
// call functions with these parameters.
typedef struct {
  int32_t Size;
  const char * Ptr;
} Varchar;

class FieldAddress
{
private:
  uint32_t mOffset;
  uint32_t mPosition;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mOffset);
    ar & BOOST_SERIALIZATION_NVP(mPosition);
  }

public:
  FieldAddress(uint32_t offset=0, uint32_t position=0)
    :
    mOffset(offset),
    mPosition(position)
  {
  }
  void setNull(RecordBuffer buffer) const
  {
    if (mPosition == 0xffffffff)
      throw std::runtime_error("Can't set NULL on a non-nullable field");

    // put 0 into bit
    uint32_t dwordPos = mPosition >> 5;
    uint32_t mask = 1 << (mPosition - (dwordPos << 5));
    ((uint32_t *) buffer.Ptr)[dwordPos] &= (~mask);
  }
  void clearNull(RecordBuffer buffer) const
  {
    if (mPosition != 0xffffffff) {
      // put 1 into bit
      uint32_t dwordPos = mPosition >> 5;
      uint32_t mask = 1 << (mPosition - (dwordPos << 5));
      ((uint32_t *) buffer.Ptr)[dwordPos] |= mask;
    }
  }
  bool isNull(RecordBuffer buffer) const
  {
    if (mPosition != 0xffffffff) {
      // NULL means there is a zero bit
      uint32_t dwordPos = mPosition >> 5;
      uint32_t mask = 1 << (mPosition - (dwordPos << 5));
      uint32_t ret = ((uint32_t *) buffer.Ptr)[dwordPos] & mask;
      return ret == 0;
    } else {
      return false;
    }
  }
  void setInt32(int32_t val, RecordBuffer buffer) const
  {
    clearNull(buffer);
    *(int32_t *) (buffer.Ptr + mOffset) = val;
  }
  void setInt64(int64_t val, RecordBuffer buffer) const
  {
    clearNull(buffer);
    *(int64_t *) (buffer.Ptr + mOffset) = val;
  }
  void setDouble(double val, RecordBuffer buffer) const
  {
    clearNull(buffer);
    *(double *) (buffer.Ptr + mOffset) = val;
  }
  void setDatetime(boost::posix_time::ptime val, RecordBuffer buffer) const
  {
    clearNull(buffer);
    *(boost::posix_time::ptime *) (buffer.Ptr + mOffset) = val;
  }
  void setDate(boost::gregorian::date val, RecordBuffer buffer) const
  {
    clearNull(buffer);
    *(boost::gregorian::date *) (buffer.Ptr + mOffset) = val;
  }
  void setDecimal(decimal128 & val, RecordBuffer buffer) const
  {
    clearNull(buffer);
    *(decimal128 *) (buffer.Ptr + mOffset) = val;
  }
  int32_t getInt32(RecordBuffer buffer) const
  {
    return *(int32_t *) (buffer.Ptr + mOffset);
  }
  int32_t getArrayInt32(RecordBuffer buffer, int idx) const
  {
    return ((int32_t *) (buffer.Ptr + mOffset))[idx];
  }
  int64_t getInt64(RecordBuffer buffer) const
  {
    return *(int64_t *) (buffer.Ptr + mOffset);
  }
  int64_t getArrayInt64(RecordBuffer buffer, int idx) const
  {
    return ((int64_t *) (buffer.Ptr + mOffset))[idx];
  }
  double getDouble(RecordBuffer buffer) const
  {
    return *(double *) (buffer.Ptr + mOffset);
  }
  double getArrayDouble(RecordBuffer buffer, int idx) const
  {
    return ((double *) (buffer.Ptr + mOffset))[idx];
  }
  boost::posix_time::ptime getDatetime(RecordBuffer buffer) const
  {
    return *(boost::posix_time::ptime *) (buffer.Ptr + mOffset);
  }
  boost::gregorian::date getDate(RecordBuffer buffer) const
  {
    return *(boost::gregorian::date *) (buffer.Ptr + mOffset);
  }
  void SetFixedLengthString(RecordBuffer buffer, const char * begin, std::size_t sz) const
  {
    clearNull(buffer);
    memcpy(buffer.Ptr + mOffset, begin, sz);
    *(buffer.Ptr + mOffset + sz) = 0;
  }
  void SetFixedLengthString(RecordBuffer buffer, const char * begin, int32_t strSz, uint8_t padChar, int32_t totalSz) const
  {
    clearNull(buffer);
    memcpy(buffer.Ptr + mOffset, begin, strSz);
    if (totalSz != strSz)
      memset(buffer.Ptr + mOffset + strSz, padChar, (totalSz - strSz));
    // Null terminate
    *(buffer.Ptr + mOffset + totalSz) = 0;
  }
  void SetVariableLengthString(RecordBuffer buffer, const char * begin, std::size_t sz) const
  {
    clearNull(buffer);
    Varchar * internalString =  (Varchar *) (buffer.Ptr + mOffset);
    internalString->Size = sz;
    char * tmp = (char *) ::malloc(sz + 1);
    memcpy(tmp, begin, sz);
    *(tmp + sz) = 0;
    internalString->Ptr = tmp;
  }
  int32_t * getInt32Ptr(RecordBuffer buffer) const
  {
    return (int32_t *) (buffer.Ptr + mOffset);
  }

  int64_t * getInt64Ptr(RecordBuffer buffer) const
  {
    return (int64_t *) (buffer.Ptr + mOffset);
  }

  double * getDoublePtr(RecordBuffer buffer) const
  {
    return (double *) (buffer.Ptr + mOffset);
  }

  boost::posix_time::ptime * getDatetimePtr(RecordBuffer buffer) const
  {
    return (boost::posix_time::ptime *) (buffer.Ptr + mOffset);
  }

  boost::gregorian::date * getDatePtr(RecordBuffer buffer) const
  {
    return (boost::gregorian::date *) (buffer.Ptr + mOffset);
  }

  Varchar * getVarcharPtr(RecordBuffer buffer) const
  {
    return (Varchar *) (buffer.Ptr + mOffset);
  }

  char * getCharPtr(RecordBuffer buffer) const
  {
    return (char *) (buffer.Ptr + mOffset);
  }

  decimal128 * getDecimalPtr(RecordBuffer buffer) const
  {
    return (decimal128 *) (buffer.Ptr + mOffset);
  }

  bool operator<(const FieldAddress & rhs) const
  {
    return mOffset < rhs.mOffset;
  }

  bool operator==(const FieldAddress & rhs) const
  {
    return mOffset == rhs.mOffset;
  }

  bool operator!=(const FieldAddress & rhs) const
  {
    return mOffset != rhs.mOffset;
  }

  std::ptrdiff_t operator-(const FieldAddress & rhs) const
  {
    return std::ptrdiff_t(mOffset) - std::ptrdiff_t(rhs.mOffset);
  }

  bool contiguous(const FieldAddress& rhs, std::size_t sz) const
  {
    return mOffset + sz == rhs.mOffset;
  }

  /**
   * Get the address of the dword containing the bit for
   * this field.
   */
  FieldAddress getBitwordAddress() const
  {
    return FieldAddress((mPosition >> 5)*sizeof(uint32_t), 0xffffffff);
  }

  // Code generate instructions to get an untyped pointer to member given a base pointer.
  llvm::Value * getPointer(const std::string& member, 
			   CodeGenerationContext * ctxt, 
			   llvm::Value * basePointer) const;
  /**
   * Code generate instructions to get the NULL value.
   */
  llvm::Value * isNull(const std::string& member, 
		       CodeGenerationContext * ctxt, 
		       llvm::Value * basePointer) const;
  /**
   * Code generate instructions to set/clear the NULL value.
   */
  void setNull(const std::string& member, 
	       CodeGenerationContext * ctxt, 
	       llvm::Value * basePointer,
	       bool isNull) const;
  void dump() const;
};

class Digest
{
private:
  uint8_t mDigest[16];
public:
  Digest()
  {
    memset(&mDigest[0], 0, 16);
  }
  Digest(unsigned char digest[16])
  {
    memcpy(&mDigest[0], &digest[0], 16);
  }
  ~Digest()
  {
  }
  bool operator < (const Digest & rhs) const
  {
    for(int i=0; i<16; i++) {
      if (mDigest[i] < rhs.mDigest[i]) return true;
      else if (mDigest[i] > rhs.mDigest[i]) return false;
    }
    return false;
  }
};

class DynamicRecordContext
{
private:
  std::map<Digest, FieldType *> mTypes;
  std::set<const RecordType *> mRecords;
  std::set<class IQLExpression *> mExprs;
public:
  DynamicRecordContext();
  ~DynamicRecordContext();
  FieldType * lookup(const Digest& id) const;
  void add(const Digest& id, FieldType * val);
  void add(const RecordType * ty);
  void add(class IQLExpression * expr);
};

class FieldType
{
public:
  enum FieldTypeEnum { VARCHAR, /* Variable length strings */
		       CHAR, /* Fixed length string */
		       BIGDECIMAL, /* 128-bit decimals */
		       INT32, /* Signed 32 bit Integers */
		       INT64, /* Signed 64 bit Integers */
		       DOUBLE, /* IEEE double precision */
		       DATETIME, /* Boost datetime */
		       DATE, /* Boost gregorian date */
		       FUNCTION, /* Function types are NOT allowed as fields at this point. */
		       FIXED_ARRAY, /* Fixed Length Array. */
		       INTERVAL, /* Interval types */
		       NIL /* Type of literal NULL */
  };
private:
  DynamicRecordContext & mContext;
  FieldTypeEnum mType;
  int32_t mSize;
  bool mNullable;
public:
  FieldType(DynamicRecordContext& ctxt, FieldTypeEnum ty, bool nullable)
    :
    mContext(ctxt),
    mType(ty),
    mSize(0),
    mNullable(nullable)
  {
  }
  FieldType(DynamicRecordContext& ctxt, FieldTypeEnum ty, 
	    int32_t sz, bool nullable)
    :
    mContext(ctxt),
    mType(ty),
    mSize(sz),
    mNullable(nullable)
  {
  }
  virtual ~FieldType()
  {
  }
  FieldTypeEnum GetEnum() const
  {
    return mType; 
  }
  int32_t GetSize() const 
  {
    return mSize;
  }
  bool isNullable() const 
  {
    return mNullable;
  }

  // TODO: Convert the following into double dispatch calls 
  // as they depend on the actual field type and the system architecture.
  virtual std::size_t GetAlignment() const 
  {
    switch(mType) {
    case VARCHAR:
      return 8;
    case CHAR:
      return 1;
    case BIGDECIMAL:
      return 1;
    case INT32:
      return 4;
    case INT64:
      return 8;
    case DOUBLE:
      return sizeof(double);
    case DATETIME:
      return sizeof(boost::posix_time::ptime);
    case DATE:
      return sizeof(boost::gregorian::date);
    case FUNCTION:
      throw std::runtime_error("Function types cannot be field values");
    case INTERVAL:
      return 4;
    default:
      throw std::runtime_error((boost::format("Invalid Type value: %1%") % mType).str());
    }
  }
  virtual std::size_t GetAllocSize() const
  {
    switch(mType) {
    case VARCHAR:
      return sizeof(Varchar);
    case CHAR:
      return mSize + 1;
    case BIGDECIMAL:
      return 16;
    case INT32:
      return 4;
    case INT64:
      return 8;
    case DOUBLE:
      return sizeof(double);
    case DATETIME:
      return sizeof(boost::posix_time::ptime);
    case DATE:
      return sizeof(boost::gregorian::date);
    case INTERVAL:
      return 4;
    case FUNCTION:
      throw std::runtime_error("Function types cannot be field values");
    default:
      throw std::runtime_error("Invalid Type value");
    }
  }

  virtual llvm::Type * LLVMGetType(CodeGenerationContext * ctxt) const;

  /**
   * Append my state to an md5 hash
   */
  virtual void AppendTo(struct md5_state_s * md5) const;

  /**
   * Text representation of type.
   */
  virtual std::string toString() const=0;

  /**
   * Create a version of this type with appropriate
   * nullability.
   * TODO: Make pure virutal
   */
  virtual const FieldType * clone(bool nullable) const
  {
    if (nullable == isNullable()) return this;
    throw std::runtime_error("Cloning to change nullability not supported");
  }

  /**
   * Get special values if possible 
   */
  virtual llvm::Value * getMinValue(class CodeGenerationContext * ctxt) const;
  virtual llvm::Value * getMaxValue(CodeGenerationContext * ctxt) const;
  virtual llvm::Value * getZero(CodeGenerationContext * ctxt) const;

  /**
   * Context this type was created in.
   */
  DynamicRecordContext & getContext() const
  {
    return mContext;
  }
};

class VarcharType : public FieldType
{
public:
  typedef Varchar runtime_type;
private:
  VarcharType(DynamicRecordContext& ctxt, int32_t sz, bool nullable)
    :
    FieldType(ctxt, FieldType::VARCHAR, sz, nullable)
  {
  }
public:
  static VarcharType * Get(DynamicRecordContext& ctxt);
  static VarcharType * Get(DynamicRecordContext& ctxt, bool nullable);
  static VarcharType * Get(DynamicRecordContext& ctxt, int32_t sz, bool nullable);
  ~VarcharType();
  /**
   * Text representation of type.
   */
  std::string toString() const;

  const FieldType * clone(bool nullable) const;

  llvm::Value * getMinValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getMaxValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getZero(CodeGenerationContext * ctxt) const;
};

class CharType : public FieldType
{
private: 
  CharType(DynamicRecordContext& ctxt, int32_t sz, bool nullable)
    :
    FieldType(ctxt, FieldType::CHAR, sz, nullable)
  {
  }
public:
  static CharType * Get(DynamicRecordContext& ctxt, int32_t sz, bool nullable=false);
  ~CharType();

  /**
   * Text representation of type.
   */
  std::string toString() const;

  const FieldType * clone(bool nullable) const;

  llvm::Value * getMinValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getMaxValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getZero(CodeGenerationContext * ctxt) const;
};

class Int32Type : public FieldType
{
private:
  Int32Type(DynamicRecordContext& ctxt, bool nullable)
    :
    FieldType(ctxt, FieldType::INT32, 4, nullable)
  {
  }
public:
  static Int32Type * Get(DynamicRecordContext& ctxt, bool nullable = false);
  ~Int32Type();
  /**
   * Text representation of type.
   */
  std::string toString() const;

  const FieldType * clone(bool nullable) const;

  llvm::Value * getMinValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getMaxValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getZero(CodeGenerationContext * ctxt) const;
};

class Int64Type : public FieldType
{
private:
  Int64Type(DynamicRecordContext& ctxt, bool nullable)
    :
    FieldType(ctxt, FieldType::INT64, 8, nullable)
  {
  }
public:
  static Int64Type * Get(DynamicRecordContext& ctxt, bool nullable=false);
  ~Int64Type();
  /**
   * Text representation of type.
   */
  std::string toString() const;

  const FieldType * clone(bool nullable) const;

  llvm::Value * getMinValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getMaxValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getZero(CodeGenerationContext * ctxt) const;
};

class DoubleType : public FieldType
{
private:
  DoubleType(DynamicRecordContext& ctxt, bool nullable)
    :
    FieldType(ctxt, FieldType::DOUBLE, sizeof(double), nullable)
  {
  }
public:
  static DoubleType * Get(DynamicRecordContext& ctxt, bool nullable = false);
  ~DoubleType();
  /**
   * Text representation of type.
   */
  std::string toString() const;

  const FieldType * clone(bool nullable) const;

  /**
   * Special Values
   */
  llvm::Value * getMinValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getMaxValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getZero(CodeGenerationContext * ctxt) const;
};

/**
 * A decimal type.
 */
class DecimalType : public FieldType
{
private:
  DecimalType(DynamicRecordContext& ctxt, bool nullable)
    :
    FieldType(ctxt, FieldType::BIGDECIMAL, 16, nullable)
  {
  }
  llvm::Value * createGlobalValue(CodeGenerationContext * ctxt,
				  const decimal128& dec) const;
public:
  static DecimalType * Get(DynamicRecordContext& ctxt, bool nullable=false);
  ~DecimalType();
  /**
   * Text representation of type.
   */
  std::string toString() const;

  const FieldType * clone(bool nullable) const;

  /**
   * Special Values
   */
  llvm::Value * getMinValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getMaxValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getZero(CodeGenerationContext * ctxt) const;
};

/**
 * A datetime type.
 */
class DatetimeType : public FieldType
{
private:
  DatetimeType(DynamicRecordContext& ctxt, bool nullable)
    :
    FieldType(ctxt, FieldType::DATETIME, sizeof(double), nullable)
  {
  }
public:
  static DatetimeType * Get(DynamicRecordContext& ctxt, bool nullable = false);
  ~DatetimeType();
  /**
   * Text representation of type.
   */
  std::string toString() const;

  const FieldType * clone(bool nullable) const;

  llvm::Value * getMinValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getMaxValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getZero(CodeGenerationContext * ctxt) const;
};

/**
 * A date type for Gregorian calendar.
 */
class DateType : public FieldType
{
private:
  DateType(DynamicRecordContext& ctxt, bool nullable)
    :
    FieldType(ctxt, FieldType::DATE, sizeof(boost::gregorian::date), nullable)
  {
  }
public:
  static DateType * Get(DynamicRecordContext& ctxt, bool nullable=false);
  ~DateType();
  /**
   * Text representation of type.
   */
  std::string toString() const;


  const FieldType * clone(bool nullable) const;

  llvm::Value * getMinValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getMaxValue(CodeGenerationContext * ctxt) const;
  llvm::Value * getZero(CodeGenerationContext * ctxt) const;
};

class FunctionType : public FieldType
{
private:
  std::vector<const FieldType *> mArgs;
  const FieldType * mRet;

  FunctionType(DynamicRecordContext& ctxt, 
	       const std::vector<const FieldType *>& args, 
	       const FieldType * ret)
    :
    FieldType(ctxt, FieldType::FUNCTION, args.size()),
    mArgs(args),
    mRet(ret)
  {
  }
  static void AppendTo(const std::vector<const FieldType *>& args, 
		       const FieldType * ret,
		       struct md5_state_s * md5);
public:
  static FunctionType * Get(DynamicRecordContext& ctxt, 
			    const std::vector<const FieldType *>& args, 
			    const FieldType * ret);
  static FunctionType * Get(DynamicRecordContext& ctxt, 
			    const FieldType * ret);
  static FunctionType * Get(DynamicRecordContext& ctxt, 
			    const FieldType * arg1,
			    const FieldType * ret);
  static FunctionType * Get(DynamicRecordContext& ctxt, 
			    const FieldType * arg1,
			    const FieldType * arg2,
			    const FieldType * ret);
  static FunctionType * Get(DynamicRecordContext& ctxt, 
			    const FieldType * arg1,
			    const FieldType * arg2,
			    const FieldType * arg3,
			    const FieldType * ret);
  ~FunctionType();
  const std::vector<const FieldType *>& GetArgs() const { return mArgs; }
  const FieldType * GetReturn() const { return mRet; }
  /**
   * Append my state to an md5 hash
   */
  void AppendTo(struct md5_state_s * md5) const;
  /**
   * Text representation of type.
   */
  std::string toString() const;
};

/**
 * A fixed length array.
 */
class FixedArrayType : public FieldType
{
private:
  const FieldType * mElementTy;

  FixedArrayType(DynamicRecordContext& ctxt, 
		 int32_t sz,
		 const FieldType * elementTy,
		 bool nullable)
    :
    FieldType(ctxt, FieldType::FIXED_ARRAY, sz, nullable),    
    mElementTy(elementTy)
  {
  }
  static void AppendTo(int32_t sz, const FieldType * element,
		       bool nullable, struct md5_state_s * md5);
public:
  static FixedArrayType * Get(DynamicRecordContext& ctxt, 
			      int32_t sz,
			      const FieldType * element,
			      bool nullable);
  ~FixedArrayType();
  const FieldType * getElementType() const 
  {
    return mElementTy;
  }
  std::size_t GetAlignment() const 
  {
    return mElementTy->GetAlignment();
  }
  std::size_t GetAllocSize() const
  {
    return mElementTy->GetAllocSize()*((std::size_t)GetSize());
  }
  /**
   * Append my state to an md5 hash
   */
  void AppendTo(struct md5_state_s * md5) const;
  /**
   * Text representation of type.
   */
  std::string toString() const;

  llvm::Type * LLVMGetType(CodeGenerationContext * ctxt) const;
};

class IntervalType : public FieldType
{
public:
  enum IntervalUnit { DAY, HOUR, MINUTE, MONTH, SECOND, YEAR };
  static const int32_t native_type_size = sizeof(int32_t);
private:
  IntervalUnit mIntervalUnit;

  IntervalType(DynamicRecordContext& ctxt, 
	       bool nullable,
	       IntervalUnit intervalUnit)
    :
    FieldType(ctxt, FieldType::INTERVAL, native_type_size, nullable),
    mIntervalUnit(intervalUnit)
  {
  }
public:
  static IntervalType * Get(DynamicRecordContext& ctxt, 
			    IntervalUnit intervalUnit,
			    bool nullable = false);
  ~IntervalType();
  /**
   * What is the unit of the interval.
   */
  IntervalUnit getIntervalUnit() const 
  { 
    return mIntervalUnit; 
  }

  /**
   * When added to a date type what is the result type?
   */
  const FieldType * getDateResultType(DynamicRecordContext& ctxt, 
				      bool nullable) const;

  /**
   * Append my state to an md5 hash
   */
  void AppendTo(struct md5_state_s * md5) const;
  /**
   * Text representation of type.
   */
  std::string toString() const;

  const FieldType * clone(bool nullable) const;
};

class NilType : public FieldType
{
private:
  NilType(DynamicRecordContext& ctxt)
    :
    FieldType(ctxt, FieldType::NIL, 0, true)
  {
  }
public:
  static NilType * Get(DynamicRecordContext& ctxt);
  ~NilType();
  /**
   * Text representation of type.
   */
  std::string toString() const;
};

class TaggedFieldAddress
{
private:
  FieldAddress mAddress;
  FieldType::FieldTypeEnum mTag;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mAddress);
    ar & BOOST_SERIALIZATION_NVP(mTag);
  }

public:
  TaggedFieldAddress()
    :
    mTag(FieldType::INT32)
  {
  }

  TaggedFieldAddress(const FieldAddress& address, FieldType::FieldTypeEnum tag)
    :
    mAddress(address),
    mTag(tag)
  {
  }
  void print(RecordBuffer buf, std::ostream& ostr) const;
};

class RecordTypePrint
{
private:
  std::vector<TaggedFieldAddress> mFields;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mFields);
  }
public:
  RecordTypePrint();
  RecordTypePrint(const std::vector<TaggedFieldAddress>& fields);
  RecordTypePrint(const TaggedFieldAddress& field)
    :
    mFields(1, field)
  {
  }
  ~RecordTypePrint();
  void imbue(std::ostream& ostr) const;
  void print(RecordBuffer buf, std::ostream& ostr, bool emitNewLine=true) const;
};

struct RecordBufferIterator
{
  // Which variable length field (if any) have I descended into.
  std::size_t offset;
  uint8_t * ptr;

  RecordBufferIterator()
  {
    clear();
  }

  void init(RecordBuffer buf)
  {
    offset = 0;
    ptr = buf.Ptr;
  }

  void clear()
  {
    // Make sure this causes the maximum damage!
    offset = std::numeric_limits<std::size_t>::max();
    ptr = NULL;
  }
};

class RecordTypeSerialize
{
private:
  std::size_t mSize;
  std::vector<FieldAddress> mOffsets;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mSize);
    ar & BOOST_SERIALIZATION_NVP(mOffsets);
  }
  
public:
  RecordTypeSerialize();
  RecordTypeSerialize(std::size_t sz, const std::vector<FieldAddress>& offsets);
  ~RecordTypeSerialize();
  bool doit(uint8_t * & output, uint8_t * outputEnd, RecordBufferIterator & inputPos, RecordBuffer buf) const;
  std::size_t getRecordLength(RecordBuffer buf) const;
};

class RecordTypeDeserialize
{
private:
  std::size_t mSize;
  std::vector<FieldAddress> mOffsets;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mSize);
    ar & BOOST_SERIALIZATION_NVP(mOffsets);
  }
public:
  RecordTypeDeserialize();
  RecordTypeDeserialize(std::size_t sz, const std::vector<FieldAddress>& offsets);
  ~RecordTypeDeserialize();
  bool Do(uint8_t * & input, uint8_t * inputEnd, RecordBufferIterator & outputPos, RecordBuffer buf) const;
};

class RecordTypeFree
{
private:
  std::vector<FieldAddress> mOffsets;
  std::size_t mSize;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mOffsets);
    ar & BOOST_SERIALIZATION_NVP(mSize);
  }
public:
  RecordTypeFree();
  RecordTypeFree(std::size_t sz, const std::vector<FieldAddress>& offsets);
  ~RecordTypeFree();
  void free(RecordBuffer buf) const;
};

class RecordTypeMalloc
{
private:
  std::size_t mSize;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mSize);
  }
public:
  RecordTypeMalloc(std::size_t sz=0);
  ~RecordTypeMalloc();
  RecordBuffer malloc() const;
};

class RecordMember
{
private:
  const FieldType * mType;
  std::string mName;
public:
  RecordMember(const std::string& name, const FieldType * ty)
    :
    mType(ty),
    mName(name)
  {
    BOOST_ASSERT(ty != NULL);
  }

  const std::string& GetName() const {
    return mName;
  }

  const FieldType * GetType() const {
    return mType; 
  }
};

/**
 * Represents the copying of some number of 
 * bits from dword offset to another.
 * TODO: Templatize on chunk size.
 */
class BitcpyOp
{
public:
  // The Source and Target byte offsets of the dwords
  // Not really honest FieldAddresses, but we leverage
  // the existing abstraction FieldAddress.getPointer
  // to get the location of the bitfield
  FieldAddress mSourceOffset;
  FieldAddress mTargetOffset;
  // Mask of the bits in the source we are copying.
  uint32_t mSourceBitmask;
  // Shift of the source bits to target.  May be positive
  // or negative (shift left vs. right); so bit i in the source
  // bitmask goes to bit i+mShift in the target.
  int32_t mShift;

  BitcpyOp(FieldAddress sourceOffset, FieldAddress targetOffset,
	   int32_t shift, uint32_t sourceBitmask)
    :
    mSourceOffset(sourceOffset),
    mTargetOffset(targetOffset),
    mSourceBitmask(sourceBitmask),
    mShift(shift)
  {
  }

  bool operator<(const BitcpyOp & rhs) const
  {
    return this->mSourceOffset < rhs.mSourceOffset ||
      (this->mSourceOffset == rhs.mSourceOffset && this->mTargetOffset < rhs.mTargetOffset) ||
      (this->mSourceOffset == rhs.mSourceOffset && this->mTargetOffset == rhs.mTargetOffset && this->mShift < rhs.mShift);
      
  }

  /**
   * N.B. This resorts the input in place.
   * Essential we group by sourceOffset,targetOffset,shift with aggregate of
   * the OR of all masks.
   */
  static void coalesce(std::vector<BitcpyOp>& input, std::vector<BitcpyOp>& output);
};

class BitsetOp
{
public:
  // The Target byte offsets of the dwords
  // Not really honest FieldAddresses, but we leverage
  // the existing abstraction FieldAddress.getPointer
  // to get the location of the bitfield
  FieldAddress mTargetOffset;
  // Mask of the bits we are to set
  uint32_t mTargetBitmask;

  BitsetOp(FieldAddress targetOffset, uint32_t targetBitmask)
    :
    mTargetOffset(targetOffset),
    mTargetBitmask(targetBitmask)
  {
  }

  bool operator<(const BitsetOp & rhs) const
  {
    return this->mTargetOffset < rhs.mTargetOffset;
  }

  /**
   * N.B. This resorts the input in place.
   * Essentially we group by targetOffset with aggregate of
   * the OR of all masks.
   */
  static void coalesce(std::vector<BitsetOp>& input, std::vector<BitsetOp>& output);
};

class MemcpyOp
{
public:
  FieldAddress mSourceOffset;
  FieldAddress mTargetOffset;
  size_t mSize;

  MemcpyOp(const FieldAddress& sourceOffset, const FieldAddress& targetOffset, size_t sz)
    :
    mSourceOffset(sourceOffset),
    mTargetOffset(targetOffset),
    mSize(sz)
  {
  }

  bool operator<(const MemcpyOp & rhs) const
  {
    return this->mSourceOffset < rhs.mSourceOffset;
  }

  /**
   * N.B. This resorts the input in place.
   */
  static void coalesce(std::vector<MemcpyOp>& input, std::vector<MemcpyOp>& output);
};

class MemsetOp
{
public:
  FieldAddress mSourceOffset;
  int mValue;
  size_t mSize;

  MemsetOp(const FieldAddress& sourceOffset, int value, size_t sz)
    :
    mSourceOffset(sourceOffset),
    mValue(value),
    mSize(sz)
  {
  }

  bool operator<(const MemsetOp & rhs) const
  {
    return this->mSourceOffset < rhs.mSourceOffset;
  }

  /**
   * N.B. This resorts the input in place.
   */
  static void coalesce(std::vector<MemsetOp>& input, std::vector<MemsetOp>& output);
};

class RecordType
{
  friend class RecordTypeMove;
  friend class RecordTypeCopy;

private:
  std::vector<RecordMember> mMembers;
  std::map<std::string, std::size_t> mMemberNames;

  boost::shared_ptr<RecordTypeMalloc> mMalloc;
  boost::shared_ptr<RecordTypeFree> mFree;
  boost::shared_ptr<RecordTypeSerialize> mSerialize;
  boost::shared_ptr<RecordTypeDeserialize> mDeserialize;
  boost::shared_ptr<RecordTypePrint> mPrint;
  std::vector<FieldAddress> mMemberOffsets;
  // Index to lookup up position of a field by its byte offset
  std::map<uint32_t, uint32_t> mByteOffsetToPosition;
  // Are any of our members nullable?
  // This determines whether we have a NULL bitmask or not.
  bool mHasNullFields;
  typedef std::map<std::string, std::size_t>::const_iterator const_member_name_iterator;
public:
  /**
   * Create a record type with members as described.
   * Record types allocated by this method are owned
   * by the ctxt argument.
   * TODO: Structurally equivalent types should always
   * return the same pointer?  Probably not for our purposes
   * since names matter.
   */
  static const RecordType * get(DynamicRecordContext & ctxt,
				const std::vector<RecordMember>& members);
  template<typename _InputIterator>
  static const RecordType * get(DynamicRecordContext & ctxt,
				const RecordType * input,
				_InputIterator begin, _InputIterator end)
  {
    std::set<std::string> ref(begin, end);
    std::vector<RecordMember> members;
    for(const_member_iterator m = input->begin_members(), e=input->end_members();
	e != m;
	++m) {
      if (ref.find(m->GetName()) != ref.end()) {
	members.push_back(*m);
      }
    }
    return get(ctxt, members);
  }

  RecordType(const std::vector<RecordMember>& members);
  ~RecordType();
  const RecordTypeMalloc * GetMalloc() const;
  const RecordTypeFree * GetFree() const;
  const RecordTypeMalloc & getMalloc() const { return *GetMalloc(); }
  const RecordTypeFree & getFree() const { return *GetFree(); }
  const RecordTypeSerialize& getSerialize() const { return *mSerialize.get(); }
  const RecordTypeDeserialize& getDeserialize() const { return *mDeserialize.get(); }
  const RecordTypePrint& getPrint() const { return *mPrint.get(); }

  // This does not belong here!
  void Print(RecordBuffer buf, std::ostream& ostr) const;

  typedef std::vector<RecordMember>::const_iterator const_member_iterator;
  const_member_iterator begin_members() const { return mMembers.begin(); }
  const_member_iterator end_members() const { return mMembers.end(); }

  typedef std::vector<FieldAddress>::const_iterator const_offset_iterator;
  const_offset_iterator begin_offsets() const { return mMemberOffsets.begin(); }
  const_offset_iterator end_offsets() const { return mMemberOffsets.end(); }

  /**
   * Does this record type have null fields?
   */
  bool hasNullFields() const 
  {
    return mHasNullFields;
  }

  /**
   * Number of members in the record.
   */
  std::size_t size() const { return mMembers.size(); }

  /**
   * hasMember
   * Returns true if the record have a member of this name.
   */
  bool hasMember(const std::string& memberName) const
  {
    return mMemberNames.find(memberName) != mMemberNames.end();
  }

  /**
   * Get the offset of the member with this name.
   */
  const RecordMember& getMember(const std::string& memberName) const 
  {
    const_member_name_iterator it = mMemberNames.find(memberName);
    if (it == mMemberNames.end())
      throw std::runtime_error((boost::format("Member with name %1% does not exist") % memberName).str());
    return mMembers[it->second];
  }

  /**
   * Get the offset of the member with this name.
   */
  const FieldAddress& getMemberOffset(const std::string& memberName) const 
  {
    const_member_name_iterator it = mMemberNames.find(memberName);
    if (it == mMemberNames.end())
      throw std::runtime_error((boost::format("Member with name %1% does not exist") % memberName).str());
    return mMemberOffsets[it->second];
  }

  /**
   * Get a field address object for a member.
   */
  const FieldAddress& getFieldAddress(const std::string& memberName) const
  {
    return getMemberOffset(memberName);
  }
  // LLVM Instructions to get a pointer to a member of the struct
  llvm::Value * LLVMMemberGetPointer(const std::string& member, 
				     CodeGenerationContext * ctxt, 
				     llvm::Value * basePointer, 
				     bool populateSymbolTable,
				     const char * symbolTablePrefix = "") const;
  bool isMemberPointer(llvm::Value * val,
		       llvm::Value * basePointer,
		       FieldAddress& addr) const;
  llvm::Value * LLVMMemberGetNull(const std::string& member, CodeGenerationContext * ctxt, llvm::Value * basePointer) const;
  void LLVMMemberSetNull(const std::string& member, CodeGenerationContext * ctxt, llvm::Value * basePointer, bool isNull) const;

  // TODO: I'd rather not have this
  const RecordMember & GetMember(int32_t index) const;

  // Are these types the same.
  bool operator==(const RecordType & rhs) const;
  bool operator!=(const RecordType & rhs) const
  {
    return !this->operator==(rhs);
  }

  /**
   * Physical format descriptor of the default text layout associated
   * with this record type.
   */
  std::string dumpTextFormat() const;
  void dump() const;

  // Interpreter methods for getting/setting
  void setInt32(const std::string& field, int32_t val, RecordBuffer buf) const;
  void setInt64(const std::string& field, int64_t val, RecordBuffer buf) const;
  void setDouble(const std::string& field, double val, RecordBuffer buf) const;
  void setDatetime(const std::string& field, 
		   boost::posix_time::ptime val, 
		   RecordBuffer buf) const;
  void setDate(const std::string& field, 
	       boost::gregorian::date val, 
	       RecordBuffer buf) const;
  // These have copy semantics
  void setVarchar(const std::string& field, const char* val, RecordBuffer buf) const;
  void setChar(const std::string& field, const char* val, RecordBuffer buf) const;

  int32_t getInt32(const std::string& field, RecordBuffer buf) const;
  int64_t getInt64(const std::string& field, RecordBuffer buf) const;
  double getDouble(const std::string& field, RecordBuffer buf) const;
  Varchar * getVarcharPtr(const std::string& field, RecordBuffer buf) const;
};

class IQLRecordTypeBuilder
{
private:
  DynamicRecordContext & mContext;
  std::vector<RecordMember> mMembers;
public:
  IQLRecordTypeBuilder(DynamicRecordContext& ctxt,
		       const std::string& spec,
		       bool isFile);
  ~IQLRecordTypeBuilder();

  void buildField(const char * name, 
		  const FieldType * ty)
  {
    mMembers.push_back(RecordMember(name, ty));
  }
  const RecordType * getProduct()  const
  {
    return RecordType::get(mContext, mMembers);
  }
};

class RecordTypeMove
{
private:
  std::vector<MemcpyOp> mMemcpy;
  std::vector<MemsetOp> mMemset;

public:
  // TODO: Handle cases with renaming.
  RecordTypeMove(const RecordType * source, const RecordType * target);

  /**
   * Implement move as a sequence of memcpy's (and later bitcpy for null).
   * The source must also be modified to remove references if needed.
   */
  const std::vector<MemcpyOp> & getMemcpy() const { return mMemcpy; }
  const std::vector<MemsetOp> & getMemset() const { return mMemset; }

  // Move the contents of source to target.  The references
  // to source is cleared and all resources for source are either
  // reassigned or freed.
  void Execute(RecordBuffer& source, RecordBuffer target);
};

class RecordTypeCopy
{
public:
  typedef std::vector<std::pair<std::string, int> > set_type;
private:
  std::vector<MemcpyOp> mMemcpy;
  // Bits that must be copied from source to target
  std::vector<BitcpyOp> mBitcpy;
  // Bits that must be set in the target
  std::vector<BitsetOp> mBitset;
  set_type mSet;

public:
  // TODO: Handle cases with renaming.
  RecordTypeCopy(const RecordType * source, 
		 const RecordType * target,
		 const std::string& sourceColumnsRegex,
		 int * pos);
  RecordTypeCopy(const RecordType * source, 
		 const RecordType * target,
		 const std::string& sourceColumnsRegex,
		 const std::string& targetColumnsPattern,
		 int * pos);

  /**
   * A coalesced sequence of memcpy for copying POD fields.
   */
  const std::vector<MemcpyOp> & getMemcpy() const { return mMemcpy; }
  /**
   * A coalesced sequence of bitcpy for copying POD fields.
   */
  const std::vector<BitcpyOp> & getBitcpy() const { return mBitcpy; }
  /**
   * A coalesced sequence of bitset for copying POD fields.
   */
  const std::vector<BitsetOp> & getBitset() const { return mBitset; }
  /**
   * Fields that have to be moved explicitly.
   */
  const set_type& getSet() const { return mSet; }
};

#endif
