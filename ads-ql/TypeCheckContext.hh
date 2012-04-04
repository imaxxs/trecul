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

#ifndef __TYPECHECKCONTEXT_HH__
#define __TYPECHECKCONTEXT_HH__

#include <map>
#include <stack>
#include <string>
#include "LLVMGen.h"
#include "RecordType.hh"

typedef struct IQLRecordMapStruct * IQLRecordMapRef;
typedef struct IQLToFieldTypeStruct * IQLSymbolTableRef;

class TypeCheckContext 
{
private:
  typedef std::map<std::string, IQLFieldTypeRef> symbol_table;

  std::map<std::string, const RecordType *> mInputRecords;
  std::map<std::string, IQLFieldTypeRef> mSymbolTable;
  std::map<std::string, IQLFieldTypeRef> mAggregateTable;
  std::stack<IQLFieldTypeRef> mCaseType;
public:
  typedef std::vector<RecordMember> member_list;
  class DynamicRecordContext& mContext;
  IQLRecordMapRef IQLInputRecords;
private:
  const RecordType * mOutputRecord;
public:
  IQLSymbolTableRef TypeCheckSymbolTable;
private:
  IQLSymbolTableRef mAggregateTypeCheckSymbolTable;
  IQLSymbolTableRef mSaveTypeCheckSymbolTable;
  member_list * mRecordMembers;

  // One member per aggregate function.
  member_list mAggregateMembers;
  // A record where aggregate functions
  // can be calculated.
  const RecordType * mAggregateRecord;

  bool isBuiltinFunction(const char * name);

public:
  TypeCheckContext(class DynamicRecordContext & recCtxt);
  /**
   * Initialize type check of aggregate functions.
   * When type checking the parameters of an aggregate
   * function the entire record is available to the calculation
   * but when type checking the expressions outside the scope
   * of an aggregate function only the group by keys are available.
   */
  TypeCheckContext(DynamicRecordContext & recCtxt,
		   const RecordType * input,
		   const std::vector<std::string>& groupKeys,
		   bool isOlap);

  ~TypeCheckContext();
  void loadBuiltinFunctions();

  /**
   * Retrieve the output of record construction.
   */
  const RecordType * getOutputRecord() 
  {
    return mOutputRecord;
  }

  /**
   * Retrieve the record where aggregate functions
   * may be built.
   */
  const RecordType * getAggregateRecord() 
  {
    if (mAggregateTypeCheckSymbolTable != NULL &&
	mAggregateRecord == NULL) {
      mAggregateRecord = new RecordType(mAggregateMembers);
    }
    return mAggregateRecord;
  }

  /**
   * Set value statement
   */
  void buildSetValue(const FieldType * lhs,
		     const FieldType * rhs);

  /**
   * Switch statements
   */
  void beginSwitch(const FieldType * e);

  /**
   * Array reference expression.
   */
  const FieldType * buildArray(const std::vector<const FieldType *>& e);
  const FieldType * buildArrayRef(const char * nm,
				  const FieldType * idx);

  /**
   * Operations.
   */
  const FieldType * buildModulus(const FieldType * lhs, 
				 const FieldType * rhs);
  const FieldType * buildBitwise(const FieldType * lhs, 
				 const FieldType * rhs);
  const FieldType * buildBitwise(const FieldType * lhs);


  /**
   * Primitive types.
   */
  const FieldType * buildInt32Type(bool nullable = false);
  const FieldType * buildInt64Type(bool nullable = false);
  const FieldType * buildDoubleType(bool nullable = false);
  const FieldType * buildDecimalType(bool nullable = false);
  const FieldType * buildDateType(bool nullable = false);
  const FieldType * buildDatetimeType(bool nullable = false);
  const FieldType * buildVarcharType();
  const FieldType * buildVarcharType(bool nullable);
  const FieldType * buildCharType(const char * sz, bool nullable = false);
  const FieldType * buildBooleanType(bool nullable = false);
  const FieldType * buildNilType();
  const FieldType * buildType(const char * typeName, bool nullable);

  /**
   * Interval types.
   */
  const FieldType * buildIntervalDay(const FieldType * ty);
  const FieldType * buildIntervalHour(const FieldType * ty);
  const FieldType * buildIntervalMinute(const FieldType * ty);
  const FieldType * buildIntervalMonth(const FieldType * ty);
  const FieldType * buildIntervalSecond(const FieldType * ty);
  const FieldType * buildIntervalYear(const FieldType * ty);

  /**
   * CASE expressions.
   */
  void beginCase();
  void addCondition(const FieldType * condVal);
  void addValue(const FieldType * thenVal);
  const FieldType * buildCase();

  /**
   * Ternary If/Then/Else
   */
  const FieldType * buildIfThenElse(const FieldType * condVal,
				    const FieldType * thenVal,
				    const FieldType * elseVal);

  /**
   * Record builder methods.
   */
  void beginRecord();
  void addField(const char * name, const FieldType * ty);
  void addFields(const char * recordName);
  void quotedId(const char * id, const char * format);
  void buildRecord();

  /**
   * Intrinsic functions
   */
  const FieldType * buildLeast(const std::vector<const FieldType *>& args);
  const FieldType * buildIsNull(const std::vector<const FieldType *>& args);

  /**
   * Aggregate function building.
   */
  void beginAggregateFunction();
  const FieldType * buildAggregateFunction(const FieldType * ty);

  /**
   * Automatic type promotion interface.
   */
  const FieldType * castTo(const FieldType * lhs, 
			   const FieldType * rhs);

  const FieldType * leastCommonType(const FieldType * e1, 
				    const FieldType * e2);

  const FieldType * leastCommonTypeNullable(const FieldType * e1, 
					    const FieldType * e2);
};

// Symbol Table management 
// TODO: Get rid of this C interface
IQLSymbolTableRef IQLSymbolTableCreate();
void IQLSymbolTableFree(IQLSymbolTableRef symTable);
void IQLSymbolTableClear(IQLSymbolTableRef symTable);
void IQLSymbolTableAdd(IQLSymbolTableRef symTable, const char * name, IQLFieldTypeRef value);
IQLFieldTypeRef IQLSymbolTableLookup(IQLSymbolTableRef symTable, const char * name);

class IQLTypeCheckBinaryConversion
{
public:
  /**
   * Can e1 be cast to e2?
   */
  static IQLFieldTypeRef castTo(TypeCheckContext * tctxt, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
  {
    return wrap(tctxt->castTo(unwrap(lhs), unwrap(rhs)));
  }
  /**
   * Can e1 be cast to e2 or vice versa?
   */
  static IQLFieldTypeRef leastCommonType(TypeCheckContext * ctxt, IQLFieldTypeRef e1, IQLFieldTypeRef e2)
  {
    return wrap(ctxt->leastCommonType(unwrap(e1), unwrap(e2)));
  }
  /**
   * Can e1 be cast to e2 or vice versa?
   */
  static IQLFieldTypeRef leastCommonTypeNullable(TypeCheckContext * ctxt, IQLFieldTypeRef e1, IQLFieldTypeRef e2)
  {
    return wrap(ctxt->leastCommonTypeNullable(unwrap(e1), unwrap(e2)));
  }
};

#endif
