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

#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include "TypeCheckContext.hh"

IQLSymbolTableRef IQLSymbolTableCreate()
{
  return reinterpret_cast<IQLSymbolTableRef>(new std::map<std::string, IQLFieldTypeRef>());
}

void IQLSymbolTableFree(IQLSymbolTableRef symTable)
{
  delete reinterpret_cast<std::map<std::string, IQLFieldTypeRef> *>(symTable);
}

void IQLSymbolTableClear(IQLSymbolTableRef symTable)
{
  reinterpret_cast<std::map<std::string, IQLFieldTypeRef> *>(symTable)->clear();
}

void IQLSymbolTableAdd(IQLSymbolTableRef symTable, const char * name, IQLFieldTypeRef value)
{
  (*reinterpret_cast<std::map<std::string, IQLFieldTypeRef> *>(symTable))[name] = value;
}

IQLFieldTypeRef IQLSymbolTableLookup(IQLSymbolTableRef symTable, const char * name)
{
  std::map<std::string, IQLFieldTypeRef> * tab = reinterpret_cast<std::map<std::string, IQLFieldTypeRef> *>(symTable);
  std::map<std::string, IQLFieldTypeRef>::const_iterator it = tab->find(name);
  if (tab->end() == it) {
    std::string fields;
    for(std::map<std::string, IQLFieldTypeRef>::const_iterator it = tab->begin();
	it != tab->end();
	++it) {
      if (fields.size()) fields += ", ";
      fields += it->first;
    }
    throw std::runtime_error((boost::format("Undefined variable: %1%.  Available fields = (%2%)") % name % fields).str());
    //return NULL;
  }
  return it->second;
}

TypeCheckContext::TypeCheckContext(DynamicRecordContext& recCtxt)
  :
  mContext(recCtxt),
  IQLInputRecords(NULL),
  mOutputRecord(NULL),
  TypeCheckSymbolTable(NULL),
  mAggregateTypeCheckSymbolTable(NULL),
  mSaveTypeCheckSymbolTable(NULL),
  mRecordMembers(NULL),
  mAggregateRecord(NULL)
{
}

TypeCheckContext::TypeCheckContext(DynamicRecordContext & recCtxt,
				   const RecordType * input,
				   const std::vector<std::string>& groupKeys,
				   bool isOlap)
  :
  mContext(recCtxt),
  IQLInputRecords(NULL),
  mOutputRecord(NULL),
  TypeCheckSymbolTable(NULL),
  mAggregateTypeCheckSymbolTable(NULL),
  mSaveTypeCheckSymbolTable(NULL),
  mRecordMembers(NULL),
  mAggregateRecord(NULL)
{
  mInputRecords["input"] = input;
  // Put only the group keys into the main symbol table
  // unless OLAP (running total) in which case all of
  // the input record is available in the main symbol table.
  std::set<std::string> groupKeySet;
  // To avoid O(N^2) put keys into a set (and remove dups
  // since they are abnormal but allowed).
  for(std::vector<std::string>::const_iterator it = groupKeys.begin();
      it != groupKeys.end();
      ++it) {
    if(groupKeySet.end() == groupKeySet.find(*it))
      groupKeySet.insert(*it);
  }
  for(RecordType::const_member_iterator it=input->begin_members();
      it != input->end_members();
      ++it) {
    if (isOlap || groupKeySet.end() != groupKeySet.find(it->GetName())) {
      mSymbolTable[it->GetName()] = wrap(it->GetType());
    }
  }
  // Put all fields into the agg symbol table
  for(RecordType::const_member_iterator it=input->begin_members();
      it != input->end_members();
      ++it) {
    mAggregateTable[it->GetName()] = wrap(it->GetType());
  }
  // TODO: Make this C wrapper stuff go away.
  IQLInputRecords = reinterpret_cast<IQLRecordMapRef>(&mInputRecords);
  TypeCheckSymbolTable = reinterpret_cast<IQLSymbolTableRef>(&mSymbolTable);
  mAggregateTypeCheckSymbolTable = reinterpret_cast<IQLSymbolTableRef>(&mAggregateTable);
  
  // The group by fields are always in the aggregate record as well.
  // TODO: Low priority.  Here we are saying that the aggregate record
  // structure is determined by the lexical order of group by keys passed in.
  // If the select list (the transfer) has the group keys in a different order
  // then a non-trivial transfer will result.  It might be nice to figure out a
  // way of determining the structure of the aggregate record so that it mimics
  // the select list.
  for(std::vector<std::string>::const_iterator it = groupKeys.begin();
      it != groupKeys.end();
      ++it) {
    mAggregateMembers.push_back(input->getMember(*it));
  }

  loadBuiltinFunctions();
}

TypeCheckContext::~TypeCheckContext()
{
}

bool TypeCheckContext::isBuiltinFunction(const char * name)
{
  
  symbol_table * tab = reinterpret_cast<symbol_table *>(TypeCheckSymbolTable);
  symbol_table::const_iterator it = tab->find(name);
  return (tab->end() != it &&
	  unwrap(it->second)->GetEnum() == FieldType::FUNCTION);
}

const FieldType * TypeCheckContext::castTo(const FieldType * lhs, 
					   const FieldType * rhs)
{
  // Equality without regards to nullability
  if (lhs->clone(true)==rhs->clone(true)) return lhs;

  // Supported conversions
  const FieldType * e1 = lhs;
  const FieldType * e2 = rhs;
  if (e1->GetEnum() == FieldType::NIL) {
    return rhs;
  } else if (e1->GetEnum() == FieldType::INT32) {
    if (e2->GetEnum() == FieldType::INT32)
      return rhs;
    else if (e2->GetEnum() == FieldType::INT64)
      return rhs;
    else if (e2->GetEnum() == FieldType::DOUBLE)
      return rhs;
    else if (e2->GetEnum() == FieldType::BIGDECIMAL)
      return rhs;
    else 
      return NULL;
  } else if (e1->GetEnum() == FieldType::INT64) {
    if (e2->GetEnum() == FieldType::INT64)
      return rhs;
    else if (e2->GetEnum() == FieldType::DOUBLE)
      return rhs;
    else if (e2->GetEnum() == FieldType::BIGDECIMAL)
      return rhs;
    else 
      return NULL;
  } else if (e1->GetEnum() == FieldType::CHAR) {
    // TODO: What about sizes of these types?
    if (e2->GetEnum() == FieldType::VARCHAR)
      return rhs;
    else 
      return NULL;    
  } else {
    return NULL;
  }
  return NULL;
}

const FieldType * TypeCheckContext::leastCommonType(const FieldType * e1, 
						    const FieldType * e2)
{
  const FieldType * ty = castTo(e1, e2);
  if (ty != NULL) return ty;
  return castTo(e2, e1);
}

const FieldType * TypeCheckContext::leastCommonTypeNullable(const FieldType * e1, 
							    const FieldType * e2)
{  
  const FieldType * ft = leastCommonType(e1,e2);
  if (ft != NULL) {
    bool isNullable = e1->isNullable() || e2->isNullable();
    ft = ft->clone(isNullable);
  } 
  return ft;
}

void TypeCheckContext::loadBuiltinFunctions()
{
  TypeCheckContext * ctxt = this;
  // Create function type for unary math operations.
  DynamicRecordContext & drc (ctxt->mContext);
  DoubleType * d = DoubleType::Get(drc);
  IQLFieldTypeRef unaryDoubleOp = wrap(FunctionType::Get(drc, d, d));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "sqrt",
		    unaryDoubleOp);
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "log",
		    unaryDoubleOp);
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "exp",
		    unaryDoubleOp);
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "length",
		    wrap(FunctionType::Get(drc, 
					   VarcharType::Get(drc), 
					   Int32Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "substr",
		    wrap(FunctionType::Get(drc, 
					   VarcharType::Get(drc), 
					   Int32Type::Get(drc),
					   Int32Type::Get(drc),
					   VarcharType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "trim",
		    wrap(FunctionType::Get(drc, 
					   VarcharType::Get(drc), 
					   VarcharType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "ltrim",
		    wrap(FunctionType::Get(drc, 
					   VarcharType::Get(drc), 
					   VarcharType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "rtrim",
		    wrap(FunctionType::Get(drc, 
					   VarcharType::Get(drc), 
					   VarcharType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "lower",
		    wrap(FunctionType::Get(drc, 
					   VarcharType::Get(drc), 
					   VarcharType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "upper",
		    wrap(FunctionType::Get(drc, 
					   VarcharType::Get(drc), 
					   VarcharType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "md5",
		    wrap(FunctionType::Get(drc, 
					   VarcharType::Get(drc), 
					   CharType::Get(drc, 32))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "utc_timestamp",
		    wrap(FunctionType::Get(drc, 
					   DatetimeType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "unix_timestamp",
		    wrap(FunctionType::Get(drc, 
					   DatetimeType::Get(drc),
					   Int64Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "from_unixtime",
		    wrap(FunctionType::Get(drc, 
					   Int64Type::Get(drc),
					   DatetimeType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "date",
		    wrap(FunctionType::Get(drc, 
					   DatetimeType::Get(drc),
					   DateType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "dayofweek",
		    wrap(FunctionType::Get(drc, 
					   DateType::Get(drc),
					   Int32Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "dayofmonth",
		    wrap(FunctionType::Get(drc, 
					   DateType::Get(drc),
					   Int32Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "dayofyear",
		    wrap(FunctionType::Get(drc, 
					   DateType::Get(drc),
					   Int32Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "month",
		    wrap(FunctionType::Get(drc, 
					   DateType::Get(drc),
					   Int32Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "year",
		    wrap(FunctionType::Get(drc, 
					   DateType::Get(drc),
					   Int32Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "last_day",
		    wrap(FunctionType::Get(drc, 
					   DateType::Get(drc),
					   DateType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "julian_day",
		    wrap(FunctionType::Get(drc, 
					   DateType::Get(drc),
					   Int32Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "datediff",
		    wrap(FunctionType::Get(drc, 
					   DateType::Get(drc),
					   DateType::Get(drc),
					   Int32Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "akid16_to_akid64",
		    wrap(FunctionType::Get(drc, 
					   CharType::Get(drc, 32),
					   CharType::Get(drc, 22))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "akid64_to_akid16",
		    wrap(FunctionType::Get(drc, 
					   CharType::Get(drc, 22),
					   CharType::Get(drc, 32))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "akid64_to_akid16_checksum",
		    wrap(FunctionType::Get(drc, 
					   CharType::Get(drc, 22),
					   CharType::Get(drc, 64))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "akid64_get_random",
		    wrap(FunctionType::Get(drc, 
					   CharType::Get(drc, 22),
					   Int32Type::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "akid64_get_creation_time",
		    wrap(FunctionType::Get(drc, 
					   CharType::Get(drc, 22),
					   DatetimeType::Get(drc))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "akid64_decrypt",
		    wrap(FunctionType::Get(drc, 
					   VarcharType::Get(drc),
					   CharType::Get(drc, 22))));
  IQLSymbolTableAdd(ctxt->TypeCheckSymbolTable,
		    "akid64_encrypt",
		    wrap(FunctionType::Get(drc, 
					   CharType::Get(drc, 22),
					   Int32Type::Get(drc),
					   VarcharType::Get(drc))));
}

void TypeCheckContext::buildSetValue(const FieldType * lhs,
				     const FieldType * rhs)
{
  if (NULL == castTo(rhs, lhs))
    throw std::runtime_error("Cannot set values of incompatible type");
  if (rhs->isNullable() && !lhs->isNullable())
    throw std::runtime_error("Cannot set nullable value into non nullable variable");
}

void TypeCheckContext::beginSwitch(const FieldType * e)
{
  if (e != Int32Type::Get(mContext) &&
      e != Int64Type::Get(mContext))
    throw std::runtime_error("Switch expression must be integer");
}

const FieldType * TypeCheckContext::buildArray(const std::vector<const FieldType *>& e)
{
  if (e.size() == 0) 
    throw std::runtime_error("array expression requires at least one element");
  const FieldType * elmntTy = e[0];
  for(std::size_t i=1; i<e.size(); ++i) {
    const FieldType * tmp = leastCommonTypeNullable(e[i], elmntTy);
    if (NULL == tmp) {
      throw std::runtime_error((boost::format("Can't find common type for elements of "
					      "type %1% and %2% in array") % 
				elmntTy->toString() % e[i]->toString()).str());
    }
    elmntTy = tmp;
  }
  if (elmntTy->isNullable()) {
    throw std::runtime_error("Not yet supporting arrays of nullable types");
  }
  return FixedArrayType::Get(mContext, (int32_t) e.size(), elmntTy, false);
}

const FieldType * TypeCheckContext::buildArrayRef(const char * nm,
						  const FieldType * idx)
{
  if (idx != Int32Type::Get(mContext) &&
      idx != Int64Type::Get(mContext))
    throw std::runtime_error("Array index must be integer");

  const FieldType * arrayTy = unwrap(IQLSymbolTableLookup(TypeCheckSymbolTable, nm));
  if (arrayTy->GetEnum() == FieldType::FIXED_ARRAY) {
    const FieldType * elementTy = static_cast<const FixedArrayType *>(arrayTy)->getElementType();
    return elementTy;
  } else {
    // For backward compatibility we are supporting a hack in which
    // one is allowed to array ref an arbitrary scalar variable.
    // This is used in the anrec calculation and should be removed ASAP. 
    return arrayTy;
  }
}

const FieldType * TypeCheckContext::buildModulus(const FieldType * lhs,
						 const FieldType * rhs)
{
  const FieldType * ret = leastCommonTypeNullable(lhs, rhs);
  if (ret == NULL || 
      (ret->clone(true) != Int32Type::Get(mContext, true) &&
       ret->clone(true) != Int64Type::Get(mContext, true)))
    throw std::runtime_error("Argument to modulus must be integer");
  return ret;
}

const FieldType * TypeCheckContext::buildBitwise(const FieldType * lhs,
						 const FieldType * rhs)
{
  const FieldType * ret = leastCommonTypeNullable(lhs, rhs);
  if (ret == NULL || 
      (ret->clone(true) != Int32Type::Get(mContext, true) &&
       ret->clone(true) != Int64Type::Get(mContext, true)))
    throw std::runtime_error("Argument to bitwise operation must be integer");
  return ret;
}

const FieldType * TypeCheckContext::buildBitwise(const FieldType * lhs)
{
  const FieldType * ret = lhs;
  if (ret->clone(true) != Int32Type::Get(mContext, true) &&
      ret->clone(true) != Int64Type::Get(mContext, true))
    throw std::runtime_error("Argument to bitwise operator must be integer");
  return ret;
}

const FieldType * TypeCheckContext::buildInt32Type(bool nullable)
{
  return Int32Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildInt64Type(bool nullable)
{
  return Int64Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildDoubleType(bool nullable)
{
  return DoubleType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildDecimalType(bool nullable)
{
  return DecimalType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildDatetimeType(bool nullable)
{
  return DatetimeType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildDateType(bool nullable)
{
  return DateType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildVarcharType()
{
  return VarcharType::Get(mContext);
}

const FieldType * TypeCheckContext::buildVarcharType(bool nullable)
{
  return VarcharType::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildCharType(const char * sz, 
						  bool nullable)
{
  int32_t fieldSz = boost::lexical_cast<int32_t> (sz);
  return CharType::Get(mContext, fieldSz, nullable);
}

const FieldType * TypeCheckContext::buildBooleanType(bool nullable)
{
  // TODO: Currently returning int32_t for boolean (be smarter here).
  return Int32Type::Get(mContext, nullable);
}

const FieldType * TypeCheckContext::buildNilType()
{
  // TODO: Proper NULL support
  return NilType::Get(mContext);
}

const FieldType * TypeCheckContext::buildType(const char * typeName, bool nullable)
{
  if (boost::algorithm::iequals("date", typeName)) {
    return DateType::Get(mContext, nullable);
  } else {
    throw std::runtime_error((boost::format("Invalid type: %1%") %
			      typeName).str());
  }
}

const FieldType * TypeCheckContext::buildIntervalDay(const FieldType * ty)
{
  // TODO: Support string and integer as per mySQL behavior
  if (ty != Int32Type::Get(mContext))
    throw std::runtime_error("Expected integer expression");
  return IntervalType::Get(mContext, IntervalType::DAY);
}

const FieldType * TypeCheckContext::buildIntervalHour(const FieldType * ty)
{
  // TODO: Support string and integer as per mySQL behavior
  if (ty != Int32Type::Get(mContext))
    throw std::runtime_error("Expected integer expression");
  return IntervalType::Get(mContext, IntervalType::HOUR);
}

const FieldType * TypeCheckContext::buildIntervalMinute(const FieldType * ty)
{
  // TODO: Support string and integer as per mySQL behavior
  if (ty != Int32Type::Get(mContext))
    throw std::runtime_error("Expected integer expression");
  return IntervalType::Get(mContext, IntervalType::MINUTE);
}

const FieldType * TypeCheckContext::buildIntervalMonth(const FieldType * ty)
{
  // TODO: Support string and integer as per mySQL behavior
  if (ty != Int32Type::Get(mContext))
    throw std::runtime_error("Expected integer expression");
  return IntervalType::Get(mContext, IntervalType::MONTH);
}

const FieldType * TypeCheckContext::buildIntervalSecond(const FieldType * ty)
{
  // TODO: Support string and integer as per mySQL behavior
  if (ty != Int32Type::Get(mContext))
    throw std::runtime_error("Expected integer expression");
  return IntervalType::Get(mContext, IntervalType::SECOND);
}

const FieldType * TypeCheckContext::buildIntervalYear(const FieldType * ty)
{
  // TODO: Support string and integer as per mySQL behavior
  if (ty != Int32Type::Get(mContext))
    throw std::runtime_error("Expected integer expression");
  return IntervalType::Get(mContext, IntervalType::YEAR);
}

void TypeCheckContext::beginCase()
{
  mCaseType.push(NULL);
}

void TypeCheckContext::addCondition(const FieldType * condVal)
{
  const FieldType * bType = buildBooleanType(true);
  if (condVal->clone(true) != bType)
    throw std::runtime_error("Expected boolean expression in CASE expression condition");
}

void TypeCheckContext::addValue(const FieldType * thenVal)
{
  IQLFieldTypeRef curType = mCaseType.top();
  if (curType != NULL) {
    thenVal = leastCommonTypeNullable(thenVal, unwrap(mCaseType.top()));
    if (thenVal == NULL) {
      throw std::runtime_error("Incompatible types in CASE expression");
    }
  }
  mCaseType.pop();
  mCaseType.push(wrap(thenVal));
}

const FieldType * TypeCheckContext::buildCase()
{
  // The least common super type of values.
  const FieldType * ret = unwrap(mCaseType.top());
  mCaseType.pop();
  return ret;
}

const FieldType * TypeCheckContext::buildIfThenElse(const FieldType * condVal,
						    const FieldType * thenVal,
						    const FieldType * elseVal)
{
  // Must be bool (currently int32_t for hacky reasons).
  // TODO: Currently returning int32_t for boolean (be smarter here).
  const FieldType * bType = buildBooleanType();
  if (condVal != bType)
    throw std::runtime_error("Expected boolean expression");

  const FieldType * ret = leastCommonType(thenVal, elseVal);
  if (ret == NULL) 
    throw std::runtime_error("Type check failure ?:");

  return ret;
}

void TypeCheckContext::beginRecord()
{
  if (mRecordMembers)
    throw std::runtime_error("Not supporting nested records yet");
  mRecordMembers = new TypeCheckContext::member_list ();
}

void TypeCheckContext::addField(const char * name, const FieldType * ty)
{
  if (name == NULL) {
    throw std::runtime_error("Must specify an field name for expression");
  }

  if (isBuiltinFunction(name)) {
    throw std::runtime_error((boost::format("Cannot use name of built in "
					    "function '%1%' as variable name")
			      % name).str());
  }
  
  if (ty->GetEnum() == FieldType::NIL) {
    throw std::runtime_error((boost::format("NULL value in field %1% requires"
					    " CAST to specify type") %
			      name).str());
  }
  mRecordMembers->push_back(RecordMember(name, ty));
}

void TypeCheckContext::addFields(const char * recordName)
{
  const std::map<std::string, const RecordType *>& recordTypes(*reinterpret_cast<std::map<std::string, const RecordType *> *>(IQLInputRecords));
  // Find the record struct in the named inputs.
  std::map<std::string, const RecordType *>::const_iterator it = recordTypes.find(recordName);
  if (it == recordTypes.end())
    throw std::runtime_error((boost::format("Undefined input record: %1%") % recordName).str());
  // Add each of the members of this record into the target.
  for(RecordType::const_member_iterator mit = it->second->begin_members();
      mit != it->second->end_members();
      ++mit) {
    mRecordMembers->push_back(*mit);
  }
}

void TypeCheckContext::quotedId(const char * id, const char * format)
{
  std::string idEx(id);
  boost::regex ex(idEx.substr(1, idEx.size() -2));
  std::string fmt(format ? format : "``");
  fmt = fmt.substr(1, fmt.size()-2);
  const std::map<std::string, const RecordType *>& recordTypes(*reinterpret_cast<std::map<std::string, const RecordType *> *>(IQLInputRecords));
  // Find the record struct in the named inputs.
  for(std::map<std::string, const RecordType *>::const_iterator it = recordTypes.begin();
      it != recordTypes.end();
      ++it) {
    // Add each of the members of this record into the target.
    for(RecordType::const_member_iterator mit = it->second->begin_members();
	mit != it->second->end_members();
	++mit) {
      if (boost::regex_match(mit->GetName().c_str(), ex)) {
	if (fmt.size()) {
	  std::string tgt = boost::regex_replace(mit->GetName(), ex, fmt, boost::format_no_copy | boost::format_first_only);
	  mRecordMembers->push_back(RecordMember(tgt, mit->GetType()));
	} else {
	  mRecordMembers->push_back(*mit);
	}
      }
    }
  }
}

void TypeCheckContext::buildRecord()
{
  const RecordType * tmp = RecordType::get(mContext, *mRecordMembers);
  delete mRecordMembers;
  mRecordMembers = NULL;
  mOutputRecord =  tmp;
}

const FieldType * TypeCheckContext::buildLeast(const std::vector<const FieldType *>& args)
{
  if (args.size() == 0) 
    throw std::runtime_error("LEAST requires at least one argument");
  const FieldType * retTy = args[0];
  for(std::size_t i=1; i<args.size(); ++i) {
    retTy = leastCommonType(args[i], retTy);
  }
  return retTy;
}

const FieldType * TypeCheckContext::buildIsNull(const std::vector<const FieldType *>& args)
{
  if (args.size() != 2) 
    throw std::runtime_error("ISNULL takes exactly two arguments");
  // Don't allow literal NULL for first argument
  if (args[0]->GetEnum() == FieldType::NIL) {
    throw std::runtime_error("ISNULL cannot take NULL as first argument");
  }
  // Allow implicit conversion of second arg to first.
  if (NULL == castTo(args[1], args[0])) {
    throw std::runtime_error((boost::format("ISNULL cannot implicitly cast "
					    "from %1% to %2%") %
			      args[1]->toString() % 
			      args[0]->toString()).str());
  }
  // Return type is type of first arg with nullability of
  // second.
  bool retNull = args[1]->isNullable() && args[0]->isNullable();
  const FieldType * retTy = args[0]->clone(retNull);
  return retTy;
}

void TypeCheckContext::beginAggregateFunction()
{
  // Make sure that we are in a context in which aggregates are
  // allowed.  If so switch to the type check context appropriate
  // for type checking the aggregate argument.
  if (mSaveTypeCheckSymbolTable != NULL) {
    throw std::runtime_error("Cannot nest aggregate functions");
  }
  if (mAggregateTypeCheckSymbolTable == NULL) {
    throw std::runtime_error("Cannot use aggregate functions outside of group by");
  }
  std::swap(TypeCheckSymbolTable, mSaveTypeCheckSymbolTable);
  std::swap(TypeCheckSymbolTable, mAggregateTypeCheckSymbolTable);
}

const FieldType * TypeCheckContext::buildAggregateFunction(const FieldType * ty)
{
  // Switch back to the top level context for the aggregate (e.g. group by
  // fields only in the symbol table).
  std::swap(TypeCheckSymbolTable, mAggregateTypeCheckSymbolTable);
  std::swap(TypeCheckSymbolTable, mSaveTypeCheckSymbolTable);
  // TODO: Add proper checks and conversions for ty.
  // TODO: Not all aggregate functions have nullable output
  // SUM, MIN and MAX do however (COUNT will not when it is added).
  ty = ty->clone(true);
  // Add a member to the aggregate record
  std::string aggregateMember((boost::format("__AggFn%1%__") % 
			       mAggregateMembers.size()).str());
  mAggregateMembers.push_back(RecordMember(aggregateMember,
					   ty));
  return ty;
}

