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

#ifndef __IQLEXPRESSION_HH
#define __IQLEXPRESSION_HH

#include <stdint.h>
#include <string>
#include <vector>
#include <boost/variant.hpp>
#include "IQLBuildTree.h"
#include "RecordType.hh"

class IQLExpression;

// TODO: Currently cannot make this an expression
// since our type system does not allow records as
// field types.  Probably should fix this.
class IQLFieldConstructor
{
public:
  virtual ~IQLFieldConstructor() {};
};

/**
 * A single expression with an optional name.  The name is required
 * unless the expression is a VARIABLE reference.
 */
class IQLNamedExpression : public IQLFieldConstructor
{
private:
  IQLExpression * mExpression;
  std::string mName;

  IQLNamedExpression(DynamicRecordContext & ctxt,
		     IQLExpression * expr,
		     const char * name)
    :
    mExpression(expr),
    mName(name ? name : "")
  {
  }
public:
  static IQLNamedExpression * create(DynamicRecordContext & ctxt,
				     IQLExpression * expr,
				     const char * name)
  {
    return new IQLNamedExpression(ctxt, expr, name);
  }

  IQLExpression * getExpression()
  {
    return mExpression; 
  }
  const std::string& getName() const
  {
    return mName;
  }
};

/**
 * Match and output all fields from a named input record.
 */
class IQLFieldGlob : public IQLFieldConstructor
{
private:
  std::string mRecordName;

  IQLFieldGlob(DynamicRecordContext & ctxt,
	       const char * recordName)
    :
    mRecordName(recordName)
  {
  }

public:
  static IQLFieldGlob * create(DynamicRecordContext & ctxt,
			       const char * recordName)
  {
    return new IQLFieldGlob(ctxt, recordName);
  }
};

/**
 * A regular expression that matches variables by name
 * and optionally use captures to rename the variables.
 */
class IQLFieldPattern : public IQLFieldConstructor
{
private:
  std::string mPattern;
  std::string mNames;

  IQLFieldPattern(DynamicRecordContext & ctxt,
	       const char * pattern,
	       const char * names)
    :
    mPattern(pattern),
    mNames(names ? names : "")
  {
  }
public:
  static IQLFieldPattern * create(DynamicRecordContext & ctxt,
				  const char * pattern,
				  const char * names)
  {
    return new IQLFieldPattern(ctxt, pattern, names);
  }
};

class IQLRecordConstructor
{
private:
  std::vector<IQLFieldConstructor *> mFields;
  
  template<typename _InputIterator>
  IQLRecordConstructor(_InputIterator b, _InputIterator e)
    :
    mFields(b, e)
  {
  }
public:
  static IQLRecordConstructor * create(DynamicRecordContext & ctxt,
				       const std::vector<IQLFieldConstructor *>& fields)
  {
    return new IQLRecordConstructor(fields.begin(), fields.end());
  }

  typedef std::vector<IQLFieldConstructor*>::iterator field_iterator;
  field_iterator begin_fields() 
  {
    return mFields.begin();
  }
  field_iterator end_fields()
  {
    return mFields.end();
  }
  std::size_t size_fields() const
  {
    return mFields.size();
  }
};

class SourceLocation
{
private:
  int32_t mLine;
  int32_t mColumn;
public:
  SourceLocation()
    :
    mLine(0),
    mColumn(0)
  {
  }
  SourceLocation(int32_t line, int32_t column)
    :
    mLine(line),
    mColumn(column)
  {
  }

  int32_t getLine() const 
  {
    return mLine; 
  }

  int32_t getColumn() const
  {
    return mColumn;
  }
};

// TODO: Right now we are treating DynamicRecordContext
// as an owner of IQLExpression.  We probably want something
// finer grain than that.
// TODO: Support constant folding of IQLExpressions
// Implement type checking and LLVM code gen off of IQLExpressions
class IQLExpression
{
public:
  enum NodeType { ARRAYREF, ARR, LOR, LAND, LNOT, LISNULL, CASE, BAND, BOR, BXOR, BNOT, EQ, GTN, LTN, GTEQ, LTEQ, NEQ, MINUS, PLUS, TIMES, DIVIDE, MOD, CAST, VARIABLE, CALL, INT32, INT64, DOUBLE, DECIMAL, STRING, BOOLEAN, INTERVAL, NIL };

private:
  DynamicRecordContext & mContext;

  NodeType mNodeType;
  // Type of the expression computed by type checking
  const FieldType * mFieldType;
  // Type to coerce to.  Having this as a member
  // saves a tree rewrite.
  const FieldType * mCoerceTo;
  // Line and column number
  SourceLocation mSourceLocation;

  std::vector<IQLExpression*> mArgs;
  typedef boost::variant<int64_t, std::string, double, const FieldType *> data_type;
  data_type mData;
  
protected:
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, 
		const SourceLocation& loc)
    :
    mContext(ctxt),
    mNodeType(nodeType),
    mFieldType(NULL),
    mCoerceTo(NULL),
    mSourceLocation(loc)
  {
  }
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, 
		IQLExpression * arg, 
		const SourceLocation& loc)
    :
    mContext(ctxt),
    mNodeType(nodeType),
    mFieldType(NULL),
    mCoerceTo(NULL),
    mSourceLocation(loc)
  {
    BOOST_ASSERT(arg != NULL);
    mArgs.push_back(arg);
  }
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, IQLExpression * arg1,
		IQLExpression * arg2, 
		const SourceLocation& loc)
    :
    mContext(ctxt),
    mNodeType(nodeType),
    mFieldType(NULL),
    mCoerceTo(NULL),
    mSourceLocation(loc)
  {
    BOOST_ASSERT(arg1 != NULL && arg2 != NULL);
    mArgs.push_back(arg1);
    mArgs.push_back(arg2);
  }
  template<typename _Iterator>
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, _Iterator begin,
		_Iterator end, 
		const SourceLocation& loc)
    :
    mContext(ctxt),
    mNodeType(nodeType),
    mFieldType(NULL),
    mCoerceTo(NULL),
    mSourceLocation(loc),
    mArgs(begin, end)
  {
  }
  IQLExpression(const IQLExpression & rhs)
    :
    mContext(rhs.mContext),
    mNodeType(rhs.mNodeType),
    mFieldType(rhs.mFieldType),
    mCoerceTo(rhs.mCoerceTo),
    mSourceLocation(rhs.mSourceLocation),
    mArgs(rhs.mArgs),
    mData(rhs.mData)
  {
  }

  void setData(const char * s) 
  {
    mData = data_type(s);
  }
  void setData(int64_t v) 
  {
    mData = data_type(v);
  }
  void setData(int32_t v) 
  {
    mData = data_type((int64_t) v);
  }
  void setData(double v) 
  {
    mData = data_type(v);
  }
  void setData(const FieldType * ty) 
  {
    mData = data_type(ty);
  }
  const FieldType * getTypeData() const
  {
    return boost::get<const FieldType *>(mData) ;
  }
public:

  virtual ~IQLExpression();

  bool shallow_equals(const IQLExpression * rhs) const
  {
    return getNodeType() == rhs->getNodeType() &&
      mData == rhs->mData;
  }

  bool equals(const IQLExpression * rhs) const;

  virtual IQLExpression * clone() const =0;

  NodeType getNodeType() const
  {
    return mNodeType;
  }
  
  void setFieldType(const FieldType * ty)
  {
    mFieldType = ty;
  }

  const FieldType * getFieldType() const
  {
    return mFieldType;
  }

  void setCoerceTo(const FieldType * coerceTo)
  {
    if (coerceTo != mFieldType) {
      mCoerceTo = coerceTo;
    } else {
      mCoerceTo = NULL;
    }
  }

  const FieldType * getCoercedFieldType() const
  {
    return mCoerceTo ? mCoerceTo : mFieldType;
  }

  // Argument interface
  std::size_t args_size() const
  {
    return mArgs.size();
  }
  typedef std::vector<IQLExpression*>::const_iterator arg_const_iterator;
  arg_const_iterator begin_args() const
  {
    return mArgs.begin();
  }
  arg_const_iterator end_args() const 
  {
    return mArgs.end();
  }

  typedef std::vector<IQLExpression*>::const_iterator arg_iterator;
  arg_iterator begin_args() 
  {
    return mArgs.begin();
  }
  arg_iterator end_args() 
  {
    return mArgs.end();
  }

  const SourceLocation& getSourceLocation() const
  {
    return mSourceLocation;
  }
  int32_t getLine() const
  {
    return mSourceLocation.getLine();
  }
  int32_t getColumn() const
  {
    return mSourceLocation.getColumn();
  }

  const std::string& getStringData() const
  {
    return boost::get<std::string>(mData);
  }
  bool getBooleanData() const
  {
    return boost::get<int64_t>(mData) != 0;
  }

  void replaceArg(IQLExpression * oldArg,
		  IQLExpression * newArg);

  // TODO: Move into a binary op sub class
  void rotateLeftChild()
  {
    IQLExpression * left = mArgs[0];
    mArgs[0] = left->mArgs[0];
    left->mArgs[0] = left->mArgs[1];
    left->mArgs[1] = mArgs[1];
    mArgs[1] = left;
  }
  void rotateRightChild()
  {
    IQLExpression * right = mArgs[1];
    mArgs[1] = right->mArgs[1];
    right->mArgs[1] = right->mArgs[0];
    right->mArgs[0] = mArgs[0];
    mArgs[0] = right;
  }
};

class LogicalOrExpr : public IQLExpression
{
private:
  LogicalOrExpr(const LogicalOrExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LogicalOrExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		IQLExpression * right,
		const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LOR, left, right, loc)
  {
  }

  static LogicalOrExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				IQLExpression * right,
				const SourceLocation& loc)
  {
    LogicalOrExpr * tmp = new LogicalOrExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LogicalOrExpr * clone() const;
};

class LogicalAndExpr : public IQLExpression
{
private:
  LogicalAndExpr(const LogicalAndExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LogicalAndExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		IQLExpression * right,
		const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LAND, left, right, loc)
  {
  }

  static LogicalAndExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				IQLExpression * right,
				const SourceLocation& loc)
  {
    LogicalAndExpr * tmp = new LogicalAndExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LogicalAndExpr * clone() const;
};

class LogicalNotExpr : public IQLExpression
{
private:
  LogicalNotExpr(const LogicalNotExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LogicalNotExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LNOT, left, loc)
  {
  }

  static LogicalNotExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				const SourceLocation& loc)
  {
    LogicalNotExpr * tmp = new LogicalNotExpr(ctxt, left, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LogicalNotExpr * clone() const;
};

class EqualsExpr : public IQLExpression
{
private:
  EqualsExpr(const EqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  EqualsExpr(DynamicRecordContext & ctxt, 
	     IQLExpression * left,
	     IQLExpression * right,
	     const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::EQ, left, right, loc)
  {
  }

  static EqualsExpr * create(DynamicRecordContext & ctxt,
			     IQLExpression * left,
			     IQLExpression * right,
			     const SourceLocation& loc)
  {
    EqualsExpr * tmp = new EqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  EqualsExpr * clone() const;
};

class NotEqualsExpr : public IQLExpression
{
private:
  NotEqualsExpr(const NotEqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  NotEqualsExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		IQLExpression * right,
		const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::NEQ, left, right, loc)
  {
  }

  static NotEqualsExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				IQLExpression * right,
				const SourceLocation& loc)
  {
    NotEqualsExpr * tmp = new NotEqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  NotEqualsExpr * clone() const;
};

class GreaterThanExpr : public IQLExpression
{
private:
  GreaterThanExpr(const GreaterThanExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  GreaterThanExpr(DynamicRecordContext & ctxt, 
		  IQLExpression * left,
		  IQLExpression * right,
		  const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::GTN, left, right, loc)
  {
  }

  static GreaterThanExpr * create(DynamicRecordContext & ctxt,
				  IQLExpression * left,
				  IQLExpression * right,
				  const SourceLocation& loc)
  {
    GreaterThanExpr * tmp = new GreaterThanExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  GreaterThanExpr * clone() const;
};

class LessThanExpr : public IQLExpression
{
private:
  LessThanExpr(const LessThanExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LessThanExpr(DynamicRecordContext & ctxt, 
	       IQLExpression * left,
	       IQLExpression * right,
	       const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LTN, left, right, loc)
  {
  }

  static LessThanExpr * create(DynamicRecordContext & ctxt,
			       IQLExpression * left,
			       IQLExpression * right,
			       const SourceLocation& loc)
  {
    LessThanExpr * tmp = new LessThanExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LessThanExpr * clone() const;
};

class GreaterThanEqualsExpr : public IQLExpression
{
private:
  GreaterThanEqualsExpr(const GreaterThanEqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  GreaterThanEqualsExpr(DynamicRecordContext & ctxt, 
			IQLExpression * left,
			IQLExpression * right,
			const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::GTEQ, left, right, loc)
  {
  }

  static GreaterThanEqualsExpr * create(DynamicRecordContext & ctxt,
					IQLExpression * left,
					IQLExpression * right,
					const SourceLocation& loc)
  {
    GreaterThanEqualsExpr * tmp = new GreaterThanEqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  GreaterThanEqualsExpr * clone() const;
};

class LessThanEqualsExpr : public IQLExpression
{
private:
  LessThanEqualsExpr(const LessThanEqualsExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  LessThanEqualsExpr(DynamicRecordContext & ctxt, 
		     IQLExpression * left,
		     IQLExpression * right,
		     const SourceLocation& loc)
    :
    IQLExpression(ctxt, IQLExpression::LTEQ, left, right, loc)
  {
  }

  static LessThanEqualsExpr * create(DynamicRecordContext & ctxt,
				     IQLExpression * left,
				     IQLExpression * right,
				     const SourceLocation& loc)
  {
    LessThanEqualsExpr * tmp = new LessThanEqualsExpr(ctxt, left, right, loc);
    ctxt.add(tmp);
    return tmp;
  }

  LessThanEqualsExpr * clone() const;
};

class CaseExpr : public IQLExpression
{
private:
  CaseExpr(const CaseExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:
  CaseExpr(DynamicRecordContext & ctxt,
	   const std::vector<IQLExpression *>& args,
	   const SourceLocation& loc);

  static CaseExpr * create(DynamicRecordContext & ctxt,
			   const std::vector<IQLExpression *>& args,
			   const SourceLocation& loc)
  {
    CaseExpr * tmp = new CaseExpr(ctxt, args, loc);
    ctxt.add(tmp);
    return tmp;
  }

  CaseExpr * clone() const;
};

class CallExpr : public IQLExpression
{
private:
  CallExpr(const CallExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:
  CallExpr(DynamicRecordContext & ctxt,
	   const char * fun,
	   const std::vector<IQLExpression *>& args,
	   const SourceLocation& loc);

  static CallExpr * create(DynamicRecordContext & ctxt,
			   const char * fun,
			   const std::vector<IQLExpression *>& args,
			   const SourceLocation& loc)
  {
    CallExpr * tmp = new CallExpr(ctxt, fun, args, loc);
    ctxt.add(tmp);
    return tmp;
  }

  CallExpr * clone() const;
};

class CastExpr : public IQLExpression
{
private:
  CastExpr(const CastExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:
  CastExpr(DynamicRecordContext & ctxt,
	   const FieldType * ty,
	   IQLExpression * arg,
	   const SourceLocation& loc);

  const FieldType * getCastType() const
  {
    return getTypeData();
  }

  static CastExpr * create(DynamicRecordContext & ctxt,
			   const FieldType * ty,
			   IQLExpression * arg,
			   const SourceLocation& loc)
  {
    CastExpr * tmp = new CastExpr(ctxt, ty, arg, loc);
    ctxt.add(tmp);
    return tmp;
  }

  CastExpr * clone() const;
};

class Int32Expr : public IQLExpression
{
private:
  Int32Expr(const Int32Expr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  Int32Expr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static Int32Expr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    Int32Expr * tmp = new Int32Expr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  Int32Expr * clone() const;
};

class Int64Expr : public IQLExpression
{
private:
  Int64Expr(const Int64Expr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  Int64Expr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static Int64Expr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    Int64Expr * tmp = new Int64Expr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  Int64Expr * clone() const;
};

class IntervalExpr : public IQLExpression
{
private:
  IntervalExpr(const IntervalExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:
  IntervalExpr(DynamicRecordContext & ctxt, 
	       const char * text,
	       IQLExpression * arg,
	       const SourceLocation& loc);

  static IntervalExpr * create(DynamicRecordContext & ctxt,
			       const char * text,
			       IQLExpression * arg,
			       const SourceLocation& loc)
  {
    IntervalExpr * tmp = new IntervalExpr(ctxt, text, arg, loc);
    ctxt.add(tmp);
    return tmp;
  }

  IntervalExpr * clone() const;
};

class DoubleExpr : public IQLExpression
{
private:
  DoubleExpr(const DoubleExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  DoubleExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static DoubleExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    DoubleExpr * tmp = new DoubleExpr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  DoubleExpr * clone() const;
};

class DecimalExpr : public IQLExpression
{
private:
  DecimalExpr(const DecimalExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  DecimalExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static DecimalExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    DecimalExpr * tmp = new DecimalExpr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  DecimalExpr * clone() const;
};

class StringExpr : public IQLExpression
{
private:
  StringExpr(const StringExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  StringExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static StringExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    StringExpr * tmp = new StringExpr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  StringExpr * clone() const;
};

class BooleanExpr : public IQLExpression
{
private:
  BooleanExpr(const BooleanExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }
public:

  BooleanExpr(DynamicRecordContext & ctxt, 
	      bool isTrue,
	      const SourceLocation& loc);

  static BooleanExpr * create(DynamicRecordContext & ctxt,
			      bool isTrue,
			      const SourceLocation& loc)
  {
    BooleanExpr * tmp = new BooleanExpr(ctxt, isTrue, loc);
    ctxt.add(tmp);
    return tmp;
  }

  BooleanExpr * clone() const;
};

class NilExpr : public IQLExpression
{
private:
  NilExpr(const NilExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  NilExpr(DynamicRecordContext & ctxt, 
	  const SourceLocation& loc);

  static NilExpr * create(DynamicRecordContext & ctxt,
			      const SourceLocation& loc)
  {
    NilExpr * tmp = new NilExpr(ctxt, loc);
    ctxt.add(tmp);
    return tmp;
  }

  NilExpr * clone() const;
};

class VariableExpr : public IQLExpression
{
private:
  VariableExpr(const VariableExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  VariableExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    const SourceLocation& loc);

  static VariableExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    const SourceLocation& loc)
  {
    VariableExpr * tmp = new VariableExpr(ctxt, text, loc);
    ctxt.add(tmp);
    return tmp;
  }

  VariableExpr * clone() const;
};

class ArrayReferenceExpr : public IQLExpression
{
private:
  ArrayReferenceExpr(const ArrayReferenceExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  ArrayReferenceExpr(DynamicRecordContext & ctxt, 
		     const char * text,
		     IQLExpression * idx,
		     const SourceLocation& loc);

  static ArrayReferenceExpr * create(DynamicRecordContext & ctxt,
				     const char * text,
				     IQLExpression * idx,
				     const SourceLocation& loc)
  {
    ArrayReferenceExpr * tmp = new ArrayReferenceExpr(ctxt, text, idx, loc);
    ctxt.add(tmp);
    return tmp;
  }

  ArrayReferenceExpr * clone() const;
};

class ArrayExpr : public IQLExpression
{
private:
  ArrayExpr(const ArrayExpr & rhs)
    :
    IQLExpression(rhs)
  {
  }

public:

  ArrayExpr(DynamicRecordContext & ctxt, 
	    const std::vector<IQLExpression *>& args,
	    const SourceLocation& loc);
  
  static ArrayExpr * create(DynamicRecordContext & ctxt,
			    const std::vector<IQLExpression *>& args,
			    const SourceLocation& loc)
  {
    ArrayExpr * tmp = new ArrayExpr(ctxt, args, loc);
    ctxt.add(tmp);
    return tmp;
  }

  ArrayExpr * clone() const;
};

/**
 * Walks a predicate given two input records and 
 * extracts the equijoin keys.
 */
class IQLEquiJoinDetector
{
private:
  DynamicRecordContext & mContext;
  const class RecordType * mLeft;
  const class RecordType * mRight;
  IQLExpression * mInput;

  std::vector<const std::string*> mLeftEquiJoinKeys;
  std::vector<const std::string*> mRightEquiJoinKeys;

  IQLExpression * mEquals;
  IQLExpression * mResidual;
  
  /**
   * When an equijoin pred is found, rewrite the
   * tree.
   */
  void handleEquiJoin(IQLExpression * eq, 
		      IQLExpression * p);
  /**
   * Process equality and look for equijoin pattern.
   */
  void handleEq(const class RecordType * left,
		const class RecordType * right,
		IQLExpression * eq, 
		IQLExpression * parent);
public:
  IQLEquiJoinDetector(DynamicRecordContext & ctxt,
		      const class RecordType * left,
		      const class RecordType * right,
		      IQLExpression * input);
  IQLExpression * getEquals()
  {
    return mEquals;
  }
  IQLExpression * getResidual()
  {
    return mResidual;
  }
  const std::vector<const std::string*>& getLeftEquiJoinKeys() const
  {
    return mLeftEquiJoinKeys;
  }
  const std::vector<const std::string*>& getRightEquiJoinKeys() const
  {
    return mRightEquiJoinKeys;
  }
};


class IQLFreeVariablesRule
{
private:
  // TODO: Is is worth being more efficient
  // and using a bitmap or some other kind of
  // efficient sparse set implementation?
  std::set<std::string> mVariables;
  void onExpr(IQLExpression * expr);
public:
  IQLFreeVariablesRule(IQLExpression * expr);
  const std::set<std::string>& getVariables() const;
};

/**
 * Given a pair of inputs and a predicate, splits the
 * predicate into clauses that can be evaluated entirely
 * against each input, the clauses that require both
 * inputs and the clauses that refer to variables
 * not in either input.
 */
class IQLSplitPredicateRule
{
private:
  IQLExpression * mLeft;
  IQLExpression * mRight;
  IQLExpression * mBoth;
  IQLExpression * mOther;
  static void addClause(DynamicRecordContext & ctxt,
			IQLExpression *& pred,
			IQLExpression * clause);
  void onExpr(DynamicRecordContext & ctxt,
	      const RecordType * left,
	      const RecordType * right,
	      IQLExpression * input,
	      IQLExpression * parent);
  static bool contains(const class RecordType * ty,
		       const std::set<std::string>& vars)
  {
    return contains(ty, NULL, vars);
  }
  static bool contains(const class RecordType * ty1,
		       const class RecordType * ty2,
		       const std::set<std::string>& vars);
public:
  IQLSplitPredicateRule(DynamicRecordContext & ctxt,
			const class RecordType * left,
			const class RecordType * right,
			IQLExpression * input);
  IQLExpression * getLeft() 
  {
    return mLeft; 
  }
  IQLExpression * getRight() 
  {
    return mRight; 
  }
  IQLExpression * getBoth() 
  {
    return mBoth; 
  }
  IQLExpression * getOther() 
  {
    return mOther; 
  }
};

class IQLExpressionPrinter
{
private:
  std::set<std::string> mBinaryInfix;
  std::set<std::string> mUnaryInfix;

  bool isBinaryInfix(IQLExpression * e);
  bool isUnaryPrefix(IQLExpression * e);
public:
  static const char * getExpressionSymbol(uint32_t nodeType);
  IQLExpressionPrinter(std::ostream& ostr, IQLExpression * e);
};


class IQLRecordConstructor * unwrap(IQLRecordConstructorRef r);
IQLRecordConstructorRef wrap(class IQLRecordConstructor * r);
class IQLExpression * unwrap(IQLExpressionRef r);
IQLExpressionRef wrap(class IQLExpression * r);
class DynamicRecordContext * unwrap(IQLTreeFactoryRef r);
IQLTreeFactoryRef wrap(class DynamicRecordContext * r);

#endif
