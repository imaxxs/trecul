#ifndef __IQLEXPRESSION_HH
#define __IQLEXPRESSION_HH

#include <stdint.h>
#include <string>
#include <vector>
#include <boost/variant.hpp>
#include "IQLBuildTree.h"
#include "RecordType.hh"

// TODO: Support constant folding of IQLExpressions
// TODO: Better abstraction around debug info such
// as line/column number.
class IQLExpression
{
public:
  enum NodeType { ARRAYREF, LOR, LAND, LNOT, LISNULL, CASE, BAND, BOR, BXOR, BNOT, EQ, GTN, LTN, GTEQ, LTEQ, NEQ, MINUS, PLUS, TIMES, DIVIDE, MOD, CAST, VARIABLE, CALL, INT32, INT64, DOUBLE, DECIMAL, STRING, BOOLEAN, INTERVAL, NIL };

private:
  NodeType mNodeType;
  // Type of the expression computed by type checking
  const FieldType * mFieldType;
  // Type to coerce to.  Having this as a member
  // saves a tree rewrite.
  const FieldType * mCoerceTo;
  // Line and column number
  int32_t mLine;
  int32_t mColumn;

  std::vector<IQLExpression*> mArgs;
  typedef boost::variant<int64_t, std::string, double, const FieldType *> data_type;
  data_type mData;
  
protected:
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, int32_t line=-1,
		int32_t column=-1)
    :
    mNodeType(nodeType),
    mFieldType(NULL),
    mCoerceTo(NULL),
    mLine(line),
    mColumn(column)
  {
  }
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, 
		IQLExpression * arg, 
		int32_t line=-1,
		int32_t column=-1)
    :
    mNodeType(nodeType),
    mFieldType(NULL),
    mCoerceTo(NULL),
    mLine(line),
    mColumn(column)
  {
    BOOST_ASSERT(arg != NULL);
    mArgs.push_back(arg);
  }
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, IQLExpression * arg1,
		IQLExpression * arg2, 
		int32_t line=-1,
		int32_t column=-1)
    :
    mNodeType(nodeType),
    mFieldType(NULL),
    mCoerceTo(NULL),
    mLine(line),
    mColumn(column)
  {
    BOOST_ASSERT(arg1 != NULL && arg2 != NULL);
    mArgs.push_back(arg1);
    mArgs.push_back(arg2);
  }
  template<typename _Iterator>
  IQLExpression(DynamicRecordContext & ctxt,
		NodeType nodeType, _Iterator begin,
		_Iterator end, 
		int32_t line=-1,
		int32_t column=-1)
    :
    mNodeType(nodeType),
    mFieldType(NULL),
    mCoerceTo(NULL),
    mLine(line),
    mColumn(column),
    mArgs(begin, end)
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

  int32_t getLine() const
  {
    return mLine;
  }
  int32_t getColumn() const
  {
    return mColumn;
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
public:

  LogicalOrExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		IQLExpression * right,
		int32_t line=-1,
		int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::LOR, left, right, line, column)
  {
  }

  static LogicalOrExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				IQLExpression * right,
				int32_t line, int32_t column)
  {
    return new LogicalOrExpr(ctxt, left, right, line, column);
  }
};

class LogicalAndExpr : public IQLExpression
{
public:

  LogicalAndExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		IQLExpression * right,
		int32_t line=-1,
		int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::LAND, left, right, line, column)
  {
  }

  static LogicalAndExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				IQLExpression * right,
				int32_t line, int32_t column)
  {
    return new LogicalAndExpr(ctxt, left, right, line, column);
  }
};

class LogicalNotExpr : public IQLExpression
{
public:

  LogicalNotExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		int32_t line=-1,
		int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::LNOT, left, line, column)
  {
  }

  static LogicalNotExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				int32_t line, int32_t column)
  {
    return new LogicalNotExpr(ctxt, left, line, column);
  }
};

class EqualsExpr : public IQLExpression
{
public:

  EqualsExpr(DynamicRecordContext & ctxt, 
	     IQLExpression * left,
	     IQLExpression * right,
	     int32_t line=-1,
	     int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::EQ, left, right, line, column)
  {
  }

  static EqualsExpr * create(DynamicRecordContext & ctxt,
			     IQLExpression * left,
			     IQLExpression * right,
			     int32_t line, int32_t column)
  {
    return new EqualsExpr(ctxt, left, right, line, column);
  }
};

class NotEqualsExpr : public IQLExpression
{
public:

  NotEqualsExpr(DynamicRecordContext & ctxt, 
		IQLExpression * left,
		IQLExpression * right,
		int32_t line=-1,
		int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::NEQ, left, right, line, column)
  {
  }

  static NotEqualsExpr * create(DynamicRecordContext & ctxt,
				IQLExpression * left,
				IQLExpression * right,
				int32_t line, int32_t column)
  {
    return new NotEqualsExpr(ctxt, left, right, line, column);
  }
};

class GreaterThanExpr : public IQLExpression
{
public:

  GreaterThanExpr(DynamicRecordContext & ctxt, 
		  IQLExpression * left,
		  IQLExpression * right,
		  int32_t line=-1,
		  int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::GTN, left, right, line, column)
  {
  }

  static GreaterThanExpr * create(DynamicRecordContext & ctxt,
				  IQLExpression * left,
				  IQLExpression * right,
				  int32_t line, int32_t column)
  {
    return new GreaterThanExpr(ctxt, left, right, line, column);
  }
};

class LessThanExpr : public IQLExpression
{
public:

  LessThanExpr(DynamicRecordContext & ctxt, 
	       IQLExpression * left,
	       IQLExpression * right,
	       int32_t line=-1,
	       int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::LTN, left, right, line, column)
  {
  }

  static LessThanExpr * create(DynamicRecordContext & ctxt,
			       IQLExpression * left,
			       IQLExpression * right,
			       int32_t line, int32_t column)
  {
    return new LessThanExpr(ctxt, left, right, line, column);
  }
};

class GreaterThanEqualsExpr : public IQLExpression
{
public:

  GreaterThanEqualsExpr(DynamicRecordContext & ctxt, 
			IQLExpression * left,
			IQLExpression * right,
			int32_t line=-1,
			int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::GTEQ, left, right, line, column)
  {
  }

  static GreaterThanEqualsExpr * create(DynamicRecordContext & ctxt,
					IQLExpression * left,
					IQLExpression * right,
					int32_t line, int32_t column)
  {
    return new GreaterThanEqualsExpr(ctxt, left, right, line, column);
  }
};

class LessThanEqualsExpr : public IQLExpression
{
public:

  LessThanEqualsExpr(DynamicRecordContext & ctxt, 
		     IQLExpression * left,
		     IQLExpression * right,
		     int32_t line=-1,
		     int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::LTEQ, left, right, line, column)
  {
  }

  static LessThanEqualsExpr * create(DynamicRecordContext & ctxt,
				     IQLExpression * left,
				     IQLExpression * right,
				     int32_t line, int32_t column)
  {
    return new LessThanEqualsExpr(ctxt, left, right, line, column);
  }
};

class CaseExpr : public IQLExpression
{
public:
  CaseExpr(DynamicRecordContext & ctxt,
	   const std::vector<IQLExpression *>& args,
	   int32_t line = -1,
	   int32_t column = -1);

  static CaseExpr * create(DynamicRecordContext & ctxt,
			   const std::vector<IQLExpression *>& args,
			   int32_t line = -1,
			   int32_t column = -1)
  {
    return new CaseExpr(ctxt, args, line, column);
  }
};

class CallExpr : public IQLExpression
{
public:
  CallExpr(DynamicRecordContext & ctxt,
	   const char * fun,
	   const std::vector<IQLExpression *>& args,
	   int32_t line = -1,
	   int32_t column = -1);

  static CallExpr * create(DynamicRecordContext & ctxt,
			   const char * fun,
			   const std::vector<IQLExpression *>& args,
			   int32_t line = -1,
			   int32_t column = -1)
  {
    return new CallExpr(ctxt, fun, args, line, column);
  }
};

class CastExpr : public IQLExpression
{
public:
  CastExpr(DynamicRecordContext & ctxt,
	   const FieldType * ty,
	   IQLExpression * arg,
	   int32_t line = -1,
	   int32_t column = -1);

  const FieldType * getCastType() const
  {
    return getTypeData();
  }

  static CastExpr * create(DynamicRecordContext & ctxt,
			   const FieldType * ty,
			   IQLExpression * arg,
			   int32_t line = -1,
			   int32_t column = -1)
  {
    return new CastExpr(ctxt, ty, arg, line, column);
  }
};

class Int32Expr : public IQLExpression
{
public:

  Int32Expr(DynamicRecordContext & ctxt, 
	    const char * text,
	    int32_t line=-1,
	    int32_t column=-1);

  static Int32Expr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    int32_t line, int32_t column)
  {
    return new Int32Expr(ctxt, text, line, column);
  }
};

class Int64Expr : public IQLExpression
{
public:

  Int64Expr(DynamicRecordContext & ctxt, 
	    const char * text,
	    int32_t line=-1,
	    int32_t column=-1);

  static Int64Expr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    int32_t line, int32_t column)
  {
    return new Int64Expr(ctxt, text, line, column);
  }
};

class IntervalExpr : public IQLExpression
{
public:
  IntervalExpr(DynamicRecordContext & ctxt, 
	       const char * text,
	       IQLExpression * arg,
	       int32_t line=-1,
	       int32_t column=-1);

  static IntervalExpr * create(DynamicRecordContext & ctxt,
			       const char * text,
			       IQLExpression * arg,
			       int32_t line, int32_t column)
  {
    return new IntervalExpr(ctxt, text, arg, line, column);
  }
};

class DoubleExpr : public IQLExpression
{
public:

  DoubleExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    int32_t line=-1,
	    int32_t column=-1);

  static DoubleExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    int32_t line, int32_t column)
  {
    return new DoubleExpr(ctxt, text, line, column);
  }
};

class DecimalExpr : public IQLExpression
{
public:

  DecimalExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    int32_t line=-1,
	    int32_t column=-1);

  static DecimalExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    int32_t line, int32_t column)
  {
    return new DecimalExpr(ctxt, text, line, column);
  }
};

class StringExpr : public IQLExpression
{
public:

  StringExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    int32_t line=-1,
	    int32_t column=-1);

  static StringExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    int32_t line, int32_t column)
  {
    return new StringExpr(ctxt, text, line, column);
  }
};

class BooleanExpr : public IQLExpression
{
public:

  BooleanExpr(DynamicRecordContext & ctxt, 
	      bool isTrue,
	      int32_t line=-1,
	      int32_t column=-1);

  static BooleanExpr * create(DynamicRecordContext & ctxt,
			      bool isTrue,
			      int32_t line, int32_t column)
  {
    return new BooleanExpr(ctxt, isTrue, line, column);
  }
};

class NilExpr : public IQLExpression
{
public:

  NilExpr(DynamicRecordContext & ctxt, 
	      int32_t line=-1,
	      int32_t column=-1)
    :
    IQLExpression(ctxt, IQLExpression::NIL, line, column)
  {
  }

  static NilExpr * create(DynamicRecordContext & ctxt,
			      int32_t line, int32_t column)
  {
    return new NilExpr(ctxt, line, column);
  }
};

class VariableExpr : public IQLExpression
{
public:

  VariableExpr(DynamicRecordContext & ctxt, 
	    const char * text,
	    int32_t line=-1,
	    int32_t column=-1);

  static VariableExpr * create(DynamicRecordContext & ctxt,
			    const char * text,
			    int32_t line, int32_t column)
  {
    return new VariableExpr(ctxt, text, line, column);
  }
};

class ArrayReferenceExpr : public IQLExpression
{
public:

  ArrayReferenceExpr(DynamicRecordContext & ctxt, 
		     const char * text,
		     IQLExpression * idx,
		     int32_t line=-1,
		     int32_t column=-1);

  static ArrayReferenceExpr * create(DynamicRecordContext & ctxt,
				     const char * text,
				     IQLExpression * idx,
				     int32_t line, int32_t column)
  {
    return new ArrayReferenceExpr(ctxt, text, idx, line, column);
  }
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

class IQLExpression * unwrap(IQLExpressionRef r);
IQLExpressionRef wrap(class IQLExpression * r);
class DynamicRecordContext * unwrap(IQLTreeFactoryRef r);
IQLTreeFactoryRef wrap(class DynamicRecordContext * r);

#endif
