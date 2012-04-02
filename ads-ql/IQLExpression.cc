#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/lexical_cast.hpp>
#include "IQLExpression.hh"
#include "IQLBuildTree.h"
#include "RecordType.hh"

void IQLExpression::replaceArg(IQLExpression * oldArg,
			       IQLExpression * newArg)
{
  for(std::size_t i=0, sz=mArgs.size(); i<sz; ++i) {
    if (mArgs[i] == oldArg) {
      mArgs[i] = newArg;
      return;
    }
  }
  throw std::runtime_error("Internal Error: call to IQLExpression::replaceArg"
			   " with invalid argument");
}

CaseExpr::CaseExpr(DynamicRecordContext & ctxt,
		   const std::vector<IQLExpression *>& args,
		   int32_t line,
		   int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::CASE, 
		args.begin(), args.end(), 
		line, column)
{
}

CallExpr::CallExpr(DynamicRecordContext & ctxt,
		   const char * fun,
		   const std::vector<IQLExpression *>& args,
		   int32_t line,
		   int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::CALL, 
		args.begin(), args.end(), 
		line, column)
{
  setData(fun);
}

CastExpr::CastExpr(DynamicRecordContext & ctxt,
		   const FieldType * ty,
		   IQLExpression * arg,
		   int32_t line,
		   int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::CAST, 
		arg,
		line, column)
{
  setData(ty);
}

Int32Expr::Int32Expr(DynamicRecordContext & ctxt, 
		     const char * text,
		     int32_t line,
		     int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::INT32, line, column)
{
  // TODO: Should we check the string now or later?
  // On the one hand haven't yet constructed our object
  // nor has anyone asked for type checking.  On the
  // other hand, we'll need to convert state later.
  setData(text);
}

Int64Expr::Int64Expr(DynamicRecordContext & ctxt, 
		     const char * text,
		     int32_t line,
		     int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::INT64, line, column)
{
  setData(text);
}

IntervalExpr::IntervalExpr(DynamicRecordContext & ctxt, 
			   const char * text,
			   IQLExpression * arg,
			   int32_t line,
			   int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::INTERVAL, arg, line, column)
{
  setData(text);
}

DoubleExpr::DoubleExpr(DynamicRecordContext & ctxt, 
		       const char * text,
		       int32_t line,
		       int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::DOUBLE, line, column)
{
  setData(text);
}

DecimalExpr::DecimalExpr(DynamicRecordContext & ctxt, 
			 const char * text,
			 int32_t line,
			 int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::DECIMAL, line, column)
{
  setData(text);
}

StringExpr::StringExpr(DynamicRecordContext & ctxt, 
		       const char * text,
		       int32_t line,
		       int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::STRING, line, column)
{
  setData(text);
}

BooleanExpr::BooleanExpr(DynamicRecordContext & ctxt, 
			 bool isTrue,
			 int32_t line,
			 int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::BOOLEAN, line, column)
{
  setData(isTrue ? 1 : 0);
}

VariableExpr::VariableExpr(DynamicRecordContext & ctxt, 
			   const char * text,
			   int32_t line,
			   int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::VARIABLE, line, column)
{
  setData(text);
}

ArrayReferenceExpr::ArrayReferenceExpr(DynamicRecordContext & ctxt, 
				       const char * text,
				       IQLExpression * idx,
				       int32_t line,
				       int32_t column)
  :
  IQLExpression(ctxt, IQLExpression::ARRAYREF, idx, line, column)
{
  setData(text);
}

IQLEquiJoinDetector::IQLEquiJoinDetector(DynamicRecordContext& ctxt,
					 const RecordType * left,
					 const RecordType * right,
					 IQLExpression * input)
  :
  mContext(ctxt),
  mEquals(NULL),
  mResidual(input)
{
  // First lift all ANDs
  // The trees we are building are generally left associative 
  // so we do less work if we put all ANDs on the left boundary
  // of the parse tree.
  for(IQLExpression * e = mResidual; e != NULL; ) {
    if (e->getNodeType() == IQLExpression::LAND) {
      // While right child is an AND, rotate to left.
      while ((*(e->begin_args()+1))->getNodeType() == IQLExpression::LAND) {
	e->rotateRightChild();
      }
      e = *e->begin_args();
    } else {
      e = NULL;
    }
  }  
  // Scan for patterns (EQ x y) where x and y are VARIABLE 
  // from left & right (in either order).  Only process if a
  // top level conjunctive clause.
  // TODO: Should we put the predicate in conjunctive normal
  // form first?  Is that well defined with SQL ternary logic?
  // E.g. is NOT (a <> b OR c <> d) a valid equijoin predicate?
  for(IQLExpression * e = mResidual, * p=NULL; e != NULL; ) {
    switch (e->getNodeType()) {
    case IQLExpression::LAND:
      if ((*(e->begin_args()+1))->getNodeType() == IQLExpression::EQ) {
	handleEq(left, right, *(e->begin_args()+1), e);
      }
      p = e;
      e = *e->begin_args();
      break;
    case IQLExpression::EQ:
      {
	handleEq(left, right, e, p);
	e = NULL;
	break;
      }
    default:
      e = NULL;
      break;
    }
  }

  // Eliminate inserted TRUE(s) and store residual
  for(IQLExpression * e = mResidual, * p=NULL; e != NULL; ) {
    if(e->getNodeType()==IQLExpression::LAND) {
      if((*(e->begin_args()+1))->getNodeType() == IQLExpression::BOOLEAN &&
	 (*(e->begin_args()+1))->getBooleanData()) {
	if (NULL != p) {
	  IQLExpression * tmp = *e->begin_args();
	  p->replaceArg(e, tmp);
	  e = tmp;
	} else {
	  mResidual = e = *e->begin_args();
	}
      } else if((*(e->begin_args()))->getNodeType() == IQLExpression::BOOLEAN &&
		(*(e->begin_args()))->getBooleanData()) {
	if (NULL != p) {
	  IQLExpression * tmp = *(e->begin_args()+1);
	  p->replaceArg(e, tmp);
	  e = tmp;
	} else {
	  mResidual = e = *(e->begin_args()+1);
	}
      } else {
	p = e;
	e = *e->begin_args();
      }
    } else if (e->getNodeType() == IQLExpression::BOOLEAN &&
	       e->getBooleanData()) {
      if(NULL == p) {
	e = mResidual = NULL;
      }      
    } else {
      e = NULL;
    }
  }
}

void IQLEquiJoinDetector::handleEquiJoin(IQLExpression * eq, 
					 IQLExpression * p)
{
  if (p) {
    // Replace equality with TRUE
    IQLExpression * tmp = BooleanExpr::create(mContext, true, 
					      eq->getLine(), eq->getColumn());
    p->replaceArg(eq,tmp);
  } else {
    mResidual = NULL;
  }
  
  // Append eq to equijoin predicate
  if (mEquals) {
    mEquals = LogicalAndExpr::create(mContext, mEquals, eq, 
				     eq->getLine(), eq->getColumn());
  } else {
    mEquals = eq;
  }
}

void IQLEquiJoinDetector::handleEq(const RecordType * left,
				   const RecordType * right,
				   IQLExpression * eq,
				   IQLExpression * p)
{
  IQLExpression::arg_const_iterator it = eq->begin_args();
  IQLExpression * lexpr = *it;
  IQLExpression * rexpr = *(++it);
  if (lexpr->getNodeType() == IQLExpression::VARIABLE &&
      rexpr->getNodeType() == IQLExpression::VARIABLE) {
    const std::string & lvar(lexpr->getStringData());
    const std::string & rvar(rexpr->getStringData());
    int32_t tst = (int32_t) left->hasMember(lvar)
      + 2*(int32_t) right->hasMember(lvar) 
      + 4*(int32_t) left->hasMember(rvar) 
      + 8*(int32_t) right->hasMember(rvar) ;
    switch(tst) {
    case 6:
      mLeftEquiJoinKeys.push_back(&rvar);
      mRightEquiJoinKeys.push_back(&lvar);
      handleEquiJoin(eq, p);
      break;
    case 9:
      mLeftEquiJoinKeys.push_back(&lvar);
      mRightEquiJoinKeys.push_back(&rvar);
      handleEquiJoin(eq, p);
      break;
    case 3:
    case 12:
    case 15:
      throw std::runtime_error("Ambiguous: need to implement alias support");
      break;
    default:
      break;
    }
  }
}

IQLFreeVariablesRule::IQLFreeVariablesRule(IQLExpression * expr)
{
  // TODO: Memoization of the rule????
  onExpr(expr);
}

void IQLFreeVariablesRule::onExpr(IQLExpression * expr)
{
  switch(expr->getNodeType()) {
  case IQLExpression::VARIABLE:
    if (mVariables.find(expr->getStringData()) ==
	mVariables.end()) {
      mVariables.insert(expr->getStringData());
    }
    break;
  case IQLExpression::ARRAYREF:
    if (mVariables.find(expr->getStringData()) ==
	mVariables.end()) {
      mVariables.insert(expr->getStringData());
    }
    onExpr(*expr->begin_args());
    break;
  // TODO: Handle DECLARE since these are binders.
  default:
    for(IQLExpression::arg_const_iterator a = expr->begin_args(),
	  e = expr->end_args(); a != e; ++a) {
      onExpr(*a);
    }
    break;
  }
}

const std::set<std::string>& IQLFreeVariablesRule::getVariables() const
{
  return mVariables;
}

IQLSplitPredicateRule::IQLSplitPredicateRule(DynamicRecordContext & ctxt,
					     const RecordType * left,
					     const RecordType * right,
					     IQLExpression * input)
  :
  mLeft(NULL),
  mRight(NULL),
  mBoth(NULL),
  mOther(NULL)
{
  onExpr(ctxt, left, right, input, NULL);
}

bool IQLSplitPredicateRule::contains(const class RecordType * ty1,
				     const class RecordType * ty2,
				     const std::set<std::string>& vars)
{
  for(std::set<std::string>::const_iterator v = vars.begin(),
	e = vars.end(); v != e; ++v) {
    if (!ty1->hasMember(*v) &&
	(ty2==NULL || !ty2->hasMember(*v))) {
      return false;
    }
  }
  return true;
}

void IQLSplitPredicateRule::addClause(DynamicRecordContext & ctxt,
				      IQLExpression *& pred,
				      IQLExpression * clause)
{
  if (pred) {
    pred = LogicalAndExpr::create(ctxt, pred, clause, 
				   clause->getLine(), clause->getColumn());
  } else {
    pred = clause;
  }
}

void IQLSplitPredicateRule::onExpr(DynamicRecordContext & ctxt,
				   const RecordType * left,
				   const RecordType * right,
				   IQLExpression * expr,
				   IQLExpression * parent)
{
    switch(expr->getNodeType()) {
    case IQLExpression::LAND:
      {
	onExpr(ctxt, left, right, *expr->begin_args(), expr);
	onExpr(ctxt, left, right, *(expr->begin_args()+1), expr);
      }
      break;
    default:
      {
	// Get free variables and figure out where this goes
	IQLFreeVariablesRule fv(expr);
	if (contains(left, fv.getVariables())) {
	  addClause(ctxt, mLeft, expr);
	} else if (contains(right, fv.getVariables())) {
	  addClause(ctxt, mRight, expr);
	} else if (contains(left, right, fv.getVariables())) {
	  addClause(ctxt, mBoth, expr);
	} else {
	  addClause(ctxt, mOther, expr);
	}
	if (parent) {
	  parent->replaceArg(expr, BooleanExpr::create(ctxt, true, -1, -1));
	}
	break;
      }
    }  
}

// Implementation of the C binding
class IQLExpression * unwrap(IQLExpressionRef r)
{
  return reinterpret_cast<class IQLExpression *>(r);
}

IQLExpressionRef wrap(class IQLExpression * r)
{
  return reinterpret_cast<IQLExpressionRef>(r);
}

std::vector<IQLExpression *> * unwrap(IQLExpressionListRef r)
{
  return reinterpret_cast<std::vector<IQLExpression *> *>(r);
}

IQLExpressionListRef wrap(std::vector<IQLExpression *> * r)
{
  return reinterpret_cast<IQLExpressionListRef>(r);
}

class DynamicRecordContext * unwrap(IQLTreeFactoryRef r)
{
  return reinterpret_cast<class DynamicRecordContext *>(r);
}

IQLTreeFactoryRef wrap(class DynamicRecordContext * r)
{
  return reinterpret_cast<IQLTreeFactoryRef>(r);
}

// Decl; defined in LLVMGen.cc
const FieldType * unwrap(IQLFieldTypeRef r);
IQLFieldTypeRef wrap(const FieldType * r);

IQLExpressionListRef IQLExpressionListCreate(IQLTreeFactoryRef ctxt)
{
  return wrap(new std::vector<IQLExpression *> ());
}

void IQLExpressionListFree(IQLTreeFactoryRef ctxt,
			   IQLExpressionListRef l)
{
  return delete unwrap(l);
}

void IQLExpressionListAppend(IQLTreeFactoryRef ctxt,
			     IQLExpressionListRef l,
			     IQLExpressionRef e)
{
  unwrap(l)->push_back(unwrap(e));
}

IQLExpressionRef IQLBuildLogicalOr(IQLTreeFactoryRef ctxtRef,				     
				   IQLExpressionRef leftRef,
				   IQLExpressionRef rightRef,
				   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LogicalOrExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				    line, column));
}

IQLExpressionRef IQLBuildLogicalAnd(IQLTreeFactoryRef ctxtRef,				     
				    IQLExpressionRef leftRef,
				    IQLExpressionRef rightRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LogicalAndExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				     line, column));
}

IQLExpressionRef IQLBuildLogicalNot(IQLTreeFactoryRef ctxtRef,				     
				    IQLExpressionRef leftRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LogicalNotExpr::create(ctxt, unwrap(leftRef),
				     line, column));
}

IQLExpressionRef IQLBuildEquals(IQLTreeFactoryRef ctxtRef,
				IQLExpressionRef leftRef,
				IQLExpressionRef rightRef,
				int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(EqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				 line, column));  
}

IQLExpressionRef IQLBuildNotEquals(IQLTreeFactoryRef ctxtRef,
				   IQLExpressionRef leftRef,
				   IQLExpressionRef rightRef,
				   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(NotEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				    line, column));  
}

IQLExpressionRef IQLBuildGreaterThan(IQLTreeFactoryRef ctxtRef,
				     IQLExpressionRef leftRef,
				     IQLExpressionRef rightRef,
				     int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(GreaterThanExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				      line, column));  
}

IQLExpressionRef IQLBuildLessThan(IQLTreeFactoryRef ctxtRef,
				  IQLExpressionRef leftRef,
				  IQLExpressionRef rightRef,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LessThanExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				   line, column));  
}

IQLExpressionRef IQLBuildGreaterThanEquals(IQLTreeFactoryRef ctxtRef,
					   IQLExpressionRef leftRef,
					   IQLExpressionRef rightRef,
					   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(GreaterThanEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
					    line, column));  
}

IQLExpressionRef IQLBuildLessThanEquals(IQLTreeFactoryRef ctxtRef,
					IQLExpressionRef leftRef,
					IQLExpressionRef rightRef,
					int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LessThanEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
					 line, column));  
}

IQLExpressionRef IQLBuildUnaryFun(IQLTreeFactoryRef ctxtRef,
				  const char * fun,
				  IQLExpressionRef leftRef,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> args;
  args.push_back(unwrap(leftRef));
  return wrap(CallExpr::create(ctxt, fun, args, line, column));
}

IQLExpressionRef IQLBuildBinaryFun(IQLTreeFactoryRef ctxtRef,
				   const char * fun,
				   IQLExpressionRef leftRef,
				   IQLExpressionRef rightRef,
				   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> args;
  args.push_back(unwrap(leftRef));
  args.push_back(unwrap(rightRef));
  return wrap(CallExpr::create(ctxt, fun, args, line, column));
}

IQLExpressionRef IQLBuildLike(IQLTreeFactoryRef ctxtRef,
			      IQLExpressionRef leftRef,
			      IQLExpressionRef rightRef,
			      int line, int column)
{
  throw std::runtime_error("LIKE not supported yet; use RLIKE instead");
}

IQLExpressionRef IQLBuildRLike(IQLTreeFactoryRef ctxtRef,
			       IQLExpressionRef leftRef,
			       IQLExpressionRef rightRef,
			       int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "rlike", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildPlus(IQLTreeFactoryRef ctxtRef,
			      IQLExpressionRef leftRef,
			      IQLExpressionRef rightRef,
			      int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "+", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildMinus(IQLTreeFactoryRef ctxtRef,
			       IQLExpressionRef leftRef,
			       IQLExpressionRef rightRef,
			       int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "-", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildTimes(IQLTreeFactoryRef ctxtRef,
			       IQLExpressionRef leftRef,
			       IQLExpressionRef rightRef,
			       int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "*", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildDivide(IQLTreeFactoryRef ctxtRef,
				IQLExpressionRef leftRef,
				IQLExpressionRef rightRef,
				int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "/", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildModulus(IQLTreeFactoryRef ctxtRef,
				 IQLExpressionRef leftRef,
				 IQLExpressionRef rightRef,
				 int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "%", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildUnaryPlus(IQLTreeFactoryRef ctxtRef,
				   IQLExpressionRef leftRef,
				   int line, int column)
{
  return IQLBuildUnaryFun(ctxtRef, "+", leftRef, line, column);
}

IQLExpressionRef IQLBuildUnaryMinus(IQLTreeFactoryRef ctxtRef,
				    IQLExpressionRef leftRef,
				    int line, int column)
{
  return IQLBuildUnaryFun(ctxtRef, "-", leftRef, line, column);
}

IQLExpressionRef IQLBuildBitwiseAnd(IQLTreeFactoryRef ctxtRef,
				    IQLExpressionRef leftRef,
				    IQLExpressionRef rightRef,
				    int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "&", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildBitwiseOr(IQLTreeFactoryRef ctxtRef,
				   IQLExpressionRef leftRef,
				   IQLExpressionRef rightRef,
				   int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "|", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildBitwiseXor(IQLTreeFactoryRef ctxtRef,
				    IQLExpressionRef leftRef,
				    IQLExpressionRef rightRef,
				    int line, int column)
{
  return IQLBuildBinaryFun(ctxtRef, "^", leftRef, rightRef, line, column);
}

IQLExpressionRef IQLBuildBitwiseNot(IQLTreeFactoryRef ctxtRef,
				    IQLExpressionRef leftRef,
				    int line, int column)
{
  return IQLBuildUnaryFun(ctxtRef, "~", leftRef, line, column);
}


IQLExpressionRef IQLBuildCase(IQLTreeFactoryRef ctxtRef,
			      IQLExpressionListRef argsRef,
			      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> & args(*unwrap(argsRef));
  return wrap(CaseExpr::create(ctxt, args, line, column));
}

IQLExpressionRef IQLBuildCast(IQLTreeFactoryRef ctxtRef,
			      IQLFieldTypeRef ty,
			      IQLExpressionRef arg,
			      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(CastExpr::create(ctxt, unwrap(ty), unwrap(arg), line, column));
}

IQLExpressionRef IQLBuildLiteralCast(IQLTreeFactoryRef ctxtRef,
				     const char * typeName,
				     const char * arg,
				     int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  const FieldType * ty = NULL;
  if (boost::algorithm::iequals("date", typeName)) {
    ty = DateType::Get(ctxt);
  } else if (boost::algorithm::iequals("datetime", typeName)) {
    ty = DateType::Get(ctxt);
  } else {
    throw std::runtime_error((boost::format("Invalid type: %1%") %
			      typeName).str());
  }
  return wrap(CastExpr::create(ctxt, ty, StringExpr::create(ctxt, arg, line, column),
			       line, column));
}

IQLExpressionRef IQLBuildCall(IQLTreeFactoryRef ctxtRef,
			      const char * fun,
			      IQLExpressionListRef argsRef,
			      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> & args(*unwrap(argsRef));
  return wrap(CallExpr::create(ctxt, fun, args, line, column));
}

IQLExpressionRef IQLBuildInt32(IQLTreeFactoryRef ctxtRef,
			       const char * text,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int32Expr::create(ctxt, text, line, column));  
}

IQLExpressionRef IQLBuildInt64(IQLTreeFactoryRef ctxtRef,
			       const char * text,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int64Expr::create(ctxt, text, line, column));  
}

IQLExpressionRef IQLBuildInterval(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  IQLExpressionRef arg,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IntervalExpr::create(ctxt, text, unwrap(arg), line, column));  
}

IQLExpressionRef IQLBuildBoolean(IQLTreeFactoryRef ctxtRef,
				 int isTrue,
				 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(BooleanExpr::create(ctxt, isTrue!=0, line, column));  
}

IQLExpressionRef IQLBuildNil(IQLTreeFactoryRef ctxtRef,
			     int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(NilExpr::create(ctxt, line, column));    
}

IQLExpressionRef IQLBuildIsNull(IQLTreeFactoryRef ctxtRef,
				IQLExpressionRef left,
				int isNot,
				int line, int column)
{
  IQLBuildUnaryFun(ctxtRef, isNot ? "_ISNOTNULL_" : "_ISNULL_",
		   left, line, column);
}

IQLExpressionRef IQLBuildDouble(IQLTreeFactoryRef ctxtRef,
				const char * text,
				int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DoubleExpr::create(ctxt, text, line, column));  
}

IQLExpressionRef IQLBuildDecimal(IQLTreeFactoryRef ctxtRef,
				 const char * text,
				 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DecimalExpr::create(ctxt, text, line, column));  
}

IQLExpressionRef IQLBuildString(IQLTreeFactoryRef ctxtRef,
				const char * text,
				int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  int l = strlen(text);
  if (l < 2) {
    throw std::runtime_error("Internal Error: Invalid text for "
			     "IQL string literal");
  }
  std::string s(text+1, text+l-2);
  boost::algorithm::replace_all(s, "''", "'");
  return wrap(StringExpr::create(ctxt, s.c_str(), line, column));  
}

IQLExpressionRef IQLBuildVariable(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(VariableExpr::create(ctxt, text, line, column));  
}

IQLExpressionRef IQLBuildArrayRef(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  IQLExpressionRef idx,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(ArrayReferenceExpr::create(ctxt, text, unwrap(idx), 
					 line, column));  
}

IQLFieldTypeRef IQLBuildInt32Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int32Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildInt64Type(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int64Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildDoubleType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DoubleType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildDecimalType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DecimalType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildDateType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DateType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildDatetimeType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DatetimeType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildNVarcharType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  throw std::runtime_error("NVARCHAR not yet implemented");
}
IQLFieldTypeRef IQLBuildVarcharType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(VarcharType::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildCharType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  int32_t fieldSz = boost::lexical_cast<int32_t> (sz);
  return wrap(CharType::Get(ctxt, fieldSz, nullable!=0));  
}
IQLFieldTypeRef IQLBuildBooleanType(IQLTreeFactoryRef ctxtRef, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int32Type::Get(ctxt, nullable!=0));  
}
IQLFieldTypeRef IQLBuildNilType(IQLTreeFactoryRef ctxtRef)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(NilType::Get(ctxt));  
}
IQLFieldTypeRef IQLBuildType(IQLTreeFactoryRef ctxtRef, const char * typeName, int nullable)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  if (boost::algorithm::iequals("date", typeName)) {
    return wrap(DateType::Get(ctxt, nullable));
  } else {
    throw std::runtime_error((boost::format("Invalid type: %1%") %
			      typeName).str());
  }
}
