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

#include <stack>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/lexical_cast.hpp>
#include "IQLExpression.hh"
#include "IQLBuildTree.h"
#include "RecordType.hh"

IQLExpression::~IQLExpression()
{
}

bool IQLExpression::equals(const IQLExpression * rhs) const
{
  const IQLExpression * input = this;
  const IQLExpression * pattern = rhs;

  if (!input->shallow_equals(pattern)) 
    return false;

  std::stack<std::pair<IQLExpression::arg_const_iterator, 
    IQLExpression::arg_const_iterator> > patternStk;
  std::vector<IQLExpression *> patternTmp;
  std::stack<std::pair<IQLExpression::arg_const_iterator, 
    IQLExpression::arg_const_iterator> > inputStk;
  std::vector<IQLExpression *> inputTmp;
  // Push the node with arguments on the stack.
  patternTmp.push_back(const_cast<IQLExpression *>(pattern));
  inputTmp.push_back(const_cast<IQLExpression *>(input));
  patternStk.push(std::make_pair(patternTmp.begin(), patternTmp.end()));
  inputStk.push(std::make_pair(inputTmp.begin(), inputTmp.end()));

  while(patternStk.size() && inputStk.size()) {
    if (patternStk.top().first != patternStk.top().second) {      
      pattern = *patternStk.top().first;
      input = *inputStk.top().first;
      // Make sure number of args is the same
      if (false == input->shallow_equals(pattern) ||
	  input->args_size() != pattern->args_size()) {
	return false;
      }
      patternStk.push(std::make_pair(pattern->begin_args(), 
				     pattern->end_args()));
      inputStk.push(std::make_pair(input->begin_args(), 
				   input->end_args()));
    } else {
      patternStk.pop();
      inputStk.pop();
      if (patternStk.size()) {
	++patternStk.top().first;
	++inputStk.top().first;
      } 
    }    
  }

  return patternStk.size() == 0 && inputStk.size() == 0;
}

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

LogicalOrExpr * LogicalOrExpr::clone() const
{
  return new LogicalOrExpr(*this);
}

LogicalAndExpr * LogicalAndExpr::clone() const
{
  return new LogicalAndExpr(*this);
}

LogicalNotExpr * LogicalNotExpr::clone() const
{
  return new LogicalNotExpr(*this);
}

EqualsExpr * EqualsExpr::clone() const
{
  return new EqualsExpr(*this);
}

NotEqualsExpr * NotEqualsExpr::clone() const
{
  return new NotEqualsExpr(*this);
}

GreaterThanExpr * GreaterThanExpr::clone() const
{
  return new GreaterThanExpr(*this);
}

LessThanExpr * LessThanExpr::clone() const
{
  return new LessThanExpr(*this);
}

GreaterThanEqualsExpr * GreaterThanEqualsExpr::clone() const
{
  return new GreaterThanEqualsExpr(*this);
}

LessThanEqualsExpr * LessThanEqualsExpr::clone() const
{
  return new LessThanEqualsExpr(*this);
}

CaseExpr::CaseExpr(DynamicRecordContext & ctxt,
		   const std::vector<IQLExpression *>& args,
		   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::CASE, 
		args.begin(), args.end(), 
		loc)
{
}

CaseExpr * CaseExpr::clone() const
{
  return new CaseExpr(*this);
}

CallExpr::CallExpr(DynamicRecordContext & ctxt,
		   const char * fun,
		   const std::vector<IQLExpression *>& args,
		   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::CALL, 
		args.begin(), args.end(), 
		loc)
{
  setData(fun);
}

CallExpr * CallExpr::clone() const
{
  return new CallExpr(*this);
}

CastExpr::CastExpr(DynamicRecordContext & ctxt,
		   const FieldType * ty,
		   IQLExpression * arg,
		   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::CAST, 
		arg,
		loc)
{
  setData(ty);
}

CastExpr * CastExpr::clone() const
{
  return new CastExpr(*this);
}

Int32Expr::Int32Expr(DynamicRecordContext & ctxt, 
		     const char * text,
		     const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::INT32, loc)
{
  // TODO: Should we check the string now or later?
  // On the one hand haven't yet constructed our object
  // nor has anyone asked for type checking.  On the
  // other hand, we'll need to convert state later.
  setData(text);
}

Int32Expr * Int32Expr::clone() const
{
  return new Int32Expr(*this);
}

Int64Expr::Int64Expr(DynamicRecordContext & ctxt, 
		     const char * text,
		     const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::INT64, loc)
{
  setData(text);
}

Int64Expr * Int64Expr::clone() const
{
  return new Int64Expr(*this);
}

IntervalExpr::IntervalExpr(DynamicRecordContext & ctxt, 
			   const char * text,
			   IQLExpression * arg,
			   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::INTERVAL, arg, loc)
{
  setData(text);
}

IntervalExpr * IntervalExpr::clone() const
{
  return new IntervalExpr(*this);
}

DoubleExpr::DoubleExpr(DynamicRecordContext & ctxt, 
		       const char * text,
		       const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::DOUBLE, loc)
{
  setData(text);
}

DoubleExpr * DoubleExpr::clone() const
{
  return new DoubleExpr(*this);
}

DecimalExpr::DecimalExpr(DynamicRecordContext & ctxt, 
			 const char * text,
			 const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::DECIMAL, loc)
{
  setData(text);
}

DecimalExpr * DecimalExpr::clone() const
{
  return new DecimalExpr(*this);
}

StringExpr::StringExpr(DynamicRecordContext & ctxt, 
		       const char * text,
		       const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::STRING, loc)
{
  setData(text);
}

StringExpr * StringExpr::clone() const
{
  return new StringExpr(*this);
}

BooleanExpr::BooleanExpr(DynamicRecordContext & ctxt, 
			 bool isTrue,
			 const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::BOOLEAN, loc)
{
  setData(isTrue ? 1 : 0);
}

BooleanExpr * BooleanExpr::clone() const
{
  return new BooleanExpr(*this);
}

NilExpr::NilExpr(DynamicRecordContext & ctxt, 
		 const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::NIL, loc)
{
}

NilExpr * NilExpr::clone() const
{
  return new NilExpr(*this);
}

VariableExpr::VariableExpr(DynamicRecordContext & ctxt, 
			   const char * text,
			   const char * text2,
			   const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::VARIABLE, loc)
{
  // TODO: store this parsed?
  std::string str(text);
  if (text2) {
    str += ".";
    str += text2;
  }
  setData(str.c_str());
}

VariableExpr * VariableExpr::clone() const
{
  return new VariableExpr(*this);
}

ArrayReferenceExpr::ArrayReferenceExpr(DynamicRecordContext & ctxt, 
				       const char * text,
				       IQLExpression * idx,
				       const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::ARRAYREF, idx, loc)
{
  setData(text);
}

ArrayReferenceExpr * ArrayReferenceExpr::clone() const
{
  return new ArrayReferenceExpr(*this);
}

ArrayExpr::ArrayExpr(DynamicRecordContext & ctxt,
		     const std::vector<IQLExpression *>& args,
		     const SourceLocation& loc)
  :
  IQLExpression(ctxt, IQLExpression::ARR, 
		args.begin(), args.end(), 
		loc)
{
}

ArrayExpr * ArrayExpr::clone() const
{
  return new ArrayExpr(*this);
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
					      eq->getSourceLocation());
    p->replaceArg(eq,tmp);
  } else {
    mResidual = NULL;
  }
  
  // Append eq to equijoin predicate
  if (mEquals) {
    mEquals = LogicalAndExpr::create(mContext, mEquals, eq, 
				     eq->getSourceLocation());
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
				   clause->getSourceLocation());
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
	  parent->replaceArg(expr, BooleanExpr::create(ctxt, true, SourceLocation()));
	}
	break;
      }
    }  
}

// Implementation of the C binding
class IQLRecordConstructor * unwrap(IQLRecordConstructorRef r)
{
  return reinterpret_cast<class IQLRecordConstructor *>(r);
}

IQLRecordConstructorRef wrap(class IQLRecordConstructor * r)
{
  return reinterpret_cast<IQLRecordConstructorRef>(r);
}

class IQLFieldConstructor * unwrap(IQLFieldConstructorRef r)
{
  return reinterpret_cast<class IQLFieldConstructor *>(r);
}

IQLFieldConstructorRef wrap(class IQLFieldConstructor * r)
{
  return reinterpret_cast<IQLFieldConstructorRef>(r);
}

std::vector<IQLFieldConstructor *> * unwrap(IQLFieldConstructorListRef r)
{
  return reinterpret_cast<std::vector<IQLFieldConstructor *> *>(r);
}

IQLFieldConstructorListRef wrap(std::vector<IQLFieldConstructor *> * r)
{
  return reinterpret_cast<IQLFieldConstructorListRef>(r);
}

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

IQLFieldConstructorListRef IQLFieldConstructorListCreate(IQLTreeFactoryRef ctxtRef)
{
  return wrap(new std::vector<IQLFieldConstructor *> ());
}

void IQLFieldConstructorListFree(IQLTreeFactoryRef ctxtRef,
				 IQLFieldConstructorListRef l)
{
  delete unwrap(l);
}

void IQLFieldConstructorListAppend(IQLTreeFactoryRef ctxtRef,
				   IQLFieldConstructorListRef l,
				   IQLFieldConstructorRef e)
{
  unwrap(l)->push_back(unwrap(e));
}

IQLFieldConstructorRef IQLBuildAddFields(IQLTreeFactoryRef ctxtRef, 
					 const char * recordName)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IQLFieldGlob::create(ctxt, recordName));  
}

IQLFieldConstructorRef IQLBuildAddField(IQLTreeFactoryRef ctxtRef, 
					const char * fieldName,
					IQLExpressionRef expr)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IQLNamedExpression::create(ctxt, unwrap(expr), fieldName));  
}

IQLFieldConstructorRef IQLBuildQuotedId(IQLTreeFactoryRef ctxtRef, 
					const char * pattern,
					const char * names)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IQLFieldPattern::create(ctxt, pattern, names));  
}

IQLRecordConstructorRef IQLBuildRecord(IQLTreeFactoryRef ctxtRef,
				       IQLFieldConstructorListRef fields)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IQLRecordConstructor::create(ctxt, *unwrap(fields)));  
}

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
				    SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildLogicalAnd(IQLTreeFactoryRef ctxtRef,				     
				    IQLExpressionRef leftRef,
				    IQLExpressionRef rightRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LogicalAndExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				     SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildLogicalNot(IQLTreeFactoryRef ctxtRef,				     
				    IQLExpressionRef leftRef,
				    int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LogicalNotExpr::create(ctxt, unwrap(leftRef),
				     SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildEquals(IQLTreeFactoryRef ctxtRef,
				IQLExpressionRef leftRef,
				IQLExpressionRef rightRef,
				int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(EqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				 SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildNotEquals(IQLTreeFactoryRef ctxtRef,
				   IQLExpressionRef leftRef,
				   IQLExpressionRef rightRef,
				   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(NotEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				    SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildGreaterThan(IQLTreeFactoryRef ctxtRef,
				     IQLExpressionRef leftRef,
				     IQLExpressionRef rightRef,
				     int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(GreaterThanExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				      SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildLessThan(IQLTreeFactoryRef ctxtRef,
				  IQLExpressionRef leftRef,
				  IQLExpressionRef rightRef,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LessThanExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
				   SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildGreaterThanEquals(IQLTreeFactoryRef ctxtRef,
					   IQLExpressionRef leftRef,
					   IQLExpressionRef rightRef,
					   int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(GreaterThanEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
					    SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildLessThanEquals(IQLTreeFactoryRef ctxtRef,
					IQLExpressionRef leftRef,
					IQLExpressionRef rightRef,
					int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(LessThanEqualsExpr::create(ctxt, unwrap(leftRef), unwrap(rightRef),
					 SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildUnaryFun(IQLTreeFactoryRef ctxtRef,
				  const char * fun,
				  IQLExpressionRef leftRef,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> args;
  args.push_back(unwrap(leftRef));
  return wrap(CallExpr::create(ctxt, fun, args, SourceLocation(line, column)));
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
  return wrap(CallExpr::create(ctxt, fun, args, SourceLocation(line, column)));
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
  return wrap(CaseExpr::create(ctxt, args, SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildCast(IQLTreeFactoryRef ctxtRef,
			      IQLFieldTypeRef ty,
			      IQLExpressionRef arg,
			      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(CastExpr::create(ctxt, unwrap(ty), unwrap(arg), SourceLocation(line, column)));
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
  return wrap(CastExpr::create(ctxt, ty, StringExpr::create(ctxt, arg, SourceLocation(line, column)),
			       SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildCall(IQLTreeFactoryRef ctxtRef,
			      const char * fun,
			      IQLExpressionListRef argsRef,
			      int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> & args(*unwrap(argsRef));
  return wrap(CallExpr::create(ctxt, fun, args, SourceLocation(line, column)));
}

IQLExpressionRef IQLBuildInt32(IQLTreeFactoryRef ctxtRef,
			       const char * text,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int32Expr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildInt64(IQLTreeFactoryRef ctxtRef,
			       const char * text,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(Int64Expr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildInterval(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  IQLExpressionRef arg,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(IntervalExpr::create(ctxt, text, unwrap(arg), SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildBoolean(IQLTreeFactoryRef ctxtRef,
				 int isTrue,
				 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(BooleanExpr::create(ctxt, isTrue!=0, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildNil(IQLTreeFactoryRef ctxtRef,
			     int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(NilExpr::create(ctxt, SourceLocation(line, column)));    
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
  return wrap(DoubleExpr::create(ctxt, text, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildDecimal(IQLTreeFactoryRef ctxtRef,
				 const char * text,
				 int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(DecimalExpr::create(ctxt, text, SourceLocation(line, column)));  
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
  return wrap(StringExpr::create(ctxt, s.c_str(), SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildVariable(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  const char * text2,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(VariableExpr::create(ctxt, text, text2, SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildArrayRef(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  IQLExpressionRef idx,
				  int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  return wrap(ArrayReferenceExpr::create(ctxt, text, unwrap(idx), 
					 SourceLocation(line, column)));  
}

IQLExpressionRef IQLBuildArray(IQLTreeFactoryRef ctxtRef,
			       IQLExpressionListRef argsRef,
			       int line, int column)
{
  DynamicRecordContext & ctxt(*unwrap(ctxtRef));
  std::vector<IQLExpression*> & args(*unwrap(argsRef));
  return wrap(ArrayExpr::create(ctxt, args, SourceLocation(line, column)));
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

const char * IQLExpressionPrinter::getExpressionSymbol(uint32_t nodeType)
{
  switch(nodeType) {
  case IQLExpression::LOR: return "OR";
  case IQLExpression::LAND: return "AND";
  case IQLExpression::LNOT: return "NOT";
  case IQLExpression::BAND: return "&";
  case IQLExpression::BOR: return "|";
  case IQLExpression::BXOR: return "^";
  case IQLExpression::BNOT: return "~";
  case IQLExpression::EQ: return "=";
  case IQLExpression::GTN: return ">";
  case IQLExpression::LTN: return "<";
  case IQLExpression::GTEQ: return ">=";
  case IQLExpression::LTEQ: return "<=";
  case IQLExpression::NEQ: return "<>";
  case IQLExpression::MINUS: return "-";
  case IQLExpression::PLUS: return "+";
  case IQLExpression::TIMES: return "*";
  case IQLExpression::DIVIDE: return "/";
  case IQLExpression::MOD: return "%";
  case IQLExpression::NIL: return "INTERVAL";
  default:
    throw std::runtime_error((boost::format("Unknown node type %1%") % nodeType).str());
  }
}

IQLExpressionPrinter::IQLExpressionPrinter(std::ostream& ostr, IQLExpression * input)
{
  mBinaryInfix.insert("+");
  mBinaryInfix.insert("-");
  mBinaryInfix.insert("*");
  mBinaryInfix.insert("/");
  mBinaryInfix.insert("%");
  mUnaryInfix.insert("+");
  mUnaryInfix.insert("-");

  // Bottom up pattern replacement.
  std::vector<std::pair<IQLExpression::arg_const_iterator, 
    IQLExpression::arg_const_iterator> > stk;
  std::vector<IQLExpression *> tmp;
  tmp.push_back(input);
  // Push the node with arguments on the stack.
  stk.push_back(std::make_pair(tmp.begin(), tmp.end()));
  while(stk.size()) {
    if (stk.back().first != stk.back().second) {
      IQLExpression * e = *stk.back().first;
      switch (e->getNodeType()) {
      case IQLExpression::ARRAYREF:
	ostr << e->getStringData().c_str() << "[";  
	break;
      case IQLExpression::CALL:
	if (isBinaryInfix(e)) {
	  ostr << "(";
	} else {
	  ostr << e->getStringData().c_str() << "(";
	}
	break;
      case IQLExpression::LAND:
      case IQLExpression::LOR:
      case IQLExpression::EQ:
      case IQLExpression::NEQ:
      case IQLExpression::GTEQ:
      case IQLExpression::GTN:
      case IQLExpression::LTEQ:
      case IQLExpression::LTN:
	  ostr << "(";
	break;
      case IQLExpression::CASE:
	ostr << "CASE WHEN ";  
	break;
      case IQLExpression::NIL:
	ostr << "NULL";  
	break;
      case IQLExpression::VARIABLE:
      case IQLExpression::INT32:
      case IQLExpression::INT64:
      case IQLExpression::DOUBLE:
      case IQLExpression::DECIMAL:
	ostr << e->getStringData().c_str();
	break;
      case IQLExpression::STRING:
	// TODO: Do we need to quotify?
	ostr << e->getStringData().c_str();
	break;
      case IQLExpression::BOOLEAN:
	ostr << (e->getBooleanData() ? "TRUE" : "FALSE");
	break;
      default:
	break;
      }
      stk.push_back(std::make_pair(e->begin_args(), 
				   e->end_args()));
    } else {
      stk.pop_back();
      if (stk.size()) {
	// Done with subtree at stk.back().first	
	IQLExpression * e = *stk.back().first;
	++stk.back().first;
	if (stk.size() > 1) {
	  IQLExpression * parent = *stk[stk.size()-2].first;
	  switch (parent->getNodeType()) {
	  case IQLExpression::ARRAYREF:
	    ostr << "]";  
	    break;
	  case IQLExpression::CALL:
	    if (isBinaryInfix(parent)) {
	      ostr << ")";
	      if(stk.back().first != stk.back().second) {
		ostr << parent->getStringData().c_str() << "(";
	      }
	    } else {
	      if(stk.back().first != stk.back().second) {
		ostr << ",";
	      } else {
		ostr << ")";
	      }
	    }
	    break;
	  case IQLExpression::CASE:
	    if (stk.back().first == stk.back().second) {
	      ostr << " END";  
	    } else if ((stk.back().first - parent->begin_args())  % 2) {
	      ostr << " THEN ";
	    } else if(stk.back().first+1 != stk.back().second) {
	      ostr << " WHEN ";
	    } else {
	      ostr << " ELSE ";
	    }
	    break;	    
	  case IQLExpression::LAND:
	  case IQLExpression::LOR:
	  case IQLExpression::EQ:
	  case IQLExpression::NEQ:
	  case IQLExpression::GTEQ:
	  case IQLExpression::GTN:
	  case IQLExpression::LTEQ:
	  case IQLExpression::LTN:
	      ostr << ")";
	      if(stk.back().first != stk.back().second) {
		ostr << getExpressionSymbol(parent->getNodeType()) << "(";
	      }
	    break;
	break;
	  default:
	    break;
	  }
	}
      }
    }    
  }
}

bool IQLExpressionPrinter::isBinaryInfix(IQLExpression * e)
{
  return e->getNodeType() == IQLExpression::CALL &&
    e->args_size() == 2 &&
    mBinaryInfix.end() != mBinaryInfix.find(e->getStringData());
}

bool IQLExpressionPrinter::isUnaryPrefix(IQLExpression * e)
{
  return e->getNodeType() == IQLExpression::CALL &&
    e->args_size() == 1 &&
    mUnaryInfix.end() != mUnaryInfix.find(e->getStringData());
}

