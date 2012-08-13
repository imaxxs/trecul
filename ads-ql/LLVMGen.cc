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

#include <map>
#include <stdexcept>
#include <iostream>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include "LLVMGen.h"
#include "CodeGenerationContext.hh"
#include "TypeCheckContext.hh"

#include "llvm/LLVMContext.h"
#include "llvm/Module.h"
#include "llvm/Constants.h"
#include "llvm/DerivedTypes.h"
#include "llvm/Instructions.h"
#include "llvm/ExecutionEngine/JIT.h"
#include "llvm/ExecutionEngine/Interpreter.h"
#include "llvm/ExecutionEngine/GenericValue.h"
#include "llvm/Target/TargetSelect.h"
#include "llvm/Support/ManagedStatic.h"
#include "llvm/Support/raw_ostream.h"

extern "C" {
#include "decimal128.h"
}

#include "RecordType.hh"
#include "IQLGraphBuilder.hh"

// Forward decls

void IQLToLLVMCaseBlockThen(CodeGenerationContext * ctxt, 
			    IQLToLLVMValueRef value, 
			    const FieldType * caseType, 
			    bool allowNullToNonNull);

void IQLToLLVMBuildSetValue2(IQLCodeGenerationContextRef ctxt, 
			     IQLToLLVMValueRef rhs,
			     IQLToLLVMValueRef lhs);
/**
 * Check if a value is NULL
 */
IQLToLLVMValueRef IQLToLLVMBuildIsNull(CodeGenerationContext * ctxt, 
				       IQLToLLVMValueRef val);
/**
 * Create a NULL value
 */
IQLToLLVMValueRef IQLToLLVMBuildNull(CodeGenerationContext * ctxt);

/**
 * Cast a value to BIGINT.
 */
IQLToLLVMValueRef IQLToLLVMBuildCastInt64(CodeGenerationContext * ctxt, 
					  IQLToLLVMValueRef e, 
					  void * argAttrs, void * retAttrs);
/**
 * Cast a value to BOOLEAN.
 */
IQLToLLVMValueRef IQLToLLVMBuildCastBoolean(CodeGenerationContext * ctxt, 
					    IQLToLLVMValueRef e, 
					    void * argAttrs, void * retAttrs);
/**
 * Cast a value to CHAR(N).
 */
IQLToLLVMValueRef IQLToLLVMBuildCastChar(CodeGenerationContext * ctxt, 
					 IQLToLLVMValueRef e, 
					 void * argAttrs, void * retAttrs,
					 const char * N);
/**
 * Cast a value to DATE.
 */
IQLToLLVMValueRef IQLToLLVMBuildCastDate(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs);
/**
 * Cast a value to DATETIME.
 */
IQLToLLVMValueRef IQLToLLVMBuildCastDatetime(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs);
/**
 * Cast a value to DECIMAL.
 */
IQLToLLVMValueRef IQLToLLVMBuildCastDecimal(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs);
/**
 * Cast a value to DOUBLE.
 */
IQLToLLVMValueRef IQLToLLVMBuildCastDouble(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs);
/**
 * Cast a value to 32 bit INTEGER.
 */
IQLToLLVMValueRef IQLToLLVMBuildCastInt32(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs);
/**
 * Cast a value to VARCHAR.
 */
IQLToLLVMValueRef IQLToLLVMBuildCastVarchar(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs);

/**
 * Call an external function of type decimal (decimal, decimal)
 */
IQLToLLVMValueRef 
IQLToLLVMCreateBinaryDecimalCall(CodeGenerationContext * ctxt, 
				 IQLToLLVMValueRef lhs, 
				 IQLToLLVMValueRef rhs, 
				 enum DecimalOpCode);
void IQLToLLVMBuildSetValue(CodeGenerationContext * ctxt, 
			    IQLToLLVMValueRef llvmVal, 
			    const char * loc);
IQLToLLVMValueRef IQLToLLVMBuildCompare(CodeGenerationContext * ctxt, IQLToLLVMValueRef lhs, IQLToLLVMValueRef rhs, IQLToLLVMPredicate op);
IQLToLLVMValueRef IQLToLLVMBuildIsNull(CodeGenerationContext * ctxt, IQLToLLVMValueRef val);
void IQLToLLVMBuildSetValue2(CodeGenerationContext * ctxt, 
			     IQLToLLVMValueRef iqlVal,
			     IQLToLLVMValueRef iqllvalue);

LLVMValueRef IQLToLLVMGetCachedLocal(CodeGenerationContext * ctxt,
			     LLVMTypeRef ty)
{
  typedef std::map<const llvm::Type*, 
    std::vector<const llvm::Value *> > local_cache;
  local_cache & c(*((local_cache *) ctxt->AllocaCache));
  local_cache::iterator it = c.find(llvm::unwrap(ty));
  if (it == c.end() || 0==it->second.size()) return NULL;
  LLVMValueRef v = llvm::wrap(it->second.back());
  it->second.pop_back();
  return v;
}

void IQLToLLVMReturnCachedLocal(CodeGenerationContext * ctxt,
				LLVMValueRef v)
{
  typedef std::map<const llvm::Type*, 
    std::vector<const llvm::Value *> > local_cache;
  local_cache & c(*((local_cache *) ctxt->AllocaCache));
  const llvm::Type * ty = llvm::unwrap(LLVMTypeOf(v));
  const llvm::PointerType * pty = llvm::dyn_cast<llvm::PointerType>(ty);
  if (pty == NULL) return;
  ty = pty->getElementType();
  local_cache::iterator it = c.find(ty);
  if (it == c.end()) {
    c[ty] = std::vector<const llvm::Value *>();
  }
  c[ty].push_back(llvm::unwrap(v));
}

// More forwards.  These need the def of class IQLToLLVMValue.
IQLToLLVMValue::ValueType IQLToLLVMCreateUnaryDecimalCall(CodeGenerationContext * ctxt, 
							  IQLToLLVMValueRef lhs, 
							  LLVMValueRef ret,
							  const std::string& externalFun);

IQLToLLVMValue::ValueType IQLToLLVMCreateBinaryDecimalCall(CodeGenerationContext * ctxt, 
							   IQLToLLVMValueRef lhs, 
							   IQLToLLVMValueRef rhs, 
							   LLVMValueRef ret,
							   enum DecimalOpCode opCode);

const IQLToLLVMValue * unwrap(IQLToLLVMValueRef val) {
  return reinterpret_cast<const IQLToLLVMValue*>(val);
}

IQLToLLVMValueRef wrap(const IQLToLLVMValue * val) {
  return reinterpret_cast<IQLToLLVMValueRef>(val);
}

const IQLToLLVMLValue * unwrap(IQLToLLVMLValueRef val) {
  return reinterpret_cast<const IQLToLLVMLValue*>(val);
}

IQLToLLVMLValueRef wrap(const IQLToLLVMLValue * val) {
  return reinterpret_cast<IQLToLLVMLValueRef>(val);
}

IQLToLLVMRecordMapRef wrap(std::map<std::string, std::pair<std::string, const RecordType*> > * r)
{
  return reinterpret_cast<struct IQLToLLVMRecordMapStruct *>(r);
}

std::map<std::string, std::pair<std::string, const RecordType*> > * unwrap(IQLToLLVMRecordMapRef r)
{
  return reinterpret_cast<std::map<std::string, std::pair<std::string, const RecordType*> > *>(r);
}

std::vector<const IQLToLLVMValue *> * unwrap(IQLToLLVMValueVectorRef v)
{
  return reinterpret_cast<std::vector<const IQLToLLVMValue *> *>(v);
}

IQLToLLVMValueVectorRef wrap(std::vector<const IQLToLLVMValue *> * ptr)
{
  return reinterpret_cast<IQLToLLVMValueVectorRef>(ptr);
}

class CodeGenerationContext * unwrap(IQLCodeGenerationContextRef ctxtRef)
{
  return reinterpret_cast<class CodeGenerationContext *> (ctxtRef);
}

IQLCodeGenerationContextRef wrap(class CodeGenerationContext * ctxt)
{
  return reinterpret_cast<IQLCodeGenerationContextRef> (ctxt);
}

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

IQLToLLVMValueVectorRef IQLToLLVMValueVectorCreate()
{
  return wrap(new std::vector<const IQLToLLVMValue *>());
}

void IQLToLLVMValueVectorFree(IQLToLLVMValueVectorRef v)
{
  if (v == NULL) return;
  delete unwrap(v);
}

void IQLToLLVMValueVectorPushBack(IQLToLLVMValueVectorRef v, 
				  IQLToLLVMValueRef ft,
				  void * argAttrs)
{
  unwrap(v)->push_back(unwrap(ft));
}

IQLToLLVMValueRef IQLToLLVMCreateLocalValue(CodeGenerationContext * ctxt, LLVMValueRef val)
{
  return IQLToLLVMValue::get(ctxt, val, IQLToLLVMValue::eLocal);
}

IQLToLLVMValueRef IQLToLLVMCreateGlobalValue(CodeGenerationContext * ctxt, LLVMValueRef val)
{
  return IQLToLLVMValue::get(ctxt, val, IQLToLLVMValue::eGlobal);
}

IQLToLLVMValueRef IQLToLLVMCreateNullableGlobalValue(CodeGenerationContext * ctxt, LLVMValueRef val, LLVMValueRef nv)
{
  return IQLToLLVMValue::get(ctxt, val, llvm::unwrap(nv), IQLToLLVMValue::eGlobal);
}

LLVMValueRef IQLToLLVMTrimAlloca(LLVMValueRef result)
{
  if (IQLToLLVMValue::isPointerToValueType(LLVMTypeOf(result))) {
    // Check whether the last instruction is a store to the result
    // Check whether that is the only use of result.  If so then
    // we can replace result by the value stored in it.  
    // The point here is to avoid too many alloca's from proliferating;
    // they will be removed by mem2reg during optimziation but
    // for really huge programs that takes a lot of time and memory.
    // It is much faster to fix up the problem here.
    llvm::AllocaInst * AI = 
      llvm::dyn_cast<llvm::AllocaInst>(llvm::unwrap(result));
    if (AI != NULL && 
	AI->hasOneUse()) {
      if(llvm::StoreInst * SI = 
	 llvm::dyn_cast<llvm::StoreInst>(*AI->use_begin())) {
	if (!SI->isVolatile() && SI->getPointerOperand() == AI) {
	  // Conditions satisfied. Get rid of the store and the
	  // alloca and replace result with the value of the
	  // store.
	  const llvm::Value * val = SI->getValueOperand();
	  SI->eraseFromParent();
	  AI->eraseFromParent();
	  return llvm::wrap(val);
	}
      }
    }
  }
  return result;
}

typedef IQLToLLVMValue::ValueType (*IQLToLLVMUnaryOperator) (CodeGenerationContext * ctxt, 
							     IQLToLLVMValueRef lhs, 
							     const FieldType * lhsType, 
							     LLVMValueRef ret,
							     const FieldType * retType);

/**
 * Wrap a unary operation in the canonical NULL handling code
 * which says that a result is NULL if its argument is.
 */
IQLToLLVMValueRef IQLToLLVMBuildNullableUnaryOp(CodeGenerationContext * ctxt, 
						IQLToLLVMValueRef lhs, 
						const FieldType * lhsType, 
						const FieldType * resultType,
						IQLToLLVMUnaryOperator unOp)
{
  llvm::Value * n1 = unwrap(lhs)->getNull();
  BOOST_ASSERT((lhsType->isNullable() && n1 != NULL) ||
	       (!lhsType->isNullable() && n1 == NULL));
  if (unwrap(lhs)->isLiteralNull()) {
    return IQLToLLVMBuildNull(ctxt);
  }
  llvm::Value * nv = NULL;
  // Allocate space for a result
  llvm::Type * retTy = resultType->LLVMGetType(ctxt);
  LLVMValueRef result = LLVMCreateEntryBlockAlloca(ctxt, 
						   llvm::wrap(retTy),
						   "nullableUnOp");    
  // This will be filled in by the binOp we call.
  IQLToLLVMValue::ValueType vt = IQLToLLVMValue::eLocal;
  if (n1 != NULL) {
    // Unwrap to C++
    llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
    llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
    // The function we are working on.
    llvm::Function *f = b->GetInsertBlock()->getParent();
    // Create blocks for the then/value and else (likely to be next conditional).  
    // Insert the 'then/value' block at the end of the function.
    llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
    llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "cont", f);

    // Test if NULL and branch.  
    nv = n1;
    b->CreateCondBr(b->CreateNot(nv), thenBB, mergeBB);
    // Emit then value.
    b->SetInsertPoint(thenBB);  
    vt = unOp(ctxt, lhs, lhsType, result, resultType);
    b->CreateBr(mergeBB);    
    b->SetInsertPoint(mergeBB);
  } else {
    vt = unOp(ctxt, lhs, lhsType, result, resultType);
    result = IQLToLLVMTrimAlloca(result);
  }
  // Return either pointer or value
  if (IQLToLLVMValue::isPointerToValueType(LLVMTypeOf(result))) {
    result = LLVMBuildLoad(ctxt->LLVMBuilder, result, "");
  }
  return IQLToLLVMValue::get(ctxt, result, nv, vt);
}

typedef IQLToLLVMValue::ValueType (*IQLToLLVMBinaryOperator) (CodeGenerationContext * ctxt, 
							      IQLToLLVMValueRef lhs, 
							      const FieldType * lhsType, 
							      IQLToLLVMValueRef rhs, 
							      const FieldType * rhsType,
							      LLVMValueRef ret,
							      const FieldType * retType);

/**
 * Wrap a binary operation in the canonical NULL handling code
 * which says that a result is NULL if and only one of its
 * two arguments is.
 */
IQLToLLVMValueRef IQLToLLVMBuildNullableBinaryOp(CodeGenerationContext * ctxt, 
						 IQLToLLVMValueRef lhs, 
						 const FieldType * lhsType, 
						 IQLToLLVMValueRef rhs, 
						 const FieldType * rhsType,
						 const FieldType * resultType,
						 IQLToLLVMBinaryOperator binOp)
{
  llvm::Value * n1 = unwrap(lhs)->getNull();
  llvm::Value * n2 = unwrap(rhs)->getNull();
  BOOST_ASSERT((lhsType->isNullable() && n1 != NULL) ||
	       (!lhsType->isNullable() && n1 == NULL));
  BOOST_ASSERT((rhsType->isNullable() && n2 != NULL) ||
	       (!rhsType->isNullable() && n2 == NULL));
  if (unwrap(lhs)->isLiteralNull() || unwrap(rhs)->isLiteralNull()) {
    return IQLToLLVMBuildNull(ctxt);
  }
  llvm::Value * nv = NULL;
  // Allocate space for a result
  llvm::Type * retTy = resultType->LLVMGetType(ctxt);
  LLVMValueRef result = LLVMCreateEntryBlockAlloca(ctxt, 
						   llvm::wrap(retTy),
						   "nullableBinOp");    
  // This will be filled in by the binOp we call.
  IQLToLLVMValue::ValueType vt = IQLToLLVMValue::eLocal;
  if (n1 != NULL || n2 != NULL) {
    // Unwrap to C++
    llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
    llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
    // The function we are working on.
    llvm::Function *f = b->GetInsertBlock()->getParent();
    // Create blocks for the then/value and else (likely to be next conditional).  
    // Insert the 'then/value' block at the end of the function.
    llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
    llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "cont", f);

    // Test if NULL and branch.  We may have one or two values to check
    nv = n1 != NULL ? (n2 != NULL ? b->CreateOr(n1,n2) : n1) : n2;
    b->CreateCondBr(b->CreateNot(nv), thenBB, mergeBB);
    // Emit then value.
    b->SetInsertPoint(thenBB);  
    vt = binOp(ctxt, lhs, lhsType, rhs, rhsType, result, resultType);
    b->CreateBr(mergeBB);    
    b->SetInsertPoint(mergeBB);
  } else {
    vt = binOp(ctxt, lhs, lhsType, rhs, rhsType, result, resultType);
    result = IQLToLLVMTrimAlloca(result);
  }
  // Return either pointer or value
  if (IQLToLLVMValue::isPointerToValueType(LLVMTypeOf(result))) {
    result = LLVMBuildLoad(ctxt->LLVMBuilder, result, "");
  }
  return IQLToLLVMValue::get(ctxt, result, nv, vt);
}

////////////////////////////////////////
// Helpers when accessing Varchar members
////////////////////////////////////////

LLVMValueRef IQLToLLVMVarcharGetSize(CodeGenerationContext * ctxt, LLVMValueRef varcharPtr)
{
  // LLVMValueRef gepOffsets[2];
  // gepOffsets[0] = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), 0, 1);
  // gepOffsets[1] = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), 0, 1);
  // return LLVMBuildLoad(ctxt->LLVMBuilder, 
  // 		       LLVMBuildGEP(ctxt->LLVMBuilder, varcharPtr, &gepOffsets[0], 2, "varcharszaddr"), 
  // 		       "varcharsz");
  return LLVMBuildLoad(ctxt->LLVMBuilder,
		       LLVMBuildStructGEP(ctxt->LLVMBuilder, varcharPtr, 0, "varcharszaddr"), "varchar_sz");
}

void IQLToLLVMVarcharSetSize(CodeGenerationContext * ctxt, LLVMValueRef varcharPtr, LLVMValueRef sz)
{
  LLVMValueRef elt = LLVMBuildStructGEP(ctxt->LLVMBuilder, varcharPtr, 0, "varcharszaddr");
  LLVMBuildStore(ctxt->LLVMBuilder,
  		 sz, 
  		 elt);
}

LLVMValueRef IQLToLLVMVarcharGetPtr(CodeGenerationContext * ctxt, LLVMValueRef varcharPtr)
{
  // LLVMValueRef gepOffsets[2];
  // gepOffsets[0] = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), 0, 1);
  // gepOffsets[1] = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), 1, 1);
  // return LLVMBuildLoad(ctxt->LLVMBuilder, 
  // 		       LLVMBuildGEP(ctxt->LLVMBuilder, varcharPtr, &gepOffsets[0], 2, "varcharptraddr"),
  // 		       "varcharptr");
  return LLVMBuildLoad(ctxt->LLVMBuilder,
		       LLVMBuildStructGEP(ctxt->LLVMBuilder, varcharPtr, 1, "varcharptraddr"), "varchar_ptr");
}

void IQLToLLVMVarcharSetPtr(CodeGenerationContext * ctxt, LLVMValueRef varcharPtr, LLVMValueRef ptr)
{
  LLVMValueRef elt = LLVMBuildStructGEP(ctxt->LLVMBuilder, varcharPtr, 1, "varcharptraddr");
  LLVMBuildStore(ctxt->LLVMBuilder,
		 ptr, 
		 elt);
}

IQLToLLVMValue::ValueType IQLToLLVMCreateBinaryVarcharCall(CodeGenerationContext * ctxt, IQLToLLVMValueRef lhs, IQLToLLVMValueRef rhs, LLVMValueRef ret, enum VarcharOpCode opCode) {
  const char * externFuncs [] = { "InternalVarcharAdd" };
  LLVMValueRef callArgs[4];
  LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, externFuncs[opCode]);
  callArgs[0] = unwrap(lhs)->getValue();
  callArgs[1] = unwrap(rhs)->getValue();
  callArgs[2] = ret;
  callArgs[3] = LLVMBuildLoad(ctxt->LLVMBuilder, 
			      ctxt->getContextArgumentRef(), 
			      "ctxttmp");
  LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 4, "");
  // All external varchar functions MUST allocate memory locally
  return IQLToLLVMValue::eLocal;
}


////////////////////////////////////////
// Helpers for calling intrinsics
////////////////////////////////////////

void IQLToLLVMCallMemcpy(CodeGenerationContext * ctxt,
			 LLVMValueRef sourcePtr,
			 const FieldAddress& sourceOffset,
			 LLVMValueRef targetPtr, 
			 const FieldAddress& targetOffset,
			 int64_t sz)
{
  LLVMValueRef args[5];
  args[0] = llvm::wrap(targetOffset.getPointer("memcpy_tgt", ctxt, llvm::unwrap(targetPtr)));
  args[1] = llvm::wrap(sourceOffset.getPointer("memcpy_src", ctxt, llvm::unwrap(sourcePtr)));
  args[2] = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), sz, 1);
  // TODO: Make use of alignment info to speed this up.  This assumption of 1 is pessimistics.
  args[3] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 1, 1);
  args[4] = LLVMConstInt(LLVMInt1TypeInContext(ctxt->LLVMContext), 0, 1);
  LLVMBuildCall(ctxt->LLVMBuilder, ctxt->LLVMMemcpyIntrinsic, &args[0], 5, "");
}

void IQLToLLVMCallMemcpy(CodeGenerationContext * ctxt,
			 const std::string& sourceArg,
			 const FieldAddress& sourceOffset,
			 const std::string& targetArg, 
			 const FieldAddress& targetOffset,
			 int64_t sz)
{
    LLVMValueRef sourcePtr = LLVMBuildLoad(ctxt->LLVMBuilder,
					   ctxt->lookupBasePointer(sourceArg.c_str())->getValue(),
					   "srccpy");	
    LLVMValueRef targetPtr = LLVMBuildLoad(ctxt->LLVMBuilder,
					   ctxt->lookupBasePointer(targetArg.c_str())->getValue(),
					   "tgtcpy");
    IQLToLLVMCallMemcpy(ctxt, sourcePtr, sourceOffset,
			targetPtr, targetOffset, sz);
}

void IQLToLLVMCallMemset(CodeGenerationContext * ctxt,
			 LLVMValueRef targetPtr,
			 const FieldAddress& targetOffset,
			 int8_t value,
			 int64_t sz)
{
  LLVMValueRef args[5];
  // GEP to get pointers at offsets and then call the intrinsic.
  args[0] = llvm::wrap(targetOffset.getPointer("memset_tgt", ctxt, llvm::unwrap(targetPtr)));
  args[1] = LLVMConstInt(LLVMInt8TypeInContext(ctxt->LLVMContext), value, 1);
  args[2] = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), sz, 1);
  // TODO: Speed things up by making use of alignment info we have.
  args[3] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 1, 1);
  args[4] = LLVMConstInt(LLVMInt1TypeInContext(ctxt->LLVMContext), 0, 1);
  LLVMBuildCall(ctxt->LLVMBuilder, ctxt->LLVMMemsetIntrinsic, &args[0], 5, "");
}

LLVMValueRef IQLToLLVMCallMemcmp(CodeGenerationContext * ctxt,
				 LLVMValueRef sourcePtr,
				 const FieldAddress& sourceOffset,
				 LLVMValueRef targetPtr, 
				 const FieldAddress& targetOffset,
				 int64_t sz)
{
  // Implement bit casting to the right argument types.
  if (LLVMPointerTypeKind != LLVMGetTypeKind(LLVMTypeOf(sourcePtr)))
    throw std::runtime_error("sourcePtr argument to memcmp not pointer type");
  if (LLVMPointerTypeKind != LLVMGetTypeKind(LLVMTypeOf(targetPtr)))
    throw std::runtime_error("targetPtr argument to memcmp not pointer type");
  // This is what we expect.
  LLVMTypeRef int8Ptr = LLVMPointerType(LLVMInt8TypeInContext(ctxt->LLVMContext), 0);
  // If not already pointers to int8, bitcast to that
  if (LLVMTypeOf(sourcePtr) != int8Ptr)
    sourcePtr = LLVMBuildBitCast(ctxt->LLVMBuilder, sourcePtr, int8Ptr, "memcmp_lhs_cvt");
  if (LLVMTypeOf(targetPtr) != int8Ptr)
    targetPtr = LLVMBuildBitCast(ctxt->LLVMBuilder, targetPtr, int8Ptr, "memcmp_rhs_cvt");

  LLVMValueRef args[3];
  // GEP to get pointers at offsets and then call the intrinsic.
  args[0] = llvm::wrap(targetOffset.getPointer("memcmp_lhs", ctxt, llvm::unwrap(targetPtr)));
  args[1] = llvm::wrap(sourceOffset.getPointer("memcmp_rhs", ctxt, llvm::unwrap(sourcePtr)));
  // args[2] = LLVMConstInt(sizeof(std::size_t) == 4 ? 
  // 			 LLVMInt32TypeInContext(ctxt->LLVMContext): 
  // 			 LLVMInt64TypeInContext(ctxt->LLVMContext), sz, 1);
  args[2] = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), sz, 1);
  
  return LLVMBuildCall(ctxt->LLVMBuilder, ctxt->LLVMMemcmpIntrinsic, &args[0], 3, "memcmp");
}

LLVMTypeRef IQLToLLVMBuildType(IQLCodeGenerationContextRef ctxtRef, void * attrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * ft = (const FieldType *) attrs;
  return llvm::wrap(ft->LLVMGetType(ctxt));
}

void IQLToLLVMBuildDeclareLocal(IQLCodeGenerationContextRef ctxtRef, const char * nm, void * attrs) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * ft = (const FieldType * ) attrs;
  llvm::Type * ty = ft->LLVMGetType(ctxt);
  // TODO: Check for duplicate declaration
  LLVMValueRef allocAVal = LLVMCreateEntryBlockAlloca(ctxt, 
						      llvm::wrap(ty), 
						      nm);
  // NULL handling
  LLVMValueRef nullVal = NULL;
  if (ft->isNullable()) {
    llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
    nullVal = LLVMCreateEntryBlockAlloca(ctxt, 
					 LLVMInt1TypeInContext(ctxt->LLVMContext), 
					 (boost::format("%1%NullBit") %
					  nm).str().c_str());
    b->CreateStore(b->getFalse(), llvm::unwrap(nullVal));
  } 

  ctxt->defineVariable(nm, llvm::unwrap(allocAVal), llvm::unwrap(nullVal),
		       IQLToLLVMValue::eLocal);
}

void IQLToLLVMBuildLocalVariable(IQLCodeGenerationContextRef ctxtRef, const char * nm, IQLToLLVMValueRef init, void * attrs) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  const FieldType * ft = (const FieldType * ) attrs;

  // TODO: Temporary hack dealing with a special case in which the
  // initializing expression has already allocated a slot for the
  // lvalue.
  if (ft->GetEnum() == FieldType::FIXED_ARRAY && 
      !ft->isNullable() &&
      unwrap(init)->getValueType() == IQLToLLVMValue::eLocal) {
    ctxt->defineVariable(nm, llvm::unwrap(unwrap(init)->getValue()), NULL,
			 IQLToLLVMValue::eLocal);    
  } else {
    // Allocate local
    IQLToLLVMBuildDeclareLocal(ctxtRef, nm, attrs);
    // Create a temporary LValue object
    const IQLToLLVMLValue * localLVal = unwrap(IQLToLLVMBuildLValue(ctxtRef, nm));
    // Set the value
    IQLToLLVMBuildSetNullableValue(ctxt, localLVal, init);
  }
}

LLVMValueRef LLVMCreateEntryBlockAlloca(CodeGenerationContext * ctxt, LLVMTypeRef ty, const char * name) {
  // Create a new builder positioned at the beginning of the entry block of the function
  llvm::Function* TheFunction = llvm::dyn_cast<llvm::Function>(llvm::unwrap(ctxt->LLVMFunction));
  llvm::IRBuilder<> TmpB(&TheFunction->getEntryBlock(),
                 TheFunction->getEntryBlock().begin());
  return llvm::wrap(TmpB.CreateAlloca(llvm::unwrap(ty), 0, name));
}

////////////////////////////////////////////////////
// Here begins the actual external builder interface
////////////////////////////////////////////////////

IQLToLLVMValueRef IQLToLLVMBuildHash(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueVectorRef lhs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Call out to external function.  The trick is that we always pass a pointer to data.
  // Handle the cases here.  For value types, we must alloca storage so
  // we have a pointer to pass.
  LLVMValueRef callArgs[3];
  LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, "SuperFastHash");

  // Save the previous hash so we can feed it into the next.  
  LLVMValueRef previousHash=NULL;
  std::vector<const IQLToLLVMValue *> & args(*unwrap(lhs));

  for(std::size_t i=0; i<args.size(); i++) {
    LLVMValueRef argVal = args[i]->getValue();
    LLVMTypeRef argTy = LLVMPointerType(LLVMInt8TypeInContext(ctxt->LLVMContext), 0);

    // TODO: I should be able to reliably get the length of fixed size fields from the LLVM type.
    if (LLVMTypeOf(argVal) == LLVMInt32TypeInContext(ctxt->LLVMContext)) {
      LLVMValueRef tmpVal = LLVMCreateEntryBlockAlloca(ctxt, LLVMTypeOf(argVal), "hashtmpint32");
      LLVMBuildStore(ctxt->LLVMBuilder, argVal, tmpVal);
      callArgs[0] = LLVMBuildBitCast(ctxt->LLVMBuilder, tmpVal, argTy, "hashint32cast");
      callArgs[1] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 4, 1);
      callArgs[2] = i==0 ? LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 4, 0) : previousHash;
    } else if (LLVMTypeOf(argVal) == LLVMInt64TypeInContext(ctxt->LLVMContext)) {
      LLVMValueRef tmpVal = LLVMCreateEntryBlockAlloca(ctxt, LLVMTypeOf(argVal), "hashtmpint64");
      LLVMBuildStore(ctxt->LLVMBuilder, argVal, tmpVal);
      callArgs[0] = LLVMBuildBitCast(ctxt->LLVMBuilder, tmpVal, argTy, "hashint64cast");
      callArgs[1] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 8, 1);
      callArgs[2] = i==0 ? LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 8, 0) : previousHash;    
    } else if (LLVMTypeOf(argVal) == LLVMDoubleTypeInContext(ctxt->LLVMContext)) {
      LLVMValueRef tmpVal = LLVMCreateEntryBlockAlloca(ctxt, LLVMTypeOf(argVal), "hashtmpdouble");
      LLVMBuildStore(ctxt->LLVMBuilder, argVal, tmpVal);
      callArgs[0] = LLVMBuildBitCast(ctxt->LLVMBuilder, tmpVal, argTy, "hashdoublecast");
      callArgs[1] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 8, 1);
      callArgs[2] = i==0 ? LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 8, 0) : previousHash;    
    } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(argVal)) &&
	       LLVMGetElementType(LLVMTypeOf(argVal))== ctxt->LLVMVarcharType) {
      callArgs[0] = IQLToLLVMVarcharGetPtr(ctxt, argVal);
      callArgs[1] = IQLToLLVMVarcharGetSize(ctxt, argVal);
      callArgs[2] = i==0 ? callArgs[1] : previousHash;
    } else if (IQLToLLVMTypePredicate::isChar(argVal)) {
      // Hash on array length - 1 because of trailing null char and we want
      // consistency with varchar hashing.
      unsigned arrayLen = IQLToLLVMTypeInspector::getCharArrayLength(argVal)-1;
      callArgs[0] = LLVMBuildBitCast(ctxt->LLVMBuilder, argVal, argTy, "hashcharcast");
      callArgs[1] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), arrayLen, 1);
      callArgs[2] = i==0 ? LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), arrayLen, 0) : previousHash;    
    } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(argVal)) &&
	       LLVMGetElementType(LLVMTypeOf(argVal))== ctxt->LLVMDecimal128Type) {
      callArgs[0] = LLVMBuildBitCast(ctxt->LLVMBuilder, argVal, argTy, "hashdeccast");
      callArgs[1] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 16, 1);
      callArgs[2] = i==0 ? LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 16, 0) : previousHash;
    } else {
      throw std::runtime_error("IQLToLLVMBuildHash unexpected type");
    }
    previousHash = LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 3, "hash");
  }

  return IQLToLLVMValue::get(ctxt, previousHash, IQLToLLVMValue::eLocal);
}

// TODO: Handle all types.
// TODO: Handle case of char[N] for N < 4 and BOOLEAN which don't
// fully utilize the prefix.
IQLToLLVMValueRef IQLToLLVMBuildSortPrefix(CodeGenerationContext * ctxt, 
					   const IQLToLLVMValue * arg)
{
  // Unwrap to C++
  // llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  llvm::Value * argVal = llvm::unwrap(arg->getValue());
  llvm::Value * retVal = NULL;
  // 31 bits for the prefix
  static const int32_t prefixBits = 31;
  if (argVal->getType() == b->getInt32Ty()) {
    // For signed integer quantity, reverse top bit then shift down to get 31 bits
    // of precision.
    llvm::Value * offsetVal = 
      b->CreateXor(argVal, b->getInt32(0x80000000), "int32PrefixOffset");
    retVal = b->CreateLShr(offsetVal, b->getInt32(32-prefixBits), "int32Prefix");
  } else if (argVal->getType() == b->getInt64Ty()) {
    // For signed integer quantity, reverse top bit then shift down to get 31 bits
    // of precision.
    llvm::Value * offsetVal = 
      b->CreateXor(argVal, b->getInt64(0x8000000000000000LL), "int64PrefixOffset");
    retVal = b->CreateLShr(offsetVal, b->getInt64(64-prefixBits), "int64PrefixAs64");
    retVal = b->CreateTrunc(retVal, b->getInt32Ty(), "int64Prefix");
  // } else if (argVal->getType() == b->getDoubleTy()) {
  // } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(argVal)) &&
  // 	       LLVMGetElementType(LLVMTypeOf(argVal))== ctxt->LLVMVarcharType) {
  } else if (IQLToLLVMTypePredicate::isChar(llvm::wrap(argVal))) {
    // Take top 32 bits of array, byte swap into a uint32_t and then divide by 2
    // TODO: If there are less than 32 bits in the array then there is room
    // for parts of other field(s) in the prefix...
    retVal = b->getInt32(0);
    int numBytes = IQLToLLVMTypeInspector::getCharArrayLength(llvm::wrap(argVal)) >= 4 ? 
      4 : (int) IQLToLLVMTypeInspector::getCharArrayLength(llvm::wrap(argVal));
    for(int i = 0; i<numBytes; ++i) {
      llvm::Value * tmp = b->CreateLoad(b->CreateConstInBoundsGEP2_64(argVal, 0, i));
      tmp = b->CreateShl(b->CreateZExt(tmp, b->getInt32Ty()),
			 b->getInt32(8*(3-i)));
      retVal = b->CreateOr(retVal, tmp, "a");
    }
    retVal = b->CreateLShr(retVal, b->getInt32(32-prefixBits));
  // } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(argVal)) &&
  // 	       LLVMGetElementType(LLVMTypeOf(argVal))== ctxt->LLVMDecimal128Type) {
  } else {
    // TODO: This is suboptimal but allows sorting to work.
    retVal = b->getInt32(1);
  }

  return IQLToLLVMValue::get(ctxt, llvm::wrap(retVal), IQLToLLVMValue::eLocal);
}

IQLToLLVMValueRef IQLToLLVMBuildSortPrefix(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueVectorRef lhs, void * attr)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * retTy = (const FieldType *) attr;
  // Assume for the moment that one field generates
  // the entire prefix.
  std::vector<const IQLToLLVMValue *> & args(*unwrap(lhs));
  const IQLToLLVMValue * arg = args[0];
  if (arg->getNull()) {
    IQLToLLVMValueRef zero = 
      IQLToLLVMValue::get(ctxt, 
			  llvm::wrap(retTy->getZero(ctxt)),
			  IQLToLLVMValue::eLocal);
    ::IQLToLLVMCaseBlockBegin(ctxtRef, attr);        
    ::IQLToLLVMCaseBlockIf(ctxtRef, ::IQLToLLVMBuildIsNull(ctxt, wrap(arg)));
    ::IQLToLLVMCaseBlockThen(ctxtRef, zero, attr);
    IQLToLLVMValueRef p = ::IQLToLLVMBuildSortPrefix(ctxt, arg);
    ::IQLToLLVMCaseBlockThen(ctxtRef, p, attr);
    return ::IQLToLLVMCaseBlockFinish(ctxtRef);
  } else {
    return ::IQLToLLVMBuildSortPrefix(ctxt, arg);
  }
}

IQLToLLVMValue::ValueType InternalBuildCall(CodeGenerationContext * ctxt, 
					    const char * f,
					    const std::vector<const IQLToLLVMValue *> & args,
					    LLVMValueRef retTmp,
					    const FieldType * retType)
{
  return ctxt->buildCall(f, args, llvm::unwrap(retTmp), retType);
}

IQLToLLVMValueRef InternalBuildCall(CodeGenerationContext * ctxt, 
				    const char * f,
				    const std::vector<const IQLToLLVMValue *> & args,
				    const FieldType * retType)
{
    const llvm::Type * retTy = retType->LLVMGetType(ctxt);
    LLVMValueRef retTmp = LLVMCreateEntryBlockAlloca(ctxt, 
						     llvm::wrap(retTy), 
						     "callReturnTemp");
    IQLToLLVMValue::ValueType vt = InternalBuildCall(ctxt, f, args, retTmp, retType);
    // Get rid of unecessary temporary if applicable.
    retTmp = IQLToLLVMTrimAlloca(retTmp);
    if (IQLToLLVMValue::isPointerToValueType(LLVMTypeOf(retTmp))) {
      // Unlikely but possible to get here.  Pointers to 
      // value type are almost surely trimmed above.
      retTmp = LLVMBuildLoad(ctxt->LLVMBuilder, retTmp, "");
    }
    return IQLToLLVMValue::get(ctxt, retTmp, vt);
}

IQLToLLVMValueRef IQLToLLVMBuildLeastGreatest(CodeGenerationContext * ctxt, 
					      const std::vector<const IQLToLLVMValue *> & args,
					      const FieldType * retTy,
					      bool isLeast)
{
  // Recursively generate Greatest(x1,x2,..., xN) = Greatest(Greatest(x1,x2),x3, ..., xN)
  // Greatest(x1,x2) = x1 < x2 ? x2 : x1
  // and similarly for least.
  if (args.size() == 2) {
    IQLCodeGenerationContextRef ctxtRef = wrap(ctxt);
    void * attr = const_cast<FieldType *>(retTy);
    ::IQLToLLVMCaseBlockBegin(ctxtRef, attr);        
    ::IQLToLLVMCaseBlockIf(ctxtRef, 
			   IQLToLLVMBuildCompare(ctxt,
						 wrap(args[0]), 
						 wrap(args[1]), 
						 isLeast ? IQLToLLVMOpLT : IQLToLLVMOpGT));
    ::IQLToLLVMCaseBlockThen(ctxtRef, wrap(args[0]), attr);
    ::IQLToLLVMCaseBlockThen(ctxtRef, wrap(args[1]), attr);
    return ::IQLToLLVMCaseBlockFinish(ctxtRef);
  } else if (args.size() == 1) {
    // Convert type if necessary.
    return IQLToLLVMBinaryConversion::convertTo(ctxt, wrap(args[0]), retTy);
  } else {
    BOOST_ASSERT(args.size() > 2);
    std::vector<const IQLToLLVMValue *> a(args.begin(), args.begin()+2);
    std::vector<const IQLToLLVMValue *> b(1, unwrap(IQLToLLVMBuildLeastGreatest(ctxt, a, retTy, isLeast)));
    b.insert(b.end(), args.begin()+2, args.end());
    return IQLToLLVMBuildLeastGreatest(ctxt, b, retTy, isLeast);
  }
}					     

IQLToLLVMValueRef IQLToLLVMBuildIsNullFunction(CodeGenerationContext * ctxt, 
					       const std::vector<const IQLToLLVMValue *> & args,
					       const FieldType * retTy)
{
  const IQLToLLVMValue * val = args[0];
  const IQLToLLVMValue * alt = args[1];
  IQLCodeGenerationContextRef ctxtRef = wrap(ctxt);
  void * attr = const_cast<FieldType *>(retTy);
  if (val->getNull() == NULL) {
    // If val is not nullable just return it.
    return wrap(val);
  } else {
    ::IQLToLLVMCaseBlockBegin(ctxtRef, attr);        
    ::IQLToLLVMCaseBlockIf(ctxtRef, 
			   IQLToLLVMBuildIsNull(ctxt,
						wrap(args[0])));

    ::IQLToLLVMCaseBlockThen(ctxt, wrap(args[1]), retTy, false);
    ::IQLToLLVMCaseBlockThen(ctxt, wrap(args[0]), retTy, true);
    return ::IQLToLLVMCaseBlockFinish(ctxtRef); 
  }
}

IQLToLLVMValueRef IQLToLLVMBuildCall(IQLCodeGenerationContextRef ctxtRef, 
				     const char * f,
				     IQLToLLVMValueVectorRef lhs,
				     void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Call out to external function.  
  std::vector<const IQLToLLVMValue *> & args(*unwrap(lhs));
  const FieldType * retType = (const FieldType *) retAttrs;
  // Handle intrinsics
  if (boost::algorithm::iequals(f, "least")) {
    return IQLToLLVMBuildLeastGreatest(ctxt, args, retType, true);
  } else if (boost::algorithm::iequals(f, "greatest")) {
    return IQLToLLVMBuildLeastGreatest(ctxt, args, retType, false);
  } else if (boost::algorithm::iequals(f, "isnull")) {
    return IQLToLLVMBuildIsNullFunction(ctxt, args, retType);
  } else {
    return InternalBuildCall(ctxt, f, args, retType);
  }
}

IQLToLLVMValueRef IQLToLLVMBuildCompareResult(CodeGenerationContext * ctxt, LLVMValueRef boolVal)
{
  LLVMValueRef int32RetVal = LLVMBuildZExt(ctxt->LLVMBuilder, 
					   boolVal,
					   LLVMInt32TypeInContext(ctxt->LLVMContext),
					   "cmpresultcast");
  return IQLToLLVMValue::get(ctxt, int32RetVal, IQLToLLVMValue::eLocal);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildCompareResult(CodeGenerationContext * ctxt, 
						      LLVMValueRef boolVal,
						      LLVMValueRef ret)
{
  LLVMValueRef int32RetVal = LLVMBuildZExt(ctxt->LLVMBuilder, 
					   boolVal,
					   LLVMInt32TypeInContext(ctxt->LLVMContext),
					   "cmpresultcast");
  LLVMBuildStore(ctxt->LLVMBuilder, int32RetVal, ret);
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValue::ValueType IQLToLLVMBuildVarcharCompare(CodeGenerationContext * ctxt, 
						       LLVMValueRef e1, 
						       LLVMValueRef e2,
						       LLVMValueRef ret,
						       IQLToLLVMPredicate opCode)
{
  // TODO: Inline.  For now call an external function.
  const char * booleanFuncs [] = { "InternalVarcharEquals",
				   "InternalVarcharNE",
				   "InternalVarcharGT",
				   "InternalVarcharGE",
				   "InternalVarcharLT",
				   "InternalVarcharLE",
                                   "InternalVarcharRLike"
  };
  const char * tmpNames [] = { "varchareqtmp",
			       "varcharnetmp",
			       "varchargttmp",
			       "varchargetmp",
			       "varcharlttmp",
			       "varcharletmp",
                               "varcharrliketmp"
  };
  LLVMValueRef callArgs[3];
  LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, booleanFuncs[opCode]);
  callArgs[0] = e1;
  callArgs[1] = e2;
  callArgs[2] = LLVMBuildLoad(ctxt->LLVMBuilder, 
  			      ctxt->getContextArgumentRef(), 
  			      "ctxttmp");
  LLVMValueRef cmp = LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 3, tmpNames[opCode]);
  LLVMBuildStore(ctxt->LLVMBuilder, cmp, ret);
  return IQLToLLVMValue::eLocal;
}

// TODO: Get rid of this function.  Everything should call through the
// nullable wrapper.
IQLToLLVMValueRef IQLToLLVMBuildVarcharCompare(CodeGenerationContext * ctxt, 
						       LLVMValueRef e1, 
						       LLVMValueRef e2,
						       IQLToLLVMPredicate opCode)
{
  LLVMValueRef ret = LLVMCreateEntryBlockAlloca(ctxt, 
						LLVMInt32TypeInContext(ctxt->LLVMContext), 
						"varcharCmpRetPtr");
  IQLToLLVMValue::ValueType vt = 
    IQLToLLVMBuildVarcharCompare(ctxt, e1, e2, ret, opCode);
  return IQLToLLVMValue::get(ctxt, 
			     LLVMBuildLoad(ctxt->LLVMBuilder, ret, "varcharCmp"), 
			     vt);
}

IQLToLLVMValueRef IQLToLLVMBuildCompare(CodeGenerationContext * ctxt, IQLToLLVMValueRef lhs, IQLToLLVMValueRef rhs, IQLToLLVMPredicate op)
{
  LLVMIntPredicate intOp = LLVMIntEQ;
  LLVMRealPredicate realOp = LLVMRealPredicateFalse;
  switch(op) {
  case IQLToLLVMOpEQ:
    intOp = LLVMIntEQ;
    realOp = LLVMRealOEQ;
    break;
  case IQLToLLVMOpNE:
    intOp = LLVMIntNE;
    realOp = LLVMRealONE;
    break;
  case IQLToLLVMOpGT:
    intOp = LLVMIntSGT;
    realOp = LLVMRealOGT;
    break;
  case IQLToLLVMOpGE:
    intOp = LLVMIntSGE;
    realOp = LLVMRealOGE;
    break;
  case IQLToLLVMOpLT:
    intOp = LLVMIntSLT;
    realOp = LLVMRealOLT;
    break;
  case IQLToLLVMOpLE:
    intOp = LLVMIntSLE;
    realOp = LLVMRealOLE;
    break;
  case IQLToLLVMOpRLike:
    // We should only get here with string types
    break;
  }
  IQLToLLVMBinaryConversion cvt(ctxt, lhs, rhs);
  lhs = cvt.getLHS();
  rhs = cvt.getRHS();
  // Call out to external function.  The trick is that we always pass a pointer to data.
  // Handle the cases here.  For value types, we must alloca storage so
  // we have a pointer to pass.
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  LLVMValueRef e2 = unwrap(rhs)->getValue();

  // TODO: I should be able to reliably get the length of fixed size fields from the LLVM type.
  if (LLVMTypeOf(e1) == LLVMInt32TypeInContext(ctxt->LLVMContext)) {
    return IQLToLLVMBuildCompareResult(ctxt, LLVMBuildICmp(ctxt->LLVMBuilder, intOp, e1, e2, "int32cmp"));
  } else if (LLVMTypeOf(e1) == LLVMInt64TypeInContext(ctxt->LLVMContext)) {
    return IQLToLLVMBuildCompareResult(ctxt, LLVMBuildICmp(ctxt->LLVMBuilder, intOp, e1, e2, "int64cmp"));
  } else if (LLVMTypeOf(e1) == LLVMDoubleTypeInContext(ctxt->LLVMContext)) {
    // TODO: Should this be OEQ or UEQ?
    // TODO: Should be comparison within EPS?
    return IQLToLLVMBuildCompareResult(ctxt, LLVMBuildFCmp(ctxt->LLVMBuilder, realOp, e1, e2, "doublecmp"));
  } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(e1)) &&
	     LLVMGetElementType(LLVMTypeOf(e1))== ctxt->LLVMVarcharType) {
    return IQLToLLVMBuildVarcharCompare(ctxt, e1, e2, op);
  } else if (IQLToLLVMTypePredicate::isChar(e1)) {

    // TODO: I don't really know what the semantics of char(N) equality are (e.g. when the sizes aren't equal).
    // FWIW, semantics of char(N) equality is that one compares assuming
    // space padding to MAX(M,N)
    // For now I am draconian and say they are never equal????  
    if (IQLToLLVMTypeInspector::getCharArrayLength(e1) != IQLToLLVMTypeInspector::getCharArrayLength(e2))
      return IQLToLLVMValue::get(ctxt, 
				 LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 0, 1), 
				 IQLToLLVMValue::eLocal);

    LLVMValueRef ret = IQLToLLVMCallMemcmp(ctxt, e1, FieldAddress(), e2, FieldAddress(), IQLToLLVMTypeInspector::getCharArrayLength(e1));
    // Compare result to zero and return
    return IQLToLLVMBuildCompareResult(ctxt, LLVMBuildICmp(ctxt->LLVMBuilder, 
							   intOp, 
							   LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 0, 1), 
							   ret, 
							   "memcmpz"));
  } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(e1)) &&
	     LLVMGetElementType(LLVMTypeOf(e1))== ctxt->LLVMDecimal128Type) {
    // Call decNumberCompare
    LLVMValueRef callArgs[4];
    LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, "InternalDecimalCmp");
    callArgs[0] = e1;
    callArgs[1] = e2;
    callArgs[2] = LLVMCreateEntryBlockAlloca(ctxt, 
					     LLVMInt32TypeInContext(ctxt->LLVMContext), 
					     "decimalCmpRetPtr");
    callArgs[3] = LLVMBuildLoad(ctxt->LLVMBuilder, 
				ctxt->getContextArgumentRef(),
				"ctxttmp");
    LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 4, "");
    
    // Compare result to zero and return
    return IQLToLLVMBuildCompareResult(ctxt, LLVMBuildICmp(ctxt->LLVMBuilder, 
							   intOp, 
							   LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 0, 1), 
							   LLVMBuildLoad(ctxt->LLVMBuilder,
									 callArgs[2],
									 "decimalCmpRet"),
							   "decimalCmp"));
  } else {
    throw std::runtime_error("IQLToLLVMBuildCompare unexpected type");
  }
}

IQLToLLVMValue::ValueType IQLToLLVMBuildCompare(CodeGenerationContext * ctxt, 
						IQLToLLVMValueRef lhs, 
						const FieldType * lhsType, 
						IQLToLLVMValueRef rhs, 
						const FieldType * rhsType,
						LLVMValueRef ret,
						const FieldType * retType,
						IQLToLLVMPredicate op)
{
  LLVMIntPredicate intOp = LLVMIntEQ;
  LLVMRealPredicate realOp = LLVMRealPredicateFalse;
  switch(op) {
  case IQLToLLVMOpEQ:
    intOp = LLVMIntEQ;
    realOp = LLVMRealOEQ;
    break;
  case IQLToLLVMOpNE:
    intOp = LLVMIntNE;
    realOp = LLVMRealONE;
    break;
  case IQLToLLVMOpGT:
    intOp = LLVMIntSGT;
    realOp = LLVMRealOGT;
    break;
  case IQLToLLVMOpGE:
    intOp = LLVMIntSGE;
    realOp = LLVMRealOGE;
    break;
  case IQLToLLVMOpLT:
    intOp = LLVMIntSLT;
    realOp = LLVMRealOLT;
    break;
  case IQLToLLVMOpLE:
    intOp = LLVMIntSLE;
    realOp = LLVMRealOLE;
    break;
  case IQLToLLVMOpRLike:
    // This will only happen with string types
    break;
  }
  IQLToLLVMBinaryConversion cvt(ctxt, lhs, rhs);
  lhs = cvt.getLHS();
  rhs = cvt.getRHS();
  // Call out to external function.  The trick is that we always pass a pointer to data.
  // Handle the cases here.  For value types, we must alloca storage so
  // we have a pointer to pass.
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  LLVMValueRef e2 = unwrap(rhs)->getValue();

  // TODO: I should be able to reliably get the length of fixed size fields from the LLVM type.
  LLVMValueRef r = NULL;
  if (LLVMTypeOf(e1) == LLVMInt32TypeInContext(ctxt->LLVMContext)) {
    return IQLToLLVMBuildCompareResult(ctxt, LLVMBuildICmp(ctxt->LLVMBuilder, intOp, e1, e2, "int32cmp"), ret);
  } else if (LLVMTypeOf(e1) == LLVMInt64TypeInContext(ctxt->LLVMContext)) {
    return IQLToLLVMBuildCompareResult(ctxt, LLVMBuildICmp(ctxt->LLVMBuilder, intOp, e1, e2, "int64cmp"), ret);
  } else if (LLVMTypeOf(e1) == LLVMDoubleTypeInContext(ctxt->LLVMContext)) {
    // TODO: Should this be OEQ or UEQ?
    // TODO: Should be comparison within EPS?
    return IQLToLLVMBuildCompareResult(ctxt, LLVMBuildFCmp(ctxt->LLVMBuilder, realOp, e1, e2, "doublecmp"), ret);
  } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(e1)) &&
	     LLVMGetElementType(LLVMTypeOf(e1))== ctxt->LLVMVarcharType) {
    return IQLToLLVMBuildVarcharCompare(ctxt, e1, e2, ret, op);
  } else if (IQLToLLVMTypePredicate::isChar(e1)) {

    // TODO: I don't really know what the semantics of char(N) equality are (e.g. when the sizes aren't equal).
    // FWIW, semantics of char(N) equality is that one compares assuming
    // space padding to MAX(M,N)
    // For now I am draconian and say they are never equal????  
    if (IQLToLLVMTypeInspector::getCharArrayLength(e1) != IQLToLLVMTypeInspector::getCharArrayLength(e2)) {
      r = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 0, 1);
      
      return IQLToLLVMValue::eLocal;
    } else {
      LLVMValueRef m = IQLToLLVMCallMemcmp(ctxt, e1, FieldAddress(), e2, FieldAddress(), IQLToLLVMTypeInspector::getCharArrayLength(e1));
      // Compare result to zero and return
      return IQLToLLVMBuildCompareResult(ctxt,
					 LLVMBuildICmp(ctxt->LLVMBuilder, 
						       intOp, 
						       LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 0, 1),
						       m, 
						       "memcmpz"),
					 ret);
    }
  } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(e1)) &&
	     LLVMGetElementType(LLVMTypeOf(e1))== ctxt->LLVMDecimal128Type) {
    // Call decNumberCompare
    LLVMValueRef callArgs[4];
    LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, "InternalDecimalCmp");
    callArgs[0] = e1;
    callArgs[1] = e2;
    callArgs[2] = LLVMCreateEntryBlockAlloca(ctxt, 
					     LLVMInt32TypeInContext(ctxt->LLVMContext), 
					     "decimalCmpRetPtr");
    callArgs[3] = LLVMBuildLoad(ctxt->LLVMBuilder, 
				ctxt->getContextArgumentRef(),
				"ctxttmp");
    LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 4, "");
    
    // Compare result to zero and return
    return IQLToLLVMBuildCompareResult(ctxt, LLVMBuildICmp(ctxt->LLVMBuilder, 
							   intOp, 
							   LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 0, 1), 
							   LLVMBuildLoad(ctxt->LLVMBuilder,
									 callArgs[2],
									 "decimalCmpRet"),
							   "decimalCmp"),
				       ret);
  } else {
    throw std::runtime_error("IQLToLLVMBuildCompare unexpected type");
  }
  // Copy result into provided storage.
  LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
  return IQLToLLVMValue::eLocal;
}

template <IQLToLLVMPredicate _Op>
IQLToLLVMValue::ValueType IQLToLLVMBuildCompare(CodeGenerationContext * ctxt, 
						IQLToLLVMValueRef lhs, 
						const FieldType * lhsType, 
						IQLToLLVMValueRef rhs, 
						const FieldType * rhsType,
						LLVMValueRef ret,
						const FieldType * retType)
{
  return IQLToLLVMBuildCompare(ctxt, lhs, lhsType, rhs, rhsType, ret, retType, _Op);
}

IQLToLLVMValueRef IQLToLLVMBuildCompare(CodeGenerationContext * ctxt, 
					IQLToLLVMValueRef lhs, 
					const FieldType * lhsType, 
					IQLToLLVMValueRef rhs, 
					const FieldType * rhsType,
					const FieldType * resultType,
					IQLToLLVMPredicate op)
{
  IQLToLLVMBinaryOperator opFun=NULL;
  switch(op) {
  case IQLToLLVMOpEQ:
    opFun = &IQLToLLVMBuildCompare<IQLToLLVMOpEQ>;
    break;
  case IQLToLLVMOpNE:
    opFun = &IQLToLLVMBuildCompare<IQLToLLVMOpNE>;
    break;
  case IQLToLLVMOpGT:
    opFun = &IQLToLLVMBuildCompare<IQLToLLVMOpGT>;
    break;
  case IQLToLLVMOpGE:
    opFun = &IQLToLLVMBuildCompare<IQLToLLVMOpGE>;
    break;
  case IQLToLLVMOpLT:
    opFun = &IQLToLLVMBuildCompare<IQLToLLVMOpLT>;
    break;
  case IQLToLLVMOpLE:
    opFun = &IQLToLLVMBuildCompare<IQLToLLVMOpLE>;
    break;
  case IQLToLLVMOpRLike:
    opFun = &IQLToLLVMBuildCompare<IQLToLLVMOpRLike>;
    break;
  default:
    throw std::runtime_error("Unexpected predicate type");
  }
  return IQLToLLVMBuildNullableBinaryOp(ctxt, lhs, lhsType, rhs, rhsType, resultType,
					opFun);
}

IQLToLLVMValueRef IQLToLLVMBuildCompare(IQLCodeGenerationContextRef ctxtRef, 
					IQLToLLVMValueRef lhs, 
					void * lhsAttributes, 
					IQLToLLVMValueRef rhs, 
					void * rhsAttributes,
					void * resultAttributes,
					IQLToLLVMPredicate op)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildCompare(ctxt, lhs, lhsType, rhs, rhsType,
			       resultType, op);
}

IQLToLLVMValueRef IQLToLLVMBuildEquals(IQLCodeGenerationContextRef ctxtRef, 
				       IQLToLLVMValueRef lhs, 
				       void * lhsAttributes, 
				       IQLToLLVMValueRef rhs, 
				       void * rhsAttributes,
				       void * resultAttributes)
{
  return IQLToLLVMBuildCompare(ctxtRef, lhs, lhsAttributes, rhs, rhsAttributes,
			       resultAttributes, IQLToLLVMOpEQ);
}

IQLToLLVMValueRef IQLToLLVMBuildIsNull(CodeGenerationContext * ctxt, 
				       IQLToLLVMValueRef val)
{
  llvm::Value * nv = unwrap(val)->getNull();
  if (nv) {
    // Extend to 32 bit integer.
    return IQLToLLVMBuildCompareResult(ctxt, llvm::wrap(nv));
  } else {
    return IQLToLLVMBuildTrue(wrap(ctxt));
  }
}
IQLToLLVMValueRef IQLToLLVMBuildIsNull(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return IQLToLLVMBuildIsNull(ctxt, val);
}

/**
 * Implement e1 && e2 as CASE WHEN e1 THEN e2 ELSE false END for now.
 */
void IQLToLLVMBeginAnd(IQLCodeGenerationContextRef ctxtRef, void * retAttrs)
{
  IQLToLLVMCaseBlockBegin(ctxtRef, retAttrs);
}

void IQLToLLVMAddAnd(IQLCodeGenerationContextRef ctxtRef, 
		     IQLToLLVMValueRef lhs,
		     void * argAttrs,
		     void * retAttrs)
{
  IQLToLLVMCaseBlockIf(ctxtRef, lhs);
}

IQLToLLVMValueRef IQLToLLVMBuildAnd(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef rhs,
				    void * rhsAttrs,
				    void * retAttrs)
{
  IQLToLLVMCaseBlockThen(ctxtRef, rhs, retAttrs);
  IQLToLLVMCaseBlockThen(ctxtRef, IQLToLLVMBuildFalse(ctxtRef), retAttrs); 
  return IQLToLLVMCaseBlockFinish(ctxtRef);
}

/**
 * Implement e1 || e2 as CASE WHEN e1 THEN true ELSE e2 END for now.
 */
void IQLToLLVMBeginOr(IQLCodeGenerationContextRef ctxtRef, void * retAttrs)
{
  IQLToLLVMCaseBlockBegin(ctxtRef, retAttrs);
}

void IQLToLLVMAddOr(IQLCodeGenerationContextRef ctxtRef, 
		     IQLToLLVMValueRef lhs,
		     void * argAttrs,
		     void * retAttrs)
{
  IQLToLLVMCaseBlockIf(ctxtRef, lhs);
  IQLToLLVMCaseBlockThen(ctxtRef, IQLToLLVMBuildTrue(ctxtRef), retAttrs); 
}

IQLToLLVMValueRef IQLToLLVMBuildOr(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef rhs,
				    void * rhsAttrs,
				    void * retAttrs)
{
  IQLToLLVMCaseBlockThen(ctxtRef, rhs, retAttrs);
  return IQLToLLVMCaseBlockFinish(ctxtRef);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildNot(CodeGenerationContext * ctxt, IQLToLLVMValueRef lhs,
				    const FieldType * argAttrs, 
				    LLVMValueRef ret, const FieldType * retAttrs)
{
  LLVMContextRef c = ctxt->LLVMContext;
  LLVMValueRef one = LLVMConstInt(LLVMInt32TypeInContext(c), 1, 1);
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  // TODO: Currently representing booleans as int32_t
  // Implement NOT as x+1 & 1
  LLVMValueRef r = LLVMBuildAnd(ctxt->LLVMBuilder,
				LLVMBuildAdd(ctxt->LLVMBuilder,
					     e1,
					     one,
					     "not_add"),
				one,
				"not_and");
  LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValueRef IQLToLLVMBuildNot(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef lhs,
				    void * argAttrs, void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, lhs, lhsType, resultType,
				       IQLToLLVMBuildNot);
}

IQLToLLVMValueRef IQLToLLVMBuildIsNull(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef lhs,
				       void * argAttrs, void * retAttrs, int isNotNull)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  llvm::Value * nv = unwrap(lhs)->getNull();
  if (nv) {
    // Nullable value.  Must check the null... 
    llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
    if (isNotNull) {
      nv = b->CreateNot(nv);
    }
    return IQLToLLVMBuildCompareResult(ctxt, llvm::wrap(nv));
  } else {
    return isNotNull ? IQLToLLVMBuildTrue(ctxtRef) : IQLToLLVMBuildFalse(ctxtRef);
  }
}

IQLToLLVMValue::ValueType IQLToLLVMBuildCastInt32(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, const FieldType * argAttrs, LLVMValueRef ret, const FieldType * retAttrs)
{
  return ctxt->buildCastInt32(unwrap(e), argAttrs, 
			      llvm::unwrap(ret), retAttrs);
}

IQLToLLVMValueRef IQLToLLVMBuildCastInt32(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs)
{
  const FieldType * lhsType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, e, lhsType, resultType,
				       IQLToLLVMBuildCastInt32);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildCastInt64(CodeGenerationContext * ctxt, 
						  IQLToLLVMValueRef e, 
						  const FieldType * argAttrs, 
						  LLVMValueRef ret, 
						  const FieldType * retAttrs)
{
  return ctxt->buildCastInt64(unwrap(e), argAttrs, 
			      llvm::unwrap(ret), retAttrs);
}

IQLToLLVMValueRef IQLToLLVMBuildCastInt64(CodeGenerationContext * ctxt, 
					  IQLToLLVMValueRef e, 
					  void * argAttrs, void * retAttrs)
{
  const FieldType * lhsType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, e, lhsType, resultType,
				       IQLToLLVMBuildCastInt64);
}

// TODO: Support all directions of casting.
IQLToLLVMValueRef IQLToLLVMBuildCastBoolean(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs)
{
  throw std::runtime_error ("CAST(expr TO BOOLEAN) not implemented yet");
}

IQLToLLVMValue::ValueType IQLToLLVMBuildCastDecimal(CodeGenerationContext * ctxt, 
						    IQLToLLVMValueRef e, 
						    const FieldType * argAttrs, 
						    LLVMValueRef ret, 
						    const FieldType * retAttrs)
{
  return ctxt->buildCastDecimal(unwrap(e), argAttrs, 
				llvm::unwrap(ret), retAttrs);
}

IQLToLLVMValueRef IQLToLLVMBuildCastDecimal(CodeGenerationContext * ctxt, 
					    IQLToLLVMValueRef e, 
					    void * argAttrs, void * retAttrs)
{
  const FieldType * lhsType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, e, lhsType, resultType,
				       IQLToLLVMBuildCastDecimal);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildCastDouble(CodeGenerationContext * ctxt, 
						  IQLToLLVMValueRef e, 
						  const FieldType * argAttrs, 
						  LLVMValueRef ret, 
						  const FieldType * retAttrs)
{
  return ctxt->buildCastDouble(unwrap(e), argAttrs, 
			       llvm::unwrap(ret), retAttrs);
}

IQLToLLVMValueRef IQLToLLVMBuildCastDouble(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs)
{
  const FieldType * lhsType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, e, lhsType, resultType,
				       IQLToLLVMBuildCastDouble);
}

// TODO: Support all directions of casting.
IQLToLLVMValueRef IQLToLLVMBuildCast(IQLCodeGenerationContextRef ctxtRef, 
				     IQLToLLVMValueRef e, 
				     void * argAttrs, void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * ft = (const FieldType *) retAttrs;
  switch(ft->GetEnum()) {
  case FieldType::INT32:
    return IQLToLLVMBuildCastInt32(ctxt, e, argAttrs, retAttrs);
  case FieldType::INT64:
    return IQLToLLVMBuildCastInt64(ctxt, e, argAttrs, retAttrs);
  case FieldType::CHAR:
    return IQLToLLVMBuildCastChar(ctxt, e, argAttrs, retAttrs, NULL);
  case FieldType::VARCHAR:
    return IQLToLLVMBuildCastVarchar(ctxt, e, argAttrs, retAttrs);
  case FieldType::BIGDECIMAL:
    return IQLToLLVMBuildCastDecimal(ctxt, e, argAttrs, retAttrs);
  case FieldType::DOUBLE:
    return IQLToLLVMBuildCastDouble(ctxt, e, argAttrs, retAttrs);
  case FieldType::DATETIME:
    return IQLToLLVMBuildCastDatetime(ctxt, e, argAttrs, retAttrs);
  case FieldType::DATE:
    return IQLToLLVMBuildCastDate(ctxt, e, argAttrs, retAttrs);
  default:
    // Programming error; this should have been caught during type check.
    throw std::runtime_error("Invalid type cast");    
  }
}

IQLToLLVMValue::ValueType IQLToLLVMBuildCastDate(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, const FieldType * argAttrs, LLVMValueRef ret, const FieldType * retAttrs)
{
  LLVMValueRef e1 = unwrap(e)->getValue();
  if (LLVMTypeOf(e1) == LLVMInt32TypeInContext(ctxt->LLVMContext)) {
    LLVMBuildStore(ctxt->LLVMBuilder, e1, ret);
    return IQLToLLVMValue::eLocal;
  } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(e1)) &&
	   LLVMGetElementType(LLVMTypeOf(e1))== ctxt->LLVMVarcharType) {
    LLVMValueRef callArgs[4];
    LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, "InternalDateFromVarchar");
    callArgs[0] = e1;
    e1 = LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 1, "varcharToDate"); 
    LLVMBuildStore(ctxt->LLVMBuilder, e1, ret);
   return IQLToLLVMValue::eLocal;
  } else {
    // TODO: Cast INTEGER to DECIMAL
    throw std::runtime_error ("CAST(expr AS DATE) not implemented");
  }
}

IQLToLLVMValueRef IQLToLLVMBuildCastDate(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs)
{
  const FieldType * argType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, e, argType, resultType,
				       IQLToLLVMBuildCastDate);
}

// TODO: Support all directions of casting.
// TODO: Distinguish datetime from int64
IQLToLLVMValue::ValueType IQLToLLVMBuildCastDatetime(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, const FieldType * argAttrs, LLVMValueRef ret, const FieldType * retAttrs)
{
  LLVMValueRef e1 = unwrap(e)->getValue();
  if (LLVMTypeOf(e1) == LLVMInt64TypeInContext(ctxt->LLVMContext)) {
    LLVMBuildStore(ctxt->LLVMBuilder, e1, ret);
    return IQLToLLVMValue::eLocal;
  } else if (argAttrs->GetEnum() == FieldType::DATE) { 
    LLVMValueRef callArgs[4];
    LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, "InternalDatetimeFromDate");
    callArgs[0] = e1;
    e1 = LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 1, "dateToDatetime");
    LLVMBuildStore(ctxt->LLVMBuilder, e1, ret);    
    return IQLToLLVMValue::eLocal;   
  } else if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(e1)) &&
	   LLVMGetElementType(LLVMTypeOf(e1))== ctxt->LLVMVarcharType) {
    LLVMValueRef callArgs[4];
    LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, "InternalDatetimeFromVarchar");
    callArgs[0] = e1;
    e1 = LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 1, "varcharToDatetime");
    LLVMBuildStore(ctxt->LLVMBuilder, e1, ret);    
    return IQLToLLVMValue::eLocal;
  } else {
    // TODO: Cast INTEGER to DECIMAL
    throw std::runtime_error ("CAST(expr AS DATETIME) not implemented");
  }
}

IQLToLLVMValueRef IQLToLLVMBuildCastDatetime(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs)
{
  const FieldType * argType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, e, argType, resultType,
				       IQLToLLVMBuildCastDatetime);
}

// TODO: Support all directions of casting.
IQLToLLVMValue::ValueType IQLToLLVMBuildCastChar(CodeGenerationContext * ctxt, 
						 IQLToLLVMValueRef e, 
						 const FieldType * argType, 
						 LLVMValueRef ret, 
						 const FieldType * retType)
{
  LLVMValueRef e1 = unwrap(e)->getValue();
  // Must bitcast to match calling convention.
  LLVMTypeRef int8Ptr = LLVMPointerType(LLVMInt8TypeInContext(ctxt->LLVMContext), 0);
  LLVMValueRef ptr = LLVMBuildBitCast(ctxt->LLVMBuilder, ret, int8Ptr, "charcnvcasttmp1");
  
  if (LLVMPointerTypeKind == LLVMGetTypeKind(LLVMTypeOf(e1)) &&
      LLVMGetElementType(LLVMTypeOf(e1))== ctxt->LLVMVarcharType) {
    LLVMValueRef callArgs[3];
    LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, "InternalCharFromVarchar");
    callArgs[0] = e1;
    callArgs[1] = ptr;
    callArgs[2] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 
			       retType->GetSize(),
			       1);
    LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 3, "");
  } else {
    throw std::runtime_error("Only supporting CAST from VARCHAR to CHAR(N)");
  }
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValueRef IQLToLLVMBuildCastChar(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs,
					 const char * N)
{
  const FieldType * argType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  if (N) {
    int32_t n = boost::lexical_cast<int32_t>(N);
    BOOST_ASSERT(n == resultType->GetSize());
  }
  return IQLToLLVMBuildNullableUnaryOp(ctxt, e, argType, resultType,
				       IQLToLLVMBuildCastChar);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildCastVarchar(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, const FieldType * argType, LLVMValueRef ret, const FieldType * retType)
{
  std::vector<const IQLToLLVMValue *> args;
  args.push_back(unwrap(e));
  if (argType->GetEnum() == FieldType::INT32) {
    return InternalBuildCall(ctxt, "InternalVarcharFromInt32", args, ret, retType);
  } else if (argType->GetEnum() == FieldType::INT64) {
    return InternalBuildCall(ctxt, "InternalVarcharFromInt64", args, ret, retType);
  } else if (argType->GetEnum() == FieldType::DOUBLE) {
    return InternalBuildCall(ctxt, "InternalVarcharFromDouble", args, ret, retType);
  } else if (argType->GetEnum() == FieldType::CHAR) {
    return InternalBuildCall(ctxt, "InternalVarcharFromChar", args, ret, retType);
 } else if (argType->GetEnum() == FieldType::VARCHAR) {
    // Identity
    IQLToLLVMValueRef tgt = IQLToLLVMValue::get(ctxt, ret, IQLToLLVMValue::eLocal);
    IQLToLLVMBuildSetValue2(wrap(ctxt), e, tgt);
    return IQLToLLVMValue::eLocal;
 } else if (argType->GetEnum() == FieldType::BIGDECIMAL) {
    return InternalBuildCall(ctxt, "InternalVarcharFromDecimal", args, ret, retType);
 } else if (argType->GetEnum() == FieldType::DATE) {
    return InternalBuildCall(ctxt, "InternalVarcharFromDate", args, ret, retType);
 } else if (argType->GetEnum() == FieldType::DATETIME) {
    return InternalBuildCall(ctxt, "InternalVarcharFromDatetime", args, ret, retType);
  } else {
    throw std::runtime_error("CAST(expr AS VARCHAR) not yet implemented");
  }
}

IQLToLLVMValueRef IQLToLLVMBuildCastVarchar(CodeGenerationContext * ctxt, IQLToLLVMValueRef e, void * argAttrs, void * retAttrs)
{
  const FieldType * argType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, e, argType, resultType,
				       IQLToLLVMBuildCastVarchar);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildNegate(CodeGenerationContext * ctxt, 
					       IQLToLLVMValueRef lhs,
					       const FieldType * lhsTy,
					       LLVMValueRef ret,
					       const FieldType * resultAttributes)
{
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  if (LLVMTypeOf(e1) == LLVMInt32TypeInContext(ctxt->LLVMContext)) {
    LLVMValueRef r = LLVMBuildNeg(ctxt->LLVMBuilder, e1, "negtmp");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (LLVMTypeOf(e1) == LLVMInt64TypeInContext(ctxt->LLVMContext)) {
    LLVMValueRef r = LLVMBuildNeg(ctxt->LLVMBuilder, e1, "negtmp");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (LLVMTypeOf(e1) == LLVMDoubleTypeInContext(ctxt->LLVMContext)) {
    LLVMValueRef r = LLVMBuildFNeg(ctxt->LLVMBuilder, e1, "negtmp");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    return IQLToLLVMCreateUnaryDecimalCall(ctxt, lhs, ret, "InternalDecimalNeg");
  }
}

IQLToLLVMValueRef IQLToLLVMBuildNegate(IQLCodeGenerationContextRef ctxtRef, 
				       IQLToLLVMValueRef lhs,
				       void * lhsAttributes,
				       void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, lhs, lhsType, resultType,
				       IQLToLLVMBuildNegate);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildDateAdd(CodeGenerationContext * ctxt, 
						IQLToLLVMValueRef lhs, 
						const FieldType * lhsType, 
						IQLToLLVMValueRef rhs, 
						const FieldType * rhsType,
						LLVMValueRef retVal,
						const FieldType * retType)
{
  return ctxt->buildDateAdd(unwrap(lhs), lhsType, unwrap(rhs), rhsType,
			    llvm::unwrap(retVal), retType);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildCharAdd(CodeGenerationContext * ctxt, 
						IQLToLLVMValueRef lhs, 
						const FieldType * lhsType, 
						IQLToLLVMValueRef rhs, 
						const FieldType * rhsType,
						LLVMValueRef retVal)
{
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  LLVMValueRef e2 = unwrap(rhs)->getValue();

  LLVMTypeRef int8Ptr = LLVMPointerType(LLVMInt8TypeInContext(ctxt->LLVMContext), 0);
  unsigned lhsSz = IQLToLLVMTypeInspector::getCharArrayLength(e1)-1;
  unsigned rhsSz = IQLToLLVMTypeInspector::getCharArrayLength(e2)-1;
  // Allocate target storage and
  // memcpy the two char variables to the target at appropriate offsets.
  // Allocate storage for return value.  Bit cast for argument to memcpy
  LLVMValueRef retPtrVal = LLVMBuildBitCast(ctxt->LLVMBuilder, 
					    retVal, 
					    int8Ptr, 
					    "charaddretptr");
    
  // Get a pointer to int8_t for args
  LLVMValueRef tmp1 = LLVMBuildBitCast(ctxt->LLVMBuilder, e1, int8Ptr, "charaddargptrtmp1");
  LLVMValueRef tmp2 = LLVMBuildBitCast(ctxt->LLVMBuilder, e2, int8Ptr, "charaddargptrtmp2");

  LLVMValueRef args[5];
  // memcpy arg1 at offset 0
  args[0] = retPtrVal;
  args[1] = tmp1;
  args[2] = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), lhsSz, 1);
  // TODO: Make use of alignment info to speed this up.  This assumption of 1 is pessimistics.
  args[3] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 1, 1);
  args[4] = LLVMConstInt(LLVMInt1TypeInContext(ctxt->LLVMContext), 0, 1);
  LLVMBuildCall(ctxt->LLVMBuilder, ctxt->LLVMMemcpyIntrinsic, &args[0], 5, "");
  // memcpy arg2 at offset lhsSz (be sure to copy the trailing 0 to null terminate).
  llvm::IRBuilder<> * builder = llvm::unwrap(ctxt->LLVMBuilder);
  args[0] = llvm::wrap(builder->CreateGEP(llvm::unwrap(retPtrVal),
					  builder->getInt64(lhsSz),
					  ""));
  // LLVMValueRef offset = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), lhsSz, 1);
  // args[0] = LLVMBuildGEP(ctxt->LLVMBuilder, retPtrVal, &offset, 1, "charaddretptroffset");
  args[1] = tmp2;
  args[2] = LLVMConstInt(LLVMInt64TypeInContext(ctxt->LLVMContext), rhsSz+1, 1);
  // TODO: Make use of alignment info to speed this up.  This assumption of 1 is pessimistics.
  args[3] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 1, 1);
  args[4] = LLVMConstInt(LLVMInt1TypeInContext(ctxt->LLVMContext), 0, 1);
  LLVMBuildCall(ctxt->LLVMBuilder, ctxt->LLVMMemcpyIntrinsic, &args[0], 5, "");
    
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValue::ValueType IQLToLLVMBuildAdd(CodeGenerationContext * ctxt, 
					    IQLToLLVMValueRef lhs, 
					    const FieldType * lhsType, 
					    IQLToLLVMValueRef rhs, 
					    const FieldType * rhsType,
					    LLVMValueRef ret,
					    const FieldType * retType)
{
  if ((lhsType != NULL && lhsType->GetEnum() == FieldType::INTERVAL) ||
      (rhsType != NULL && rhsType->GetEnum() == FieldType::INTERVAL)) {
    // Special case handling of datetime/interval addition.
    return IQLToLLVMBuildDateAdd(ctxt, lhs, lhsType, rhs, rhsType, ret, retType);
  }
  if ((lhsType != NULL && lhsType->GetEnum() == FieldType::CHAR)) {
    return IQLToLLVMBuildCharAdd(ctxt, lhs, lhsType, rhs, rhsType, ret);
  }

  IQLToLLVMBinaryConversion cvt(ctxt, lhs, rhs);
  lhs = cvt.getLHS();
  rhs = cvt.getRHS();
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  LLVMValueRef e2 = unwrap(rhs)->getValue();

  // TODO: Can we replace type comparisons with Type::isIntegerTy
  // Specifically, I want to make sure constants work
  if (LLVMTypeOf(e1) == LLVMInt32TypeInContext(ctxt->LLVMContext) ||
      LLVMTypeOf(e1) == LLVMInt64TypeInContext(ctxt->LLVMContext)) {
    LLVMValueRef r = LLVMBuildAdd(ctxt->LLVMBuilder, e1, e2, "addtmp");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (LLVMTypeOf(e1) == LLVMDoubleTypeInContext(ctxt->LLVMContext)) {
    // TODO: Can we replace type comparisons with Type::isFloatingPointTy
    // Specifically, I want to make sure constants work
    LLVMValueRef r = LLVMBuildFAdd(ctxt->LLVMBuilder, e1, e2, "addtmp");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->GetEnum() == FieldType::BIGDECIMAL) {
    /* call the decimal add library function */
    /* for decimal types we are getting alloca pointers in our expressions */
    return IQLToLLVMCreateBinaryDecimalCall(ctxt, lhs, rhs, ret, iqlOpDecPlus);
  } else if (retType->GetEnum() == FieldType::VARCHAR) {
    return IQLToLLVMCreateBinaryVarcharCall(ctxt, lhs, rhs, ret, iqlOpVarcharPlus);
  } else {
    llvm::unwrap(e1)->getType()->dump();
    throw std::runtime_error("INTERNAL ERROR: Invalid Type");
  }
}

IQLToLLVMValueRef IQLToLLVMBuildAdd(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildNullableBinaryOp(ctxt, lhs, lhsType, rhs, rhsType, resultType,
					IQLToLLVMBuildAdd);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildSub(CodeGenerationContext * ctxt, 
					    IQLToLLVMValueRef lhs, 
					    const FieldType * lhsType, 
					    IQLToLLVMValueRef rhs, 
					    const FieldType * rhsType,
					    LLVMValueRef ret,
					    const FieldType * retType)
{
  return ctxt->buildSub(unwrap(lhs), lhsType, unwrap(rhs), rhsType,
			llvm::unwrap(ret), retType);
}

IQLToLLVMValueRef IQLToLLVMBuildSub(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, 
				    void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildNullableBinaryOp(ctxt, lhs, lhsType, rhs, rhsType, resultType,
					IQLToLLVMBuildSub);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildMul(CodeGenerationContext * ctxt, 
					    IQLToLLVMValueRef lhs, 
					    const FieldType * lhsType, 
					    IQLToLLVMValueRef rhs, 
					    const FieldType * rhsType,
					    LLVMValueRef ret,
					    const FieldType * retType)
{
  IQLToLLVMBinaryConversion cvt(ctxt, lhs, rhs);
  lhs = cvt.getLHS();
  rhs = cvt.getRHS();
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  LLVMValueRef e2 = unwrap(rhs)->getValue();
  if (LLVMTypeOf(e1) == LLVMInt32TypeInContext(ctxt->LLVMContext) ||
      LLVMTypeOf(e1) == LLVMInt64TypeInContext(ctxt->LLVMContext)) {
    LLVMValueRef r = LLVMBuildMul(ctxt->LLVMBuilder, e1, e2, "multmp");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (LLVMTypeOf(e1) == LLVMDoubleTypeInContext(ctxt->LLVMContext)) {
    LLVMValueRef r = LLVMBuildFMul(ctxt->LLVMBuilder, e1, e2, "fmultmp");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    /* call the decimal add library function */
    /* for decimal types we are getting alloca pointers in our expressions */
    return IQLToLLVMCreateBinaryDecimalCall(ctxt, lhs, rhs, ret, iqlOpDecTimes);
  }
}

IQLToLLVMValueRef IQLToLLVMBuildMul(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, 
				    void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildNullableBinaryOp(ctxt, lhs, lhsType, rhs, rhsType, resultType,
					IQLToLLVMBuildMul);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildDiv(CodeGenerationContext * ctxt, 
					    IQLToLLVMValueRef lhs, 
					    const FieldType * lhsType, 
					    IQLToLLVMValueRef rhs, 
					    const FieldType * rhsType,
					    LLVMValueRef ret,
					    const FieldType * retType)
{
  IQLToLLVMBinaryConversion cvt(ctxt, lhs, rhs);
  lhs = cvt.getLHS();
  rhs = cvt.getRHS();
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  LLVMValueRef e2 = unwrap(rhs)->getValue();
  if (LLVMTypeOf(e1) == LLVMInt32TypeInContext(ctxt->LLVMContext) ||
      LLVMTypeOf(e1) == LLVMInt64TypeInContext(ctxt->LLVMContext)) {
    LLVMValueRef r = LLVMBuildSDiv(ctxt->LLVMBuilder, e1, e2, "divtmp");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (LLVMTypeOf(e1) == LLVMDoubleTypeInContext(ctxt->LLVMContext)) {
    LLVMValueRef r = LLVMBuildFDiv(ctxt->LLVMBuilder, e1, e2, "fdivtmp");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    /* call the decimal add library function */
    /* for decimal types we are getting alloca pointers in our expressions */
    return IQLToLLVMCreateBinaryDecimalCall(ctxt, lhs, rhs, ret, iqlOpDecDivide);
  }
}

IQLToLLVMValueRef IQLToLLVMBuildDiv(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, 
				    void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildNullableBinaryOp(ctxt, lhs, lhsType, rhs, rhsType, resultType,
					IQLToLLVMBuildDiv);
}

typedef LLVMValueRef (*LLVMBinaryBuilderOp) (LLVMBuilderRef, LLVMValueRef, LLVMValueRef, const char *);

IQLToLLVMValue::ValueType 
IQLToLLVMBuildBinaryIntegerOp(CodeGenerationContext * ctxt, 
			      IQLToLLVMValueRef lhs, 
			      const FieldType * lhsType, 
			      IQLToLLVMValueRef rhs, 
			      const FieldType * rhsType,
			      LLVMValueRef ret,
			      const FieldType * retType,
			      LLVMBinaryBuilderOp op)
{
  IQLToLLVMBinaryConversion cvt(ctxt, lhs, rhs);
  lhs = cvt.getLHS();
  rhs = cvt.getRHS();
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  LLVMValueRef e2 = unwrap(rhs)->getValue();
  if (LLVMTypeOf(e1) == LLVMInt32TypeInContext(ctxt->LLVMContext) ||
      LLVMTypeOf(e1) == LLVMInt64TypeInContext(ctxt->LLVMContext)) {
    LLVMValueRef r = op(ctxt->LLVMBuilder, e1, e2, "binop");
    LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    throw std::runtime_error("Modulus only supported for integer values");
  }
}

IQLToLLVMValue::ValueType IQLToLLVMBuildMod(CodeGenerationContext * ctxt, 
					    IQLToLLVMValueRef lhs, 
					    const FieldType * lhsType, 
					    IQLToLLVMValueRef rhs, 
					    const FieldType * rhsType,
					    LLVMValueRef ret,
					    const FieldType * retType)
{
  return IQLToLLVMBuildBinaryIntegerOp(ctxt, lhs, lhsType, rhs, rhsType, 
				       ret, retType, LLVMBuildSRem);
}

IQLToLLVMValueRef IQLToLLVMBuildMod(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, 
				    void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildNullableBinaryOp(ctxt, lhs, lhsType, rhs, rhsType, resultType,
					IQLToLLVMBuildMod);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildBitwiseAnd(CodeGenerationContext * ctxt, 
						   IQLToLLVMValueRef lhs, 
						   const FieldType * lhsType, 
						   IQLToLLVMValueRef rhs, 
						   const FieldType * rhsType,
						   LLVMValueRef ret,
						   const FieldType * retType)
{
  return IQLToLLVMBuildBinaryIntegerOp(ctxt, lhs, lhsType, rhs, rhsType, 
				       ret, retType, LLVMBuildAnd);
}

IQLToLLVMValueRef IQLToLLVMBuildBitwiseAnd(IQLCodeGenerationContextRef ctxtRef, 
					   IQLToLLVMValueRef lhs, 
					   void * lhsAttributes, 
					   IQLToLLVMValueRef rhs, 
					   void * rhsAttributes,
					   void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildNullableBinaryOp(ctxt, lhs, lhsType, rhs, rhsType, resultType,
					IQLToLLVMBuildBitwiseAnd);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildBitwiseOr(CodeGenerationContext * ctxt, 
						  IQLToLLVMValueRef lhs, 
						  const FieldType * lhsType, 
						  IQLToLLVMValueRef rhs, 
						  const FieldType * rhsType,
						  LLVMValueRef ret,
						  const FieldType * retType)
{
  return IQLToLLVMBuildBinaryIntegerOp(ctxt, lhs, lhsType, rhs, rhsType, 
				       ret, retType, LLVMBuildOr);
}

IQLToLLVMValueRef IQLToLLVMBuildBitwiseOr(IQLCodeGenerationContextRef ctxtRef, 
					  IQLToLLVMValueRef lhs, 
					  void * lhsAttributes, 
					  IQLToLLVMValueRef rhs, 
					  void * rhsAttributes,
					  void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildNullableBinaryOp(ctxt, lhs, lhsType, rhs, rhsType, resultType,
					IQLToLLVMBuildBitwiseOr);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildBitwiseXor(CodeGenerationContext * ctxt, 
						   IQLToLLVMValueRef lhs, 
						   const FieldType * lhsType, 
						   IQLToLLVMValueRef rhs, 
						   const FieldType * rhsType,
						   LLVMValueRef ret,
						   const FieldType * retType)
{
  return IQLToLLVMBuildBinaryIntegerOp(ctxt, lhs, lhsType, rhs, rhsType, 
				       ret, retType, LLVMBuildXor);
}

IQLToLLVMValueRef IQLToLLVMBuildBitwiseXor(IQLCodeGenerationContextRef ctxtRef, 
					   IQLToLLVMValueRef lhs, 
					   void * lhsAttributes, 
					   IQLToLLVMValueRef rhs, 
					   void * rhsAttributes,
					   void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return IQLToLLVMBuildNullableBinaryOp(ctxt, lhs, lhsType, rhs, rhsType, resultType,
					IQLToLLVMBuildBitwiseXor);
}

IQLToLLVMValue::ValueType IQLToLLVMBuildBitwiseNot(CodeGenerationContext * ctxt, 
						   IQLToLLVMValueRef lhs,
						   const FieldType * argAttrs, 
						   LLVMValueRef ret, 
						   const FieldType * retAttrs)
{
  LLVMValueRef e1 = unwrap(lhs)->getValue();
  LLVMValueRef r = LLVMBuildNot(ctxt->LLVMBuilder, e1, "bitwise_not");
  LLVMBuildStore(ctxt->LLVMBuilder, r, ret);
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValueRef IQLToLLVMBuildBitwiseNot(IQLCodeGenerationContextRef ctxtRef, 
					   IQLToLLVMValueRef lhs,
					   void * argAttrs, void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return IQLToLLVMBuildNullableUnaryOp(ctxt, lhs, lhsType, resultType,
				       IQLToLLVMBuildBitwiseNot);
}

IQLToLLVMValue::ValueType IQLToLLVMCreateUnaryDecimalCall(CodeGenerationContext * ctxt, 
							  IQLToLLVMValueRef lhs, 
							  LLVMValueRef ret,
							  const std::string& externalFun) {
  LLVMValueRef callArgs[3];
  LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, externalFun.c_str());
  callArgs[0] = unwrap(lhs)->getValue();
  callArgs[1] = ret;
  callArgs[2] = LLVMBuildLoad(ctxt->LLVMBuilder, 
			      ctxt->getContextArgumentRef(), 
			      "ctxttmp");
  // Important Note: I wasted a bunch of time learning that you can't set a name on a CallInst to a function
  // with a void return type (this make sense when you think about it but the LLVM errors don't make this as clear as they could).
  LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 3, "");
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValue::ValueType IQLToLLVMCreateBinaryDecimalCall(CodeGenerationContext * ctxt, 
							   IQLToLLVMValueRef lhs, 
							   IQLToLLVMValueRef rhs, 
							   LLVMValueRef ret,
							   enum DecimalOpCode opCode) {
  const char * externFuncs [] = { "InternalDecimalAdd", "InternalDecimalSub", "InternalDecimalMul", "InternalDecimalDiv" };
  LLVMValueRef callArgs[4];
  LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, externFuncs[opCode]);
  callArgs[0] = unwrap(lhs)->getValue();
  callArgs[1] = unwrap(rhs)->getValue();
  callArgs[2] = ret;
  callArgs[3] = LLVMBuildLoad(ctxt->LLVMBuilder, 
			      ctxt->getContextArgumentRef(), 
			      "ctxttmp");
  // Important Note: I wasted a bunch of time learning that you can't set a name on a CallInst to a function
  // with a void return type (this make sense when you think about it but the LLVM errors don't make this as clear as they could).
  LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 4, "");
  return IQLToLLVMValue::eLocal;
}

void IQLToLLVMBuildReturnValue(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef iqlVal) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Due to our uniform implementation of mutable variables, our return location is actually
  // a pointer to a pointer.
  LLVMValueRef loc = LLVMBuildLoad(ctxt->LLVMBuilder, 
				   ctxt->lookupValue("__ReturnValue__", NULL)->getValue(),
				   "retloc");

  LLVMValueRef llvmVal = unwrap(iqlVal)->getValue();

  llvm::BasicBlock * mergeBB = NULL;
  llvm::Value * nv = unwrap(iqlVal)->getNull();
  if (nv) {
    // We are only prepared to deal with NULLABLE int right now
    // which in fact is only return boolean values.  For backward
    // compatibility we return 0 for NULL.
    llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
    llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
    llvm::Function *f = b->GetInsertBlock()->getParent();
    if (llvm::unwrap(llvmVal)->getType() != b->getInt32Ty()) {
      throw std::runtime_error("Only supporting return of nullable values for INTEGERs");
    }
    llvm::BasicBlock * nullBB = llvm::BasicBlock::Create(*c, "retNullBlock", f);
    llvm::BasicBlock * notNullBB = llvm::BasicBlock::Create(*c, "retNotNullBlock", f);
    mergeBB = llvm::BasicBlock::Create(*c, "retMergeBlock", f);
    b->CreateCondBr(b->CreateNot(nv), notNullBB, nullBB);
    b->SetInsertPoint(nullBB);
    b->CreateStore(b->getInt32(0), llvm::unwrap(loc));
    b->CreateBr(mergeBB);
    b->SetInsertPoint(notNullBB);
  }

  // Expression are either by value or by reference.  For reference types
  // we load before store (maybe we should use the memcpy instrinsic instead).
  if (unwrap(iqlVal)->isValueType()) {
    LLVMBuildStore(ctxt->LLVMBuilder, 
		   llvmVal, 
		   loc); 
  } else {
    LLVMValueRef val = LLVMBuildLoad(ctxt->LLVMBuilder, llvmVal, "retval");
    if (LLVMGetElementType(LLVMTypeOf(loc)) != LLVMTypeOf(val)) {
      llvm::unwrap(LLVMTypeOf(loc))->dump();
      llvm::unwrap(LLVMTypeOf(val))->dump();
      throw std::runtime_error("Type mismatch with return value");
    }
    LLVMBuildStore(ctxt->LLVMBuilder, 
		   val, 
		   loc);
  }
  // The caller expects a BB into which it can insert LLVMBuildRetVoid.
  if (mergeBB) {
    llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
    b->CreateBr(mergeBB);
    b->SetInsertPoint(mergeBB);
  }
}

IQLToLLVMValueRef IQLToLLVMBuildRef(CodeGenerationContext * ctxt, IQLToLLVMValueRef allocAVal)
{
  /* Two cases here.  Value types (8 bytes and less) are loaded whereas larger types 
     such as varchar and decimal are passed by reference.  Values that are value are always local */
  if (LLVMPointerTypeKind != LLVMGetTypeKind(LLVMTypeOf(unwrap(allocAVal)->getValue())))
    printf("Error: symbol table value not a pointer");
  if (unwrap(allocAVal)->isPointerToValueType()) {
    //std::cout << "Loading variable " << var << "\n";
    return IQLToLLVMValue::get(ctxt, 
			       LLVMBuildLoad(ctxt->LLVMBuilder, 
					     unwrap(allocAVal)->getValue(), 
					     ""),
			       unwrap(allocAVal)->getNull(),
			       IQLToLLVMValue::eLocal);
  } else {
    //std::cout << "Variable reference " << var << "\n";
    return allocAVal;
  }
}

IQLToLLVMValueRef IQLToLLVMBuildVariableRef(IQLCodeGenerationContextRef ctxtRef, 
					    const char * var,
					    const char * var2) 
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  /* Lookup value in symbol table */
  /* TODO: Handle undefined reference */
  const IQLToLLVMValue * allocAVal = ctxt->lookupValue(var, var2);
  return IQLToLLVMBuildRef (ctxt, wrap(allocAVal));
}

IQLToLLVMValueRef IQLToLLVMBuildArrayRef(IQLCodeGenerationContextRef ctxtRef, 
					 const char * var,
					 IQLToLLVMValueRef idx) 
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  IQLToLLVMLValueRef allocAVal=IQLToLLVMBuildArrayLValue(ctxtRef, var, idx);
  return IQLToLLVMBuildRef (ctxt, wrap(unwrap(allocAVal)->getEntirePointer(ctxt)));
}

IQLToLLVMValueRef IQLToLLVMBuildArray(IQLCodeGenerationContextRef ctxtRef, 
				      IQLToLLVMValueVectorRef lhs, void * arrayAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  std::vector<const IQLToLLVMValue *> &vals(*unwrap(lhs));
  const FieldType * ty = (const FieldType *) arrayAttrs;
  return wrap(ctxt->buildArray(vals, ty));
}

void IQLToLLVMBuildSetValue2(IQLCodeGenerationContextRef ctxtRef, 
			     IQLToLLVMValueRef iqlVal,
			     IQLToLLVMValueRef iqllvalue)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  LLVMValueRef lvalue = unwrap(iqllvalue)->getValue();
  if (NULL == lvalue)
    throw std::runtime_error("Undefined variable ");

  //
  // Perform any necessary type promotion of rvalue.
  //
  // TODO: What are the interactions between type promotion and the
  // local/global value stuff going on in here?  Namely should we really
  // do type promotions first before handling global/local?
  // TODO: It really would have been easier to determine the type promotion
  // at type checking time when we had the IQL types handy.
  BOOST_ASSERT(LLVMGetTypeKind(LLVMTypeOf(lvalue)) == LLVMPointerTypeKind);
  LLVMValueRef llvmVal = unwrap(iqlVal)->getValue();
  // If value type, then lvalue is a pointer and rvalue is value.
  // If reference type, both lvalue and rvalue are pointers.
  LLVMTypeRef lvalueTy = unwrap(iqlVal)->isValueType() ?
    LLVMGetElementType(LLVMTypeOf(lvalue)) :
    LLVMTypeOf(lvalue);
  LLVMTypeRef rvalueTy = LLVMTypeOf(llvmVal);
  if (NULL == IQLToLLVMBinaryConversion::castTo(ctxt, rvalueTy, lvalueTy)) {
    // This really should have been handled in type checking!
    llvm::unwrap(LLVMTypeOf(lvalue))->dump();
    llvm::unwrap(LLVMTypeOf(llvmVal))->dump();
    ctxt->dumpSymbolTable();
    throw std::runtime_error("Type mismatch setting value in variable");
  }
  iqlVal = IQLToLLVMBinaryConversion::convertTo(ctxt, iqlVal, lvalueTy);
  llvmVal = unwrap(iqlVal)->getValue();

  // DECIMAL/VARCHAR expressions return a reference/pointer.  
  // Before setting we must load.  Perhaps we'd be better off with
  // a memcpy instrinsic here.
  if (LLVMTypeOf(llvmVal) == LLVMPointerType(ctxt->LLVMDecimal128Type, 0)) {
    llvmVal = LLVMBuildLoad(ctxt->LLVMBuilder, llvmVal, "setdecval");
  } else if (IQLToLLVMTypePredicate::isChar(llvmVal)) {
    llvmVal = LLVMBuildLoad(ctxt->LLVMBuilder, llvmVal, "setcharval");
  } else if (IQLToLLVMTypePredicate::isArrayType(llvmVal)) {
    llvmVal = LLVMBuildLoad(ctxt->LLVMBuilder, llvmVal, "setarrayval");
  } else if (LLVMTypeOf(llvmVal) == LLVMPointerType(ctxt->LLVMVarcharType, 0)) {
    // TODO:
    // Four cases here depending on the global/local dichotomy for the source
    // value and the target variable.
    // Source value global, Target Variable global : Must deep copy string (unless move semantics specified) remove from interpreter heap
    // Source value global, Target Variable local : Must deep copy string, must record in interpreter heap
    // Source value local, Target Variable global : May shallow copy but must remove source memory from interpreter heap.  TODO: This doesn't seem right!  What if two globals refer to the same local value?  Is this possible?  It may not be if the local is a temporary created for a string expression but it may be if we have a string variable on the stack.  It seems we need  use-def chain to know what is the right thing to do here.
    // Source value local, Target Variable local : May shallow copy string
    // Note that above rules depend critically on the assumption that strings are not
    // modifiable (because the above rules allow multiple local variables to 
    // reference the same underlying pointer).  
    // Also the above rules depend on the fact that
    // we know in all case where a value came from.  With mutable local variables it would
    // possible that the variable could have a local value at some point and a global value at
    // another. Removing this ambiguity is why global value assignments to local variables must
    // deep copy using the local heap.
    // Move semantics make all of this much more complicated and I don't think I
    // understand how to do this properly at this point.
    // For global values that contain pointers (e.g. strings)
    // we must copy the string onto the appropriate heap or at least disassociate from
    // internal heap tracking.  Here we take the latter path.  It is cheaper but less general
    // in that it assumes that the heap used by the IQL runtime is the same as that used
    // by the client of the runtime.
    if (unwrap(iqlVal)->getValueType() != IQLToLLVMValue::eLocal) {
      // Call to copy the varchar before setting.
      LLVMValueRef callArgs[4];
      LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, "InternalVarcharCopy");
      callArgs[0] = llvmVal;
      callArgs[1] = LLVMCreateEntryBlockAlloca(ctxt, ctxt->LLVMVarcharType, "varcharliteral");
      callArgs[2] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 
				 unwrap(iqllvalue)->getValueType() == IQLToLLVMValue::eGlobal ? 0 : 1,
				 1);
      callArgs[3] = LLVMBuildLoad(ctxt->LLVMBuilder, 
				  ctxt->getContextArgumentRef(), 
				  "ctxttmp");
      LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 4, "");
      llvmVal = callArgs[1];
    } else if (unwrap(iqlVal)->getValueType() == IQLToLLVMValue::eLocal &&
	unwrap(iqllvalue)->getValueType() == IQLToLLVMValue::eGlobal) {
      LLVMValueRef callArgs[2];
      LLVMValueRef fn = LLVMGetNamedFunction(ctxt->LLVMModule, "InternalVarcharErase");
      callArgs[0] = llvmVal;
      callArgs[1] = LLVMBuildLoad(ctxt->LLVMBuilder, 
				  ctxt->getContextArgumentRef(),
				  "ctxttmp");
      LLVMBuildCall(ctxt->LLVMBuilder, fn, &callArgs[0], 2, "");
    }
    // Load before store since we have a pointer to the Varchar.
    llvmVal = LLVMBuildLoad(ctxt->LLVMBuilder, llvmVal, "setvarcharval");    
  } 

  // Finally we can just issue the store.
  LLVMBuildStore(ctxt->LLVMBuilder, llvmVal, lvalue);
}

IQLToLLVMLValueRef IQLToLLVMBuildLValue(IQLCodeGenerationContextRef ctxtRef,
					const char * var)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->lookup(var, NULL));
}

IQLToLLVMLValueRef IQLToLLVMBuildArrayLValue(IQLCodeGenerationContextRef ctxtRef, 
					     const char * var,
					     IQLToLLVMValueRef idx)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  llvm::Module * m = llvm::unwrap(ctxt->LLVMModule);
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  llvm::Function *f = b->GetInsertBlock()->getParent();
  const IQLToLLVMValue * lvalue = ctxt->lookupValue(var, NULL);
  llvm::Value * lval = llvm::unwrap(lvalue->getValue());

  // Convert index to int64
  idx = IQLToLLVMBinaryConversion::convertTo(ctxt, 
					     idx, 
					     LLVMInt64TypeInContext(ctxt->LLVMContext));

  // TODO: The "should" be a pointer to an array type and for us to GEP it
  // we need to bitcast to a pointer to the element type.
  // However...  There is a hack that we are being backward compatible
  // with for the moment that allows one to array reference a scalar
  // which is already a pointer to element type!
  const llvm::PointerType * ptrType = llvm::dyn_cast<llvm::PointerType>(lval->getType());
  BOOST_ASSERT(ptrType != NULL);
  const llvm::ArrayType * arrayType = llvm::dyn_cast<llvm::ArrayType>(ptrType->getElementType());
  if (arrayType) {    
    // Seeing about 50% performance overhead for the bounds checking.
    // Not implementing this until I have some optimization mechanism
    // for safely removing them in some cases (perhaps using LLVM).

    // llvm::Value * lowerBound = b->CreateICmpSLT(llvm::unwrap(unwrap(idx)->getValue()),
    // 						b->getInt64(0));
    // llvm::Value * upperBound = b->CreateICmpSGE(llvm::unwrap(unwrap(idx)->getValue()),
    // 						b->getInt64(arrayType->getNumElements()));
    // llvm::Value * cond = b->CreateOr(lowerBound, upperBound);
    // // TODO: Make a single module-wide basic block for the exceptional case?
    // llvm::BasicBlock * goodBlock = 
    //   llvm::BasicBlock::Create(*c, "arrayDereference", f);
    // llvm::BasicBlock * exceptionBlock = 
    //   llvm::BasicBlock::Create(*c, "arrayIndexException", f);
    // // Branch and set block
    // b->CreateCondBr(cond, exceptionBlock, goodBlock);
    // // Array out of bounds exception
    // b->SetInsertPoint(exceptionBlock); 
    // b->CreateCall(m->getFunction("InternalArrayException"));
    // // We should never make the branch since we actually
    // // throw in the called function.
    // b->CreateBr(goodBlock);

    // // Array check good: emit the value.
    // b->SetInsertPoint(goodBlock);  
    lval = b->CreateBitCast(lval, llvm::PointerType::get(arrayType->getElementType(),0));
  }
  
  // GEP to get pointer to the correct offset.
  llvm::Value * gepIndexes[1] = { llvm::unwrap(unwrap(idx)->getValue())};
  lval = b->CreateInBoundsGEP(lval, &gepIndexes[0], &gepIndexes[1]);
  return wrap(new IQLToLLVMLocal(unwrap(IQLToLLVMValue::get(ctxt, 
							    llvm::wrap(lval),
							    IQLToLLVMValue::eLocal)),
				 NULL));
}

void IQLToLLVMBuildSetValue(CodeGenerationContext * ctxt, IQLToLLVMValueRef iqlVal, const char * loc) {
  const IQLToLLVMLValue * lval = unwrap(IQLToLLVMBuildLValue(wrap(ctxt),loc));
  IQLToLLVMBuildSetNullableValue(ctxt,
				 lval,
				 iqlVal);
}

void IQLToLLVMCaseBlockBegin(IQLCodeGenerationContextRef ctxtRef, void * caseAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  llvm::Function *f = b->GetInsertBlock()->getParent();
  const FieldType * caseType = (const FieldType *) caseAttrs;

  // Create a merge block with a PHI node 
  // Save the block and PHI so that incoming values may be added.
  // The block itself will be emitted at the end of the CASE expression.
  std::stack<IQLToLLVMCaseState *> & stk(ctxt->IQLCase);
  llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "casemerge", f);
  // Allocate space for a result.  Try to reuse space used for a
  // completed CASE.
  llvm::Type * retTy = caseType->LLVMGetType(ctxt);
  LLVMValueRef result = IQLToLLVMGetCachedLocal(ctxt, llvm::wrap(retTy));
  if (result == NULL) {
    result = LLVMCreateEntryBlockAlloca(ctxt, 
					llvm::wrap(retTy),
					"caseResult");    
  }
  LLVMValueRef nullVal = NULL;
  if (caseType->isNullable()) {
    nullVal = LLVMCreateEntryBlockAlloca(ctxt, 
					 LLVMInt1TypeInContext(ctxt->LLVMContext), 
					 "caseNullBit");
  }     
  IQLToLLVMLocal * lVal = new IQLToLLVMLocal(unwrap(IQLToLLVMValue::get(ctxt, result, IQLToLLVMValue::eLocal)), llvm::unwrap(nullVal));
  stk.push(new IQLToLLVMCaseState(lVal, mergeBB));  
  BOOST_ASSERT(stk.size() != 0);
}

void IQLToLLVMCaseBlockIf(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef condVal)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // The function we are working on.
  llvm::Function *f = b->GetInsertBlock()->getParent();
  // Create blocks for the then/value and else (likely to be next conditional).  
  // Insert the 'then/value' block at the end of the function.
  llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
  // Save else block so we can append it after the value is emitted 
  std::stack<IQLToLLVMCaseState*> & stk (ctxt->IQLCase);
  BOOST_ASSERT(stk.size() != 0);
  BOOST_ASSERT(stk.top()->ElseBB == NULL);
  stk.top()->ElseBB = llvm::BasicBlock::Create(*c, "else", f);
  // Handle ternary logic here
  llvm::Value * nv = unwrap(condVal)->getNull();
  if (nv) {
    llvm::BasicBlock * notNullBB = llvm::BasicBlock::Create(*c, "notNull", f);
    b->CreateCondBr(b->CreateNot(nv), notNullBB, stk.top()->ElseBB);
    b->SetInsertPoint(notNullBB);
  }
  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(llvm::unwrap(unwrap(condVal)->getValue()),
					  b->getInt32(0),
					  "boolCast");
  // Branch and set block
  b->CreateCondBr(boolVal, thenBB, stk.top()->ElseBB);
  // Emit then value.
  b->SetInsertPoint(thenBB);  
}

void IQLToLLVMCaseBlockThen(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef value, void * caseAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Return type of CASE expr
  const FieldType * caseType = (const FieldType *) caseAttrs;
  IQLToLLVMCaseBlockThen(ctxt, value, caseType, false);
}

void IQLToLLVMCaseBlockThen(CodeGenerationContext * ctxt, IQLToLLVMValueRef value, const FieldType * caseType, bool allowNullToNonNull)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // Basic blocks we saved.
  std::stack<IQLToLLVMCaseState*> & stk (ctxt->IQLCase);

  // Convert to required type 
  IQLToLLVMValueRef cvtVal =  
    IQLToLLVMBinaryConversion::convertTo(ctxt, value, caseType);
  if (NULL == cvtVal) {
    // This should always succeed as type checking should have
    // validated the conversion.
    llvm::Type * cvtTy = caseType->LLVMGetType(ctxt);
    std::cerr << "INTERNAL ERROR: type promotion failed.\nTarget LLVM Type: ";
    cvtTy->dump();
    std::cerr << "\nSource LLVM Type: ";
    llvm::unwrap(LLVMTypeOf(unwrap(value)->getValue()))->dump();
    std::cerr << std::endl;
    throw std::runtime_error("INTERNAL ERROR: Failed type promotion during code generation");
  }
  BOOST_ASSERT(stk.size() != 0);
  IQLToLLVMCaseState * state = stk.top();
  // Store converted value in the case return variable
  IQLToLLVMBuildSetNullableValue(ctxt, state->Local, cvtVal, 
				 allowNullToNonNull);
  // Branch to block with PHI
  b->CreateBr(state->getMergeBlock());
  
  // Emit else block if required (will be required
  // if we are WHEN but not for an ELSE).
  if (state->ElseBB) {
    b->SetInsertPoint(state->ElseBB);
    state->ElseBB = NULL;
  }
}

IQLToLLVMValueRef IQLToLLVMCaseBlockFinish(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // Basic blocks we saved.
  std::stack<IQLToLLVMCaseState*> & stk (ctxt->IQLCase);

  // Emit merge block 
  BOOST_ASSERT(stk.size() != 0);
  b->SetInsertPoint(stk.top()->getMergeBlock());

  // This is our return value
  IQLToLLVMLocal * lVal = stk.top()->Local;
  const IQLToLLVMValue * rVal = lVal->getEntirePointer(ctxt);
  // Pointer to value we allocated
  LLVMValueRef result=rVal->getValue();
  // Get the null bit if a nullable value
  llvm::Value * nullBit = rVal->getNull();
  // Return either pointer or value
  if (result != NULL &&
      IQLToLLVMValue::isPointerToValueType(LLVMTypeOf(result))) {
    // The pointer to the local will not be used beyond this point,
    // so it may be reused.
    IQLToLLVMReturnCachedLocal(ctxt, result);
    result = LLVMBuildLoad(ctxt->LLVMBuilder, result, "");
  }
  // Done with this CASE so pop off.
  delete stk.top();
  stk.pop();

  return IQLToLLVMValue::get(ctxt, result, nullBit, IQLToLLVMValue::eLocal);
}

void IQLToLLVMWhileBegin(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->whileBegin();
}

void IQLToLLVMWhileStatementBlock(IQLCodeGenerationContextRef ctxtRef, 
				  IQLToLLVMValueRef condVal, 
				  void * condAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->whileStatementBlock(unwrap(condVal), (const FieldType *) condAttrs);
}

void IQLToLLVMWhileFinish(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->whileFinish();
}

void IQLToLLVMBeginIfThenElse(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef condVal)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Create blocks for the then and else cases.  Insert the 'then' block at the
  // end of the function.
  std::stack<class IQLToLLVMStackRecord* > & stk(ctxt->IQLStack);
  stk.push(new IQLToLLVMStackRecord());
  stk.top()->ThenBB = llvm::BasicBlock::Create(*c, "then", TheFunction);
  stk.top()->ElseBB = llvm::BasicBlock::Create(*c, "else");
  stk.top()->MergeBB = llvm::BasicBlock::Create(*c, "ifcont");

  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(llvm::unwrap(unwrap(condVal)->getValue()),
					  b->getInt32(0),
					  "boolCast");

  // Branch and set block
  b->CreateCondBr(boolVal, stk.top()->ThenBB, stk.top()->ElseBB);
  // Emit then value.
  b->SetInsertPoint(stk.top()->ThenBB);  
}

void IQLToLLVMElseIfThenElse(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Basic blocks we saved.
  std::stack<class IQLToLLVMStackRecord* > & stk(ctxt->IQLStack);

  // We saved block.
  b->CreateBr(stk.top()->MergeBB);
  // Codegen of 'Then' can change the current block, update ThenBB for the PHI.
  stk.top()->ThenBB = b->GetInsertBlock();
  
  // Emit else block.
  TheFunction->getBasicBlockList().push_back(stk.top()->ElseBB);
  b->SetInsertPoint(stk.top()->ElseBB);
}

IQLToLLVMValueRef IQLToLLVMEndIfThenElse(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef thenVal, IQLToLLVMValueRef elseVal)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Basic blocks we saved.
  std::stack<class IQLToLLVMStackRecord* > & stk(ctxt->IQLStack);

  b->CreateBr(stk.top()->MergeBB);
  // Codegen of 'Else' can change the current block, update ElseBB for the PHI.
  stk.top()->ElseBB = b->GetInsertBlock();
  
  // It is possible for the branches to differ with respect to their
  // locality.  If one side is local then we have to make a local
  // copy a global value.
  if (unwrap(thenVal)->getValueType() != unwrap(elseVal)->getValueType()) {
    // TODO: Handle this case by making a local copy of a global.
    throw std::runtime_error("Internal Error: If then else with a mix of local and global values not handled");
  }


  // Emit merge block.
  TheFunction->getBasicBlockList().push_back(stk.top()->MergeBB);
  b->SetInsertPoint(stk.top()->MergeBB);

  // Perform conversion in the merge block
  IQLToLLVMBinaryConversion cvt(ctxt, thenVal, elseVal);
  thenVal = cvt.getLHS();
  elseVal = cvt.getRHS();
  llvm::Value * e1 = llvm::unwrap(unwrap(thenVal)->getValue());
  llvm::Value * e2 = llvm::unwrap(unwrap(elseVal)->getValue());

  llvm::PHINode *PN = b->CreatePHI(llvm::unwrap(cvt.getResultType()), "iftmp");

  PN->addIncoming(e1, stk.top()->ThenBB);
  PN->addIncoming(e2, stk.top()->ElseBB);

  // Done with these blocks. Pop the stack of blocks.
  delete stk.top();
  stk.pop();

  return IQLToLLVMValue::get(ctxt,
			     llvm::wrap(PN),
			     unwrap(thenVal)->getValueType());
}

void IQLToLLVMBeginSwitch(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);

  // Create a new builder for switch
  std::stack<class IQLToLLVMSwitchRecord* > & stk(ctxt->IQLSwitch);
  stk.push(new IQLToLLVMSwitchRecord());
  stk.top()->Top = b->GetInsertBlock();
  stk.top()->Exit = llvm::BasicBlock::Create(*c, "switchEnd");
}

void IQLToLLVMEndSwitch(IQLCodeGenerationContextRef ctxtRef, 
			IQLToLLVMValueRef switchExpr)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  llvm::Value * v = llvm::unwrap(unwrap(switchExpr)->getValue());

  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  // Get switch builder and pop.
  std::stack<class IQLToLLVMSwitchRecord* > & stk(ctxt->IQLSwitch);
  IQLToLLVMSwitchRecord * s = stk.top();
  stk.pop();
  
  // Add the switch statement and cases.
  llvm::SwitchInst * si = llvm::SwitchInst::Create(v,
						   s->Exit, 
						   s->Cases.size(),
						   s->Top);
  typedef std::vector<std::pair<llvm::ConstantInt*, 
    llvm::BasicBlock*> > CaseVec;
  for(CaseVec::iterator it = s->Cases.begin();
      it != s->Cases.end();
      ++it) {
    si->addCase(it->first, it->second);
  }

  // Set builder to exit block and declare victory.
  TheFunction->getBasicBlockList().push_back(s->Exit);
  b->SetInsertPoint(s->Exit);

  delete s;
}

void IQLToLLVMBeginSwitchCase(IQLCodeGenerationContextRef ctxtRef, 
			      const char * caseVal)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);

  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  // Add block and set as insertion point
  std::stack<class IQLToLLVMSwitchRecord* > & stk(ctxt->IQLSwitch);
  llvm::BasicBlock * bb = llvm::BasicBlock::Create(*c, "switchCase");;
  llvm::ConstantInt * llvmConst = llvm::ConstantInt::get(b->getInt32Ty(),
							 caseVal,
							 10);
  stk.top()->Cases.push_back(std::make_pair(llvmConst, bb));

  TheFunction->getBasicBlockList().push_back(bb);
  b->SetInsertPoint(bb);
}

void IQLToLLVMEndSwitchCase(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // Get switch builder for exit block
  std::stack<class IQLToLLVMSwitchRecord* > & stk(ctxt->IQLSwitch);
  IQLToLLVMSwitchRecord * s = stk.top();
  // Unconditional branch to exit block (implicit break).
  b->CreateBr(s->Exit);
}

void IQLToLLVMBeginAggregateFunction(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Currently in the "transfer context"; must shift into aggregate
  // context.
  ctxt->restoreAggregateContext(&ctxt->Update);
}

class AggregateFunction
{
public:
  static boost::shared_ptr<AggregateFunction> get(const char * fn);
  virtual ~AggregateFunction() {}
  virtual void update(CodeGenerationContext * ctxt,
		      const std::string& old,
		      IQLToLLVMValueRef inc,
		      const FieldType * ft) =0;
  virtual IQLToLLVMValueRef initialize(CodeGenerationContext * ctxt,
				       const FieldType * ft) =0;
};

class NullInitializedAggregate : public AggregateFunction
{
protected:
  virtual void updateNull(CodeGenerationContext * ctxt,
			  const std::string& aggFn,
			  IQLToLLVMValueRef inc,
			  const FieldType * inputTy,
			  const IQLToLLVMLValue * fieldLVal,
			  const FieldType * ft)=0;
  virtual void updateNotNull(CodeGenerationContext * ctxt,
			     const std::string& aggFn,
			     IQLToLLVMValueRef inc,
			     const FieldType * inputTy,
			     const IQLToLLVMLValue * fieldLVal,
			     const FieldType * ft,
			     llvm::BasicBlock * mergeBlock)=0;
  
public:
  ~NullInitializedAggregate() {}
  void update(CodeGenerationContext * ctxt,
	      const std::string& old,
	      IQLToLLVMValueRef inc,
	      const FieldType * ft) ;
  IQLToLLVMValueRef initialize(CodeGenerationContext * ctxt,
			       const FieldType * ft);  
};

void NullInitializedAggregate::update(CodeGenerationContext * ctxt,
			  const std::string& aggFn,
			  IQLToLLVMValueRef inc,
			  const FieldType * ft)
{
  // Code generate:
  //    IF inc IS NOT NULL THEN (only if input is nullable)
  //      IF old IS NOT NULL THEN 
  //        SET old = old + inc
  //      ELSE
  //        SET old = inc

  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Merge block 
  llvm::BasicBlock * mergeBlock = 
    llvm::BasicBlock::Create(*c, "cont", TheFunction);
  // LValue around the field
  std::map<std::string, std::pair<std::string, const RecordType *> >& recordTypes(*unwrap(ctxt->IQLRecordArguments));
  const RecordType * outputRecord = recordTypes.find("input1")->second.second;
  BOOST_ASSERT(outputRecord != NULL);
  IQLToLLVMField fieldLVal(ctxt, outputRecord, aggFn, "__BasePointer1__");
  // Type of input
  const FieldType * inputTy = NULL;
  // If a nullable value check for nullability
  llvm::Value * nv = unwrap(inc)->getNull();
  if (nv) {
    // We must skip nullable inputs
    llvm::BasicBlock * updateBlock = 
      llvm::BasicBlock::Create(*c, "aggCkNullInc", TheFunction);
    // Check NULL bit, branch and set block
    b->CreateCondBr(b->CreateNot(nv), updateBlock, mergeBlock);
    // Emit update and branch to merge.
    b->SetInsertPoint(updateBlock);    
    inputTy = ft;
  } else {
    inputTy = ft->clone(false);
  }
  
  llvm::BasicBlock * notNullBlock = 
    llvm::BasicBlock::Create(*c, "aggNotNull", TheFunction);
  llvm::BasicBlock * nullBlock = 
    llvm::BasicBlock::Create(*c, "aggNull", TheFunction);
  IQLToLLVMValueRef old = IQLToLLVMBuildVariableRef(wrap(ctxt), aggFn.c_str(), NULL);
  BOOST_ASSERT(unwrap(old)->getNull());
  b->CreateCondBr(b->CreateNot(unwrap(old)->getNull()), notNullBlock, nullBlock);
  // Increment value not null case
  b->SetInsertPoint(notNullBlock);    
  // IQLToLLVMValueRef sum = IQLToLLVMBuildAdd(ctxt, 
  // 					    inc,
  // 					    (void *) inputTy,
  // 					    IQLToLLVMBuildVariableRef(ctxt, aggFn.c_str()),
  // 					    (void *) ft,
  // 					    (void *) ft);
  //  IQLToLLVMBuildSetNullableValue(ctxt,
  // 				  &fieldLVal,
  // 				  sum);
  updateNotNull(ctxt, aggFn, inc, inputTy, &fieldLVal, ft, mergeBlock);
  b->CreateBr(mergeBlock);
  // set value null case
  b->SetInsertPoint(nullBlock);    
  // IQLToLLVMBuildSetNullableValue(ctxt, &fieldLVal, inc);
  updateNull(ctxt, aggFn, inc, inputTy, &fieldLVal, ft);
  b->CreateBr(mergeBlock);

  b->SetInsertPoint(mergeBlock);
}

IQLToLLVMValueRef NullInitializedAggregate::initialize(CodeGenerationContext * ctxt,
					   const FieldType * ft)
{
  return IQLToLLVMBuildNull(ctxt);
}

class SumAggregate : public NullInitializedAggregate
{
protected:
  // Pattern for aggregates that initialize to NULL
  // and skip input NULL values.
  void updateNull(CodeGenerationContext * ctxt,
		  const std::string& aggFn,
		  IQLToLLVMValueRef inc,
		  const FieldType * inputTy,
		  const IQLToLLVMLValue * fieldLVal,
		  const FieldType * ft);
  void updateNotNull(CodeGenerationContext * ctxt,
		     const std::string& aggFn,
		     IQLToLLVMValueRef inc,
		     const FieldType * inputTy,
		     const IQLToLLVMLValue * fieldLVal,
		     const FieldType * ft,
		     llvm::BasicBlock * mergeBlock);
public:
  ~SumAggregate() {}
};

void SumAggregate::updateNull(CodeGenerationContext * ctxt,
			      const std::string& aggFn,
			      IQLToLLVMValueRef inc,
			      const FieldType * inputTy,
			      const IQLToLLVMLValue * fieldLVal,
			      const FieldType * ft)
{
  IQLToLLVMBuildSetNullableValue(ctxt, fieldLVal, inc);
}

void SumAggregate::updateNotNull(CodeGenerationContext * ctxt,
				 const std::string& aggFn,
				 IQLToLLVMValueRef inc,
				 const FieldType * inputTy,
				 const IQLToLLVMLValue * fieldLVal,
				 const FieldType * ft,
				 llvm::BasicBlock * mergeBlock)
{
  IQLToLLVMValueRef sum = IQLToLLVMBuildAdd(wrap(ctxt),
  					    inc,
  					    (void *) inputTy,
  					    IQLToLLVMBuildVariableRef(wrap(ctxt), 
								      aggFn.c_str(), NULL),
  					    (void *) ft,
  					    (void *) ft);
   IQLToLLVMBuildSetNullableValue(ctxt,
  				  fieldLVal,
  				  sum);
}


class MaxMinAggregate : public NullInitializedAggregate
{
private:
  bool mIsMax;
protected:
  // Pattern for aggregates that initialize to NULL
  // and skip input NULL values.
  void updateNull(CodeGenerationContext * ctxt,
		  const std::string& aggFn,
		  IQLToLLVMValueRef inc,
		  const FieldType * inputTy,
		  const IQLToLLVMLValue * fieldLVal,
		  const FieldType * ft);
  void updateNotNull(CodeGenerationContext * ctxt,
		     const std::string& aggFn,
		     IQLToLLVMValueRef inc,
		     const FieldType * inputTy,
		     const IQLToLLVMLValue * fieldLVal,
		     const FieldType * ft,
		     llvm::BasicBlock * mergeBlock);
public:
  MaxMinAggregate(bool isMax);
};

MaxMinAggregate::MaxMinAggregate(bool isMax)
  :
  mIsMax(isMax)
{
}

void MaxMinAggregate::updateNull(CodeGenerationContext * ctxt,
			      const std::string& aggFn,
			      IQLToLLVMValueRef inc,
			      const FieldType * inputTy,
			      const IQLToLLVMLValue * fieldLVal,
			      const FieldType * ft)
{
  IQLToLLVMBuildSetNullableValue(ctxt, fieldLVal, inc);
}

void MaxMinAggregate::updateNotNull(CodeGenerationContext * ctxt,
				    const std::string& aggFn,
				    IQLToLLVMValueRef inc,
				    const FieldType * inputTy,
				    const IQLToLLVMLValue * fieldLVal,
				    const FieldType * ft,
				    llvm::BasicBlock * mergeBlock)
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // A block in which to do the update 
  llvm::BasicBlock * updateBlock = 
    llvm::BasicBlock::Create(*c, "then", TheFunction);

  IQLToLLVMValueRef old = IQLToLLVMBuildVariableRef(wrap(ctxt), aggFn.c_str(), NULL);
  IQLToLLVMValueRef condVal = IQLToLLVMBuildCompare(wrap(ctxt), 
						    old, (void *) ft, 
						    inc, (void *) inputTy,
						    // Currently returning int32_t for bool
						    Int32Type::Get(ft->getContext(), true),
						    mIsMax ? IQLToLLVMOpLT : IQLToLLVMOpGT);
  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(llvm::unwrap(unwrap(condVal)->getValue()),
					  b->getInt32(0),
					  "boolCast");
  // Branch and set block
  b->CreateCondBr(boolVal, updateBlock, mergeBlock);
  // Emit update and branch to merge.
  b->SetInsertPoint(updateBlock);    
  IQLToLLVMBuildSetValue(ctxt, inc, aggFn.c_str());
}

boost::shared_ptr<AggregateFunction> AggregateFunction::get(const char * fn)
{
  AggregateFunction * agg = NULL;
  if (boost::algorithm::iequals(fn, "max")) {
    agg = new MaxMinAggregate(true);
  } else if (boost::algorithm::iequals(fn, "min")) {
    agg = new MaxMinAggregate(false);
  } else if (boost::algorithm::iequals(fn, "sum")) {
    agg = new SumAggregate();
  } else {
    throw std::runtime_error ((boost::format("Unknown aggregate function: %1%")%
			       fn).str());
  }
  return boost::shared_ptr<AggregateFunction>(agg);
}

IQLToLLVMValueRef IQLToLLVMBuildAggregateFunction(IQLCodeGenerationContextRef ctxtRef, 
						  const char * fn,
						  IQLToLLVMValueRef e,
						  void * attrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Name of variable of this function
  std::string aggFn = (boost::format("__AggFn%1%__") % ctxt->AggFn).str();
  // Return Type of aggregate function
  const FieldType * ty = (const FieldType *) attrs;
  // Take the return value and increment the aggregate variable
  // in the update context

  // TODO: Get the aggregate function instance
  // from the symbol table.
  boost::shared_ptr<AggregateFunction> agg = AggregateFunction::get(fn);
  agg->update(ctxt, aggFn, e, ty);
  // Move temporarily to the initialization context and provide init
  // for the aggregate variable
  ctxt->restoreAggregateContext(&ctxt->Initialize);

  int saveIsIdentity = ctxt->IsIdentity;
  LLVMSetField(ctxtRef, &ctxt->AggFn, agg->initialize(ctxt, ty));
  ctxt->IsIdentity = saveIsIdentity;

  // Shift back to transfer context and return a reference to the
  // aggregate variable corresponding to this aggregate function.
  ctxt->restoreAggregateContext(&ctxt->Transfer);

  return IQLToLLVMBuildVariableRef(ctxtRef, aggFn.c_str(), NULL);
}

IQLToLLVMValueRef IQLToLLVMBuildIntervalDay(CodeGenerationContext * ctxt,
					    IQLToLLVMValueRef e)
{
  // Only using an Int32 representation of intervals now.
  return e;
}

IQLToLLVMValueRef IQLToLLVMBuildIntervalHour(CodeGenerationContext * ctxt,
					     IQLToLLVMValueRef e)
{
  // Only using an Int32 representation of intervals now.
  return e;
}

IQLToLLVMValueRef IQLToLLVMBuildIntervalMinute(CodeGenerationContext * ctxt,
					       IQLToLLVMValueRef e)
{
  // Only using an Int32 representation of intervals now.
  return e;
}

IQLToLLVMValueRef IQLToLLVMBuildIntervalMonth(CodeGenerationContext * ctxt,
					      IQLToLLVMValueRef e)
{
  // Only using an Int32 representation of intervals now.
  return e;
}

IQLToLLVMValueRef IQLToLLVMBuildIntervalSecond(CodeGenerationContext * ctxt,
					       IQLToLLVMValueRef e)
{
  // Only using an Int32 representation of intervals now.
  return e;
}

IQLToLLVMValueRef IQLToLLVMBuildIntervalYear(CodeGenerationContext * ctxt,
					     IQLToLLVMValueRef e)
{
  // Only using an Int32 representation of intervals now.
  return e;
}

IQLToLLVMValueRef IQLToLLVMBuildInterval(IQLCodeGenerationContextRef ctxtRef,
					 const char * intervalType,
					 IQLToLLVMValueRef e)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  if (boost::algorithm::iequals(intervalType, "day")) {
    return IQLToLLVMBuildIntervalDay(ctxt, e);
  } else if (boost::algorithm::iequals(intervalType, "hour")) {
    return IQLToLLVMBuildIntervalHour(ctxt, e);
  } else if (boost::algorithm::iequals(intervalType, "minute")) {
    return IQLToLLVMBuildIntervalMinute(ctxt, e);
  } else if (boost::algorithm::iequals(intervalType, "month")) {
    return IQLToLLVMBuildIntervalMonth(ctxt, e);
  } else if (boost::algorithm::iequals(intervalType, "second")) {
    return IQLToLLVMBuildIntervalSecond(ctxt, e);
  } else if (boost::algorithm::iequals(intervalType, "year")) {
    return IQLToLLVMBuildIntervalYear(ctxt, e);
  } else {
    throw std::runtime_error((boost::format("Unknown interval type %1%") %
			      intervalType).str());
  }
}

IQLToLLVMValueRef IQLToLLVMBuildLiteralCast(IQLCodeGenerationContextRef ctxtRef, 
					    const char * val,
					    const char * typeName)
{
  if (boost::algorithm::iequals("date", typeName)) {
    return IQLToLLVMBuildDateLiteral(ctxtRef, val);
  } else {
    throw std::runtime_error((boost::format("Invalid type: %1%") %
			      typeName).str());
  }  
}

IQLToLLVMValueRef IQLToLLVMBuildDateLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // Strip quotes
  std::string str(val);
  str = str.substr(1, str.size()-2);
  boost::gregorian::date d = boost::gregorian::from_string(str);
  BOOST_STATIC_ASSERT(sizeof(boost::gregorian::date) == sizeof(int32_t));
  int32_t int32Date = *reinterpret_cast<int32_t *>(&d);

  return IQLToLLVMValue::get(ctxt, 
			     llvm::wrap(b->getInt32(int32Date)),
			     IQLToLLVMValue::eLocal); 
}

IQLToLLVMValueRef IQLToLLVMBuildDatetimeLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // Strip quotes
  std::string str(val);
  str = str.substr(1, str.size()-2);
  BOOST_STATIC_ASSERT(sizeof(boost::posix_time::ptime) == sizeof(int64_t));
  int64_t int64Date = 0;
  if (str.size() == 10) {
    boost::posix_time::ptime t(boost::gregorian::from_string(str));
    int64Date = *reinterpret_cast<int64_t *>(&t);
  } else {
    boost::posix_time::ptime t = boost::posix_time::time_from_string(str);
    int64Date = *reinterpret_cast<int64_t *>(&t);
  }
  return IQLToLLVMValue::get(ctxt, 
			     llvm::wrap(b->getInt64(int64Date)),
			     IQLToLLVMValue::eLocal); 
}

IQLToLLVMValueRef IQLToLLVMBuildDecimalInt32Literal(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return IQLToLLVMValue::get(ctxt, 
			     LLVMConstIntOfString(LLVMInt32TypeInContext(ctxt->LLVMContext), val, 10), 
			     IQLToLLVMValue::eLocal); 
}

IQLToLLVMValueRef IQLToLLVMBuildDecimalInt64Literal(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Peel off the LL suffix
  std::string lit(val);
  lit = lit.substr(0, lit.size()-2);
  return IQLToLLVMValue::get(ctxt, 
			     LLVMConstIntOfString(LLVMInt64TypeInContext(ctxt->LLVMContext), lit.c_str(), 10), 
			     IQLToLLVMValue::eLocal); 
}

IQLToLLVMValueRef IQLToLLVMBuildFloatLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return IQLToLLVMValue::get(ctxt, 
			     LLVMConstRealOfString(LLVMDoubleTypeInContext(ctxt->LLVMContext), val), 
			     IQLToLLVMValue::eLocal); 
}

IQLToLLVMValueRef IQLToLLVMBuildVarcharLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Strip off quotes
  // TODO: Proper unquotification
  int32_t len = strlen(val);
  BOOST_ASSERT(len >= 2);
  
  std::string str(val);
  str = str.substr(1, str.size()-2);
  // Unescape stuff
  boost::replace_all(str, "\\\\", "\\");
  boost::replace_all(str, "\\b", "\b");
  boost::replace_all(str, "\\t", "\t");
  boost::replace_all(str, "\\n", "\n");
  boost::replace_all(str, "\\f", "\f");
  boost::replace_all(str, "\\r", "\r");
  boost::replace_all(str, "\\'", "'");

  // Put the string in as a global variable and then create a struct that references it.  
  // This is the global variable itself
  LLVMValueRef globalVar = LLVMAddGlobal(ctxt->LLVMModule, 
					 LLVMArrayType(LLVMInt8TypeInContext(ctxt->LLVMContext), 
						       str.size() + 1), 
					 "str");
  // This is the string value.  Set it as an initializer.
  LLVMValueRef constStr = LLVMConstStringInContext(ctxt->LLVMContext, 
						   str.c_str(), 
						   str.size(), 
						   0);
  LLVMSetInitializer(globalVar, constStr);

  // Now to reference the global we have to use a const GEP.  To pass
  // by value into the copy method we have to create a stack variable
  // in order to get an address.
  LLVMValueRef constStructMembers[2];
  constStructMembers[0] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), (int32_t) str.size(), true);
  LLVMValueRef constGEPIndexes[2];
  constGEPIndexes[0] = LLVMConstIntOfString(LLVMInt64TypeInContext(ctxt->LLVMContext), "0", 10);
  constGEPIndexes[1] = LLVMConstIntOfString(LLVMInt64TypeInContext(ctxt->LLVMContext), "0", 10);  
  constStructMembers[1] = LLVMConstGEP(globalVar, &constGEPIndexes[0], 2);
  LLVMValueRef globalString = LLVMConstStructInContext(ctxt->LLVMContext, &constStructMembers[0], 2, 0);
  LLVMValueRef globalStringAddr = LLVMCreateEntryBlockAlloca(ctxt, ctxt->LLVMVarcharType, "globalliteral");
  LLVMBuildStore(ctxt->LLVMBuilder, globalString, globalStringAddr);

  return IQLToLLVMValue::get(ctxt, globalStringAddr, IQLToLLVMValue::eGlobal);
}

IQLToLLVMValueRef IQLToLLVMBuildDecimalLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  decimal128 dec;
  decContext decCtxt;
  decContextDefault(&decCtxt, DEC_INIT_DECIMAL128); // no traps, please
  decimal128FromString(&dec, val, &decCtxt);

  // This is the global variable itself
  LLVMValueRef globalVar = LLVMAddGlobal(ctxt->LLVMModule, 
					 ctxt->LLVMDecimal128Type,
					 val);
  // The value to initialize the global
  LLVMValueRef constStructMembers[4];
  for(int i=0 ; i<4; i++) {
    constStructMembers[i] = LLVMConstInt(LLVMInt32TypeInContext(ctxt->LLVMContext), 
					 ((int32_t *) &dec)[i], 
					 true);
  }
  LLVMValueRef globalVal = LLVMConstStructInContext(ctxt->LLVMContext, 
						    &constStructMembers[0], 
						    4, 
						    1);  
  LLVMSetInitializer(globalVar, globalVal);
  return IQLToLLVMValue::get(ctxt, 
			     globalVar,
			     IQLToLLVMValue::eLocal); 
}

IQLToLLVMValueRef IQLToLLVMBuildTrue(IQLCodeGenerationContextRef ctxtRef) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  LLVMContextRef c = ctxt->LLVMContext;
  LLVMValueRef t = LLVMConstInt(LLVMInt32TypeInContext(c), 1, 1);
  return IQLToLLVMValue::get(ctxt, t, IQLToLLVMValue::eLocal);
}

IQLToLLVMValueRef IQLToLLVMBuildFalse(IQLCodeGenerationContextRef ctxtRef) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  LLVMContextRef c = ctxt->LLVMContext;
  LLVMValueRef f = LLVMConstInt(LLVMInt32TypeInContext(c), 0, 1);
  return IQLToLLVMValue::get(ctxt, f, IQLToLLVMValue::eLocal);
}

IQLToLLVMValueRef IQLToLLVMBuildNull(CodeGenerationContext * ctxt) {
  LLVMContextRef c = ctxt->LLVMContext;
  LLVMValueRef f = LLVMConstInt(LLVMInt1TypeInContext(c), 0, 1);
  return IQLToLLVMValue::get(ctxt, NULL, llvm::unwrap(f), IQLToLLVMValue::eLocal);
}

IQLToLLVMValueRef IQLToLLVMBuildNull(IQLCodeGenerationContextRef ctxtRef) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return IQLToLLVMBuildNull(ctxt);
}

void IQLToLLVMNotImplemented()
{
  throw std::runtime_error("IQLToLLVM: Not yet implemented");
}

void IQLToLLVMBuildSetNullableValue(IQLCodeGenerationContextRef ctxtRef,
				    IQLToLLVMLValueRef lvalRef,
				    IQLToLLVMValueRef val,
				    void * attrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const IQLToLLVMLValue * lval = unwrap(lvalRef);
  const FieldType * resultTy = (const FieldType *) attrs;
  IQLToLLVMValueRef cvt = 
    IQLToLLVMBinaryConversion::convertTo(ctxt, val, resultTy);
  IQLToLLVMBuildSetNullableValue(ctxt, lval, cvt);
}

void IQLToLLVMBuildSetNullableValue(CodeGenerationContext * ctxt,
				    const IQLToLLVMLValue * lval,
				    IQLToLLVMValueRef val)
{
  return IQLToLLVMBuildSetNullableValue(ctxt, lval, val, false);
}

// set an rvalue into an lvalue handling nullability
// if allowNullToNonNull is false this throws if the 
// rvalue is nullable and the lvalue is not nullable.
// if true this is allowed.  It is incumbent on the caller
// to know if sufficient runtime checks have been performed
// in order for us to safely coerce a nullable value into
// a nonnullable one (e.g. ISNULL(x,y) is the canonical example).
void IQLToLLVMBuildSetNullableValue(CodeGenerationContext * ctxt,
				    const IQLToLLVMLValue * lval,
				    IQLToLLVMValueRef val,
				    bool allowNullToNonNull)
{
  // Check nullability of value and target
  if (lval->isNullable()) {
    // Unwrap to C++
    llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
    llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
    if (unwrap(val)->isLiteralNull()) {
      // NULL literal
      lval->setNull(ctxt, true);
    } else if (unwrap(val)->getNull() != NULL) {
      // Set or clear NULL bit.  Only set value if val is NOT NULL
      // Code we are generating is the following pseudocode:
      // if (!isNull(val)) {
      //   clearNull(member);
      //   setValue(member, val);
      // } else {
      //   setNull(member);
      //
    
      // The function we are working on.
      llvm::Function *f = b->GetInsertBlock()->getParent();
      // Create blocks for the then/value and else (likely to be next conditional).  
      // Insert the 'then/value' block at the end of the function.
      llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
      llvm::BasicBlock * elseBB = llvm::BasicBlock::Create(*c, "else", f);
      llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "cont", f);
      // Test if NULL and branch
      b->CreateCondBr(b->CreateNot(unwrap(val)->getNull()), 
		      thenBB, elseBB);
      // Emit then value.
      b->SetInsertPoint(thenBB);  
      lval->setNull(ctxt, false);
      IQLToLLVMBuildSetValue2 (wrap(ctxt), val, lval->getValuePointer(ctxt));
      b->CreateBr(mergeBB);

      // Now the NULL case: here we just clear the NULL bit
      b->SetInsertPoint(elseBB);
      lval->setNull(ctxt, true);
      b->CreateBr(mergeBB);
      b->SetInsertPoint(mergeBB);
    } else {
      // Setting non-nullable value into a nullable lvalue.
      IQLToLLVMBuildSetValue2 (wrap(ctxt), val, lval->getValuePointer(ctxt));
      lval->setNull(ctxt, false);
    }
  } else {
    BOOST_ASSERT(allowNullToNonNull ||
		 unwrap(val)->getNull() == NULL);
    IQLToLLVMBuildSetValue2 (wrap(ctxt), val, lval->getValuePointer(ctxt));
  }
}

// This logic identifies LLVM values that are direct references 
// to an input record.  The purpose here is to identify transfers
// that are actually identity functions.  The point is that there
// are cases in which knowing a transfer is identity may allow a
// record buffer to be passed through without modification thus
// completely bypassing memory allocation and transfer code.
// References to fields may be reference or value.

void LLVMSetField(IQLCodeGenerationContextRef ctxtRef, 
		  int * pos, IQLToLLVMValueRef val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const RecordType * outputRecord = unwrap(ctxt->IQLOutputRecord);
  std::string memberName = outputRecord->GetMember(*pos).GetName();
  FieldAddress outputAddress = outputRecord->getFieldAddress(memberName);

  IQLToLLVMField fieldLVal(ctxt, outputRecord, memberName, "__OutputPointer__");

  *pos += 1;

  // Track whether this transfer is an identity or not.
  std::map<std::string, std::pair<std::string, const RecordType *> >& recordTypes(*unwrap(ctxt->IQLRecordArguments));
  if (recordTypes.size() == 1 && 
      recordTypes.begin()->second.second->size() == outputRecord->size()) {
    // TODO: Verify that the types are structurally the same.
    const std::string& inputArg(recordTypes.begin()->second.first);
    const RecordType * inputType = recordTypes.begin()->second.second;
    // Now check whether we are copying a field directly from input
    // to output at exactly the same offset/address
    llvm::Value * llvmVal = llvm::unwrap(unwrap(val)->getValue());
    if (NULL == llvmVal) {
      ctxt->IsIdentity = false;
    } else {
      if (llvm::LoadInst * load = llvm::dyn_cast<llvm::LoadInst>(llvmVal)) {
	llvmVal = load->getOperand(0);
      }
      FieldAddress inputAddress;
      llvm::Value * inputBase = llvm::unwrap(ctxt->lookupBasePointer(inputArg.c_str())->getValue());
      if (!inputType->isMemberPointer(llvmVal, inputBase, inputAddress) ||
	  inputAddress != outputAddress) {    
	ctxt->IsIdentity = false;
      }
    }
  } else {
    ctxt->IsIdentity = false;
  }

  IQLToLLVMBuildSetNullableValue(ctxt, &fieldLVal, val);
}

void IQLToLLVMBuildBitcpy(CodeGenerationContext * ctxt,
			  const BitcpyOp& op,
			  LLVMValueRef sourcePtr,
			  LLVMValueRef targetPtr)
{
  // Implement shift as targetDword = targetDword&targetMask | ((sourceWord & sourceMask) >> N)
  // for an appropriate targetMask

  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  
  // Get int32 ptrs for source and target
  llvm::Type * int32PtrTy = llvm::PointerType::get(b->getInt32Ty(), 0);
  llvm::Value * source = op.mSourceOffset.getPointer("bitcpySource", ctxt, 
						     llvm::unwrap(sourcePtr));
  source = b->CreateBitCast(source, int32PtrTy);
  llvm::Value * target = op.mTargetOffset.getPointer("bitcpyTarget", ctxt, 
						     llvm::unwrap(targetPtr));
  target = b->CreateBitCast(target, int32PtrTy);

  if (op.mShift < 0) {
    // We are shift source to the right.  Get mask for target
    // bits that we should leave in place.  Be careful here to
    // get 1's in any shifted in positions of targetMask by shifting before
    // negating.
    int32_t sourceShift = -op.mShift;
    uint32_t targetMask = ~((op.mSourceBitmask) >> sourceShift);
    llvm::Value * maskedTgt = b->CreateAnd(b->CreateLoad(target),
					   b->getInt32(targetMask));
    llvm::Value * maskedSource = b->CreateAnd(b->CreateLoad(source),
					      b->getInt32(op.mSourceBitmask));
    maskedSource = b->CreateLShr(maskedSource, sourceShift);
    b->CreateStore(b->CreateOr(maskedTgt, maskedSource),
		   target);
  } else if (op.mShift > 0) {
    // We are shift source to the left
    int32_t sourceShift = op.mShift;
    uint32_t targetMask = ~((op.mSourceBitmask) << sourceShift);
    llvm::Value * maskedTgt = b->CreateAnd(b->CreateLoad(target),
					   b->getInt32(targetMask));
    llvm::Value * maskedSource = b->CreateAnd(b->CreateLoad(source),
					      b->getInt32(op.mSourceBitmask));
    maskedSource = b->CreateShl(maskedSource, sourceShift);
    b->CreateStore(b->CreateOr(maskedTgt, maskedSource),
		   target);
  } else {
    uint32_t targetMask = ~op.mSourceBitmask;
    llvm::Value * maskedTgt = b->CreateAnd(b->CreateLoad(target),
					   b->getInt32(targetMask));
    llvm::Value * maskedSource = b->CreateAnd(b->CreateLoad(source),
					      b->getInt32(op.mSourceBitmask));
    b->CreateStore(b->CreateOr(maskedTgt, maskedSource),
		   target);
  }
}

void IQLToLLVMBuildBitset(CodeGenerationContext * ctxt,
			  const BitsetOp& op,
			  LLVMValueRef targetPtr)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  
  // Get int32 ptr for target
  llvm::Type * int32PtrTy = llvm::PointerType::get(b->getInt32Ty(), 0);
  llvm::Value * target = op.mTargetOffset.getPointer("bitsetTarget", ctxt, 
						     llvm::unwrap(targetPtr));
  target = b->CreateBitCast(target, int32PtrTy);

  // Set bits in bitmask preserving any bits outside mask that
  // are already set.
  llvm::Value * tgt = b->CreateLoad(target);
  llvm::Value * newBits = b->getInt32(op.mTargetBitmask);
  b->CreateStore(b->CreateOr(tgt, newBits),
		 target);
}

static void LLVMSetFieldsRegex(CodeGenerationContext * ctxt, 
			       const std::string& sourceName,
			       const RecordType * sourceType,
			       const std::string& expr,
			       const std::string& rename,
			       const std::string& recordName,
			       int * pos)
{
  LLVMValueRef sourcePtr = LLVMBuildLoad(ctxt->LLVMBuilder,
					 ctxt->lookupBasePointer(sourceName.c_str())->getValue(),
					 "srccpy");	
  LLVMValueRef targetPtr = LLVMBuildLoad(ctxt->LLVMBuilder,
					 ctxt->lookupBasePointer("__OutputPointer__")->getValue(),
					 "tgtcpy");
			       
  RecordTypeCopy c(sourceType,
		   unwrap(ctxt->IQLOutputRecord),
		   expr,
		   rename, 
		   pos);

  // Perform bitset as needed
  for(std::vector<BitsetOp>::const_iterator opit = c.getBitset().begin();
      opit != c.getBitset().end();
      ++opit) {
    IQLToLLVMBuildBitset(ctxt, *opit, targetPtr);			  
  }
  // Perform bitcpy as needed
  for(std::vector<BitcpyOp>::const_iterator opit = c.getBitcpy().begin();
      opit != c.getBitcpy().end();
      ++opit) {
    IQLToLLVMBuildBitcpy(ctxt, *opit, sourcePtr, targetPtr);
  }
  // Perform memcpy's as needed.
  // Explicitly copy fields as needed.
  for(std::vector<MemcpyOp>::const_iterator opit = c.getMemcpy().begin();
      opit != c.getMemcpy().end();
      ++opit) {
    IQLToLLVMCallMemcpy(ctxt,
			sourcePtr,
			opit->mSourceOffset,
			targetPtr,
			opit->mTargetOffset,
			opit->mSize);
			  
  }
  for(RecordTypeCopy::set_type::const_iterator fit = c.getSet().begin();
      fit != c.getSet().end();
      ++fit) {
    int tmp=fit->second;
    LLVMSetField(wrap(ctxt),
		 &tmp, 
		 IQLToLLVMBuildVariableRef(wrap(ctxt), 
					   recordName.size() ? recordName.c_str() : fit->first.c_str(), 
					   recordName.size() ? fit->first.c_str() : NULL));
  }
}

void LLVMSetFields(IQLCodeGenerationContextRef ctxtRef, 
		   const char * recordName, int * pos)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  typedef std::map<std::string, std::pair<std::string, const RecordType *> > RecordArgs;
  // Copy all fields from the source record to the output.
  const RecordArgs& recordTypes(*unwrap(ctxt->IQLRecordArguments));
  // Find the record struct in the named inputs.
  RecordArgs::const_iterator it = recordTypes.find(recordName);
  if (it == recordTypes.end()) {
    std::string namedInputs;
    for(RecordArgs::const_iterator argIt = recordTypes.begin();
	argIt != recordTypes.end();
	++argIt) {
      if (namedInputs.size()) namedInputs += ", ";
      namedInputs += argIt->first;
    }
    throw std::runtime_error((boost::format("Undefined input record: %1%."
					    " Defined records: %2%.") % 
			      recordName %
			      namedInputs).str());
  }
  // If move semantics specified then we can just reduce this to a
  // couple of memcpy's and memset's.  Replace all source fields in the symbol table with
  // corresponding targets.
  if(ctxt->IQLMoveSemantics) {
    RecordTypeMove mv(it->second.second, unwrap(ctxt->IQLOutputRecord));
    for(std::vector<MemcpyOp>::const_iterator opit = mv.getMemcpy().begin();
	opit != mv.getMemcpy().end();
	++opit) {
      // TODO: How do I actually know what input argument these are bound to?  It should be resolved
      // by the above query to IQLInputRecords.
      IQLToLLVMCallMemcpy(ctxt,
			  it->second.first,
			  opit->mSourceOffset,
			  "__OutputPointer__",
			  opit->mTargetOffset,
			  opit->mSize);
    }
    for(std::vector<MemsetOp>::const_iterator opit = mv.getMemset().begin();
	opit != mv.getMemset().end();
	++opit) {
      IQLToLLVMCallMemset(ctxt,
			  LLVMBuildLoad(ctxt->LLVMBuilder,
					ctxt->lookupBasePointer(it->second.first.c_str())->getValue(),
					"srccpy"),
			  opit->mSourceOffset,
			  0,
			  opit->mSize);
    }
    
    // For each input member moved, get address of the corresponding field in
    // the target but don't put into symbol table.  
    // TODO: Any expressions that reference these values will be broken now!
    // TODO: If the target does not have the same name as the source then
    // we have a bad situation since the source has now been cleared.
    // It is probably better to create a new basic block at the end of
    // the function and put the memset stuff there.
    for(RecordType::const_member_iterator mit = it->second.second->begin_members();
	mit != it->second.second->end_members();
	++mit) {
      const RecordType * out = unwrap(ctxt->IQLOutputRecord);
      std::string memberName = out->GetMember(*pos).GetName();
      out->LLVMMemberGetPointer(memberName, 
				ctxt, 
				llvm::unwrap(ctxt->lookupBasePointer("__OutputPointer__")->getValue()),
				false
				);

      *pos += 1;
    }
  } else {    
    LLVMSetFieldsRegex(ctxt, it->second.first,
		       it->second.second, ".*", "", recordName, pos);
  }
}

void LLVMBuildQuotedId(IQLCodeGenerationContextRef ctxtRef, 
		       const char * quotedId, const char * rename, int * pos)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Strip off backticks.
  std::string strExpr(quotedId);
  strExpr = strExpr.substr(1, strExpr.size() - 2);
  std::string renameExpr(rename ? rename : "``");
  renameExpr = renameExpr.substr(1, renameExpr.size() - 2);
  typedef std::map<std::string, std::pair<std::string, const RecordType *> > RecordArgs;
  const RecordArgs& recordTypes(*unwrap(ctxt->IQLRecordArguments));
  for(RecordArgs::const_iterator it = recordTypes.begin();
      it != recordTypes.end();
      ++it) {
    LLVMSetFieldsRegex(ctxt, it->second.first,
		       it->second.second, strExpr, renameExpr, "", pos);
  }
}

const FieldType * unwrap(IQLFieldTypeRef r)
{
  return reinterpret_cast<const FieldType *> (r);
}

IQLFieldTypeRef wrap(const FieldType * r)
{
  return reinterpret_cast<IQLFieldTypeRef> (r);
}

const RecordType * unwrap(IQLRecordTypeRef r)
{
  return reinterpret_cast<const RecordType *> (r);
}

IQLRecordTypeRef wrap(const RecordType * r)
{
  return reinterpret_cast<IQLRecordTypeRef> (r);
}

std::vector<const FieldType *> * unwrap(IQLFieldTypeVectorRef v)
{
  return reinterpret_cast<std::vector<const FieldType *> *>(v);
}

IQLFieldTypeVectorRef wrap(std::vector<const FieldType *> * ptr)
{
  return reinterpret_cast<IQLFieldTypeVectorRef>(ptr);
}

IQLFieldTypeVectorRef IQLFieldTypeVectorCreate()
{
  return wrap(new std::vector<const FieldType *>());
}

void IQLFieldTypeVectorFree(IQLFieldTypeVectorRef v)
{
  if (v == NULL) return;
  delete unwrap(v);
}

void IQLFieldTypeVectorPushBack(IQLFieldTypeVectorRef v, IQLFieldTypeRef ft)
{
  unwrap(v)->push_back(unwrap(ft));
}

bool IQLTypeCheckCanConvertForCompare(IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  return lhs == rhs;
}

void IQLTypeCheckLoadBuiltinFunctions(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->loadBuiltinFunctions();
}

void IQLTypeCheckBeginRecord(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->beginRecord();
}

void IQLTypeCheckAddFields(IQLTypeCheckContextRef ctxtRef, const char * recordName)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->addFields(recordName);
}

void IQLTypeCheckAddField(IQLTypeCheckContextRef ctxtRef, const char * name, IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->addField(name, unwrap(ty));
}

void IQLTypeCheckQuotedId(IQLTypeCheckContextRef ctxtRef, const char * id, const char * format)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->quotedId(id, format);
}

void IQLTypeCheckBuildRecord(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->buildRecord();
}

void IQLTypeCheckBuildLocal(IQLTypeCheckContextRef ctxtRef, const char * name, IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->buildLocal(name, unwrap(ty));
}

void IQLTypeCheckSetValue2(IQLTypeCheckContextRef ctxt, 
			   IQLFieldTypeRef lhs, 
			   IQLFieldTypeRef rhs)
{
  unwrap(ctxt)->buildSetValue(unwrap(lhs), unwrap(rhs));
}

IQLFieldTypeRef IQLTypeCheckArrayRef(IQLTypeCheckContextRef ctxt, 
				     const char * nm,
				     IQLFieldTypeRef idx)
{
  return wrap(unwrap(ctxt)->buildArrayRef(nm, unwrap(idx)));
}

IQLFieldTypeRef IQLTypeCheckBuildVariableRef(IQLTypeCheckContextRef ctxt, 
					     const char * nm,
					     const char * nm2)
{
  return wrap(unwrap(ctxt)->buildVariableRef(nm, nm2));
}

void IQLTypeCheckBeginSwitch(IQLTypeCheckContextRef ctxt, 
			     IQLFieldTypeRef e)
{
  unwrap(ctxt)->beginSwitch(unwrap(e));
}

IQLFieldTypeRef IQLTypeCheckModulus(IQLTypeCheckContextRef ctxt, 
				    IQLFieldTypeRef lhs, 
				    IQLFieldTypeRef rhs)
{
  return wrap(unwrap(ctxt)->buildModulus(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckSub(IQLTypeCheckContextRef ctxt, 
				    IQLFieldTypeRef lhs, 
				    IQLFieldTypeRef rhs)
{
  return wrap(unwrap(ctxt)->buildSub(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckAdditiveType(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  // TODO: Move this code into TypeCheckContext
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  // Special case handling of datetime/interval
  const FieldType * lhsTy = unwrap(lhs);
  const FieldType * rhsTy = unwrap(rhs);
  bool nullable = lhsTy->isNullable() || rhsTy->isNullable();
  if ((lhsTy->GetEnum() == FieldType::INTERVAL && rhsTy->GetEnum()==FieldType::DATETIME) ||
      (rhsTy->GetEnum() == FieldType::INTERVAL && lhsTy->GetEnum()==FieldType::DATETIME)) {
    return wrap(ctxt->buildDatetimeType(nullable));
  } else if ((lhsTy->GetEnum() == FieldType::INTERVAL && rhsTy->GetEnum()==FieldType::DATE) ||
      (rhsTy->GetEnum() == FieldType::INTERVAL && lhsTy->GetEnum()==FieldType::DATE)) {
    
    return wrap(ctxt->buildAdd(lhsTy, rhsTy));
  } else if (lhsTy->GetEnum() == FieldType::CHAR && rhsTy->GetEnum()==FieldType::CHAR) {
    std::string retSz = boost::lexical_cast<std::string>(lhsTy->GetSize() + 
							 rhsTy->GetSize());
    return wrap(ctxt->buildCharType(retSz.c_str(), nullable));
  }
  // TODO: Implement proper checking for supported types here.
  IQLFieldTypeRef ty = IQLTypeCheckBinaryConversion::leastCommonTypeNullable(ctxt, lhs, rhs);
  if (ty == NULL)
    throw std::runtime_error("Type check error");
  return ty;
}

IQLFieldTypeRef IQLTypeCheckNegateType(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  // Get corresponding non NULL type for checking.
  const FieldType * ty = unwrap(lhs)->clone(false);
  if (ty != Int32Type::Get(ctxt->mContext) &&
      ty != Int64Type::Get(ctxt->mContext) &&
      ty != DoubleType::Get(ctxt->mContext) &&
      ty != DecimalType::Get(ctxt->mContext))
    throw std::runtime_error("Type check error: expected INTEGER, BIGINT, DOUBLE PRECISION or DECIMAL");
  return lhs;
}

IQLFieldTypeRef IQLTypeCheckMultiplicativeType(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = unwrap(lhs);
  const FieldType * rhsType = unwrap(rhs);
  // TODO: Implement proper checking for supported types here.
  IQLFieldTypeRef ty = 
    IQLTypeCheckBinaryConversion::leastCommonTypeNullable(ctxt, lhs, rhs);
  if (ty == NULL)
    throw std::runtime_error((boost::format("Cannot multiply types %1% and %2%") %
			      lhsType->toString() % rhsType->toString()).str());
  return ty;
}

IQLFieldTypeRef IQLTypeCheckBitwiseType(IQLTypeCheckContextRef ctxtRef, 
					IQLFieldTypeRef lhs, 
					IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildBitwise(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckUnaryBitwiseType(IQLTypeCheckContextRef ctxtRef, 
					     IQLFieldTypeRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildBitwise(unwrap(lhs)));
}

IQLFieldTypeRef IQLTypeCheckHash(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeVectorRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  // For the moment, we can hash anything.  Always returns an integer.
  return wrap(Int32Type::Get(ctxt->mContext));
}

IQLFieldTypeRef IQLTypeCheckSortPrefix(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeVectorRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  // For the moment, we can sort anything.  Always returns an integer.
  return wrap(Int32Type::Get(ctxt->mContext));
}

IQLFieldTypeRef IQLTypeCheckCall(IQLTypeCheckContextRef ctxtRef, 
				 const char * f,
				 IQLFieldTypeVectorRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  // Check that the arguments can be converted to formals.
  std::vector<const FieldType *>& args(*unwrap(lhs));
  // Check for intrinsics
  if (boost::algorithm::iequals(f, "least") ||
      boost::algorithm::iequals(f, "greatest")) {
    return wrap(ctxt->buildLeast(args));
  } else if (boost::algorithm::iequals(f, "isnull")) {
    return wrap(ctxt->buildIsNull(args));
  }
  // TODO: Implement operator/function overloading.
  IQLFieldTypeRef funTypeRef = IQLTypeCheckSymbolTableGetType(ctxtRef, f);
  if (funTypeRef == NULL || unwrap(funTypeRef)->GetEnum() != FieldType::FUNCTION)
    throw std::runtime_error((boost::format("Undefined function in call %1%") %
			      f).str());
  const FunctionType * funType = static_cast<const FunctionType *> (unwrap(funTypeRef));
  if (funType->GetArgs().size() !=  args.size())
    throw std::runtime_error((boost::format("Function %1% takes %2% arguments") %
			      f %
			      funType->GetArgs().size()).str());
  for(std::size_t i=0; i<args.size(); ++i) {
    const FieldType * formalTy = funType->GetArgs()[i];
    IQLFieldTypeRef ty = IQLTypeCheckBinaryConversion::castTo(ctxt, 
							      wrap(args[i]),
							      wrap(formalTy));
    if (ty == NULL)
      throw std::runtime_error((boost::format("Argument %1% of function %2% type mismatch; expected %3% received %4%") %
				i % f % formalTy->toString() %
				args[i]->toString()).str());
  }
  return wrap(funType->GetReturn());
}

IQLFieldTypeRef IQLTypeCheckEquals(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  IQLFieldTypeRef ty = IQLTypeCheckBinaryConversion::leastCommonTypeNullable(ctxt, lhs, rhs);
  if (ty == NULL) 
    throw std::runtime_error((boost::format("Type check error: cannot compare "
					    "expressions of type %1% and %2%")
			      % unwrap(lhs)->toString() 
			      % unwrap(rhs)->toString()).str());
  // TODO: Currently returning int32_t for boolean (be smarter here).
  return wrap(ctxt->buildBooleanType(unwrap(ty)->isNullable()));
}

IQLFieldTypeRef IQLTypeCheckAnd(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsTy = unwrap(lhs);
  const FieldType * rhsTy = unwrap(rhs);
  // Must be bool (currently int32_t for hacky reasons).
  const FieldType * bType = ctxt->buildBooleanType(true);
  // Check type without regard to nullability
  if (lhsTy->clone(true) != bType || rhsTy->clone(true) != bType)
    throw std::runtime_error("Expected boolean expression");
  bool isNullable = lhsTy->isNullable() || rhsTy->isNullable();
  return wrap(bType->clone(isNullable));
}

IQLFieldTypeRef IQLTypeCheckNot(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  // Must be bool (currently int32_t for hacky reasons).
  const FieldType * bType = ctxt->buildBooleanType(true);
  if (unwrap(lhs)->clone(true) != bType)
    throw std::runtime_error("Expected boolean expression");
  return lhs;
}

IQLFieldTypeRef IQLTypeCheckRLike(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsTy = unwrap(lhs);
  const FieldType * rhsTy = unwrap(rhs);
  if (lhsTy->GetEnum() != FieldType::VARCHAR) {
    throw std::runtime_error("Expected varchar expression for left hand side of RLIKE expression");
  }
  if (rhsTy->GetEnum() != FieldType::VARCHAR) {
    throw std::runtime_error("Expected varchar expression for right hand side of RLIKE expression");
  }
  const bool isNullable = lhsTy->isNullable() || rhsTy->isNullable();
  return wrap(ctxt->buildBooleanType(isNullable));
}

IQLFieldTypeRef IQLTypeCheckIsNull(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, int isNotNull)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildBooleanType(false));
}

IQLFieldTypeRef IQLTypeCheckBuildInt32Type(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildInt32Type(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildInt64Type(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildInt64Type(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildDoubleType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDoubleType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildDecimalType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDecimalType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildDatetimeType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDatetimeType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildDateType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDateType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildNVarcharType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  throw std::runtime_error("NVARCHAR type not yet supported in IQL");
}

IQLFieldTypeRef IQLTypeCheckBuildVarcharType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildVarcharType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildCharType(IQLTypeCheckContextRef ctxtRef,
					  const char * sz, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildCharType(sz, nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildBooleanType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildBooleanType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildNilType(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildNilType());
}

IQLFieldTypeRef IQLTypeCheckBuildType(IQLTypeCheckContextRef ctxtRef,
				      const char * typeName, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildType(typeName, nullable != 0));
}

void IQLTypeCheckBeginCase(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->beginCase();
}

void IQLTypeCheckAddCondition(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef cond)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->addCondition(unwrap(cond));
}

void IQLTypeCheckAddValue(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef val)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->addValue(unwrap(val));
}

IQLFieldTypeRef IQLTypeCheckBuildCase(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildCase());
}

IQLFieldTypeRef IQLTypeCheckIfThenElse(IQLTypeCheckContextRef ctxtRef, 
				       IQLFieldTypeRef condVal, 
				       IQLFieldTypeRef thenVal, 
				       IQLFieldTypeRef elseVal)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIfThenElse(unwrap(condVal),
				    unwrap(thenVal),
				    unwrap(elseVal)));
}

IQLFieldTypeRef IQLTypeCheckCast(IQLTypeCheckContextRef ctxt, 
				 IQLFieldTypeRef lhs, 
				 IQLFieldTypeRef target)
{
  bool isNullable = unwrap(target)->isNullable() ||
    unwrap(lhs)->isNullable();
  return wrap(unwrap(target)->clone(isNullable));
}

void IQLTypeCheckBeginAggregateFunction(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->beginAggregateFunction();
}

IQLFieldTypeRef IQLTypeCheckBuildAggregateFunction(IQLTypeCheckContextRef ctxtRef, 
						   IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildAggregateFunction(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalDay(IQLTypeCheckContextRef ctxtRef, 
					     IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalDay(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalHour(IQLTypeCheckContextRef ctxtRef, 
					      IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalHour(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalMinute(IQLTypeCheckContextRef ctxtRef, 
						IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalMinute(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalMonth(IQLTypeCheckContextRef ctxtRef, 
					       IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalMonth(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalSecond(IQLTypeCheckContextRef ctxtRef, 
						IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalSecond(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalYear(IQLTypeCheckContextRef ctxtRef, 
					      IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalYear(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildInterval(IQLTypeCheckContextRef ctxtRef,
					 const char * intervalType,
					 IQLFieldTypeRef ty)
{
  if (boost::algorithm::iequals(intervalType, "day")) {
    return IQLTypeCheckBuildIntervalDay(ctxtRef, ty);
  } else if (boost::algorithm::iequals(intervalType, "hour")) {
    return IQLTypeCheckBuildIntervalHour(ctxtRef, ty);
  } else if (boost::algorithm::iequals(intervalType, "minute")) {
    return IQLTypeCheckBuildIntervalMinute(ctxtRef, ty);
  } else if (boost::algorithm::iequals(intervalType, "month")) {
    return IQLTypeCheckBuildIntervalMonth(ctxtRef, ty);
  } else if (boost::algorithm::iequals(intervalType, "second")) {
    return IQLTypeCheckBuildIntervalSecond(ctxtRef, ty);
  } else if (boost::algorithm::iequals(intervalType, "year")) {
    return IQLTypeCheckBuildIntervalYear(ctxtRef, ty);
  } else {
    throw std::runtime_error("INTERVAL type must be one of DAY, HOUR, MINUTE, "
			     "MONTH, SECOND or YEAR");
  }
}

IQLFieldTypeRef IQLTypeCheckArray(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeVectorRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  const std::vector<const FieldType*> & ty(*unwrap(lhs));
  return wrap(ctxt->buildArray(ty));
}

IQLFieldTypeRef IQLTypeCheckSymbolTableGetType(IQLTypeCheckContextRef ctxtRef, const char * name)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->lookupType(name, NULL));
}

void IQLGraphNodeStart(IQLGraphContextRef ctxt, const char * type, const char * name)
{
  unwrap(ctxt)->nodeStart(type, name);
}

void IQLGraphNodeBuildIntegerParam(IQLGraphContextRef ctxt, const char * name, const char * val)
{
  unwrap(ctxt)->nodeAddIntegerParam(name, val);
}

void IQLGraphNodeBuildStringParam(IQLGraphContextRef ctxt, const char * name, const char * val)
{
  unwrap(ctxt)->nodeAddStringParam(name, val);
}

void IQLGraphNodeComplete(IQLGraphContextRef ctxt)
{
  unwrap(ctxt)->nodeComplete();
}

void IQLGraphNodeBuildEdge(IQLGraphContextRef ctxt, const char * from, const char * to)
{
  unwrap(ctxt)->edgeBuild(from, to);
}

class IQLGraphBuilder * unwrap(IQLGraphContextRef val)
{
  return reinterpret_cast<IQLGraphBuilder *>(val);
}

IQLGraphContextRef wrap(class IQLGraphBuilder * val)
{
  return reinterpret_cast<IQLGraphContextRef>(val);
}

void IQLRecordTypeBuildField(IQLRecordTypeContextRef ctxt, 
			     const char * name, 
			     IQLFieldTypeRef ty)
{
  if (NULL == ty) {
    throw std::runtime_error("Invalid primitive type");
  }
  const FieldType * ft = unwrap(ty);
  unwrap(ctxt)->buildField(name, ft);
}

class IQLRecordTypeBuilder * unwrap(IQLRecordTypeContextRef val)
{
  return reinterpret_cast<IQLRecordTypeBuilder *>(val);
}

IQLRecordTypeContextRef wrap(class IQLRecordTypeBuilder * val)
{
  return reinterpret_cast<IQLRecordTypeContextRef>(val);
}

class TypeCheckContext * unwrap(IQLTypeCheckContextRef ctxt)
{
  return reinterpret_cast<class TypeCheckContext *>(ctxt);
}

IQLTypeCheckContextRef wrap(class TypeCheckContext * val)
{
  return reinterpret_cast<IQLTypeCheckContextRef>(val);
}

