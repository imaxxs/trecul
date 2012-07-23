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

#ifndef __CODEGENERATIONCONTEXT_HH
#define __CODEGENERATIONCONTEXT_HH

#include <map>
#include <stack>
#include <string>
#include <llvm-c/Core.h>

#include <boost/dynamic_bitset.hpp>

namespace llvm {
  class BasicBlock;
  class ConstantInt;
  class Type;
  class Value;
}
class CodeGenerationContext;
class FieldType;

typedef struct IQLToLLVMStruct * LLVMSymbolTableRef;
typedef struct IQLToLLVMRecordMapStruct * IQLToLLVMRecordMapRef;
typedef const struct IQLRecordTypeStruct * IQLRecordTypeRef;
typedef const struct IQLToLLVMValueStruct * IQLToLLVMValueRef;

IQLToLLVMRecordMapRef wrap(std::map<std::string, std::pair<std::string, const class RecordType*> > * r);
std::map<std::string, std::pair<std::string, const class RecordType*> > * unwrap(IQLToLLVMRecordMapRef r);
const class RecordType * unwrap(IQLRecordTypeRef r);
IQLRecordTypeRef wrap(const class RecordType * r);

/**
 * An IQL r-value.
 * 
 * Represents the value of an IQL expression.  The value
 * may be nullable or not.  In the former case, this contains
 * a boolean value indicating whether the value is NULL or not.
 * Note that some IQL values are passed around "by-value" and some
 * are passed around "by reference".  In general, values that are
 * 8 bytes or less are passed by value and those bigger than 8 bytes
 * are passed by reference.  Exceptions to this rule are CHAR(N) values
 * which are currently passed by reference even when they are less than 
 * 8 bytes.
 * For all fixed length types, storage is allocated on the
 * stack or inline in the target record struct so there is no
 * need for reference counting.  For variable length types (e.g.
 * VARCHAR) there is heap allocated data in play and we must 
 * make sure that data is not leaked.
 */
class IQLToLLVMValue
{
public:
  enum ValueType { eGlobal, eShallowGlobal, eLocal };
private:
  // The actual value
  LLVMValueRef mValue;
  // NULL bit.  This should be non-NULL only on nullable values.
  llvm::Value * mIsNull;
  // Is this value a pointer to a global location,
  // a shallow copy of a global location 
  // (hence it may contain global pointers) or a completely
  // local value.
  ValueType mValueType;
public:
  IQLToLLVMValue (LLVMValueRef val, ValueType globalOrLocal);
  
  IQLToLLVMValue (LLVMValueRef val, llvm::Value * isNull, 
		  ValueType globalOrLocal);

  LLVMValueRef getValue() const;
  llvm::Value * getNull() const;
  void setNull(llvm::Value * nv);
  bool isLiteralNull() const;
  ValueType getValueType() const;
  static LLVMTypeRef getVariableType(CodeGenerationContext * ctxt,
				     const FieldType * ft);
  static bool isValueType(LLVMTypeRef ty) ;
  static bool isPointerToValueType(LLVMTypeRef ty) ;
  bool isValueType() const ;
  bool isPointerToValueType() const ;
  bool isReferenceType() const;
  static IQLToLLVMValueRef get(CodeGenerationContext * ctxt, 
			       LLVMValueRef val, 
			       IQLToLLVMValue::ValueType globalOrLocal);
  static IQLToLLVMValueRef get(CodeGenerationContext * ctxt, 
			       LLVMValueRef val,
			       llvm::Value * nv,
			       IQLToLLVMValue::ValueType globalOrLocal);
};

/**
 * An IQL l-value
 *
 * The subtle thing about this abstraction is the opaqueness of the
 * NULL bit.  We do not assume that a NULL bit has an address so
 * we provide an operational interface that allows the bit to be
 * set but hides the representation.  This is necessary because we want
 * to allow a bit in a bitfield to represent the NULL and we cannot
 * take the address of a bit.
 */
class IQLToLLVMLValue
{
public:
  virtual ~IQLToLLVMLValue() {}
  /**
   * Retrieve the current pointer to value without retrieving
   * the NULL bit.
   */  
  virtual IQLToLLVMValueRef getValuePointer(CodeGenerationContext * ctxt) const =0;
  /**
   * Retrieve pointer to the value + the current value of
   * the NULL bit.
   */
  virtual const IQLToLLVMValue * getEntirePointer(CodeGenerationContext * ctxt) const =0;
  virtual void setNull(CodeGenerationContext * ctxt, bool isNull) const =0;
  virtual bool isNullable() const =0;
};

/**
 * LValue abstraction around a field in an IQL record.
 * These guys have an ordinary pointer to the data value
 * but in the nullable case they sit on top of a bit position
 * in a bit field for the null indicator.
 */
class IQLToLLVMField : public IQLToLLVMLValue
{
private:
  std::string mMemberName;
  llvm::Value * mBasePointer;
  const RecordType * mRecordType;
public:
  IQLToLLVMField(CodeGenerationContext * ctxt,
		 const RecordType * recordType,
		 const std::string& memberName,
		 const std::string& recordName);
  IQLToLLVMField(const RecordType * recordType,
		 const std::string& memberName,
		 llvm::Value * basePointer);
  ~IQLToLLVMField();
  IQLToLLVMValueRef getValuePointer(CodeGenerationContext * ctxt) const;
  const IQLToLLVMValue * getEntirePointer(CodeGenerationContext * ctxt) const;
  void setNull(CodeGenerationContext * ctxt, bool isNull) const;
  bool isNullable() const;
};

class IQLToLLVMLocal : public IQLToLLVMLValue
{
private:
  const IQLToLLVMValue * mValue;
  // Pointer to alloca'd i1 (will likely be lowered to an i8).
  llvm::Value * mNullBit;
public:
  IQLToLLVMLocal(const IQLToLLVMValue * lval,
		 llvm::Value * lvalNull);

  ~IQLToLLVMLocal();

  IQLToLLVMValueRef getValuePointer(CodeGenerationContext * ctxt) const;
  const IQLToLLVMValue * getEntirePointer(CodeGenerationContext * ctxt) const;
  llvm::Value * getNullBitPointer() const;
  void setNull(CodeGenerationContext * ctxt, bool isNull) const;
  bool isNullable() const;
};

class IQLToLLVMStackRecord
{
public:
  llvm::BasicBlock * ThenBB;
  llvm::BasicBlock * ElseBB;
  llvm::BasicBlock * MergeBB;
};

class IQLToLLVMSwitchRecord
{
public:
  // The basic block from which we switch.
  // Saved so we can insert switch inst after we know
  // all about the cases.
  llvm::BasicBlock * Top;
  // The cases.
  std::vector<std::pair<llvm::ConstantInt*, llvm::BasicBlock *> > Cases;
  // The Exit block.  Here so cases can branch to it.
  llvm::BasicBlock * Exit;
};

// The merge block of current CASE
class IQLToLLVMCaseState
{
public:
  class IQLToLLVMLocal * Local;
  llvm::BasicBlock * MergeBB;
  llvm::BasicBlock * ElseBB;
  IQLToLLVMCaseState(class IQLToLLVMLocal * local, llvm::BasicBlock * mergeBB)
    :
    Local(local),
    MergeBB(mergeBB),
    ElseBB(NULL)
  {
  }
  ~IQLToLLVMCaseState()
  {
    delete Local;
  }

  llvm::BasicBlock * getMergeBlock()
  {
    return MergeBB;
  }
};

class SymbolTable
{
private:
  typedef std::map<std::string, IQLToLLVMLValue *> table_type;
  table_type mSymbols;
public:
  SymbolTable();
  ~SymbolTable();
  IQLToLLVMLValue * lookup(const char * nm) const;
  void add(const char * nm, IQLToLLVMLValue * value);
  void clear();
  void dump() const;
};

class CodeGenerationFunctionContext {
public:
  LLVMBuilderRef Builder;
  class TreculSymbolTable * mSymbolTable;
  LLVMValueRef Function;
  IQLToLLVMRecordMapRef RecordArguments;
  IQLRecordTypeRef OutputRecord;
  void * AllocaCache;
  CodeGenerationFunctionContext();
};

// TODO: Should have just made this whole thing opaque rather
// than trying to expose a lower level API to the ANTLR tree
// parser.
class CodeGenerationContext {
public:
  /**
   * Type of the cache of alloca'd locals
   * that we can reuse.  Keeping the number
   * of these small can make a big impact on the
   * amount of memory used during SSA creation (Mem2Reg pass).
   */
  typedef std::map<const llvm::Type*, 
		   std::vector<const llvm::Value *> > local_cache;
  
private:
  bool mOwnsModule;
  class TreculSymbolTable * mSymbolTable;
public:
  LLVMContextRef LLVMContext;
  LLVMModuleRef LLVMModule;
  LLVMBuilderRef LLVMBuilder;
  LLVMTypeRef LLVMDecContextPtrType;
  LLVMTypeRef LLVMDecimal128Type;
  LLVMTypeRef LLVMVarcharType;
  LLVMTypeRef LLVMDatetimeType;
  // This is set by the code generator not by the caller
  LLVMValueRef LLVMFunction;
  // Alias to record type mapping for inputs
  IQLToLLVMRecordMapRef IQLRecordArguments;
  // Output record type for expression lists.
  IQLRecordTypeRef IQLOutputRecord;
  // Memcpy
  LLVMValueRef LLVMMemcpyIntrinsic;
  // Memset
  LLVMValueRef LLVMMemsetIntrinsic;
  // Memcmp
  LLVMValueRef LLVMMemcmpIntrinsic;
  // Move or copy semantics
  int32_t IQLMoveSemantics;
  // A stack for constructs like if/then/else
  std::stack<class IQLToLLVMStackRecord* > IQLStack;
  // A stack of switch builders
  std::stack<class IQLToLLVMSwitchRecord* > IQLSwitch;
  // A stack of CASE builders
  std::stack<class IQLToLLVMCaseState* > IQLCase;
  // Indicator whether we have generated an
  // identity transfer
  bool IsIdentity;

  // For aggregate function these
  // are for update operations
  CodeGenerationFunctionContext Update;
  // For aggregate function these
  // are for initialize operations
  CodeGenerationFunctionContext Initialize;
  // For aggregate function these
  // are for transfer operations
  CodeGenerationFunctionContext Transfer;
  // HACK: used for naming variables corresponding
  // to aggregate functions.
  int AggFn;
  // Value factory
  std::vector<IQLToLLVMValue *> ValueFactory;
  // Alloca cache
  local_cache * AllocaCache;

  CodeGenerationContext();
  ~CodeGenerationContext();
  /**
   * Give up ownership of Module.  This happens
   * when an execution engine is created since the
   * EE takes ownership of the module.
   */
  void disownModule();

  /**
   * Define a variable
   */
  void defineVariable(const char * name,
		      llvm::Value * val,
		      llvm::Value * nullVal,
		      IQLToLLVMValue::ValueType globalOrLocal);

  /**
   * Define a field of a record
   */
  void defineFieldVariable(llvm::Value * basePointer,
			   const char * prefix,
			   const char * memberName,
			   const RecordType * recordType);

  /**
   * Lookup an l-value in the symbol table.
   */
  const IQLToLLVMLValue * lookup(const char * name, const char * name2);

  /**
   * Lookup an r-value in the symbol table.
   */
  const IQLToLLVMValue * lookupValue(const char * name, const char * name2);

  /**
   * Lookup an r-value in the symbol table.
   */
  const IQLToLLVMValue * lookupBasePointer(const char * name);

  /**
   * Lookup an r-value in the symbol table.
   */
  const IQLToLLVMValue * lookupFunction(const char * name);

  /**
   * Get pointer to the execution context argument as
   * an LLVM C wrapper datatype.
   */
  LLVMValueRef getContextArgumentRef();

  /**
   * Initialize the members of code generation that correspond
   * to the function context.
   */
  void reinitializeForTransfer();

  /**
   * Reinitialize some state for compiling a new function.
   * TODO: This and reinitializeForTransfer should be merged.
   */
  void reinitialize();

  /**
   * Initialize the members of code generation that correspond
   * to the function context.
   */
  void createFunctionContext();

  /**
   * Dump contents of symbol table.
   */
  void dumpSymbolTable();

  /**
   * Handle the changes between compilation contexts for aggregates
   */
  void restoreAggregateContext(CodeGenerationFunctionContext * fCtxt);

  /**
   * Save the Aggregate function state.
   */
  void saveAggregateContext(CodeGenerationFunctionContext * fCtxt);

  /**
   * Add addresses of the members of the input record into the symbol table.
   */
  void addInputRecordType(const char * name, 
			  const char * argumentName, 
			  const RecordType * rec);
  void addInputRecordType(const char * name, 
			  const char * argumentName, 
			  const RecordType * rec,
			  const boost::dynamic_bitset<>& mask);

  /**
   * Build a while loop.
   * 
   * To use this:
   * Call whileBegin before generating the condition predicate.
   * Call whileStatementBlock after generating the condition predicate and before
   * any of the statements.
   * Call whileFinish after generating all of the statements in the
   * body of the loop.
   */
  void whileBegin();
  void whileStatementBlock(const IQLToLLVMValue * condVal,
			   const FieldType * condTy);
  void whileFinish();

  /**
   * Conditionally branch using a possibly nullable boolean
   * condition value.
   */
  void conditionalBranch(const IQLToLLVMValue * condVal,
			 const FieldType * condTy,
			 llvm::BasicBlock * trueBranch,
			 llvm::BasicBlock * falseBranch);

  /**
   * Build an array expression
   */
  const IQLToLLVMValue * buildArray(std::vector<const IQLToLLVMValue *>& vals,
				    const FieldType * arrayTy);
  const IQLToLLVMValue * buildGlobalConstArray(std::vector<const IQLToLLVMValue *>& vals,
					       const FieldType * arrayTy);

  /**
   * Call a function.
   */
  IQLToLLVMValue::ValueType buildCall(const char * f,
				      const std::vector<const IQLToLLVMValue *> & args,
				      llvm::Value * retTmp,
				      const FieldType * retType);
  /**
   * Cast non null value to INT32.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastInt32(const IQLToLLVMValue * e, 
					   const FieldType * argAttrs, 
					   llvm::Value * ret, 
					   const FieldType * retAttrs);

  /**
   * Cast non null value to INT64.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastInt64(const IQLToLLVMValue * e, 
					   const FieldType * argAttrs, 
					   llvm::Value * ret, 
					   const FieldType * retAttrs);

  /**
   * Cast non null value to DOUBLE.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastDouble(const IQLToLLVMValue * e, 
					    const FieldType * argAttrs, 
					    llvm::Value * ret, 
					    const FieldType * retAttrs);

  /**
   * Cast non null value to DECIMAL.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildCastDecimal(const IQLToLLVMValue * e, 
					     const FieldType * argAttrs, 
					     llvm::Value * ret, 
					     const FieldType * retAttrs);

  /**
   * Subtract rhs from lhs.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildSub(const IQLToLLVMValue * lhs, 
				     const FieldType * lhsType, 
				     const IQLToLLVMValue * rhs, 
				     const FieldType * rhsType, 
				     llvm::Value * ret, 
				     const FieldType * retType);

  /**
   * Add INTERVAL and DATE/DATETIME.  Put return value in ret.
   */
  IQLToLLVMValue::ValueType buildDateAdd(const IQLToLLVMValue * lhs, 
					 const FieldType * lhsType, 
					 const IQLToLLVMValue * rhs, 
					 const FieldType * rhsType, 
					 llvm::Value * ret, 
					 const FieldType * retType);
};


/**   
 * Create a mutable variable by doing an alloca in the entry block of the function.
 */
LLVMValueRef LLVMCreateEntryBlockAlloca(CodeGenerationContext * ctxt, 
					LLVMTypeRef ty, const char * name);
/**
 * Create a local llvm value.
 */
IQLToLLVMValueRef IQLToLLVMCreateLocalValue(CodeGenerationContext * ctxt, LLVMValueRef val);
  
/**
 * Create a global llvm value.
 */
IQLToLLVMValueRef IQLToLLVMCreateGlobalValue(CodeGenerationContext * ctxt, LLVMValueRef val);
  
/**
 * Create a global nullable llvm value.
 */
IQLToLLVMValueRef IQLToLLVMCreateNullableGlobalValue(CodeGenerationContext * ctxt, LLVMValueRef val, LLVMValueRef nv);

void IQLToLLVMBuildSetNullableValue(CodeGenerationContext * ctxt,
				    const IQLToLLVMLValue * lval,
				    IQLToLLVMValueRef val);
void IQLToLLVMBuildSetNullableValue(CodeGenerationContext * ctxt,
				    const IQLToLLVMLValue * lval,
				    IQLToLLVMValueRef val,
				    bool allowNullToNonNull);

void IQLToLLVMVarcharSetSize(CodeGenerationContext * ctxt, LLVMValueRef varcharPtr, LLVMValueRef sz);
void IQLToLLVMVarcharSetPtr(CodeGenerationContext * ctxt, LLVMValueRef varcharPtr, LLVMValueRef sz);

class IQLToLLVMTypePredicate
{
public:
  static bool isChar(LLVMTypeRef ty);
  static bool isChar(LLVMValueRef val);
  static bool isArrayType(LLVMTypeRef ty);
  static bool isArrayType(LLVMValueRef val);
};

class IQLToLLVMBinaryConversion
{
private:
  static IQLToLLVMValueRef convertIntToDec(CodeGenerationContext * ctxt,
					   LLVMValueRef llvmVal,
					   bool isInt64);
  static IQLToLLVMValueRef convertDecToDouble(CodeGenerationContext * ctxt,
					      LLVMValueRef llvmVal);

public:
  /**
   * Can e1 be cast to e2?
   */
  static LLVMTypeRef castTo(CodeGenerationContext * ctxt, LLVMTypeRef e1, LLVMTypeRef e2);
  /**
   * Can e1 be cast to e2 or vice versa?
   */
  static LLVMTypeRef leastCommonType(CodeGenerationContext * ctxt, LLVMTypeRef e1, LLVMTypeRef e2);
  /**
   * Convert a value to the target type.
   */
  static IQLToLLVMValueRef convertTo(CodeGenerationContext * ctxt, 
				     IQLToLLVMValueRef v, 
				     const FieldType * ty);
  static IQLToLLVMValueRef convertTo(CodeGenerationContext * ctxt, 
				     IQLToLLVMValueRef v, 
				     LLVMTypeRef e2);
private:
  IQLToLLVMValueRef mLHS;
  IQLToLLVMValueRef mRHS;
  LLVMTypeRef mResultType;
public:
  IQLToLLVMBinaryConversion(CodeGenerationContext * ctxt, IQLToLLVMValueRef lhs, IQLToLLVMValueRef rhs);

  IQLToLLVMValueRef getLHS() { return mLHS; }
  IQLToLLVMValueRef getRHS() { return mRHS; }
  LLVMTypeRef getResultType() { return mResultType; }
};

class IQLToLLVMTypeInspector
{
public:
  static int32_t getCharArrayLength(LLVMTypeRef ty);
  static int32_t getCharArrayLength(LLVMValueRef val);
};

#endif
