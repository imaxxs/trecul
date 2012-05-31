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

#ifndef __LLVMGEN_H
#define __LLVMGEN_H

#include <llvm-c/Core.h>

/* C interface to semantic analysis apparatus including symbol tables, environments and the like */
#ifdef __cplusplus
extern "C" {
#endif

  typedef struct IQLGraphContextStruct * IQLGraphContextRef;
  typedef struct IQLRecordTypeContextStruct * IQLRecordTypeContextRef;
  typedef struct CodeGenerationContextStruct * IQLCodeGenerationContextRef;

  typedef struct IQLTypeCheckContextStruct * IQLTypeCheckContextRef;
  typedef const struct IQLFieldTypeStruct * IQLFieldTypeRef;
  typedef const struct IQLToLLVMValueStruct * IQLToLLVMValueRef;
  typedef const struct IQLToLLVMLValueStruct * IQLToLLVMLValueRef;

  enum DecimalOpCode {
    iqlOpDecPlus,
    iqlOpDecMinus,
    iqlOpDecTimes,
    iqlOpDecDivide
  };

  enum VarcharOpCode {
    iqlOpVarcharPlus
  };

  enum IQLToLLVMPredicate {
    IQLToLLVMOpEQ,
    IQLToLLVMOpNE,
    IQLToLLVMOpGT,
    IQLToLLVMOpGE,
    IQLToLLVMOpLT,
    IQLToLLVMOpLE,
    IQLToLLVMOpRLike
  };


  /**
   * Yucky C wrapper for std::vector<const IQLToLLVMValue *>.
   */
  typedef struct IQLToLLVMValueVectorStruct * IQLToLLVMValueVectorRef;
  IQLToLLVMValueVectorRef IQLToLLVMValueVectorCreate();
  void IQLToLLVMValueVectorFree(IQLToLLVMValueVectorRef v);
  void IQLToLLVMValueVectorPushBack(IQLToLLVMValueVectorRef v, 
				    IQLToLLVMValueRef ft,
				    void * argAttrs);

  /**
   * Create an uninitialized local variable.
   */
  void IQLToLLVMBuildDeclareLocal(IQLCodeGenerationContextRef ctxt, const char * nm, void * attrs);
  /**
   * Create a local variable.
   */
  void IQLToLLVMBuildLocalVariable(IQLCodeGenerationContextRef ctxt, const char * nm, IQLToLLVMValueRef val, void * attrs);
  /**
   * Create an LLVM type for a primitive type.
   */
  LLVMTypeRef IQLToLLVMBuildType(IQLCodeGenerationContextRef ctxt, void * typeAttrs);
  /**
   * Hash some values.
   */
  IQLToLLVMValueRef IQLToLLVMBuildHash(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueVectorRef lhs);
  /**
   * Create a sort prefix (poor man's normalized key) for a sequence of values.
   */
  IQLToLLVMValueRef IQLToLLVMBuildSortPrefix(IQLCodeGenerationContextRef ctxt, 
					     IQLToLLVMValueVectorRef lhs,
					     void * retAttrs);
  /**
   * Function call.
   */
  IQLToLLVMValueRef IQLToLLVMBuildCall(IQLCodeGenerationContextRef ctxt, 
				       const char * f,
				       IQLToLLVMValueVectorRef lhs,
				       void * retAttrs);
  /**
   * Negate a value. 
   */
  IQLToLLVMValueRef IQLToLLVMBuildNegate(IQLCodeGenerationContextRef ctxt, 
					 IQLToLLVMValueRef lhs,
					 void * lhsAttributes,
					 void * resultAttributes);
  /**
   * Multiply two values. 
   */
  IQLToLLVMValueRef IQLToLLVMBuildMul(IQLCodeGenerationContextRef ctxt, 
				      IQLToLLVMValueRef lhs, void * lhsAttributes, 
				      IQLToLLVMValueRef rhs, void * rhsAttributes,
				      void * resultAttributes);
  /**
   * Divide two values. 
   */
  IQLToLLVMValueRef IQLToLLVMBuildDiv(IQLCodeGenerationContextRef ctxt, 
				      IQLToLLVMValueRef lhs, void * lhsAttributes, 
				      IQLToLLVMValueRef rhs, void * rhsAttributes,
				      void * resultAttributes);
  /**
   * Add two values. 
   */
  IQLToLLVMValueRef IQLToLLVMBuildAdd(IQLCodeGenerationContextRef ctxt, 
				      IQLToLLVMValueRef lhs, void * lhsAttributes, 
				      IQLToLLVMValueRef rhs, void * rhsAttributes,
				      void * resultAttributes);
  /**
   * Subtract two values. 
   */
  IQLToLLVMValueRef IQLToLLVMBuildSub(IQLCodeGenerationContextRef ctxt, 
				      IQLToLLVMValueRef lhs, void * lhsAttributes, 
				      IQLToLLVMValueRef rhs, void * rhsAttributes,
				      void * resultAttributes);
  /**
   * Modulus of two values. 
   */
  IQLToLLVMValueRef IQLToLLVMBuildMod(IQLCodeGenerationContextRef ctxt, 
				      IQLToLLVMValueRef lhs, void * lhsAttributes, 
				      IQLToLLVMValueRef rhs, void * rhsAttributes,
				      void * resultAttributes);

  /**
   * Bitwise AND of two values.
   */
  IQLToLLVMValueRef IQLToLLVMBuildBitwiseAnd(IQLCodeGenerationContextRef ctxt, 
					     IQLToLLVMValueRef lhs, void * lhsAttributes, 
					     IQLToLLVMValueRef rhs, void * rhsAttributes,
					     void * resultAttributes);

  /**
   * Bitwise OR of two values.
   */
  IQLToLLVMValueRef IQLToLLVMBuildBitwiseOr(IQLCodeGenerationContextRef ctxt, 
					    IQLToLLVMValueRef lhs, void * lhsAttributes, 
					    IQLToLLVMValueRef rhs, void * rhsAttributes,
					    void * resultAttributes);

  /**
   * Bitwise XOR of two values.
   */
  IQLToLLVMValueRef IQLToLLVMBuildBitwiseXor(IQLCodeGenerationContextRef ctxt, 
					     IQLToLLVMValueRef lhs, void * lhsAttributes, 
					     IQLToLLVMValueRef rhs, void * rhsAttributes,
					     void * resultAttributes);

  /**
   * Bitwise NOT of a value.
   */
  IQLToLLVMValueRef IQLToLLVMBuildBitwiseNot(IQLCodeGenerationContextRef ctxt, 
					     IQLToLLVMValueRef lhs, void * lhsAttributes, 
					     void * resultAttributes);

  /**
   * Compare two values for equality 
   */
  IQLToLLVMValueRef IQLToLLVMBuildEquals(IQLCodeGenerationContextRef ctxt, 
					 IQLToLLVMValueRef lhs, 
					 void * lhsAttributes, 
					 IQLToLLVMValueRef rhs, 
					 void * rhsAttributes,
					 void * resultAttributes);
  /**
   * Compare two values 
   */
  IQLToLLVMValueRef IQLToLLVMBuildCompare(IQLCodeGenerationContextRef ctxt, 
					  IQLToLLVMValueRef lhs, 
					  void * lhsAttributes, 
					  IQLToLLVMValueRef rhs, 
					  void * rhsAttributes,
					  void * resultAttributes,
					  enum IQLToLLVMPredicate op);
  /**
   * Logical and of two values. 
   */
  void IQLToLLVMBeginAnd(IQLCodeGenerationContextRef ctxt, void * retAttrs);
  void IQLToLLVMAddAnd(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueRef lhs,
		       void * lhsAttrs, void * retAttrs);
  IQLToLLVMValueRef IQLToLLVMBuildAnd(IQLCodeGenerationContextRef ctxt, 
				      IQLToLLVMValueRef rhs, void * rhsAttrs,
				      void * retAttrs);
  /**
   * Logical or of two values. 
   */
  void IQLToLLVMBeginOr(IQLCodeGenerationContextRef ctxt, void * retAttrs);
  void IQLToLLVMAddOr(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueRef lhs,
		      void * lhsAttrs, void * retAttrs);
  IQLToLLVMValueRef IQLToLLVMBuildOr(IQLCodeGenerationContextRef ctxt, 
				     IQLToLLVMValueRef rhs, void * rhsAttrs,
				     void * retAttrs);
  /**
   * Logical not of a boolean value
   */
  IQLToLLVMValueRef IQLToLLVMBuildNot(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueRef lhs,
				      void * argAttrs, void * retAttrs);
  /**
   * Check whether an expression is NULL
   */
  IQLToLLVMValueRef IQLToLLVMBuildIsNull(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueRef lhs,
					 void * argAttrs, void * retAttrs, int isNotNull);
  /**
   * Cast a value to a type.
   */
  IQLToLLVMValueRef IQLToLLVMBuildCast(IQLCodeGenerationContextRef ctxt, 
				       IQLToLLVMValueRef e, 
				       void * argAttrs, void * retAttrs);

  void IQLToLLVMBuildReturnValue(IQLCodeGenerationContextRef ctxt, 
				 IQLToLLVMValueRef llvmVal);
  void IQLToLLVMBuildSetNullableValue(IQLCodeGenerationContextRef ctxtRef,
				      IQLToLLVMLValueRef lvalRef,
				      IQLToLLVMValueRef val,
				      void * attrs);
  IQLToLLVMLValueRef IQLToLLVMBuildLValue(IQLCodeGenerationContextRef ctxt, 
					  const char * var);
  IQLToLLVMLValueRef IQLToLLVMBuildArrayLValue(IQLCodeGenerationContextRef ctxt, 
					       const char * var,
					       IQLToLLVMValueRef idx);

  IQLToLLVMValueRef IQLToLLVMBuildVariableRef(IQLCodeGenerationContextRef ctxt, const char * var, const char * var2);
  IQLToLLVMValueRef IQLToLLVMBuildArrayRef(IQLCodeGenerationContextRef ctxt, 
					   const char * var,
					   IQLToLLVMValueRef idx);
  /**
   * Array constructor
   */
  IQLToLLVMValueRef IQLToLLVMBuildArray(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueVectorRef lhs, void * arrayAttrs);


  /**
   * Construct a CASE expression.
   */
  void IQLToLLVMCaseBlockBegin(IQLCodeGenerationContextRef ctxt, void * caseAttrs);
  void IQLToLLVMCaseBlockIf(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueRef condVal);
  void IQLToLLVMCaseBlockThen(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueRef value, void * caseAttrs);
  IQLToLLVMValueRef IQLToLLVMCaseBlockFinish(IQLCodeGenerationContextRef ctxt);

  /**
   * Construct a WHILE statement
   */
  void IQLToLLVMWhileBegin(IQLCodeGenerationContextRef ctxt);
  void IQLToLLVMWhileStatementBlock(IQLCodeGenerationContextRef ctxt, 
				    IQLToLLVMValueRef condVal, 
				    void * condAttrs);
  void IQLToLLVMWhileFinish(IQLCodeGenerationContextRef ctxt);

  /**
   * Construct an if/then/else expression
   */
  void IQLToLLVMBeginIfThenElse(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueRef condVal);
  void IQLToLLVMElseIfThenElse(IQLCodeGenerationContextRef ctxt);
  IQLToLLVMValueRef IQLToLLVMEndIfThenElse(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueRef thenVal, IQLToLLVMValueRef elseVal);

  /**
   * Builder for switch statements
   */
  void IQLToLLVMBeginSwitch(IQLCodeGenerationContextRef ctxt);
  void IQLToLLVMEndSwitch(IQLCodeGenerationContextRef ctxt, IQLToLLVMValueRef switchExpr);
  void IQLToLLVMBeginSwitchCase(IQLCodeGenerationContextRef ctxt, 
				const char * caseVal);
  void IQLToLLVMEndSwitchCase(IQLCodeGenerationContextRef ctxt);

  /**
   * Builder for aggregate function expressions.
   */
  void IQLToLLVMBeginAggregateFunction(IQLCodeGenerationContextRef ctxt);
  IQLToLLVMValueRef IQLToLLVMBuildAggregateFunction(IQLCodeGenerationContextRef ctxt, 
						    const char * fn,
						    IQLToLLVMValueRef e,
						    void * attrs);
  /**
   * Constructor for interval types.
   */
  IQLToLLVMValueRef IQLToLLVMBuildInterval(IQLCodeGenerationContextRef ctxt,
					   const char * intervalType,
					   IQLToLLVMValueRef e);

  /**
   * Create a literal value from a string
   */
  IQLToLLVMValueRef IQLToLLVMBuildLiteralCast(IQLCodeGenerationContextRef ctxt, const char * val, const char * typeName);
  /**
   * Create a literal date value
   */
  IQLToLLVMValueRef IQLToLLVMBuildDateLiteral(IQLCodeGenerationContextRef ctxt, const char * val);
  /**
   * Create a literal datetime value
   */
  IQLToLLVMValueRef IQLToLLVMBuildDatetimeLiteral(IQLCodeGenerationContextRef ctxt, const char * val);
  /**
   * Create a literal decimal integer.
   */
  IQLToLLVMValueRef IQLToLLVMBuildDecimalInt32Literal(IQLCodeGenerationContextRef ctxt, const char * val);
  /**
   * Create a literal decimal integer.
   */
  IQLToLLVMValueRef IQLToLLVMBuildDecimalInt64Literal(IQLCodeGenerationContextRef ctxt, const char * val);
  /**
   * Create a literal floating point value.
   */
  IQLToLLVMValueRef IQLToLLVMBuildFloatLiteral(IQLCodeGenerationContextRef ctxt, const char * val);
  /**
   * Create a literal varchar value.
   */
  IQLToLLVMValueRef IQLToLLVMBuildVarcharLiteral(IQLCodeGenerationContextRef ctxt, const char * val);
  /**
   * Create a literal 128-bit decimal value.
   */
  IQLToLLVMValueRef IQLToLLVMBuildDecimalLiteral(IQLCodeGenerationContextRef ctxt, const char * val);
  /**
   * Create a literal TRUE boolean value.
   */
  IQLToLLVMValueRef IQLToLLVMBuildTrue(IQLCodeGenerationContextRef ctxt);
  /**
   * Create a literal FALSE boolean value.
   */
  IQLToLLVMValueRef IQLToLLVMBuildFalse(IQLCodeGenerationContextRef ctxt);
  /**
   * Create a NULL value.
   */
  IQLToLLVMValueRef IQLToLLVMBuildNull(IQLCodeGenerationContextRef ctxt);
  /**
   * Throw not yet implemented exception
   * TODO: implement everything.
   */
  void IQLToLLVMNotImplemented();
  void LLVMSetField(IQLCodeGenerationContextRef ctxt, int32_t * pos, IQLToLLVMValueRef val);
  void LLVMSetFields(IQLCodeGenerationContextRef ctxt, const char * recordName, int * pos);
  void LLVMBuildQuotedId(IQLCodeGenerationContextRef ctxt, const char * quotedId, const char * rename, int * pos);

  // YUCK.  Having the C interface is so grotesque.  Here are C wrappers to
  // a std::vector<IQLFieldTypeRef>.  Necessary for variadic constructs.
  typedef struct IQLFieldTypeVectorStruct * IQLFieldTypeVectorRef;
  IQLFieldTypeVectorRef IQLFieldTypeVectorCreate();
  void IQLFieldTypeVectorFree(IQLFieldTypeVectorRef v);
  void IQLFieldTypeVectorPushBack(IQLFieldTypeVectorRef v, IQLFieldTypeRef ft);

  /**
   * Initialize a type check context with built in function library.
   */
  void IQLTypeCheckLoadBuiltinFunctions(IQLTypeCheckContextRef ctxt);

  /**
   * A builder interface for records
   */
  void IQLTypeCheckBeginRecord(IQLTypeCheckContextRef ctxt);
  void IQLTypeCheckAddFields(IQLTypeCheckContextRef ctxt, const char * recordName);
  void IQLTypeCheckAddField(IQLTypeCheckContextRef ctxt, const char * name, IQLFieldTypeRef ty);
  void IQLTypeCheckQuotedId(IQLTypeCheckContextRef ctxt, const char * quotedId, const char * rename);
  void IQLTypeCheckBuildRecord(IQLTypeCheckContextRef ctxt);

  /**
   * Create a local variable.
   */
  void IQLTypeCheckBuildLocal(IQLTypeCheckContextRef ctxt, const char * name, IQLFieldTypeRef ty);

  /**
   * Set a variable to a value.
   */
  void IQLTypeCheckSetValue2(IQLTypeCheckContextRef ctxt, 
			     IQLFieldTypeRef lhs, 
			     IQLFieldTypeRef rhs);

  /**
   * Get the type of an array variable.
   */
  IQLFieldTypeRef IQLTypeCheckArrayRef(IQLTypeCheckContextRef ctxt, 
				       const char * nm,
				       IQLFieldTypeRef idx);

  /**
   * Get the type of an variable (possibly qualified).
   */
  IQLFieldTypeRef IQLTypeCheckBuildVariableRef(IQLTypeCheckContextRef ctxt, 
					       const char * nm,
					       const char * nm2);

  /**
   * Get the type of an array variable.
   */
  void IQLTypeCheckBeginSwitch(IQLTypeCheckContextRef ctxt, 
			       IQLFieldTypeRef e);

  /**
   * CASE expression type checking.
   */
  void IQLTypeCheckBeginCase(IQLTypeCheckContextRef ctxt);
  void IQLTypeCheckAddCondition(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef cond);
  void IQLTypeCheckAddValue(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef val);
  IQLFieldTypeRef IQLTypeCheckBuildCase(IQLTypeCheckContextRef ctxt);

  /**
   * Type check additive operation and validate conversions as necessary.
   */
  IQLFieldTypeRef IQLTypeCheckAdditiveType(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs);
  IQLFieldTypeRef IQLTypeCheckNegateType(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs);
  IQLFieldTypeRef IQLTypeCheckMultiplicativeType(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs); 
  IQLFieldTypeRef IQLTypeCheckModulus(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs);
  IQLFieldTypeRef IQLTypeCheckBitwiseType(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs);
  IQLFieldTypeRef IQLTypeCheckUnaryBitwiseType(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs);
  IQLFieldTypeRef IQLTypeCheckHash(IQLTypeCheckContextRef ctxt, IQLFieldTypeVectorRef lhs);
  IQLFieldTypeRef IQLTypeCheckSortPrefix(IQLTypeCheckContextRef ctxt, IQLFieldTypeVectorRef lhs);
  IQLFieldTypeRef IQLTypeCheckCall(IQLTypeCheckContextRef ctxt, 
				   const char * f, 
				   IQLFieldTypeVectorRef lhs);
  IQLFieldTypeRef IQLTypeCheckEquals(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs);
  IQLFieldTypeRef IQLTypeCheckAnd(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs);
  IQLFieldTypeRef IQLTypeCheckRLike(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs);
  IQLFieldTypeRef IQLTypeCheckNot(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs);
  IQLFieldTypeRef IQLTypeCheckBuildInt32Type(IQLTypeCheckContextRef ctxt, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildInt64Type(IQLTypeCheckContextRef ctxt, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildDoubleType(IQLTypeCheckContextRef ctxt, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildDecimalType(IQLTypeCheckContextRef ctxt, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildDateType(IQLTypeCheckContextRef ctxt, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildDatetimeType(IQLTypeCheckContextRef ctxt, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildNVarcharType(IQLTypeCheckContextRef ctxt, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildVarcharType(IQLTypeCheckContextRef ctxt, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildCharType(IQLTypeCheckContextRef ctxt, const char * sz, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildBooleanType(IQLTypeCheckContextRef ctxt, int nullable);
  IQLFieldTypeRef IQLTypeCheckBuildNilType(IQLTypeCheckContextRef ctxt);
  IQLFieldTypeRef IQLTypeCheckBuildType(IQLTypeCheckContextRef ctxt, const char * typeName, int nullable);
  IQLFieldTypeRef IQLTypeCheckSymbolTableGetType(IQLTypeCheckContextRef ctxt, const char * name);
  IQLFieldTypeRef IQLTypeCheckIfThenElse(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef condVal, IQLFieldTypeRef thenVal, IQLFieldTypeRef elseVal);
  void IQLTypeCheckBeginAggregateFunction(IQLTypeCheckContextRef ctxt);
  IQLFieldTypeRef IQLTypeCheckBuildAggregateFunction(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef ty);
  IQLFieldTypeRef IQLTypeCheckCast(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs, IQLFieldTypeRef target);
  IQLFieldTypeRef IQLTypeCheckIsNull(IQLTypeCheckContextRef ctxt, IQLFieldTypeRef lhs, int isNotNull);

  /** 
   * Type constructor for interval types.
   */
  IQLFieldTypeRef IQLTypeCheckBuildInterval(IQLTypeCheckContextRef ctxt, 
					    const char * intervalType,
					    IQLFieldTypeRef e);
  IQLFieldTypeRef IQLTypeCheckArray(IQLTypeCheckContextRef ctxt, IQLFieldTypeVectorRef lhs);

  /**
   * Interface for building graphs/plans.
   */
  void IQLGraphNodeStart(IQLGraphContextRef ctxt, const char * type, const char * name);
  void IQLGraphNodeBuildIntegerParam(IQLGraphContextRef ctxt, const char * name, const char * val);
  void IQLGraphNodeBuildStringParam(IQLGraphContextRef ctxt, const char * name, const char * val);
  void IQLGraphNodeComplete(IQLGraphContextRef ctxt);
  void IQLGraphNodeBuildEdge(IQLGraphContextRef ctxt, const char * from, const char * to);

  /**
   * Interface for building record type.
   */
  void IQLRecordTypeBuildField(IQLRecordTypeContextRef ctxt, const char * name, IQLFieldTypeRef ty);
#ifdef __cplusplus
}

const class FieldType * unwrap(IQLFieldTypeRef r);
IQLFieldTypeRef wrap(const class FieldType * r);

class TypeCheckContext * unwrap(IQLTypeCheckContextRef ctxt);
IQLTypeCheckContextRef wrap(class TypeCheckContext *);

class IQLGraphBuilder * unwrap(IQLGraphContextRef ctxt);
IQLGraphContextRef wrap(class IQLGraphBuilder *);

class IQLRecordTypeBuilder * unwrap(IQLRecordTypeContextRef ctxt);
IQLRecordTypeContextRef wrap(class IQLRecordTypeBuilder *);

class CodeGenerationContext * unwrap(IQLCodeGenerationContextRef ctxtRef);
IQLCodeGenerationContextRef wrap(class CodeGenerationContext * ctxt);

const class IQLToLLVMValue * unwrap(IQLToLLVMValueRef val);
IQLToLLVMValueRef wrap(const class IQLToLLVMValue * val);

const class IQLToLLVMLValue * unwrap(IQLToLLVMLValueRef val);
IQLToLLVMLValueRef wrap(const class IQLToLLVMLValue * val);

#endif

#endif
