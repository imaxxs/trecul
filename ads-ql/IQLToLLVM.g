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

tree grammar IQLToLLVM;
options {
  tokenVocab=IQL;
  language=C;
  ASTLabelType    = pANTLR3_BASE_TREE;
}

@header {
#include "LLVMGen.h"
}

localVarOrId returns [pANTLR3_STRING argName]
    :
    ID { $argName = $ID.text; }
    ;

statement[IQLCodeGenerationContextRef ctxt]
	:
	(
	setStatement[$ctxt]
	| variableDeclaration[$ctxt]
	| printStatement[$ctxt]
	| ifStatement[$ctxt]
	| statementBlock[$ctxt]
	| ^(TK_RETURN (e = expression[$ctxt] { IQLToLLVMBuildReturnValue($ctxt, e.llvmVal); })?)
	| TK_BREAK
	| TK_CONTINUE
	| ^(TK_RAISERROR expression[$ctxt] expression[$ctxt]?)
	| switchStatement[$ctxt] 
	| whileStatement[$ctxt] 
	)
	;

variableDeclaration [IQLCodeGenerationContextRef ctxt] 
	:
	^(TK_DECLARE nm = localVarOrId ty = builtInType[$ctxt] {             
            IQLToLLVMBuildDeclareLocal($ctxt, (const char *) nm->chars, ty);
        })
	;

setStatement[IQLCodeGenerationContextRef ctxt]
	:
	^(c=TK_SET lvalue = variableReference[$ctxt] rvalue = expression[$ctxt]) {
            IQLToLLVMBuildSetNullableValue(ctxt, lvalue, rvalue.llvmVal, $c->u);
        }
	;

variableReference[IQLCodeGenerationContextRef ctxt] returns [IQLToLLVMLValueRef lhs]
    :
    ID { $lhs = IQLToLLVMBuildLValue(ctxt, (char *)$ID.text->chars); }
    |
    ^('[' ID e = expression[$ctxt] { $lhs = IQLToLLVMBuildArrayLValue(ctxt, (char *)$ID.text->chars, e.llvmVal); })
    ;

switchStatement[IQLCodeGenerationContextRef ctxt]
    :
    ^(TK_SWITCH e = expression[$ctxt] { IQLToLLVMBeginSwitch($ctxt); } switchBlock[$ctxt]+ { IQLToLLVMEndSwitch($ctxt, e.llvmVal); })
    ;

switchBlock[IQLCodeGenerationContextRef ctxt]
    :
    ^(TK_CASE DECIMAL_INTEGER_LITERAL { IQLToLLVMBeginSwitchCase($ctxt, (const char *) $DECIMAL_INTEGER_LITERAL.text->chars); } statement[$ctxt]+ { IQLToLLVMEndSwitchCase($ctxt); })
    ;

printStatement[IQLCodeGenerationContextRef ctxt]
	:
	^(TK_PRINT expression[$ctxt])
    ;

ifStatement[IQLCodeGenerationContextRef ctxt]
	:
	^(TK_IF expression[$ctxt] statement[$ctxt] statement[$ctxt]?)
	;

statementBlock[IQLCodeGenerationContextRef ctxt]
	:
	^(TK_BEGIN statement[$ctxt]*)
	;

whileStatement[IQLCodeGenerationContextRef ctxt]
	:
	^(TK_WHILE expression[$ctxt] statement[$ctxt])
	;

singleExpression[IQLCodeGenerationContextRef ctxt]
    :
    declareStatement[$ctxt]* returnExpression[$ctxt] 
    ;
    
declareStatement[IQLCodeGenerationContextRef ctxt]
    :
    ^(TK_DECLARE e = expression[$ctxt] ID { IQLToLLVMBuildLocalVariable($ctxt, (const char *) $ID.text->chars, e.llvmVal, $e.start->u); })
    ;

returnExpression[IQLCodeGenerationContextRef ctxt]
    :
    ^(TK_RETURN e = expression[$ctxt] { IQLToLLVMBuildReturnValue($ctxt, e.llvmVal); })
    ;

recordConstructor[IQLCodeGenerationContextRef ctxt]
@init {
    int32_t fieldPos = 0;
}
    :
    fieldConstructor[$ctxt, &fieldPos]+
    ;

fieldConstructor[IQLCodeGenerationContextRef ctxt, int32_t * fieldPos]
@init {
  const char * nm = NULL;
}
    :
    ^(ID '*') { LLVMSetFields($ctxt, (const char *) $ID.text->chars, fieldPos); }
    |
    ^(TK_SET e = expression[$ctxt] (ID)?  { LLVMSetField($ctxt, fieldPos, e.llvmVal); } )
    |
    declareStatement[$ctxt]
    |
    ^(a=QUOTED_ID (b=QUOTED_ID { nm = (const char *) $b.text->chars; })?) { LLVMBuildQuotedId($ctxt, (const char *) $a.text->chars, nm, fieldPos); }
    ;

builtInType [IQLCodeGenerationContextRef ctxt] returns [void * llvmType]
	: ^(c=TK_INTEGER typeNullability?) { $llvmType = $c->u; }
	  | ^(c=TK_DOUBLE typeNullability?) { $llvmType = $c->u; }
	  | ^(c=TK_CHAR DECIMAL_INTEGER_LITERAL typeNullability?) { $llvmType = $c->u; }
	  | ^(c=TK_VARCHAR typeNullability?) { $llvmType = $c->u; }
	  | ^(c=TK_NVARCHAR typeNullability?) { $llvmType = $c->u; }
	  | ^(c=TK_DECIMAL typeNullability?) { $llvmType = $c->u; }
	  | ^(c=TK_BOOLEAN typeNullability?) { $llvmType = $c->u; }
	  | ^(c=TK_DATETIME typeNullability?) { $llvmType = $c->u; }
      | ^(c=TK_BIGINT typeNullability?) { $llvmType = $c->u; }
	  | ^(c=ID typeNullability?) { $llvmType = $c->u; }
	;

typeNullability
    :
    TK_NULL 
    | 
    TK_NOT TK_NULL
    ;

/** dummy return value here to force Antlr3 to generate a return struct. 
 * The struct will contain the start token that we need to get access to
 * the AST associated with a rule.  We want access to the AST because 
 * type checking has dangled stuff off of it.  If we just have a single
 * return then Antlr appears to not include the AST in the return.
 */
expression[IQLCodeGenerationContextRef ctxt] returns [IQLToLLVMValueRef llvmVal, int32_t dummy]
@init {
    int32_t isBinary=0;
    IQLToLLVMValueVectorRef values = NULL;
}
	: 
    ^(c = TK_OR { IQLToLLVMBeginOr($ctxt, $c->u); } e1 = expression[$ctxt] { IQLToLLVMAddOr($ctxt, e1.llvmVal, $e1.start->u, $c->u); } e2 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildOr($ctxt, e2.llvmVal, $e2.start->u, $c->u); } )
    | ^(c = TK_AND { IQLToLLVMBeginAnd($ctxt, $c->u); } e1 = expression[$ctxt] { IQLToLLVMAddAnd($ctxt, e1.llvmVal, $e1.start->u, $c->u); } e2 = expression[$ctxt]  { $llvmVal = IQLToLLVMBuildAnd($ctxt, e2.llvmVal, $e2.start->u, $c->u); })
    | ^(c = TK_NOT e1 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildNot($ctxt, e1.llvmVal, $e1.start->u, $c->u); })
    | ^(c = TK_IS e1 = expression[$ctxt] (TK_NOT { isBinary = 1; })? { $llvmVal = IQLToLLVMBuildIsNull($ctxt, e1.llvmVal, $e1.start->u, $c->u, isBinary); })
    | ^(c = TK_CASE { IQLToLLVMCaseBlockBegin($ctxt, $c->u); } (whenExpression[$ctxt, $c->u])+ elseExpression[$ctxt, $c->u] { $llvmVal = IQLToLLVMCaseBlockFinish($ctxt); } )
    | ^('?' e1 = expression[$ctxt] { IQLToLLVMBeginIfThenElse($ctxt, e1.llvmVal); } e2 = expression[$ctxt] { IQLToLLVMElseIfThenElse($ctxt); } e3 = expression[$ctxt] { $llvmVal = IQLToLLVMEndIfThenElse($ctxt, e2.llvmVal, e3.llvmVal); })
    | ^(c='^' e1 = expression[$ctxt] e2 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildBitwiseXor($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u); })
    | ^(c='|' e1 = expression[$ctxt] e2 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildBitwiseOr($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u); })
    | ^(c='&' e1 = expression[$ctxt] e2 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildBitwiseAnd($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u); })
    | ^(c='=' e1 = expression[$ctxt] e2 = expression[$ctxt]  { $llvmVal = IQLToLLVMBuildEquals($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u); })
    | ^(c='>' e1 = expression[$ctxt] e2 = expression[$ctxt]  { $llvmVal = IQLToLLVMBuildCompare($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u, IQLToLLVMOpGT); })
    | ^(c='<' e1 = expression[$ctxt] e2 = expression[$ctxt]  { $llvmVal = IQLToLLVMBuildCompare($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u, IQLToLLVMOpLT); })
    | ^(c='>=' e1 = expression[$ctxt] e2 = expression[$ctxt]  { $llvmVal = IQLToLLVMBuildCompare($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u, IQLToLLVMOpGE); })
    | ^(c='<=' e1 = expression[$ctxt] e2 = expression[$ctxt]  { $llvmVal = IQLToLLVMBuildCompare($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u, IQLToLLVMOpLE); })
    | ^(c='<>' e1 = expression[$ctxt] e2 = expression[$ctxt]  { $llvmVal = IQLToLLVMBuildCompare($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u, IQLToLLVMOpNE); })
    | ^(TK_LIKE e1 = expression[$ctxt] e2 = expression[$ctxt] { IQLToLLVMNotImplemented(); })
    | ^(TK_RLIKE e1 = expression[$ctxt] e2 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildCompare($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $TK_RLIKE->u, IQLToLLVMOpRLike); })
    | ^(c='-' e1 = expression[$ctxt] (e2 = expression[$ctxt] { isBinary=1; })? { $llvmVal = isBinary ? IQLToLLVMBuildSub($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u) : IQLToLLVMBuildNegate($ctxt, e1.llvmVal, $e1.start->u, $c->u); })
    | ^(c='+' e1 = expression[$ctxt] (e2 = expression[$ctxt] { isBinary=1; })? { $llvmVal = isBinary ? IQLToLLVMBuildAdd($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u) : e1.llvmVal; })
    | ^(c='*' e1 = expression[$ctxt] e2 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildMul($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u); })
    | ^(c='/' e1 = expression[$ctxt] e2 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildDiv($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u); })
    | ^(c='%' e1 = expression[$ctxt] e2 = expression[$ctxt]  { $llvmVal = IQLToLLVMBuildMod($ctxt, e1.llvmVal, $e1.start->u, e2.llvmVal, $e2.start->u, $c->u); })
    | ^(c='~' e1 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildBitwiseNot($ctxt, e1.llvmVal, $e1.start->u, $c->u); })
    | ^('#' { values = IQLToLLVMValueVectorCreate(); } (e1 = expression[$ctxt] { IQLToLLVMValueVectorPushBack(values, e1.llvmVal, $e1.start->u); })* { $llvmVal = IQLToLLVMBuildHash($ctxt, values); IQLToLLVMValueVectorFree(values); })
    | ^(c='$' { values = IQLToLLVMValueVectorCreate(); } (e1 = expression[$ctxt] { IQLToLLVMValueVectorPushBack(values, e1.llvmVal, $e1.start->u); })* { $llvmVal = IQLToLLVMBuildSortPrefix($ctxt, values, $c->u); IQLToLLVMValueVectorFree(values); })
    | ^(c = TK_CAST builtInType[$ctxt] e1 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildCast($ctxt, e1.llvmVal, $e1.start->u, $c->u); })
    | ^(c = '(' fun = ID 
        { 
            values = IQLToLLVMValueVectorCreate(); 
        }
        (e1 = expression[$ctxt] { IQLToLLVMValueVectorPushBack(values, e1.llvmVal, $e1.start->u); })* 
        { 
                $llvmVal = IQLToLLVMBuildCall($ctxt, (char *) $fun.text->chars, values, $c->u);
                IQLToLLVMValueVectorFree(values); 
        }
    )
    | ^(LITERAL_CAST id=ID STRING_LITERAL { $llvmVal = IQLToLLVMBuildLiteralCast($ctxt, (const char *) $STRING_LITERAL.text->chars, (const char *) $id.text->chars); })
    | ^(DATETIME_LITERAL STRING_LITERAL { $llvmVal = IQLToLLVMBuildDatetimeLiteral($ctxt, (const char *) $STRING_LITERAL.text->chars); })
    | DECIMAL_INTEGER_LITERAL { $llvmVal = IQLToLLVMBuildDecimalInt32Literal($ctxt, (char *) $DECIMAL_INTEGER_LITERAL.text->chars); }
	| HEX_INTEGER_LITERAL  { IQLToLLVMNotImplemented(); }
    | DECIMAL_BIGINT_LITERAL { $llvmVal = IQLToLLVMBuildDecimalInt64Literal($ctxt, (char *) $DECIMAL_BIGINT_LITERAL.text->chars); }
    | FLOATING_POINT_LITERAL { $llvmVal = IQLToLLVMBuildFloatLiteral($ctxt, (char *) $FLOATING_POINT_LITERAL.text->chars); }
	| DECIMAL_LITERAL  { $llvmVal = IQLToLLVMBuildDecimalLiteral($ctxt, (char *) $DECIMAL_LITERAL.text->chars); }
	| STRING_LITERAL { $llvmVal = IQLToLLVMBuildVarcharLiteral($ctxt, (char *) $STRING_LITERAL.text->chars); }
	| WSTRING_LITERAL { IQLToLLVMNotImplemented(); }
	| TK_TRUE { $llvmVal = IQLToLLVMBuildTrue($ctxt); }
	| TK_FALSE { $llvmVal = IQLToLLVMBuildFalse($ctxt); }
	| ^(id=ID ID?) { $llvmVal = IQLToLLVMBuildVariableRef($ctxt, (char *) $id.text->chars); }
	| ^('[' id=ID e1 = expression[$ctxt]) { $llvmVal = IQLToLLVMBuildArrayRef($ctxt, (char *) $id.text->chars, e1.llvmVal); }
    | TK_NULL { $llvmVal = IQLToLLVMBuildNull($ctxt); }
    | ^(c=TK_SUM { IQLToLLVMBeginAggregateFunction($ctxt); } e1 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildAggregateFunction($ctxt, (char *) $TK_SUM.text->chars, e1.llvmVal, $c->u); } )
    | ^(c=TK_MAX { IQLToLLVMBeginAggregateFunction($ctxt); } e1 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildAggregateFunction($ctxt, (char *) $TK_MAX.text->chars, e1.llvmVal, $c->u); } )
    | ^(c=TK_MIN { IQLToLLVMBeginAggregateFunction($ctxt); } e1 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildAggregateFunction($ctxt, (char *) $TK_MIN.text->chars, e1.llvmVal, $c->u); } )
    | ^(TK_INTERVAL intervalType = ID e1 = expression[$ctxt] { $llvmVal = IQLToLLVMBuildInterval($ctxt, (const char *)$intervalType.text->chars, e1.llvmVal); } )
    ;    

whenExpression[IQLCodeGenerationContextRef ctxt, void * attr]
    :
    ^(TK_WHEN e1 = expression[$ctxt] { IQLToLLVMCaseBlockIf($ctxt, e1.llvmVal); } e2 = expression[$ctxt] { IQLToLLVMCaseBlockThen($ctxt, e2.llvmVal, attr); } )
    ;    

elseExpression[IQLCodeGenerationContextRef ctxt, void * attr]
    :
    ^(TK_ELSE e3 = expression[$ctxt] { IQLToLLVMCaseBlockThen($ctxt, e3.llvmVal, attr); })
    ;    
