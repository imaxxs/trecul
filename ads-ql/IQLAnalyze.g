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

tree grammar IQLAnalyze;
options {
  tokenVocab=IQL;
  language=C;
  ASTLabelType    = pANTLR3_BASE_TREE;
}

@header {
#include "IQLBuildTree.h"
}

singleExpression[IQLTreeFactoryRef ctxt] returns [IQLExpressionRef e]
  :
  declareStatement[$ctxt]* e1=returnExpression[$ctxt] { $e = e1; }
  ;

declareStatement[IQLTreeFactoryRef ctxt]
    :
    ^(TK_DECLARE expression[$ctxt] ID)
    ;

returnExpression[IQLTreeFactoryRef ctxt] returns [IQLExpressionRef e]
    :
    ^(TK_RETURN e1=expression[$ctxt] { $e = e1; })
    ;

program[IQLTreeFactoryRef ctxt]
  :
  ^(TK_CREATE programArgDecl[$ctxt]* returnsDecl[$ctxt]? statementBlock[$ctxt])
  ;

returnsDecl[IQLTreeFactoryRef ctxt]
        :
        ^(TK_RETURNS builtInType[$ctxt])
        ;

recordConstructor[IQLTreeFactoryRef ctxt] returns [IQLRecordConstructorRef ctor]
@init {
    IQLFieldConstructorListRef fields = IQLFieldConstructorListCreate($ctxt);
}
@after {
    IQLFieldConstructorListFree($ctxt, fields);
}
    :
        (f = fieldConstructor[$ctxt] { IQLFieldConstructorListAppend($ctxt, fields, f); } )+ { $ctor = IQLBuildRecord($ctxt, fields); }
    ;

fieldConstructor[IQLTreeFactoryRef ctxt] returns [IQLFieldConstructorRef ctor]
@init {
    const char * nm = NULL;
}
    :
    ^(ID '*') { $ctor = IQLBuildAddFields($ctxt, (const char *) $ID.text->chars); }
    |
    ^(TK_SET e = expression[$ctxt] (ID {nm = (const char *) $ID.text->chars; })?  { $ctor = IQLBuildAddField($ctxt, nm, e); } )
    |
    declareStatement[$ctxt] { /* TODO: Fix */ $ctor = NULL; }
    |
    ^(a=QUOTED_ID (b=QUOTED_ID { nm = (const char *) $b.text->chars; })?) { $ctor = IQLBuildQuotedId($ctxt, (const char *) $a.text->chars, nm); }
    ;

localVarOrId
    :
    ID
    ;

programArgDecl[IQLTreeFactoryRef ctxt]
    :
	^(TK_DECLARE localVarOrId builtInType[$ctxt] TK_OUTPUT?) 
	;

statement[IQLTreeFactoryRef ctxt]
	:
	(
	setStatement[$ctxt]
	| variableDeclaration[$ctxt]
	| declareStatement[$ctxt]
	| printStatement[$ctxt]
	| ifStatement[$ctxt]
	| statementBlock[$ctxt]
	| ^(TK_RETURN expression[$ctxt]?)
	| TK_BREAK
	| TK_CONTINUE
	| ^(TK_RAISERROR expression[$ctxt] expression[$ctxt]?)
	| switchStatement[$ctxt] 
	| whileStatement[$ctxt] 
	)
	;

variableDeclaration[IQLTreeFactoryRef ctxt]
	:
	^(TK_DECLARE localVarOrId builtInType[$ctxt])
	;

builtInType[IQLTreeFactoryRef ctxt] returns [IQLFieldTypeRef ty]
@init {
nullable=0;
}
	: ^(c=TK_INTEGER (nullable=typeNullability)?) { $ty = IQLBuildInt32Type($ctxt, nullable); }
	  | ^(c=TK_DOUBLE (nullable=typeNullability)?) { $ty = IQLBuildDoubleType($ctxt, nullable); }
	  | ^(c=TK_CHAR DECIMAL_INTEGER_LITERAL (nullable=typeNullability)?) { $ty = IQLBuildCharType($ctxt, (const char *) $DECIMAL_INTEGER_LITERAL.text->chars, nullable); }
	  | ^(c=TK_VARCHAR (nullable=typeNullability)?) { $ty = IQLBuildVarcharType($ctxt, nullable); }
	  | ^(c=TK_NVARCHAR (nullable=typeNullability)?) { $ty = IQLBuildNVarcharType($ctxt, nullable); }
	  | ^(c=TK_DECIMAL (nullable=typeNullability)?) { $ty = IQLBuildDecimalType($ctxt, nullable); } 
	  | ^(c=TK_BOOLEAN (nullable=typeNullability)?) { $ty = IQLBuildBooleanType($ctxt, nullable); } 
	  | ^(c=TK_DATETIME (nullable=typeNullability)?) { $ty = IQLBuildDatetimeType($ctxt, nullable); } 
      | ^(c=TK_BIGINT (nullable=typeNullability)?) { $ty = IQLBuildInt64Type($ctxt, nullable); }
	  | ^(c=ID (nullable=typeNullability)?) { $ty = IQLBuildType($ctxt, (const char *) $ID.text->chars, nullable); } 
	;

typeNullability returns [int nullable]
    :
    TK_NULL { $nullable = 1; }
    | 
    TK_NOT { $nullable = 0; }
    ;

setStatement[IQLTreeFactoryRef ctxt]
	:
	^(TK_SET variableReference[$ctxt] expression[$ctxt])
	;

variableReference[IQLTreeFactoryRef ctxt]
    :
    ID
    |
    ^('[' ID expression[$ctxt])
    ;

switchStatement[IQLTreeFactoryRef ctxt]
    :
    ^(TK_SWITCH expression[$ctxt] switchBlock[$ctxt]+)
    ;

switchBlock[IQLTreeFactoryRef ctxt]
    :
    ^(TK_CASE DECIMAL_INTEGER_LITERAL statement[$ctxt]+)
    ;

printStatement[IQLTreeFactoryRef ctxt]
	:
	^(TK_PRINT expression[$ctxt])
    ;

ifStatement[IQLTreeFactoryRef ctxt]
	:
	^(TK_IF expression[$ctxt] statement[$ctxt] statement[$ctxt]?)
	;

statementBlock[IQLTreeFactoryRef ctxt]
	:
	^(TK_BEGIN statement[$ctxt]*)
	;

whileStatement[IQLTreeFactoryRef ctxt]
	:
	^(TK_WHILE expression[$ctxt] statement[$ctxt])
	;

expression[IQLTreeFactoryRef ctxt] returns [IQLExpressionRef e]
@init {
    int isBinary = 0;
    IQLExpressionListRef l = NULL;
}
	: 
    ^(t=TK_OR e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLogicalOr($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_AND e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLogicalAnd($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_NOT e1=expression[$ctxt] { $e = IQLBuildLogicalNot($ctxt, e1, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_IS e1=expression[$ctxt] (TK_NOT { isBinary = 1; })?  { $e = IQLBuildIsNull($ctxt, e1, isBinary, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_CASE { l = IQLExpressionListCreate($ctxt); } (whenExpression[$ctxt,l])+ (elseExpression[$ctxt,l]) { $e = IQLBuildCase($ctxt, l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    | ^(t='?' { l = IQLExpressionListCreate($ctxt); } e1=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e1); } e2=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e2); } e3=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e3); $e = IQLBuildCase($ctxt, l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); } )
    | ^(t='^' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildBitwiseXor($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='|' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildBitwiseOr($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='&' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildBitwiseAnd($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='=' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildEquals($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='>' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildGreaterThan($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='<' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLessThan($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='>=' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildGreaterThanEquals($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='<=' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLessThanEquals($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='<>' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildNotEquals($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t=TK_LIKE e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildLike($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t=TK_RLIKE e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildRLike($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='+' e1=expression[$ctxt] (e2=expression[$ctxt] { isBinary = 1; })? { $e = isBinary ? IQLBuildPlus($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)) : IQLBuildUnaryPlus($ctxt, e1, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='-' e1=expression[$ctxt] (e2=expression[$ctxt] { isBinary = 1; })? { $e = isBinary ? IQLBuildMinus($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)) : IQLBuildUnaryMinus($ctxt, e1, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='*' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildTimes($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='/' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildDivide($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='%' e1=expression[$ctxt] e2=expression[$ctxt] { $e = IQLBuildModulus($ctxt, e1, e2, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='~' e1=expression[$ctxt] { $e = IQLBuildBitwiseNot($ctxt, e1, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='#'  { l = IQLExpressionListCreate($ctxt); } (e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1); })+ { $e = IQLBuildCall($ctxt, "#", l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    | ^(t='$'  { l = IQLExpressionListCreate($ctxt); } (e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1); })+ { $e = IQLBuildCall($ctxt, "$", l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    | ^(t=TK_CAST ty=builtInType[$ctxt] e1=expression[$ctxt] { $e = IQLBuildCast($ctxt, ty, e1, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t='(' { l = IQLExpressionListCreate($ctxt); } fun=ID (e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1); })* { $e = IQLBuildCall($ctxt, (const char *)$fun.text->chars, l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    | ^(t=LITERAL_CAST fun=ID lit=STRING_LITERAL) { $e = IQLBuildLiteralCast($ctxt, (const char *)$fun.text->chars, (const char *)$lit.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | ^(t=DATETIME_LITERAL STRING_LITERAL) { $e = IQLBuildLiteralCast($ctxt, "DATETIME", (const char *)$lit.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | t=DECIMAL_INTEGER_LITERAL { $e = IQLBuildInt32($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| HEX_INTEGER_LITERAL
    | t=DECIMAL_BIGINT_LITERAL { $e = IQLBuildInt64($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | t=FLOATING_POINT_LITERAL { $e = IQLBuildDouble($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | t=DECIMAL_LITERAL { $e = IQLBuildDecimal($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| t=STRING_LITERAL { $e = IQLBuildString($ctxt, (const char *)$t.text->chars, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| WSTRING_LITERAL
	| t=TK_TRUE { $e = IQLBuildBoolean($ctxt, 1, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| t=TK_FALSE { $e = IQLBuildBoolean($ctxt, 0, $t->getLine($t), $t->getCharPositionInLine($t)); }
	| ^(t=ID (fun=ID { isBinary=1; } )?) { $e = IQLBuildVariable($ctxt, (const char *)$t.text->chars, isBinary ? (const char *)$fun.text->chars : 0, $t->getLine($t), $t->getCharPositionInLine($t)); } 
	| ^(t='[' fun=ID e1=expression[$ctxt] { $e = IQLBuildArrayRef($ctxt, (const char *)$fun.text->chars, e1, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | t=TK_NULL { $e = IQLBuildNil($ctxt, $t->getLine($t), $t->getCharPositionInLine($t)); }
    | ^(t=TK_SUM e1=expression[$ctxt] { $e = IQLBuildUnaryFun($ctxt, "SUM", e1, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_MAX e1=expression[$ctxt] { $e = IQLBuildUnaryFun($ctxt, "MAX", e1, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_MIN e1=expression[$ctxt] { $e = IQLBuildUnaryFun($ctxt, "MIN", e1, $t->getLine($t), $t->getCharPositionInLine($t)); } )
    | ^(t=TK_INTERVAL fun=ID e1=expression[$ctxt] { $e = IQLBuildInterval($ctxt, (const char *)$fun.text->chars, e1, $t->getLine($t), $t->getCharPositionInLine($t)); })
    | ^(t=ARRAY { l = IQLExpressionListCreate($ctxt); } (e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1); })* { $e = IQLBuildArray($ctxt, l, $t->getLine($t), $t->getCharPositionInLine($t)); IQLExpressionListFree($ctxt, l); })
    ;    

whenExpression[IQLTreeFactoryRef ctxt, IQLExpressionListRef l]
    :
    ^(TK_WHEN e1=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e1); } e2=expression[$ctxt]  { IQLExpressionListAppend($ctxt, l, e2); } )
    ;    

elseExpression[IQLTreeFactoryRef ctxt, IQLExpressionListRef l]
    :
    ^(TK_ELSE e1=expression[$ctxt] { IQLExpressionListAppend($ctxt, l, e1); } )
    ;    
