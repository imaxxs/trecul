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

tree grammar IQLTypeCheck;
options {
  tokenVocab=IQL;
  language=C;
  ASTLabelType    = pANTLR3_BASE_TREE;
}

@header {
#include "LLVMGen.h"
}

graph[IQLGraphContextRef ctxt]
    :  
        (node[$ctxt] | edge[$ctxt])*
    ;
     
node[IQLGraphContextRef ctxt]
@init {
const char * ty=NULL;
const char * nm=NULL;
}
    :
    ^('[' id1 = ID { ty = nm = (const char *)$id1.text->chars; } (id2 = ID { nm = (const char *) $id2.text->chars; })? { IQLGraphNodeStart($ctxt, ty, nm); } nodeParam[$ctxt]* {IQLGraphNodeComplete($ctxt); } )
    ;

nodeParam[IQLGraphContextRef ctxt]
    :
    ^('=' ID (DECIMAL_INTEGER_LITERAL { IQLGraphNodeBuildIntegerParam($ctxt, (const char *)$ID.text->chars, (const char *) $DECIMAL_INTEGER_LITERAL.text->chars); } | DOUBLE_QUOTED_STRING_LITERAL { IQLGraphNodeBuildStringParam($ctxt, (const char *)$ID.text->chars, (const char *) $DOUBLE_QUOTED_STRING_LITERAL.text->chars); }))
    ;

edge[IQLGraphContextRef ctxt]
    :
    ^('->' id1 = ID id2 = ID { IQLGraphNodeBuildEdge($ctxt, (const char *)$id1.text->chars, (const char *)$id2.text->chars); })
    ;

recordFormat[IQLRecordTypeContextRef ctxt, IQLTypeCheckContextRef tyCtxt]
    :
    fieldFormat[$ctxt, $tyCtxt]+
    ;

fieldFormat[IQLRecordTypeContextRef ctxt, IQLTypeCheckContextRef tyCtxt]
    :
    ^(ID ty = builtInType[$tyCtxt] { IQLRecordTypeBuildField($ctxt, (const char *)$ID.text->chars, ty); })
    ;

singleExpression[IQLTypeCheckContextRef ctxt] returns [IQLFieldTypeRef ret]
    :
    declareStatement[$ctxt]* ty = returnExpression[$ctxt] { ret = ty; }
    ;
    
declareStatement[IQLTypeCheckContextRef ctxt]
    :
    ^(TK_DECLARE e = expression[$ctxt] ID { IQLTypeCheckBuildLocal($ctxt, (const char *) $ID.text->chars, e.ty); })
    ;

returnExpression[IQLTypeCheckContextRef ctxt] returns [IQLFieldTypeRef ret]
    :
    ^(TK_RETURN e = expression[$ctxt] { ret = e.ty; })
    ;
    
recordConstructor[IQLTypeCheckContextRef ctxt]
@init {
    IQLTypeCheckBeginRecord($ctxt);
}
@after {
    IQLTypeCheckBuildRecord($ctxt);
}
    :
    fieldConstructor[$ctxt]+
    ;

fieldConstructor[IQLTypeCheckContextRef ctxt]
@init {
    const char * nm = NULL;
}
    :
    ^(ID '*') { IQLTypeCheckAddFields($ctxt, (const char *) $ID.text->chars); }
    |
    ^(TK_SET e = expression[$ctxt] (ID {nm = (const char *) $ID.text->chars; })?  { IQLTypeCheckAddField($ctxt, nm != NULL ? nm : e.name ? (const char *) e.name->chars : NULL, e.ty); } )
    |
    declareStatement[$ctxt]
    |
    ^(a=QUOTED_ID (b=QUOTED_ID { nm = (const char *) $b.text->chars; })?) { IQLTypeCheckQuotedId($ctxt, (const char *) $a.text->chars, nm); }
    ;

localVarOrId returns [pANTLR3_STRING argName]
    :
    ID { $argName = $ID.text; }
    ;

statement[IQLTypeCheckContextRef ctxt]
	:
	(
	setStatement[$ctxt]
	| variableDeclaration[$ctxt]
	| declareStatement[$ctxt]
	| printStatement[$ctxt]
	| ifStatement[$ctxt]
	| statementBlock[$ctxt]
	| ^(TK_RETURN (expression[$ctxt])?)
	| TK_BREAK
	| TK_CONTINUE
	| ^(TK_RAISERROR expression[$ctxt] expression[$ctxt]?)
	| switchStatement[$ctxt] 
	| whileStatement[$ctxt] 
	)
	;

variableDeclaration [IQLTypeCheckContextRef ctxt] 
	:
	^(TK_DECLARE nm = localVarOrId ty = builtInType[$ctxt] {             
            IQLTypeCheckBuildLocal($ctxt, (const char *) nm->chars, ty);
        })
	;

setStatement[IQLTypeCheckContextRef ctxt]
	:
	^(c=TK_SET lvalue = variableReference[$ctxt] rvalue = expression[$ctxt]) {
            IQLTypeCheckSetValue2(ctxt, lvalue, rvalue.ty);
            $c->u = lvalue;
        }
	;

variableReference[IQLTypeCheckContextRef ctxt] returns [IQLFieldTypeRef ty]
    :
    ID { $ty = IQLTypeCheckSymbolTableGetType($ctxt, (const char *) $ID.text->chars); }
    |
    ^('[' ID e = expression[$ctxt] { $ty = IQLTypeCheckArrayRef(ctxt, (char *)$ID.text->chars, e.ty); })
    ;

switchStatement[IQLTypeCheckContextRef ctxt]
    :
    ^(TK_SWITCH e = expression[$ctxt] { IQLTypeCheckBeginSwitch($ctxt, e.ty); } switchBlock[$ctxt]+)
    ;

switchBlock[IQLTypeCheckContextRef ctxt]
    :
    ^(TK_CASE DECIMAL_INTEGER_LITERAL statement[$ctxt]+)
    ;

printStatement[IQLTypeCheckContextRef ctxt]
	:
	^(TK_PRINT expression[$ctxt])
    ;

ifStatement[IQLTypeCheckContextRef ctxt]
	:
	^(TK_IF expression[$ctxt] statement[$ctxt] statement[$ctxt]?)
	;

statementBlock[IQLTypeCheckContextRef ctxt]
	:
	^(TK_BEGIN statement[$ctxt]*)
	;

whileStatement[IQLTypeCheckContextRef ctxt]
	:
	^(TK_WHILE e=expression[$ctxt] { IQLTypeCheckNot($ctxt, e.ty); } statementBlock[$ctxt])
	;

builtInType[IQLTypeCheckContextRef ctxt] returns [IQLFieldTypeRef ty]
@init {
nullable=0;
}
	: ^(c=TK_INTEGER (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildInt32Type($ctxt, nullable); $c->u = $ty; }
	  | ^(c=TK_DOUBLE (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildDoubleType($ctxt, nullable); $c->u = $ty; }
	  | ^(c=TK_CHAR DECIMAL_INTEGER_LITERAL (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildCharType($ctxt, (const char *) $DECIMAL_INTEGER_LITERAL.text->chars, nullable); $c->u = $ty; }
	  | ^(c=TK_VARCHAR (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildVarcharType($ctxt, nullable); $c->u = $ty; }
	  | ^(c=TK_NVARCHAR (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildNVarcharType($ctxt, nullable); $c->u = $ty; }
	  | ^(c=TK_DECIMAL (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildDecimalType($ctxt, nullable); $c->u = $ty; } 
	  | ^(c=TK_BOOLEAN (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildBooleanType($ctxt, nullable); $c->u = $ty; } 
	  | ^(c=TK_DATETIME (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildDatetimeType($ctxt, nullable); $c->u = $ty; } 
      | ^(c=TK_BIGINT (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildInt64Type($ctxt, nullable); $c->u = $ty; }
	  | ^(c=ID (nullable=typeNullability)?) { $ty = IQLTypeCheckBuildType($ctxt, (const char *) $ID.text->chars, nullable); $c->u = $ty; } 
	;

typeNullability returns [int nullable]
    :
    TK_NULL { $nullable = 1; }
    | 
    TK_NOT { $nullable = 0; }
    ;

expression[IQLTypeCheckContextRef ctxt] returns [pANTLR3_STRING name, IQLFieldTypeRef ty]
@init {
int isBinary=0;
IQLFieldTypeVectorRef types=NULL;
$name = NULL;
$ty = NULL;
}
	: 
    ^(TK_OR e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckAnd($ctxt, e1.ty, e2.ty); $TK_OR->u = $ty; })
    | ^(TK_AND e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckAnd($ctxt, e1.ty, e2.ty); $TK_AND->u = $ty; })
    | ^(TK_NOT e1 = expression[$ctxt]  { $ty = IQLTypeCheckNot($ctxt, e1.ty); $TK_NOT->u = $ty; })
    | ^(c=TK_IS e1 = expression[$ctxt] (TK_NOT {isBinary = 1; })? { $ty = IQLTypeCheckIsNull($ctxt, e1.ty, isBinary); $c->u = $ty; })
    | ^(c=TK_CASE { IQLTypeCheckBeginCase($ctxt); } (whenExpression[$ctxt])+ elseExpression[$ctxt] { $ty = IQLTypeCheckBuildCase($ctxt); $c->u = $ty; } )
    | ^(c='?' e1 = expression[$ctxt] e2 = expression[$ctxt] e3 = expression[$ctxt] { $ty = IQLTypeCheckIfThenElse($ctxt, e1.ty, e2.ty, e3.ty); $c->u = $ty; })
    | ^(c='^' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckBitwiseType($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='|' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckBitwiseType($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='&' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckBitwiseType($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='=' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckEquals($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='>' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckEquals($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='<' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckEquals($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='>=' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckEquals($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='<=' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckEquals($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='<>' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckEquals($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(TK_LIKE e1 = expression[$ctxt] e2 = expression[$ctxt])
    | ^(c=TK_RLIKE e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckRLike($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='-' e1 = expression[$ctxt] (e2 = expression[$ctxt] { isBinary=1; })? { $ty = isBinary ? IQLTypeCheckAdditiveType($ctxt, e1.ty, e2.ty) : IQLTypeCheckNegateType($ctxt, e1.ty); $c->u = $ty; })
    | ^(c='+' e1 = expression[$ctxt] (e2 = expression[$ctxt] { isBinary=1; })? { $ty = isBinary ? IQLTypeCheckAdditiveType($ctxt, e1.ty, e2.ty) : IQLTypeCheckNegateType($ctxt, e1.ty); $c->u = $ty; })
    | ^(c='*' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckMultiplicativeType($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='/' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckMultiplicativeType($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='%' e1 = expression[$ctxt] e2 = expression[$ctxt] { $ty = IQLTypeCheckModulus($ctxt, e1.ty, e2.ty); $c->u = $ty; })
    | ^(c='~' e1 = expression[$ctxt] { $ty = IQLTypeCheckUnaryBitwiseType($ctxt, e1.ty); $c->u = $ty; })
    | ^(c='#' { types = IQLFieldTypeVectorCreate(); } (e1 = expression[$ctxt] { IQLFieldTypeVectorPushBack(types, e1.ty); })*) 
        { $ty = IQLTypeCheckHash($ctxt, types); IQLFieldTypeVectorFree(types); $c->u = $ty; }
    | ^(c='$' { types = IQLFieldTypeVectorCreate(); } (e1 = expression[$ctxt] { IQLFieldTypeVectorPushBack(types, e1.ty); })*) 
        { $ty = IQLTypeCheckSortPrefix($ctxt, types); IQLFieldTypeVectorFree(types); $c->u = $ty; }
    | ^(TK_CAST outTy = builtInType[$ctxt] e1 = expression[$ctxt] { $ty = IQLTypeCheckCast($ctxt, e1.ty, outTy); $TK_CAST->u = $ty; })
    | ^(c = '(' fun = ID 
            { types = IQLFieldTypeVectorCreate(); } 
            (e1 = expression[$ctxt]  { IQLFieldTypeVectorPushBack(types, e1.ty); })*
            { 
                $ty = IQLTypeCheckCall($ctxt, (const char *) $fun.text->chars, types); 
                IQLFieldTypeVectorFree(types); 
                $c->u = $ty; 
            }
    ) 
    | ^(c=LITERAL_CAST id=ID STRING_LITERAL { $ty = IQLTypeCheckBuildType($ctxt, (const char *) $id.text->chars, 0); $c->u = $ty; })
    | ^(DATETIME_LITERAL STRING_LITERAL { $ty = IQLTypeCheckBuildDatetimeType($ctxt, 0); $DATETIME_LITERAL->u = $ty; })
    | DECIMAL_INTEGER_LITERAL { $ty = IQLTypeCheckBuildInt32Type($ctxt, 0); $DECIMAL_INTEGER_LITERAL->u = $ty; }
	| HEX_INTEGER_LITERAL { $ty = IQLTypeCheckBuildInt32Type($ctxt, 0); $HEX_INTEGER_LITERAL->u = $ty; }
    | DECIMAL_BIGINT_LITERAL { $ty = IQLTypeCheckBuildInt64Type($ctxt, 0); $DECIMAL_BIGINT_LITERAL->u = $ty; }
    | FLOATING_POINT_LITERAL { $ty = IQLTypeCheckBuildDoubleType($ctxt, 0); $FLOATING_POINT_LITERAL->u = $ty; }
    | DECIMAL_LITERAL { $ty = IQLTypeCheckBuildDecimalType($ctxt, 0); $DECIMAL_LITERAL->u = $ty; }
	| STRING_LITERAL { $ty = IQLTypeCheckBuildVarcharType($ctxt, 0); $STRING_LITERAL->u = $ty; }
	| WSTRING_LITERAL { $ty = IQLTypeCheckBuildNVarcharType($ctxt, 0); $WSTRING_LITERAL->u = $ty; }
	| TK_TRUE { $ty = IQLTypeCheckBuildBooleanType($ctxt, 0); $TK_TRUE->u = $ty; }
	| TK_FALSE { $ty = IQLTypeCheckBuildBooleanType($ctxt, 0); $TK_FALSE->u = $ty; }
	| ^(id = ID (id2=ID { isBinary=1; })?) { $name = isBinary ? $id2.text : $id.text; $ty = IQLTypeCheckBuildVariableRef($ctxt, (const char *) $id.text->chars, isBinary ? (const char *) $id2.text->chars : 0); $id->u = $ty; }
	| ^(c = '[' id=ID e = expression[$ctxt]) { $ty = IQLTypeCheckArrayRef($ctxt, (char *) $id.text->chars, e.ty); $c->u = $ty; }
    | TK_NULL { $ty = IQLTypeCheckBuildNilType($ctxt); $TK_NULL->u = $ty; }
    | ^(TK_SUM { IQLTypeCheckBeginAggregateFunction($ctxt); } e1 = expression[$ctxt] { $ty = IQLTypeCheckBuildAggregateFunction($ctxt, e1.ty); $TK_SUM->u = $ty; } )
    | ^(TK_MAX { IQLTypeCheckBeginAggregateFunction($ctxt); } e1 = expression[$ctxt] { $ty = IQLTypeCheckBuildAggregateFunction($ctxt, e1.ty); $TK_MAX->u = $ty; } )
    | ^(TK_MIN { IQLTypeCheckBeginAggregateFunction($ctxt); } e1 = expression[$ctxt] { $ty = IQLTypeCheckBuildAggregateFunction($ctxt, e1.ty); $TK_MIN->u = $ty; } )
    | ^(TK_INTERVAL intervalType = ID e1 = expression[$ctxt] { $ty = IQLTypeCheckBuildInterval($ctxt, (const char *) $intervalType.text->chars, e1.ty); $TK_INTERVAL->u = $ty; } )
    | ^(c=ARRAY { types = IQLFieldTypeVectorCreate(); } (e1 = expression[$ctxt] { IQLFieldTypeVectorPushBack(types, e1.ty); })*) 
        { $ty = IQLTypeCheckArray($ctxt, types); IQLFieldTypeVectorFree(types); $c->u = $ty; }
    ;    

whenExpression[IQLTypeCheckContextRef ctxt]
    :
    ^(TK_WHEN e1 = expression[$ctxt] { IQLTypeCheckAddCondition($ctxt, e1.ty); } e2 = expression[$ctxt] { IQLTypeCheckAddValue($ctxt, e2.ty); } )
    ;    

elseExpression[IQLTypeCheckContextRef ctxt]
    :
    ^(TK_ELSE e1 = expression[$ctxt] { IQLTypeCheckAddValue($ctxt, e1.ty); })
    ;    
