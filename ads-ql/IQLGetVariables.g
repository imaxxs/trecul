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

tree grammar IQLGetVariables;
options {
  tokenVocab=IQL;
  language=C;
  ASTLabelType    = pANTLR3_BASE_TREE;
}

@header {
#include "GetVariablesPass.h"
}

singleExpression[IQLGetVariablesContextRef ctxt]
    :
    declareStatement[$ctxt]* returnExpression[$ctxt]    
    ;
    
declareStatement[IQLGetVariablesContextRef ctxt]
    :
    ^(TK_DECLARE expression[$ctxt] ID { IQLGetVariablesBuildLocal($ctxt, (const char *) $ID.text->chars); })
    ;

returnExpression[IQLGetVariablesContextRef ctxt]
    :
    ^(TK_RETURN expression[$ctxt])
    ;
    
recordConstructor[IQLGetVariablesContextRef ctxt]
    :
    fieldConstructor[$ctxt]+
    ;

fieldConstructor[IQLGetVariablesContextRef ctxt]
    :
    ^(ID '*')
    |
    ^(TK_SET expression[$ctxt] (ID { IQLGetVariablesAddField($ctxt, (const char *) $ID.text->chars); })? )
    |
    declareStatement[$ctxt]
    |
    ^(QUOTED_ID (QUOTED_ID)?)
    ;

builtInType[IQLGetVariablesContextRef ctxt] 
	: ^(TK_INTEGER typeNullability?)
	  | ^(TK_DOUBLE typeNullability?)
	  | ^(TK_CHAR DECIMAL_INTEGER_LITERAL typeNullability?)
	  | ^(TK_VARCHAR typeNullability?)
	  | ^(TK_NVARCHAR typeNullability?)
	  | ^(TK_DECIMAL typeNullability?)
	  | ^(TK_BOOLEAN typeNullability?)
	  | ^(TK_DATETIME typeNullability?)
      | ^(TK_BIGINT typeNullability?)
	  | ^(ID typeNullability?)
	;

typeNullability
    :
    TK_NULL 
    | 
    TK_NOT 
    ;

expression[IQLGetVariablesContextRef ctxt] 
	: 
    ^(TK_OR expression[$ctxt] expression[$ctxt])
    | ^(TK_AND expression[$ctxt] expression[$ctxt])
    | ^(TK_NOT expression[$ctxt])
    | ^(TK_IS expression[$ctxt] TK_NOT?)
    | ^(TK_CASE (whenExpression[$ctxt])+ elseExpression[$ctxt])
    | ^('?' expression[$ctxt] expression[$ctxt] expression[$ctxt] )
    | ^('^' expression[$ctxt] expression[$ctxt])
    | ^('|' expression[$ctxt] expression[$ctxt])
    | ^('&' expression[$ctxt] expression[$ctxt])
    | ^('=' expression[$ctxt] expression[$ctxt])
    | ^('>' expression[$ctxt] expression[$ctxt])
    | ^('<' expression[$ctxt] expression[$ctxt])
    | ^('>=' expression[$ctxt] expression[$ctxt])
    | ^('<=' expression[$ctxt] expression[$ctxt])
    | ^('<>' expression[$ctxt] expression[$ctxt])
    | ^(TK_LIKE expression[$ctxt] expression[$ctxt])
    | ^(TK_RLIKE expression[$ctxt] expression[$ctxt])
    | ^('-' expression[$ctxt] (expression[$ctxt])? )
    | ^('+' expression[$ctxt] (expression[$ctxt])? )
    | ^('*' expression[$ctxt] expression[$ctxt])
    | ^('/' expression[$ctxt] expression[$ctxt])
    | ^('%' expression[$ctxt] expression[$ctxt])
    | ^('~' expression[$ctxt])
    | ^('#' (expression[$ctxt])*)
    | ^('$' (expression[$ctxt])*) 
    | ^(TK_CAST outTy = builtInType[$ctxt] expression[$ctxt])
    | ^(c = '(' fun = ID 
            (expression[$ctxt])*
    ) 
    | ^(LITERAL_CAST ID STRING_LITERAL)
    | ^(DATETIME_LITERAL STRING_LITERAL)
    | DECIMAL_INTEGER_LITERAL
	| HEX_INTEGER_LITERAL
    | DECIMAL_BIGINT_LITERAL
    | FLOATING_POINT_LITERAL
    | DECIMAL_LITERAL
	| STRING_LITERAL
	| WSTRING_LITERAL
	| TK_TRUE
	| TK_FALSE
	| ^(id = ID ID?) { IQLGetVariablesBuildVariableReference($ctxt, (const char *) $id.text->chars); }
	| ^('[' id=ID expression[$ctxt])  { IQLGetVariablesBuildArrayReference($ctxt, (const char *) $id.text->chars); }
    | TK_NULL
    | ^(TK_SUM { IQLGetVariablesBeginAggregateFunction($ctxt); } expression[$ctxt] { IQLGetVariablesBuildAggregateFunction($ctxt); })
    | ^(TK_MAX { IQLGetVariablesBeginAggregateFunction($ctxt); } expression[$ctxt] { IQLGetVariablesBuildAggregateFunction($ctxt); })
    | ^(TK_MIN { IQLGetVariablesBeginAggregateFunction($ctxt); } expression[$ctxt] { IQLGetVariablesBuildAggregateFunction($ctxt); })
    | ^(TK_INTERVAL ID expression[$ctxt])
    | ^(ARRAY (expression[$ctxt])*)
    ;    

whenExpression[IQLGetVariablesContextRef ctxt]
    :
    ^(TK_WHEN expression[$ctxt] expression[$ctxt])
    ;    

elseExpression[IQLGetVariablesContextRef ctxt]
    :
    ^(TK_ELSE expression[$ctxt])
    ;    
