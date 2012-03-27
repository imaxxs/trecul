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
  output=AST;
  language=C;
  ASTLabelType    = pANTLR3_BASE_TREE;
}

singleExpression
  :
  declareStatement* returnExpression
  ;

declareStatement
    :
    ^(TK_DECLARE expression ID)
    ;

returnExpression
    :
    ^(TK_RETURN expression)
    ;

program
  :
  ^(TK_CREATE programArgDecl* returnsDecl? statementBlock)
  ;

returnsDecl
        :
        ^(TK_RETURNS builtInType)
        ;

localVarOrId
    :
    ID
    ;

programArgDecl
    :
	^(TK_DECLARE localVarOrId builtInType TK_OUTPUT?) 
	;

statement
	:
	(
	setStatement
	| variableDeclaration
	| printStatement
	| ifStatement
	| statementBlock
	| ^(TK_RETURN expression?)
	| TK_BREAK
	| TK_CONTINUE
	| ^(TK_RAISERROR expression expression?)
	| switchStatement 
	| whileStatement 
	)
	;

variableDeclaration
	:
	^(TK_DECLARE localVarOrId builtInType)
	;

builtInType
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

setStatement
	:
	^(TK_SET variableReference expression)
	;

variableReference
    :
    ID
    |
    ^('[' ID expression)
    ;

switchStatement
    :
    ^(TK_SWITCH expression switchBlock+)
    ;

switchBlock
    :
    ^(TK_CASE DECIMAL_INTEGER_LITERAL statement+)
    ;

printStatement
	:
	^(TK_PRINT expression)
    ;

ifStatement
	:
	^(TK_IF expression statement statement?)
	;

statementBlock
	:
	^(TK_BEGIN statement*)
	;

whileStatement
	:
	^(TK_WHILE expression statement)
	;

expression
	: 
    ^(TK_OR expression expression)
    | ^(TK_AND expression expression)
    | ^(TK_NOT expression)
    | ^(TK_IS expression TK_NOT?)
    | ^(TK_CASE (whenExpression)+ (elseExpression) )
    | ^('?' expression expression expression)
    | ^('^' expression expression)
    | ^('|' expression expression)
    | ^('&' expression expression)
    | ^('=' expression expression)
    | ^('>' expression expression)
    | ^('<' expression expression)
    | ^('>=' expression expression)
    | ^('<=' expression expression)
    | ^('<>' expression expression)
    | ^(TK_LIKE expression expression)
    | ^(TK_RLIKE expression expression)
    | ^('+' expression expression?)
    | ^('-' expression expression?)
    | ^('*' expression expression)
    | ^('/' expression expression)
    | ^('%' expression expression)
    | ^('~' expression)
    | ^('#' expression+)
    | ^('$' expression+)
    | ^(TK_CAST builtInType expression)
    | ^('(' expression+)
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
	| ^(ID ID?)
	| ^('[' ID expression)
    | TK_NULL
    | ^(TK_SUM expression)
    | ^(TK_MAX expression)
    | ^(TK_MIN expression)
    | ^(TK_INTERVAL ID expression)
    ;    

whenExpression
    :
    ^(TK_WHEN expression expression )
    ;    

elseExpression
    :
    ^(TK_ELSE expression)
    ;    
