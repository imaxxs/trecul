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

grammar IQL;
options {
  language=C;
  backtrack=true;
  memoize=true;
  output=AST;
  ASTLabelType    = pANTLR3_BASE_TREE;
}
tokens {
  CASE_NO_ELSE;
  LITERAL_CAST;
  DATETIME_LITERAL;
  DECIMAL_BIGINT_LITERAL; 
  ARRAY;
}

graph
    :
    graphStatement*
    ;

graphStatement
    :
    ( node | edge) ';'!
    ;
node
    :
    (id1 = ID '=')? id2=ID '[' nodeParams? ']' -> ^('[' $id2 $id1? nodeParams?)
    ;

nodeParams
    :
    nodeParam (','! nodeParam)*
    ;

protected
nodeParam
    :
    ID '='^ (DECIMAL_INTEGER_LITERAL | DOUBLE_QUOTED_STRING_LITERAL)
    ;

edge
    :
    ID '->'^ ID 
    ;

singleExpression
    :
    (declareStatement ','!)* returnExpression EOF
    ;

declareStatement
    :
    TK_DECLARE ID '=' expression -> ^(TK_DECLARE expression ID)
    ;

returnExpression
    :
    expression -> ^(TK_RETURN expression)
    ;

recordConstructor
    :
    fieldConstructor (','! fieldConstructor)* EOF
    ;

fieldConstructor
    :
    ID '.' '*' -> ^(ID '*')
    |
    expression (TK_AS ID)? -> ^(TK_SET expression ID?)
    |
    declareStatement
    |
    id1=QUOTED_ID (TK_AS id2=QUOTED_ID)? -> ^($id1 $id2?)
    ;

recordFormat
    :
    fieldFormat (','! fieldFormat)*
    ;

fieldFormat
    :
    ID^ builtInType
    ;

localVarOrId
    :
    ID
    ;

statementList
	:
	(statement)* EOF -> ^(TK_BEGIN["BEGIN"] statement*) 
	;

statement
	:
	(
	setStatement (';'!)?
	| variableDeclaration (';'!)?
	| declareStatement (';'!)?
	| printStatement (';'!)?
	| ifStatement
	| statementBlock (';'!)?
	| TK_RETURN^ (expression)? (';'!)?
	| TK_BREAK (';'!)?
	| TK_CONTINUE (';'!)?
	| TK_RAISERROR^ '('! expression (','! expression )? ')'! (';'!)?
	| switchStatement 
	| whileStatement 
	)
	;

variableDeclaration
	:
	TK_DECLARE^ localVarOrId builtInType
	;

builtInTypeNoNull
	: (TK_INTEGER^ 
	  | TK_DOUBLE^ (TK_PRECISION!) 
	  | TK_CHAR^ '('! DECIMAL_INTEGER_LITERAL ')'!
	  | TK_VARCHAR^ ('('! DECIMAL_INTEGER_LITERAL! ')'!)? 
	  | TK_NVARCHAR^ ('('! DECIMAL_INTEGER_LITERAL! ')'!)? 
	  | TK_DECIMAL^ 
	  | TK_BOOLEAN^ 
	  | TK_DATETIME^ 
      | TK_BIGINT^ 
	  | ID^ 
        ) 
	;

builtInType
	: (TK_INTEGER^ 
	  | TK_DOUBLE^ (TK_PRECISION!) 
	  | TK_CHAR^ '('! DECIMAL_INTEGER_LITERAL ')'!
	  | TK_VARCHAR^ ('('! DECIMAL_INTEGER_LITERAL! ')'!)? 
	  | TK_NVARCHAR^ ('('! DECIMAL_INTEGER_LITERAL! ')'!)? 
	  | TK_DECIMAL^ 
	  | TK_BOOLEAN^ 
	  | TK_DATETIME^ 
      | TK_BIGINT^ 
	  | ID^ 
        ) typeNullability?
	;

typeNullability
    :
    TK_NULL 
    | 
    TK_NOT TK_NULL!
    ;

setStatement
	:
	TK_SET variableRef '=' expression -> ^(TK_SET variableRef expression)
	;

variableRef
    :
        (
    ID
    |
    ID '['^ expression ']'!
        )
    ;

switchStatement
    :
    TK_SWITCH^ expression TK_BEGIN! (switchBlock)+ TK_END!
    ;

protected
switchBlock
    :
    TK_CASE^ DECIMAL_INTEGER_LITERAL statement+
    ;

printStatement
	:
	TK_PRINT expression -> ^(TK_PRINT expression)
    ;

ifStatement
	:
	TK_IF expression s1=statement (TK_ELSE s2=statement)? -> ^(TK_IF expression $s1 $s2?)
	;

statementBlock
	:
	TK_BEGIN^ (statement)* TK_END!
	;

whileStatement
	:
	TK_WHILE expression keyDo whileStatementList TK_END TK_WHILE -> ^(TK_WHILE expression whileStatementList)
	;

keyDo
	:
	{ 0==strcmp(LT(1)->getText(LT(1))->chars,"DO") }? ID
	;

whileStatementList
	:
	(statement)* -> ^(TK_BEGIN["BEGIN"] statement*)
	;

// expressions
// Note that most of these expressions follow the pattern
//   thisLevelExpression :
//       nextHigherPrecedenceExpression
//           (OPERATOR nextHigherPrecedenceExpression)*
// which is a standard recursive definition for a parsing an expression.
// The operators in java have the following precedences:
//    lowest  (13)  = *= /= %= += -= <<= >>= >>>= &= ^= |=
//            (12)  ?:
//            (11)  ||
//            (10)  &&
//            ( 9)  |
//            ( 8)  ^
//            ( 7)  &
//            ( 6)  == !=
//            ( 5)  < <= > >=
//            ( 4)  << >>
//            ( 3)  +(binary) -(binary)
//            ( 2)  * / %
//            ( 1)  ++ -- +(unary) -(unary)  ~  !  (type)
//                  []   () (method call)  . (dot -- identifier qualification)
//                  new   ()  (explicit parenthesis)
//
// the last two are not usually on a precedence chart; I put them in
// to point out that new has a higher precedence than '.', so you
// can validy use
//     new Frame().show()
// 
// Note that the above precedence levels map to the rules below...
// Once you have a precedence chart, writing the appropriate rules as below
//   is usually very straightfoward

/******************************************
Precedence of IQL operators

(1) + (Positive), - (Negative), ~ (Bitwise NOT)
(2) * (Multiply), / (Division), % (Modulo)
(3) + (Add), (+ Concatenate), - (Subtract)
(4) =,  >,  <,  >=,  <=,  <>,  !=,  !>,  !< (Comparison operators)
(5) ^ (Bitwise Exlusive OR), & (Bitwise AND), | (Bitwise OR)
(6) NOT
(7) AND
(8) ALL, ANY, BETWEEN, IN, LIKE, OR, SOME
*********************************************/

// Don't turn off default tree generation here since our goal is to append
// an EXPR node to the root of the tree.

expression
	: ternaryChoiceExpression
	;

ternaryChoiceExpression
    :
    weakExpression ( '?'^ expression ':'! expression)?    
    ;

// TODO: can't actually have multiple LIKEs
weakExpression // Level (8)
	:
		conjunctiveExpression (TK_OR^ conjunctiveExpression)*
	;

conjunctiveExpression // Level (7) 
	:
	negatedExpression (TK_AND^ negatedExpression)*
	;

negatedExpression  // Level (6)
	:
		(TK_NOT^)* isNullExpression
	;

isNullExpression // Level (5 1/2)
  : 
  bitwiseExpression (TK_IS^ TK_NOT? TK_NULL!)?
  ;

bitwiseExpression // Level (5)
	: conditionalExpression 
		( 
            (
		    '^'^ 
			| '|'^
			| '&'^ 
            ) conditionalExpression
		)*
	;

conditionalExpression // Level (4)
	: additiveExpression
	((
	'='^
	| '>'^
	| '<'^
	| '>='^ 
	| '<='^
	| '<>'^
    | TK_LIKE^
    | TK_RLIKE^
	) additiveExpression)?
	;

additiveExpression // Level (3)
	: multiplicativeExpression
	((
	'+'^ 
	| '-'^
	) multiplicativeExpression)*
	;

multiplicativeExpression // Level (2)
	: unaryExpression
	((
	'*'^ 
	| '/'^ 
	| '%'^
	) unaryExpression)*
	;

unaryExpression // Level (1)
	:
	'+'^ unaryExpression
	| '-'^ unaryExpression
    | unaryExpressionNotPlusMinus
	;

unaryExpressionNotPlusMinus
	:
	(
	'~'^ 
	)? postfixExpression
	;
    

postfixExpression
	:
	primaryExpression
	(
	'('^ 
	argList
	')'!
	)?
	| '#'^ '('! expressionList ')'!
	| '$'^ '('! expressionList ')'!
    | castExpression
    | caseExpression
    | intervalExpression
    | aggregateFunction
	;

protected
aggregateFunction
    :
    TK_SUM^ '('! expression ')'!
    |
    TK_MAX^ '('! expression ')'!
    |
    TK_MIN^ '('! expression ')'!
    ;

protected
caseExpression
	:
	(TK_CASE TK_WHEN) => TK_CASE^ whenExpression3+ elseExpression3
	| TK_CASE^ weakExpression (whenExpression[0])+ (elseExpression)? TK_END!
	;

protected
whenExpression2
    :
    TK_WHEN e1=weakExpression TK_THEN e2=weakExpression e3=whenExpression2 -> ^('?' $e1 $e2 $e3)
    |
    TK_ELSE weakExpression TK_END -> weakExpression
    |
    TK_END -> TK_NULL
    ;

protected
whenExpression3
    :
    TK_WHEN e1=weakExpression TK_THEN e2=weakExpression -> ^(TK_WHEN $e1 $e2)
    ;

protected
elseExpression3
    :
    TK_ELSE weakExpression TK_END -> ^(TK_ELSE weakExpression)
    |
    TK_END -> ^(TK_ELSE TK_NULL)
    ;

protected 
whenExpression[int isSimple]
	:
	TK_WHEN^  weakExpression TK_THEN! weakExpression
	;

protected 
elseExpression
	:
	TK_ELSE! weakExpression 
	;

protected
intervalExpression
    :
    TK_INTERVAL expression ID -> ^(TK_INTERVAL ID expression)
    ;

castExpression
    :
    (TK_CAST '(' STRING_LITERAL TK_AS TK_DATETIME ')') => TK_CAST '(' STRING_LITERAL TK_AS TK_DATETIME ')' -> ^(DATETIME_LITERAL STRING_LITERAL)
    |
    (TK_CAST '(' STRING_LITERAL TK_AS ID ')') => TK_CAST '(' STRING_LITERAL TK_AS ID ')' -> ^(LITERAL_CAST ID STRING_LITERAL)
    |
    TK_CAST '(' expression TK_AS builtInTypeNoNull ')' -> ^(TK_CAST builtInTypeNoNull expression)
	;

// Argument List can be an expression list or it can be empty
// If the arg list is empty, generate an empty expression list
argList
	:
	(expressionList
	| 
    )
	;

// This is a list of expressions.
expressionList
	:	expression (','! expression)*
	;

primaryExpression
	:
	(DECIMAL_INTEGER_LITERAL
	| HEX_INTEGER_LITERAL
	| DECIMAL_BIGINT_LITERAL
    | FLOATING_POINT_LITERAL
    | DECIMAL_LITERAL
	| STRING_LITERAL
			{ 
				// Unquotify
			}
	| WSTRING_LITERAL
			{ 
				// Unquotify
			}
	| TK_TRUE
	| TK_FALSE
	| ID^ ('.'! ID)?
	| ID '['^ expression ']'!
  | TK_NULL
  | '('! expression ')'!
  | arrayExpression
	);

arrayExpression
	:
    '[' expressionList ']' -> ^(ARRAY expressionList)
	;

//Lexer
TK_ADD : 'ADD';
TK_ALL : 'ALL';
TK_ALTER : 'ALTER';
TK_AND : 'AND';
TK_ANY : 'ANY';
TK_AS : 'AS';
TK_ASC : 'ASC';
TK_AVG : 'AVG';
TK_BEGIN : 'BEGIN';
TK_BETWEEN : 'BETWEEN';
TK_BIGINT : 'BIGINT';
TK_BOOLEAN : 'BOOLEAN';
TK_BREAK : 'BREAK';
TK_BY : 'BY';
TK_CASE : 'CASE';
TK_CAST : 'CAST';
TK_CHAR : 'CHAR';
TK_COALESCE : 'COALESCE';
TK_CONTINUE : 'CONTINUE';
TK_COUNT : 'COUNT';
TK_CREATE : 'CREATE';
TK_CROSS : 'CROSS';
TK_DATETIME : 'DATETIME';
TK_DECLARE : 'DECLARE';
TK_DECIMAL : 'DECIMAL';
TK_DESC : 'DESC';
TK_DISTINCT : 'DISTINCT';
TK_DOUBLE : 'DOUBLE';
TK_ELSE : 'ELSE';
TK_END : 'END';
TK_EXISTS : 'EXISTS';
TK_FALSE : 'FALSE';
TK_FROM : 'FROM';
TK_FULL : 'FULL';
TK_FUNCTION : 'FUNCTION';
TK_GROUP : 'GROUP';
TK_HAVING : 'HAVING';
TK_IF : 'IF';
TK_IN : 'IN';
TK_INDEX : 'INDEX';
TK_INNER : 'INNER';
TK_INTO : 'INTO';
TK_INTEGER : 'INTEGER'; 
TK_INTERVAL : 'INTERVAL';
TK_IS : 'IS';
TK_JOIN : 'JOIN';
TK_KEY : 'KEY'; 
TK_LEFT : 'LEFT';
TK_LIKE : 'LIKE';
TK_MAX : 'MAX';
TK_MIN : 'MIN';
TK_NOT : 'NOT'; 
TK_NULL : 'NULL';
TK_NVARCHAR : 'NVARCHAR';
TK_ON : 'ON';
TK_OR : 'OR';
TK_ORDER : 'ORDER';
TK_OUTER : 'OUTER';
TK_OUTPUT : 'OUTPUT';
TK_PRECISION : 'PRECISION';
TK_PRINT : 'PRINT';
TK_PROCEDURE : 'PROCEDURE';
TK_RAISERROR : 'RAISERROR';
TK_RETURN : 'RETURN';
TK_RETURNS : 'RETURNS';
TK_RIGHT : 'RIGHT';
TK_RLIKE : 'RLIKE';
TK_SELECT : 'SELECT';
TK_SET : 'SET';
TK_SOME : 'SOME';
TK_SUM : 'SUM';
TK_SWITCH : 'SWITCH';
TK_THEN : 'THEN';
TK_TRUE : 'TRUE';
TK_UNION : 'UNION';
TK_VARCHAR : 'VARCHAR';
TK_WHEN : 'WHEN';
TK_WHERE : 'WHERE';
TK_WHILE : 'WHILE';
TK_WITH : 'WITH';

STRING_LITERAL
    :  '\'' ( ESCAPE_SEQUENCE | ~('\\'|'\'') )* '\''
    ;

WSTRING_LITERAL
    :  'N\'' ( ESCAPE_SEQUENCE | ~('\\'|'\'') )* '\''
    ;

DOUBLE_QUOTED_STRING_LITERAL
    :  '"' ( ESCAPE_SEQUENCE | ~('\\'|'"') )* '"'
    ;

fragment
ESCAPE_SEQUENCE
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESCAPE
    |   OCTAL_ESCAPE
    ;

fragment
OCTAL_ESCAPE
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
HEX_DIGIT
	:	('0'..'9'|'A'..'F'|'a'..'f')
	;
fragment
UNICODE_ESCAPE
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

WS  :  (' '|'\r'|'\t'|'\u000C'|'\n') {$channel=HIDDEN;}
    ;

ML_COMMENT
    :   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;

SL_COMMENT
    : '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    ;

ID
	:	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'_'|'0'..'9')*
	;
    
QUOTED_ID
	:	'`' ~('`')* '`'
	;
    

fragment
BIGINT_SUFFIX
	:	'L' 'L'
	;

HEX_INTEGER_LITERAL 
    :
    '0' ('x'|'X') HEX_DIGIT+ BIGINT_SUFFIX?
    ;

DECIMAL_INTEGER_LITERAL
    :
    ('0' | '1'..'9' '0'..'9'*)
    ;

DECIMAL_BIGINT_LITERAL
    :
    ('0' | '1'..'9' '0'..'9'*) BIGINT_SUFFIX
    ;

FLOATING_POINT_LITERAL
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT FLOAT_SUFFIX?
    |   '.' ('0'..'9')+ EXPONENT FLOAT_SUFFIX?
    |   ('0'..'9')+ (	  EXPONENT FLOAT_SUFFIX?
						| FLOAT_SUFFIX
					)
	;
// a couple protected methods to assist in matching floating point numbers
fragment
EXPONENT
	:	('e'|'E') ('+'|'-')? ('0'..'9')+
	;


fragment
FLOAT_SUFFIX
	:	'f'|'F'|'d'|'D'
	;

DECIMAL_LITERAL
    :   ('0'..'9')+ '.' ('0'..'9')* 
    |   '.' ('0'..'9')+ 
    ;





