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

#include <cmath>
#include <iostream>

#include <boost/format.hpp>
#include <boost/progress.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "IQLInterpreter.hh"
#include "IQLExpression.hh"
#include "SuperFastHash.h"

#define BOOST_TEST_MODULE MyTest
#include <boost/test/unit_test.hpp>

boost::shared_ptr<RecordType> createLogInputType(DynamicRecordContext & ctxt)
{
  std::vector<RecordMember> members;
  members.push_back(RecordMember("ignore", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("cpcode", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("time", CharType::Get(ctxt, 11)));
  members.push_back(RecordMember("ip_address", CharType::Get(ctxt, 8)));
  members.push_back(RecordMember("method", CharType::Get(ctxt, 4)));
  members.push_back(RecordMember("http_status", CharType::Get(ctxt, 3)));
  members.push_back(RecordMember("mime_type", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("user_agent", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("url", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("referrer", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("cookies", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("custom_field", VarcharType::Get(ctxt)));
  return boost::shared_ptr<RecordType> (new RecordType(members));
}

// Test for our AST that will replace ANTLR3 ASTs and
// ANTLR3 tree walking.
BOOST_AUTO_TEST_CASE(testNativeAST)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&emptyTy);

  {
    RecordTypeFunction hasher(ctxt, "charhash", types, "23   =    \n 45992354454LL");
    IQLExpression * ast = hasher.getAST();
    BOOST_CHECK_EQUAL(IQLExpression::EQ, ast->getNodeType());
    BOOST_CHECK_EQUAL(2U, ast->args_size());
    BOOST_CHECK_EQUAL(1, ast->getLine());
    BOOST_CHECK_EQUAL(4, ast->getColumn());
    IQLExpression * left = *ast->begin_args();
    IQLExpression * right = *(ast->begin_args()+1);
    BOOST_CHECK_EQUAL(IQLExpression::INT32, left->getNodeType());
    BOOST_CHECK_EQUAL(0U, left->args_size());
    BOOST_CHECK_EQUAL(1, left->getLine());
    BOOST_CHECK_EQUAL(0, left->getColumn());
    BOOST_CHECK_EQUAL(IQLExpression::INT64, right->getNodeType());
    BOOST_CHECK_EQUAL(0U, right->args_size());
    BOOST_CHECK_EQUAL(2, right->getLine());
    BOOST_CHECK_EQUAL(1, right->getColumn());
  }
  {
    RecordTypeFunction hasher(ctxt, "charhash", types, " 2388234LL  \n > 77823e+02");
    IQLExpression * ast = hasher.getAST();
    BOOST_CHECK_EQUAL(IQLExpression::GTN, ast->getNodeType());
    BOOST_CHECK_EQUAL(2U, ast->args_size());
    BOOST_CHECK_EQUAL(2, ast->getLine());
    BOOST_CHECK_EQUAL(1, ast->getColumn());
    IQLExpression * left = *ast->begin_args();
    IQLExpression * right = *(ast->begin_args()+1);
    BOOST_REQUIRE(left != NULL);
    BOOST_CHECK_EQUAL(IQLExpression::INT64, left->getNodeType());
    BOOST_CHECK_EQUAL(0U, left->args_size());
    BOOST_CHECK_EQUAL(1, left->getLine());
    BOOST_CHECK_EQUAL(0, left->getColumn());
    BOOST_REQUIRE(right != NULL);
    BOOST_CHECK_EQUAL(IQLExpression::DOUBLE, right->getNodeType());
    BOOST_CHECK_EQUAL(0U, right->args_size());
    BOOST_CHECK_EQUAL(2, right->getLine());
    BOOST_CHECK_EQUAL(3, right->getColumn());
  }
  // Check a bunch of binary operators for basic 
  // parsing sanity.
  const char * toks [6] = {"= ", "> ", "< ", "<>", ">=", "<="};
  IQLExpression::NodeType nodeTypes[6] = {IQLExpression::EQ, IQLExpression::GTN, 
					  IQLExpression::LTN, IQLExpression::NEQ,
					  IQLExpression::GTEQ, IQLExpression::LTEQ};
  for(int i=0; i<6; ++i) {
    RecordTypeFunction hasher(ctxt, "charhash", types, 
			      (boost::format("99.77 %1% 99.7734") %
			       toks[i]).str());
    IQLExpression * ast = hasher.getAST();
    BOOST_CHECK_EQUAL(nodeTypes[i], ast->getNodeType());
    BOOST_CHECK_EQUAL(2U, ast->args_size());
    BOOST_CHECK_EQUAL(1, ast->getLine());
    BOOST_CHECK_EQUAL(5, ast->getColumn());
    IQLExpression * left = *ast->begin_args();
    IQLExpression * right = *(ast->begin_args()+1);
    BOOST_REQUIRE(left != NULL);
    BOOST_CHECK_EQUAL(IQLExpression::DECIMAL, left->getNodeType());
    BOOST_CHECK_EQUAL(0U, left->args_size());
    BOOST_CHECK_EQUAL(1, left->getLine());
    BOOST_CHECK_EQUAL(0, left->getColumn());
    BOOST_REQUIRE(right != NULL);
    BOOST_CHECK_EQUAL(IQLExpression::DECIMAL, right->getNodeType());
    BOOST_CHECK_EQUAL(0U, right->args_size());
    BOOST_CHECK_EQUAL(1, right->getLine());
    BOOST_CHECK_EQUAL(8, right->getColumn());
  }
  const char * logical_toks [2] = {"AND", "OR "};
  IQLExpression::NodeType logicalNodeTypes[2] = {IQLExpression::LAND, IQLExpression::LOR};
  for(int i=0; i<2; ++i) {
    RecordTypeFunction hasher(ctxt, "charhash", types, 
			      (boost::format("23 > 19 %1% 34<99") % 
			       logical_toks[i]).str());
    IQLExpression * ast = hasher.getAST();
    BOOST_CHECK_EQUAL(logicalNodeTypes[i], ast->getNodeType());
    BOOST_CHECK_EQUAL(2U, ast->args_size());
    BOOST_CHECK_EQUAL(1, ast->getLine());
    BOOST_CHECK_EQUAL(7, ast->getColumn());
    IQLExpression * left = *ast->begin_args();
    IQLExpression * right = *(ast->begin_args()+1);
    BOOST_REQUIRE(left != NULL);
    BOOST_CHECK_EQUAL(IQLExpression::GTN, left->getNodeType());
    BOOST_CHECK_EQUAL(2U, left->args_size());
    BOOST_CHECK_EQUAL(1, left->getLine());
    BOOST_CHECK_EQUAL(2, left->getColumn());
    BOOST_REQUIRE(right != NULL);
    BOOST_CHECK_EQUAL(IQLExpression::LTN, right->getNodeType());
    BOOST_CHECK_EQUAL(2U, right->args_size());
    BOOST_CHECK_EQUAL(1, right->getLine());
    BOOST_CHECK_EQUAL(13, right->getColumn());
  }
  const char * unary_fun_toks [2] = {"#  ", "$  "};
  for(int i=0; i<2; ++i) {
    RecordTypeFunction hasher(ctxt, "charhash", types, 
			      (boost::format("%1%(c)") % 
			       unary_fun_toks[i]).str());
    IQLExpression * ast = hasher.getAST();
    BOOST_CHECK_EQUAL(IQLExpression::CALL, ast->getNodeType());
    BOOST_CHECK_EQUAL(1U, ast->args_size());
    BOOST_CHECK_EQUAL(1, ast->getLine());
    BOOST_CHECK_EQUAL(3, ast->getColumn());
  }
  {
    RecordTypeFunction hasher(ctxt, "charhash", types, 
			      "CAST(d AS   INTEGER)");
    IQLExpression * ast = hasher.getAST();
    BOOST_CHECK_EQUAL(IQLExpression::CAST, ast->getNodeType());
    BOOST_CHECK_EQUAL(1U, ast->args_size());
    BOOST_CHECK_EQUAL(1, ast->getLine());
    BOOST_CHECK_EQUAL(11, ast->getColumn());
    CastExpr * castExpr = static_cast<CastExpr *>(ast);
    BOOST_CHECK_EQUAL(Int32Type::Get(ctxt), castExpr->getCastType());
  }
  // {
  //   RecordTypeFunction hasher(ctxt, "charhash", types, 
  // 			      "CAST(e AS   BIGINT)");
  //   IQLExpression * ast = hasher.getAST();
  //   BOOST_CHECK_EQUAL(IQLExpression::CAST, ast->getNodeType());
  //   BOOST_CHECK_EQUAL(1U, ast->args_size());
  //   BOOST_CHECK_EQUAL(1, ast->getLine());
  //   BOOST_CHECK_EQUAL(11, ast->getColumn());
  //   CastExpr * castExpr = static_cast<CastExpr *>(ast);
  //   BOOST_CHECK_EQUAL(Int64Type::Get(ctxt), castExpr->getCastType());
  // }
}

BOOST_AUTO_TEST_CASE(testEquiJoinDetector)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("y", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", CharType::Get(ctxt, 6)));
  rhsMembers.push_back(RecordMember("f", VarcharType::Get(ctxt)));
  rhsMembers.push_back(RecordMember("g", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("h", Int64Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("z", DoubleType::Get(ctxt)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  {
    RecordTypeFunction pred(ctxt, "pred", types, "a = e");
    IQLExpression * ast = pred.getAST();
    IQLEquiJoinDetector d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK(NULL == d.getResidual());
    BOOST_CHECK_EQUAL(1U, d.getLeftEquiJoinKeys().size());
    BOOST_CHECK_EQUAL(1U, d.getRightEquiJoinKeys().size());
    BOOST_CHECK(boost::algorithm::equals("a", *d.getLeftEquiJoinKeys()[0]));
    BOOST_CHECK(boost::algorithm::equals("e", *d.getRightEquiJoinKeys()[0]));
    IQLExpression * e = d.getEquals();
    BOOST_REQUIRE(NULL != e);
    BOOST_CHECK_EQUAL(IQLExpression::EQ, e->getNodeType());
  }
  {
    RecordTypeFunction pred(ctxt, "pred", types, "a = e AND z = y");
    IQLExpression * ast = pred.getAST();
    IQLEquiJoinDetector d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK(NULL == d.getResidual());
    BOOST_CHECK_EQUAL(2U, d.getLeftEquiJoinKeys().size());
    BOOST_CHECK_EQUAL(2U, d.getRightEquiJoinKeys().size());
    BOOST_CHECK(boost::algorithm::equals("y", *d.getLeftEquiJoinKeys()[0]));
    BOOST_CHECK(boost::algorithm::equals("a", *d.getLeftEquiJoinKeys()[1]));
    BOOST_CHECK(boost::algorithm::equals("z", *d.getRightEquiJoinKeys()[0]));
    BOOST_CHECK(boost::algorithm::equals("e", *d.getRightEquiJoinKeys()[1]));
    IQLExpression * e = d.getEquals();
    BOOST_REQUIRE(NULL != e);
    BOOST_CHECK_EQUAL(IQLExpression::LAND, e->getNodeType());
  }
  {
    RecordTypeFunction pred(ctxt, "pred", types, "a = e AND z = y AND c = g");
    IQLExpression * ast = pred.getAST();
    IQLEquiJoinDetector d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK(NULL == d.getResidual());
    BOOST_CHECK_EQUAL(3U, d.getLeftEquiJoinKeys().size());
    BOOST_CHECK_EQUAL(3U, d.getRightEquiJoinKeys().size());
    BOOST_CHECK(boost::algorithm::equals("c", *d.getLeftEquiJoinKeys()[0]));
    BOOST_CHECK(boost::algorithm::equals("y", *d.getLeftEquiJoinKeys()[1]));
    BOOST_CHECK(boost::algorithm::equals("a", *d.getLeftEquiJoinKeys()[2]));
    BOOST_CHECK(boost::algorithm::equals("g", *d.getRightEquiJoinKeys()[0]));
    BOOST_CHECK(boost::algorithm::equals("z", *d.getRightEquiJoinKeys()[1]));
    BOOST_CHECK(boost::algorithm::equals("e", *d.getRightEquiJoinKeys()[2]));
    IQLExpression * e = d.getEquals();
    BOOST_REQUIRE(NULL != e);
    BOOST_CHECK_EQUAL(IQLExpression::LAND, e->getNodeType());
  }
  {
    // This test is the same as the previous except for the
    // parens that change the parse tree coming out of
    // ANTLR.  So this test is testing the tree normalization
    // implicit in the equi join rewrite.
    RecordTypeFunction pred(ctxt, "pred", types, "a = e AND (z = y AND c = g)");
    IQLExpression * ast = pred.getAST();
    IQLEquiJoinDetector d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK(NULL == d.getResidual());
    BOOST_CHECK_EQUAL(3U, d.getLeftEquiJoinKeys().size());
    BOOST_CHECK_EQUAL(3U, d.getRightEquiJoinKeys().size());
    BOOST_CHECK(boost::algorithm::equals("c", *d.getLeftEquiJoinKeys()[0]));
    BOOST_CHECK(boost::algorithm::equals("y", *d.getLeftEquiJoinKeys()[1]));
    BOOST_CHECK(boost::algorithm::equals("a", *d.getLeftEquiJoinKeys()[2]));
    BOOST_CHECK(boost::algorithm::equals("g", *d.getRightEquiJoinKeys()[0]));
    BOOST_CHECK(boost::algorithm::equals("z", *d.getRightEquiJoinKeys()[1]));
    BOOST_CHECK(boost::algorithm::equals("e", *d.getRightEquiJoinKeys()[2]));
    IQLExpression * e = d.getEquals();
    BOOST_REQUIRE(NULL != e);
    BOOST_CHECK_EQUAL(IQLExpression::LAND, e->getNodeType());
  }
  {
    RecordTypeFunction pred(ctxt, "pred", types, "a = e AND z > y");
    IQLExpression * ast = pred.getAST();
    IQLEquiJoinDetector d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK_EQUAL(1U, d.getLeftEquiJoinKeys().size());
    BOOST_CHECK_EQUAL(1U, d.getRightEquiJoinKeys().size());
    BOOST_CHECK(boost::algorithm::equals("a", *d.getLeftEquiJoinKeys()[0]));
    BOOST_CHECK(boost::algorithm::equals("e", *d.getRightEquiJoinKeys()[0]));
    IQLExpression * e = d.getEquals();
    BOOST_REQUIRE(NULL != e);
    BOOST_CHECK_EQUAL(IQLExpression::EQ, e->getNodeType());
    BOOST_CHECK_EQUAL(IQLExpression::VARIABLE, 
		      (*(e->begin_args()+0))->getNodeType());
    BOOST_CHECK_EQUAL(IQLExpression::VARIABLE, 
		      (*(e->begin_args()+1))->getNodeType());
    IQLExpression * r = d.getResidual();
    BOOST_CHECK(NULL != r);
    BOOST_CHECK_EQUAL(IQLExpression::GTN, r->getNodeType());
    BOOST_CHECK_EQUAL(IQLExpression::VARIABLE, 
		      (*(r->begin_args()+0))->getNodeType());
    BOOST_CHECK_EQUAL(IQLExpression::VARIABLE, 
		      (*(r->begin_args()+1))->getNodeType());
  }
  {
    RecordTypeFunction pred(ctxt, "pred", types, "a = e OR z = y");
    IQLExpression * ast = pred.getAST();
    IQLEquiJoinDetector d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK_EQUAL(0U, d.getLeftEquiJoinKeys().size());
    BOOST_CHECK_EQUAL(0U, d.getRightEquiJoinKeys().size());
    IQLExpression * e = d.getEquals();
    BOOST_REQUIRE(NULL == e);
    IQLExpression * r = d.getResidual();
    BOOST_CHECK(NULL != r);
    BOOST_CHECK_EQUAL(IQLExpression::LOR, r->getNodeType());
    BOOST_CHECK_EQUAL(IQLExpression::EQ, 
		      (*(r->begin_args()+0))->getNodeType());
    BOOST_CHECK_EQUAL(IQLExpression::EQ, 
		      (*(r->begin_args()+1))->getNodeType());
  }
}

BOOST_AUTO_TEST_CASE(testFreeVariablesRule)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("y", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", CharType::Get(ctxt, 6)));
  rhsMembers.push_back(RecordMember("f", VarcharType::Get(ctxt)));
  rhsMembers.push_back(RecordMember("g", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("h", Int64Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("z", DoubleType::Get(ctxt)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  {
    RecordTypeFunction pred(ctxt, "pred", types, "a = e");
    IQLExpression * ast = pred.getAST();
    IQLFreeVariablesRule d(ast);
    BOOST_CHECK_EQUAL(2U, d.getVariables().size());
    BOOST_CHECK(d.getVariables().find("a") != d.getVariables().end());
    BOOST_CHECK(d.getVariables().find("e") != d.getVariables().end());
  }
}

BOOST_AUTO_TEST_CASE(testSplitPredicateRule)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("y", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", CharType::Get(ctxt, 6)));
  rhsMembers.push_back(RecordMember("f", VarcharType::Get(ctxt)));
  rhsMembers.push_back(RecordMember("g", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("h", Int64Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("z", DoubleType::Get(ctxt)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  {
    RecordTypeFunction pred(ctxt, "pred", types, "a = e");
    IQLExpression * ast = pred.getAST();
    IQLSplitPredicateRule d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK(NULL == d.getLeft());
    BOOST_CHECK(NULL == d.getRight());
    BOOST_CHECK(NULL == d.getOther());
    BOOST_REQUIRE(NULL != d.getBoth());
  }
  {
    RecordTypeFunction pred(ctxt, "pred", types, "c = d");
    IQLExpression * ast = pred.getAST();
    IQLSplitPredicateRule d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK(NULL == d.getBoth());
    BOOST_CHECK(NULL == d.getRight());
    BOOST_CHECK(NULL == d.getOther());
    BOOST_REQUIRE(NULL != d.getLeft());
  }
  {
    RecordTypeFunction pred(ctxt, "pred", types, "g = h");
    IQLExpression * ast = pred.getAST();
    IQLSplitPredicateRule d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK(NULL == d.getBoth());
    BOOST_CHECK(NULL == d.getLeft());
    BOOST_CHECK(NULL == d.getOther());
    BOOST_REQUIRE(NULL != d.getRight());
    BOOST_CHECK_EQUAL(IQLExpression::EQ, d.getRight()->getNodeType());
    BOOST_CHECK_EQUAL(IQLExpression::VARIABLE, 
		      (*d.getRight()->begin_args())->getNodeType());
    BOOST_CHECK(boost::algorithm::equals("g",
					 (*d.getRight()->begin_args())->getStringData()));
    BOOST_CHECK_EQUAL(IQLExpression::VARIABLE, 
		      (*(d.getRight()->begin_args()+1))->getNodeType());
    BOOST_CHECK(boost::algorithm::equals("h",
					 (*(d.getRight()->begin_args()+1))->getStringData()));
  }
  {
    RecordTypeFunction pred(ctxt, "pred", types, "c=d AND g = h");
    IQLExpression * ast = pred.getAST();
    IQLSplitPredicateRule d(ctxt, &recTy, &rhsTy, ast);
    BOOST_CHECK(NULL == d.getBoth());
    BOOST_CHECK(NULL == d.getOther());
    BOOST_REQUIRE(NULL != d.getLeft());
    BOOST_REQUIRE(NULL != d.getRight());
  }
}

BOOST_AUTO_TEST_CASE(testRecordTypeBuilder)
{
  DynamicRecordContext ctxt;
  const RecordType * rt = IQLRecordTypeBuilder(ctxt,
					       "a CHAR(10)"
					       ", b DOUBLE PRECISION"
					       ", c VARCHAR"
					       ", d DECIMAL"
					       ", e INTEGER"
					       ", f BIGINT"
					       ", g DATETIME"
					       ", h DATE"
					       , false).getProduct();

  BOOST_CHECK_EQUAL(rt->size(), 8U);
  BOOST_CHECK(rt->hasMember("a"));
  BOOST_CHECK(rt->hasMember("b"));
  BOOST_CHECK(rt->hasMember("c"));
  BOOST_CHECK(rt->hasMember("d"));
  BOOST_CHECK(rt->hasMember("e"));
  BOOST_CHECK(rt->hasMember("f"));
  BOOST_CHECK(rt->hasMember("g"));
  BOOST_CHECK(rt->hasMember("h"));
}

BOOST_AUTO_TEST_CASE(testIQLRecordHash)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&emptyTy);

  RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", inputBuf);
  recTy.setVarchar("b", "abcdefghijklmnop", inputBuf);
  recTy.setInt32("c", 9923432, inputBuf);
  recTy.setInt64("d", 1239923432, inputBuf);
  recTy.setDouble("e", 8234.24344, inputBuf);

  {
    RecordTypeFunction hasher(ctxt, "charhash", types, "#(a)");
    uint32_t val = (uint32_t) hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    uint32_t expected = SuperFastHash("123456", 6, 6);
    BOOST_CHECK_EQUAL(val, expected);
  }
  {
    RecordTypeFunction hasher(ctxt, "varcharhash", types, "#(b)");
    uint32_t val = (uint32_t) hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    int32_t sz = strlen("abcdefghijklmnop");
    uint32_t expected = SuperFastHash("abcdefghijklmnop", sz, sz);
    BOOST_CHECK_EQUAL(val, expected);
  }
  {
    RecordTypeFunction hasher(ctxt, "int32hash", types, "#(c)");
    uint32_t val = (uint32_t) hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    int32_t tmp = 9923432;
    uint32_t expected = SuperFastHash((char *) &tmp, 4, 4);
    BOOST_CHECK_EQUAL(val, expected);
  }
  {
    RecordTypeFunction hasher(ctxt, "int64hash", types, "#(d)");
    uint32_t val = (uint32_t) hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    int64_t tmp = 1239923432;
    uint32_t expected = SuperFastHash((char *) &tmp, 8, 8);
    BOOST_CHECK_EQUAL(val, expected);
  }
  {
    RecordTypeFunction hasher(ctxt, "doublehash", types, "#(e)");
    uint32_t val = (uint32_t) hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    double tmp = 8234.24344;
    uint32_t expected = SuperFastHash((char *) &tmp, 8, 8);
    BOOST_CHECK_EQUAL(val, expected);
  }
  {
    RecordTypeFunction hasher(ctxt, "int64hash", types, "#(d,a)");
    uint32_t val = (uint32_t) hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    int64_t tmp = 1239923432;
    uint32_t expected = SuperFastHash((char *) &tmp, 8, 8);
    expected = SuperFastHash("123456", 6, expected);
    BOOST_CHECK_EQUAL(val, expected);
  }

  recTy.GetFree()->free(inputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLRecordEquals)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("y", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", CharType::Get(ctxt, 6)));
  rhsMembers.push_back(RecordMember("f", VarcharType::Get(ctxt)));
  rhsMembers.push_back(RecordMember("g", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("h", Int64Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("z", DoubleType::Get(ctxt)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", lhs);
  recTy.setVarchar("b", "abcdefghijklmnop", lhs);
  recTy.setInt32("c", 9923432, lhs);
  recTy.setInt64("d", 1239923432, lhs);
  recTy.setDouble("y", 88823.23433, lhs);

  RecordBuffer rhs1 = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs1);
  rhsTy.setChar("e", "123457", rhs1);
  rhsTy.setVarchar("f", "abcdefghijklmnoz", rhs1);
  rhsTy.setInt32("g", 9923431, rhs1);
  rhsTy.setInt64("h", 1239923433, rhs1);
  rhsTy.setDouble("z", 62344.23411, rhs1);

  RecordBuffer rhs2 = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs2);
  rhsTy.setChar("e", "123456", rhs2);
  rhsTy.setVarchar("f", "abcdefghijklmnop", rhs2);
  rhsTy.setInt32("g", 9923432, rhs2);
  rhsTy.setInt64("h", 1239923432, rhs2);
  rhsTy.setDouble("z", 88823.23433, rhs2);

  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a = e");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    RecordTypeFunction equals(ctxt, "varchareq", types, "b = f");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    RecordTypeFunction equals(ctxt, "varchareq", types, "c = g");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    RecordTypeFunction equals(ctxt, "varchareq", types, "d = h");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    RecordTypeFunction equals(ctxt, "doubleeq", types, "y = z");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }

  recTy.GetFree()->free(lhs);
  rhsTy.GetFree()->free(rhs1);
  rhsTy.GetFree()->free(rhs2);

}

BOOST_AUTO_TEST_CASE(testIQLInt32Compare)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", Int32Type::Get(ctxt)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  recTy.setInt32("a", 123456, lhs);

  RecordBuffer rhs = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs);
  rhsTy.setInt32("e", 123457, rhs);

  RecordTypeFunction lt(ctxt, "int32lt", types, "a < e");
  RecordTypeFunction gt(ctxt, "int32gt", types, "a > e");
  RecordTypeFunction le(ctxt, "int32le", types, "a <= e");
  RecordTypeFunction ge(ctxt, "int32ge", types, "a >= e");
  int32_t val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);

  
  recTy.setInt32("a", 123456, lhs);
  rhsTy.setInt32("e", 123456, rhs);
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  recTy.setInt32("a", 213456, lhs);
  rhsTy.setInt32("e", 123456, rhs);
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  recTy.setInt32("a", 123456, lhs);
  rhsTy.setInt32("e", -123456, rhs);
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  recTy.GetFree()->free(lhs);
  rhsTy.GetFree()->free(rhs);
}

BOOST_AUTO_TEST_CASE(testIQLDatetimeCompare)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DatetimeType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", DatetimeType::Get(ctxt)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  recTy.setDatetime("a", boost::posix_time::time_from_string("2011-05-27 23:12:00"), lhs);

  RecordBuffer rhs = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs);
  rhsTy.setDatetime("e", boost::posix_time::time_from_string("2011-05-28 23:12:00"), rhs);

  RecordTypeFunction lt(ctxt, "datetimelt", types, "a < e");
  RecordTypeFunction gt(ctxt, "datetimegt", types, "a > e");
  RecordTypeFunction le(ctxt, "datetimele", types, "a <= e");
  RecordTypeFunction ge(ctxt, "datetimege", types, "a >= e");
  int32_t val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);

  
  recTy.setDatetime("a", boost::posix_time::time_from_string("2011-05-27 23:12:00"), lhs);
  rhsTy.setDatetime("e", boost::posix_time::time_from_string("2011-05-27 23:12:00"), rhs);
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  recTy.setDatetime("a", boost::posix_time::time_from_string("2011-05-27 23:12:00"), lhs);
  rhsTy.setDatetime("e", boost::posix_time::time_from_string("2011-05-27 23:11:00"), rhs);
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  recTy.setDatetime("a", boost::posix_time::time_from_string("2011-05-28 23:12:00"), lhs);
  rhsTy.setDatetime("e", boost::posix_time::time_from_string("2011-05-27 23:13:00"), rhs);
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  recTy.GetFree()->free(lhs);
  rhsTy.GetFree()->free(rhs);
}

BOOST_AUTO_TEST_CASE(testIQLDecimalCompare)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DecimalType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", DecimalType::Get(ctxt)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  ::decimal128FromString(recTy.getMemberOffset("a").getDecimalPtr(lhs), 
			 "123456", 
			 runtimeCtxt.getDecimalContext());

  RecordBuffer rhs = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs);
  ::decimal128FromString(rhsTy.getMemberOffset("e").getDecimalPtr(rhs), 
			 "123457", 
			 runtimeCtxt.getDecimalContext());

  RecordTypeFunction lt(ctxt, "decimallt", types, "a < e");
  RecordTypeFunction gt(ctxt, "decimalgt", types, "a > e");
  RecordTypeFunction le(ctxt, "decimalle", types, "a <= e");
  RecordTypeFunction ge(ctxt, "decimalge", types, "a >= e");
  int32_t val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);

  
  ::decimal128FromString(recTy.getMemberOffset("a").getDecimalPtr(lhs), 
			 "123456", 
			 runtimeCtxt.getDecimalContext());
  ::decimal128FromString(rhsTy.getMemberOffset("e").getDecimalPtr(rhs), 
			 "123456", 
			 runtimeCtxt.getDecimalContext());
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  ::decimal128FromString(recTy.getMemberOffset("a").getDecimalPtr(lhs), 
			 "213456", 
			 runtimeCtxt.getDecimalContext());
  ::decimal128FromString(rhsTy.getMemberOffset("e").getDecimalPtr(rhs), 
			 "123456", 
			 runtimeCtxt.getDecimalContext());
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  ::decimal128FromString(recTy.getMemberOffset("a").getDecimalPtr(lhs), 
			 "123456", 
			 runtimeCtxt.getDecimalContext());
  ::decimal128FromString(rhsTy.getMemberOffset("e").getDecimalPtr(rhs), 
			 "-123456", 
			 runtimeCtxt.getDecimalContext());
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  recTy.GetFree()->free(lhs);
  rhsTy.GetFree()->free(rhs);
}

BOOST_AUTO_TEST_CASE(testIQLCharCompare)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", CharType::Get(ctxt, 6)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", lhs);

  RecordBuffer rhs = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs);
  rhsTy.setChar("e", "123457", rhs);

  RecordTypeFunction lt(ctxt, "charlt", types, "a < e");
  RecordTypeFunction gt(ctxt, "chargt", types, "a > e");
  RecordTypeFunction le(ctxt, "charle", types, "a <= e");
  RecordTypeFunction ge(ctxt, "charge", types, "a >= e");
  int32_t val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);

  
  recTy.setChar("a", "123456", lhs);
  rhsTy.setChar("e", "123456", rhs);
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  recTy.setChar("a", "A13456", lhs);
  rhsTy.setChar("e", "123456", rhs);
  val = lt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = gt.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);
  val = le.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0);
  val = ge.execute(lhs, rhs, &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 1);

  recTy.GetFree()->free(lhs);
  rhsTy.GetFree()->free(rhs);
}

BOOST_AUTO_TEST_CASE(testIQLCharKeyPrefix)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", lhs);

  RecordTypeFunction prefix(ctxt, "charPrefix", types, "$(a)");
  int32_t val = prefix.execute(lhs, RecordBuffer(), &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0x31323334/2);
  
  recTy.setChar("a", "Ae0/34", lhs);
  val = prefix.execute(lhs, RecordBuffer(), &runtimeCtxt);
  BOOST_CHECK_EQUAL(val, 0x4165302F/2);

  recTy.GetFree()->free(lhs);
}

BOOST_AUTO_TEST_CASE(testIQLLiteralCompares)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("y", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", lhs);
  recTy.setVarchar("b", "abcdefghijklmnop", lhs);
  recTy.setInt32("c", 9923432, lhs);
  recTy.setInt64("d", 1239923431, lhs);
  recTy.setDouble("y", 88823.23433, lhs);

  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a >= '123456'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a <= '123456'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a > '123456'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a < '123456'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a = '123456'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a <> '123456'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a >= '123457'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a <= '123457'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a > '123457'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a < '123457'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a = '123457'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
  }
  {
    RecordTypeFunction equals(ctxt, "chareq", types, "a <> '123457'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
  {
    RecordTypeFunction equals(ctxt, "varchareq", types, "b >= 'aaaa'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
  {
    RecordTypeFunction equals(ctxt, "varchareq", types, "b >= 'zaaa'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
  }
  {
    RecordTypeFunction equals(ctxt, "varchareq", types, "b = 'abcdefghijklmnop'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
  {
    RecordTypeFunction equals(ctxt, "varchareq", types, "b <> 'abcdefghijklmnop'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
  }
  {
    RecordTypeFunction equals(ctxt, "varchareq", types, "b <> 'abcdefghijklmnoq'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
  {
    RecordTypeFunction equals(ctxt, "varchareq", types, "b <> 'abcdefghijklmno'");
    int32_t val = equals.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }
}

void testRecordLogicalOps(bool isNullable)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6, isNullable)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt, isNullable)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, isNullable)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt, isNullable)));
  members.push_back(RecordMember("y", DoubleType::Get(ctxt, isNullable)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", CharType::Get(ctxt, 6)));
  rhsMembers.push_back(RecordMember("f", VarcharType::Get(ctxt, isNullable)));
  rhsMembers.push_back(RecordMember("g", Int32Type::Get(ctxt, isNullable)));
  rhsMembers.push_back(RecordMember("h", Int64Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("z", DoubleType::Get(ctxt)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", lhs);
  recTy.setVarchar("b", "abcdefghijklmnop", lhs);
  recTy.setInt32("c", 9923432, lhs);
  recTy.setInt64("d", 1239923431, lhs);
  recTy.setDouble("y", 88823.23433, lhs);

  RecordBuffer rhs1 = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs1);
  rhsTy.setChar("e", "123456", rhs1);
  rhsTy.setVarchar("f", "abcdefghijklmnoz", rhs1);
  rhsTy.setInt32("g", 9923431, rhs1);
  rhsTy.setInt64("h", 1239923433, rhs1);
  rhsTy.setDouble("z", 62344.23411, rhs1);

  RecordBuffer rhs2 = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs2);
  rhsTy.setChar("e", "123456", rhs2);
  rhsTy.setVarchar("f", "abcdefghijklmnop", rhs2);
  rhsTy.setInt32("g", 9923432, rhs2);
  rhsTy.setInt64("h", 1239923432, rhs2);
  rhsTy.setDouble("z", 88823.23433, rhs2);

  {
    RecordTypeFunction equals(ctxt, "doubleeq", types, "NOT y = z");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);    
  }
  {
    RecordTypeFunction equals(ctxt, "doubleeq", types, "y = z AND a=e");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    RecordTypeFunction equals(ctxt, "doubleeq", types, "a = e AND y = z");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    // Test case insensitivity of keyword
    RecordTypeFunction equals(ctxt, "doubleeq", types, "a = e anD y = z");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    // Test short circuiting of evaluation
    RecordTypeFunction equals(ctxt, "doubleeq", types, "y = z AND g=(1/0)");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
  }
  {
    RecordTypeFunction equals(ctxt, "doubleeq", types, "y=z OR d = h");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    RecordTypeFunction equals(ctxt, "doubleeq", types, "d=h OR y=z");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    // case insensitivity
    RecordTypeFunction equals(ctxt, "doubleeq", types, "d=h or y=z");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 0);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    RecordTypeFunction equals(ctxt, "doubleeq", types, "a=e OR y=z");
    int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
    val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);    
  }
  {
    // Test short circuiting of evaluation
    RecordTypeFunction equals(ctxt, "doubleeq", types, "y = z OR g=(1/0)");
    int32_t val = equals.execute(lhs, rhs2, &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, 1);
  }

  if (isNullable) {
    recTy.getFieldAddress("a").setNull(lhs);
    recTy.getFieldAddress("c").setNull(lhs);
    recTy.getFieldAddress("d").setNull(lhs);
    {
      RecordTypeFunction equals(ctxt, "doubleeq", types, "a=e");
      int32_t val = equals.execute(lhs, rhs2, &runtimeCtxt);
      BOOST_CHECK_EQUAL(val, 0);
    }
    {
      RecordTypeFunction equals(ctxt, "doubleeq", types, "NOT a=e");
      int32_t val = equals.execute(lhs, rhs2, &runtimeCtxt);
      BOOST_CHECK_EQUAL(val, 0);
    }
    {
      RecordTypeFunction equals(ctxt, "doubleeq", types, "a=e OR y=z");
      int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
      BOOST_CHECK_EQUAL(val, 0);
      val = equals.execute(lhs, rhs2, &runtimeCtxt);
      BOOST_CHECK_EQUAL(val, 1);    
    }
    {
      RecordTypeFunction equals(ctxt, "doubleeq", types, "a=e AND y=z");
      int32_t val = equals.execute(lhs, rhs1, &runtimeCtxt);
      BOOST_CHECK_EQUAL(val, 0);
      val = equals.execute(lhs, rhs2, &runtimeCtxt);
      BOOST_CHECK_EQUAL(val, 0);    
    }
  }
}

BOOST_AUTO_TEST_CASE(testIQLRecordLogicalOps)
{
  testRecordLogicalOps(false);
}

BOOST_AUTO_TEST_CASE(testIQLRecordLogicalOpsNullable)
{
  testRecordLogicalOps(true);
}

BOOST_AUTO_TEST_CASE(testIQLArray)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("y", Int32Type::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  recTy.setInt32("a", 123456, lhs);
  recTy.setInt32("b", 88311, lhs);
  recTy.setInt32("c", 9923432, lhs);
  recTy.setInt32("d", 12431, lhs);
  recTy.setInt32("y", 88823, lhs);

  {
    RecordTypeInPlaceUpdate up(ctxt, 
			       "xfer5up", 
			       types, 
			       "SET a[2] = d");
    up.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(12431, recTy.getInt32("c", lhs));
  }
  {
    RecordTypeInPlaceUpdate up(ctxt, 
			       "xfer5up", 
			       types, 
			       "SET c = a[1]");
    up.execute(lhs, NULL, &runtimeCtxt);
    BOOST_CHECK_EQUAL(88311, recTy.getInt32("c", lhs));
  }
}

BOOST_AUTO_TEST_CASE(testIQLAscendingSortPrefix)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&emptyTy);

  RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", inputBuf);
  recTy.setVarchar("b", "abcdefghijklmnop", inputBuf);
  recTy.setDouble("e", 8234.24344, inputBuf);

  {
    RecordTypeFunction hasher(ctxt, "charprefix", types, "$(a)");
    int32_t val = hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, (int32_t) (0x31323334/2));
  }
  // {
  //   RecordTypeFunction hasher(ctxt, "varcharhash", types, "#(b)");
  //   uint32_t val = (uint32_t) hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
  //   int32_t sz = strlen("abcdefghijklmnop");
  //   uint32_t expected = SuperFastHash("abcdefghijklmnop", sz, sz);
  //   BOOST_CHECK_EQUAL(val, expected);
  // }
  {
    recTy.setInt32("c", 9923432, inputBuf);
    RecordTypeFunction hasher(ctxt, "int32prefix", types, "$(c)");
    int32_t val = hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, (int32_t) ((9923432U+0x80000000U)/2));
  }
  {
    recTy.setInt32("c", -9923432, inputBuf);
    RecordTypeFunction hasher(ctxt, "int32prefix", types, "$(c)");
    int32_t val = hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, (int32_t) ((0x80000000U - 9923432U)/2));
  }
  {
    recTy.setInt64("d", 12399234322344LL, inputBuf);
    RecordTypeFunction hasher(ctxt, "int64prefix", types, "$(d)");
    int32_t val = hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, (int32_t) ((12399234322344ULL+0x8000000000000000ULL)/0x0000000200000000ULL));
  }
  {
    recTy.setInt64("d", -12399234322344LL, inputBuf);
    RecordTypeFunction hasher(ctxt, "int64prefix", types, "$(d)");
    int32_t val = hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
    BOOST_CHECK_EQUAL(val, (int32_t) ((0x8000000000000000ULL-12399234322344ULL)/0x0000000200000000ULL));
  }
  // {
  //   RecordTypeFunction hasher(ctxt, "doublehash", types, "#(e)");
  //   uint32_t val = (uint32_t) hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
  //   double tmp = 8234.24344;
  //   uint32_t expected = SuperFastHash((char *) &tmp, 8, 8);
  //   BOOST_CHECK_EQUAL(val, expected);
  // }
  // {
  //   RecordTypeFunction hasher(ctxt, "int64hash", types, "#(d,a)");
  //   uint32_t val = (uint32_t) hasher.execute(inputBuf, RecordBuffer(NULL), &runtimeCtxt);
  //   int64_t tmp = 1239923432;
  //   uint32_t expected = SuperFastHash((char *) &tmp, 8, 8);
  //   expected = SuperFastHash("123456", 7, expected);
  //   BOOST_CHECK_EQUAL(val, expected);
  // }

  recTy.GetFree()->free(inputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLRecordUpdate)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("y", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("s", Int32Type::Get(ctxt, true)));
  RecordType recTy(members);
  std::vector<RecordMember> rhsMembers;
  // dummy field to make sure that the offsets of fields we are comparing
  // are different.
  rhsMembers.push_back(RecordMember("dummy", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("e", CharType::Get(ctxt, 6)));
  rhsMembers.push_back(RecordMember("f", VarcharType::Get(ctxt)));
  rhsMembers.push_back(RecordMember("g", Int32Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("h", Int64Type::Get(ctxt)));
  rhsMembers.push_back(RecordMember("z", DoubleType::Get(ctxt)));
  rhsMembers.push_back(RecordMember("t", Int32Type::Get(ctxt, true)));
  RecordType rhsTy(rhsMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", lhs);
  recTy.setVarchar("b", "abcdefghijklmnop", lhs);
  recTy.setInt32("c", 9923432, lhs);
  recTy.setInt64("d", 1239923432, lhs);
  recTy.setDouble("y", 88823.23433, lhs);
  recTy.getFieldAddress("s").setNull(lhs);

  RecordBuffer rhs1 = rhsTy.GetMalloc()->malloc();
  rhsTy.setInt32("dummy", 0, rhs1);
  rhsTy.setChar("e", "123457", rhs1);
  rhsTy.setVarchar("f", "abcdefghijklmnoz", rhs1);
  rhsTy.setInt32("g", 9923431, rhs1);
  rhsTy.setInt64("h", 1239923433, rhs1);
  rhsTy.setDouble("z", 62344.23411, rhs1);
  rhsTy.getFieldAddress("t").setNull(lhs);

  {
    RecordTypeInPlaceUpdate up(ctxt, 
			       "xfer5up", 
			       types, 
			       "SET g = c");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(9923432, rhsTy.getInt32("g", rhs1));
  }
  {
    RecordTypeInPlaceUpdate up(ctxt, 
			       "xfer5up", 
			       types, 
			       "SET g = 9923431");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(9923431, rhsTy.getInt32("g", rhs1));
  }
  {
    RecordTypeInPlaceUpdate up(ctxt, 
			       "xfer5up", 
			       types, 
			       "SET t = c");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(9923432, rhsTy.getInt32("t", rhs1));
  }
  {
    RecordTypeInPlaceUpdate up(ctxt, 
			       "xfer5up", 
			       types, 
			       "SET t = s");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK(rhsTy.getFieldAddress("t").isNull(rhs1));
  }
  {
    RecordTypeInPlaceUpdate up(ctxt, 
			       "xfer5up", 
			       types, 
			       "SET t = 923444");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(923444, rhsTy.getInt32("t", rhs1));
  }
  {
    RecordTypeInPlaceUpdate up(ctxt, 
			       "xfer5up", 
			       types, 
			       "SET t = NULL");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK(rhsTy.getFieldAddress("t").isNull(rhs1));
  }
  {
    try {
      RecordTypeInPlaceUpdate up(ctxt, 
				 "xfer5up", 
				 types, 
				 "SET g = s");
      BOOST_CHECK(false);
    } catch(std::exception & ex) {
      std::cout << "Received expected exception: " << ex.what() << "\n";
    }
  }
  {
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(62344.23411, rhsTy.getDouble("z", rhs1));
    RecordTypeInPlaceUpdate up(ctxt, 
  			       "xfer5up", 
  			       types, 
  			       "SWITCH g "
  			       "BEGIN "
  			       "CASE 9923430 "
                               "SET h = 1 "
  			       "CASE 9923431 "
  			       "SET z = 9.33e+01 "
  			       "END");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(93.3, rhsTy.getDouble("z", rhs1));
  }
  {
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(93.3, rhsTy.getDouble("z", rhs1));
    BOOST_CHECK_EQUAL(9923432, recTy.getInt32("c", lhs));
    RecordTypeInPlaceUpdate up(ctxt, 
  			       "xfer5up", 
  			       types, 
  			       "SWITCH g "
  			       "BEGIN "
  			       "CASE 9923430 "
                               "SET h = 1 "
                               "SET h = 1 "
  			       "CASE 9923431 "
  			       "SET z = 9.33e+02 "
  			       "SET c = 2 "
  			       "CASE 9923432 "
  			       "SET z = 8.33e+02 "
  			       "SET y = 7.234e+01 "
  			       "END "
  			       "SET d = 99234334 ");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(2, recTy.getInt32("c", lhs));
    BOOST_CHECK_EQUAL(933, rhsTy.getDouble("z", rhs1));
    BOOST_CHECK_EQUAL(99234334LL, recTy.getInt64("d", lhs));
  }
  {
    rhsTy.setInt32("g", 4, rhs1);
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(933, rhsTy.getDouble("z", rhs1));
    RecordTypeInPlaceUpdate up(ctxt, 
  			       "xfer5up", 
  			       types, 
  			       "SWITCH g "
  			       "BEGIN "
  			       "CASE 1 "
                               "SET h = 1 "
  			       "CASE 2 "
  			       "SET z = 9.33e+01 "
  			       "CASE 3 "
                               "SET h = 33 "
  			       "CASE 4 "
  			       "SET z = 9.44e+01 "
  			       "CASE 5 "
                               "SET h = 77 "
  			       "CASE 6 "
  			       "SET z = 9.45532e+01 "
  			       "END");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(94.4, rhsTy.getDouble("z", rhs1));
  }
  {
    // Case insensitivity of keywords
    rhsTy.setInt32("g", 4, rhs1);
    rhsTy.setDouble("z", 933, rhs1);
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(933, rhsTy.getDouble("z", rhs1));
    RecordTypeInPlaceUpdate up(ctxt, 
  			       "xfer5up", 
  			       types, 
  			       "switch g "
  			       "begin "
  			       "CASE 1 "
                               "SET h = 1 "
  			       "CASE 2 "
  			       "set z = 9.33e+01 "
  			       "CASE 3 "
                               "SET h = 33 "
  			       "case 4 "
  			       "SET z = 9.44e+01 "
  			       "CASE 5 "
                               "SET h = 77 "
  			       "CASE 6 "
  			       "SET z = 9.45532e+01 "
  			       "end");
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(94.4, rhsTy.getDouble("z", rhs1));
  }
  {
    rhsTy.setInt32("g", 4, rhs1);
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(94.4, rhsTy.getDouble("z", rhs1));
    std::string prog = "SWITCH g BEGIN ";
    for(int i=0; i<1000; i+=2) {
      prog += (boost::format("CASE %1% "
			     "SET h = %1% "
			     "CASE %2% "
			     "SET z = 0.%2%e-01 "
			     ) % (i+1) % (i+2)).str();
    }
    prog += "END";
    RecordTypeInPlaceUpdate up(ctxt, 
  			       "xfer5up", 
  			       types, 
			       prog);
    up.execute(lhs, rhs1, &runtimeCtxt);
    BOOST_CHECK_EQUAL(1239923433, rhsTy.getInt32("h", rhs1));
    BOOST_CHECK_EQUAL(0.04, rhsTy.getDouble("z", rhs1));
  }
}

BOOST_AUTO_TEST_CASE(testRecordTypeSerialize)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&emptyTy);

  RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", inputBuf);
  recTy.setVarchar("b", "abcdefghijklmnop", inputBuf);
  recTy.setInt32("c", 9923432, inputBuf);
  recTy.setInt64("d", 1239923432, inputBuf);
  recTy.setDouble("e", 8234.24344, inputBuf);

  // Give a big buffer where serialization succeeds in a single pass
  uint8_t bigBuf[128];
  uint8_t * bufPtr = &bigBuf[0];
  RecordBufferIterator recIt;
  recIt.init(inputBuf);
  bool ret = recTy.getSerialize().doit(bufPtr, bigBuf+128, recIt, inputBuf);
  BOOST_CHECK(ret);
  BOOST_CHECK_EQUAL(&bigBuf[65], bufPtr);

  // Deserialize and make sure all is well
  RecordBuffer outputBuf = recTy.GetMalloc()->malloc();
  recIt.init(outputBuf);
  bufPtr = &bigBuf[0];
  ret = recTy.getDeserialize().Do(bufPtr, &bigBuf[65], recIt, outputBuf);
  BOOST_CHECK(ret);
}

BOOST_AUTO_TEST_CASE(testRecordTypeNullBitmap)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  // Test cases 
  std::size_t testCases [] = { 3, 31, 32, 33, 45, 63, 64, 65 };
  for(std::size_t i=0; i<sizeof(testCases)/sizeof(size_t); ++i) {
    std::vector<RecordMember> members;
    std::size_t testCase = testCases[i];
    for(std::size_t j=0; j<testCase; ++j) {
      members.push_back(RecordMember((boost::format("a%1%") % j).str(), 
				     CharType::Get(ctxt, 6, true)));
    }

    RecordType recTy(members);
    RecordBuffer inputBuf = recTy.getMalloc().malloc();
    // Everything should be NULL by default
    for(std::size_t j=0; j<testCase; ++j) {
      std::string member = (boost::format("a%1%") % j).str();
      BOOST_CHECK(recTy.getMemberOffset(member).isNull(inputBuf));
    }
    // Now set each field to be non null individually,
    // the reset back
    for(std::size_t j=0; j<testCase; ++j) {
      std::string member = (boost::format("a%1%") % j).str();
      recTy.getMemberOffset(member).clearNull(inputBuf);
      for(std::size_t k=0; k<testCase; ++k) {
	std::string innerMember = (boost::format("a%1%") % k).str();
	if(k != j) {
	  BOOST_CHECK(recTy.getMemberOffset(innerMember).isNull(inputBuf));
	} else {
	  BOOST_CHECK(!recTy.getMemberOffset(innerMember).isNull(inputBuf));
	}
      }
      recTy.getMemberOffset(member).setNull(inputBuf);
      for(std::size_t k=0; k<testCase; ++k) {
	std::string innerMember = (boost::format("a%1%") % k).str();
	BOOST_CHECK(recTy.getMemberOffset(innerMember).isNull(inputBuf));
      }
    }
    recTy.getFree().free(inputBuf);
  }
}

BOOST_AUTO_TEST_CASE(testRecordTypeSerializeNullable)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6, true)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt)));
  RecordType recTy(members);
  std::vector<RecordMember> emptyMembers;
  RecordType emptyTy(emptyMembers);
  std::vector<const RecordType *> types;
  types.push_back(&recTy);
  types.push_back(&emptyTy);

  {
    RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
    recTy.setChar("a", "123456", inputBuf);
    recTy.setVarchar("b", "abcdefghijklmnop", inputBuf);
    recTy.setInt32("c", 9923432, inputBuf);
    recTy.setInt64("d", 1239923432, inputBuf);
    recTy.setDouble("e", 8234.24344, inputBuf);

    // Give a big buffer where serialization succeeds in a single pass
    uint8_t bigBuf[128];
    uint8_t * bufPtr = &bigBuf[0];
    RecordBufferIterator recIt;
    recIt.init(inputBuf);
    bool ret = recTy.getSerialize().doit(bufPtr, bigBuf+128, recIt, inputBuf);
    BOOST_CHECK(ret);
    BOOST_CHECK_EQUAL(&bigBuf[73], bufPtr);

    // Deserialize and make sure all is well
    RecordBuffer outputBuf = recTy.GetMalloc()->malloc();
    recIt.init(outputBuf);
    bufPtr = &bigBuf[0];
    ret = recTy.getDeserialize().Do(bufPtr, &bigBuf[73], recIt, outputBuf);
    BOOST_CHECK(ret);
  }
  {
    RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
    recTy.getMemberOffset("a").setNull(inputBuf);
    recTy.getMemberOffset("b").setNull(inputBuf);
    recTy.setInt32("c", 9923432, inputBuf);
    recTy.setInt64("d", 1239923432, inputBuf);
    recTy.setDouble("e", 8234.24344, inputBuf);
    BOOST_CHECK(recTy.getMemberOffset("a").isNull(inputBuf));
    BOOST_CHECK(recTy.getMemberOffset("b").isNull(inputBuf));
    BOOST_CHECK(!recTy.getMemberOffset("c").isNull(inputBuf));
    BOOST_CHECK(!recTy.getMemberOffset("d").isNull(inputBuf));
    BOOST_CHECK(!recTy.getMemberOffset("e").isNull(inputBuf));

    // Give a big buffer where serialization succeeds in a single pass
    uint8_t bigBuf[128];
    uint8_t * bufPtr = &bigBuf[0];
    RecordBufferIterator recIt;
    recIt.init(inputBuf);
    bool ret = recTy.getSerialize().doit(bufPtr, bigBuf+128, recIt, inputBuf);
    BOOST_CHECK(ret);
    BOOST_CHECK_EQUAL(&bigBuf[56], bufPtr);

    // Deserialize and make sure all is well
    RecordBuffer outputBuf = recTy.GetMalloc()->malloc();
    recIt.init(outputBuf);
    bufPtr = &bigBuf[0];
    ret = recTy.getDeserialize().Do(bufPtr, &bigBuf[56], recIt, outputBuf);
    BOOST_CHECK(ret);
    BOOST_CHECK(recTy.getMemberOffset("a").isNull(outputBuf));
    BOOST_CHECK(recTy.getMemberOffset("b").isNull(outputBuf));
    BOOST_CHECK(!recTy.getMemberOffset("c").isNull(outputBuf));
    BOOST_CHECK(!recTy.getMemberOffset("d").isNull(outputBuf));
    BOOST_CHECK(!recTy.getMemberOffset("e").isNull(outputBuf));
  }
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferIdentityDetection)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "a,b,c");
    BOOST_CHECK(t1.isIdentity());
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "input.*");
    BOOST_CHECK(t1.isIdentity());
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "a AS a1,b AS b1,c AS c1");
    BOOST_CHECK(t1.isIdentity());
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "a,c,b");
    BOOST_CHECK(!t1.isIdentity());
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "a");
    BOOST_CHECK(!t1.isIdentity());
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "a,b");
    BOOST_CHECK(!t1.isIdentity());
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "a,b,c, a+b+c AS e");
    BOOST_CHECK(!t1.isIdentity());
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "a,c,b AS d, a+b+c AS e");
    BOOST_CHECK(!t1.isIdentity());
  }
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferIntegers)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a,c,b AS d, a+b+c AS e");
  t1.getTarget()->dump();

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  recordType->Print(inputBuf, std::cout);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK_EQUAL(2553, t1.getTarget()->getInt32("e", outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLRecordModulus)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a%b AS e, c%d AS f");

  // Actually execute this thing.
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 7, inputBuf);
  recordType->setInt64("c", 2300, inputBuf);
  recordType->setInt64("d", 231, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL((23%7), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL((2300LL%231LL), t1.getTarget()->getInt64("f", outputBuf));

  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", -7, inputBuf);
  recordType->setInt64("c", 2300, inputBuf);
  recordType->setInt64("d", -231, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL((23%-7), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL((2300LL%-231LL), t1.getTarget()->getInt64("f", outputBuf));

  recordType->setInt32("a", -23, inputBuf);
  recordType->setInt32("b", 7, inputBuf);
  recordType->setInt64("c", -2300, inputBuf);
  recordType->setInt64("d", 231, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL((-23%7), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL((-2300LL%231LL), t1.getTarget()->getInt64("f", outputBuf));

  recordType->setInt32("a", -23, inputBuf);
  recordType->setInt32("b", -7, inputBuf);
  recordType->setInt64("c", -2300, inputBuf);
  recordType->setInt64("d", -231, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL((-23%-7), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL((-2300LL%-231LL), t1.getTarget()->getInt64("f", outputBuf));
}

class BinaryOp
{
private:
  const char * mOp;
public:
  BinaryOp(const char * op)
    :
    mOp(op)
  {
  }
  virtual ~BinaryOp() {}
  const char * getOp() const { return mOp; }
  virtual int32_t operator()(int32_t a, int32_t b) const =0;
  virtual int64_t operator()(int64_t a, int64_t b) const =0;
};

class MulOp : public BinaryOp
{
public:
  MulOp() : BinaryOp("*") {}
  int32_t operator()(int32_t a, int32_t b) const { return a*b; }
  int64_t operator()(int64_t a, int64_t b) const { return a*b; }  
};

class DivOp : public BinaryOp
{
public:
  DivOp() : BinaryOp("/") {}
  int32_t operator()(int32_t a, int32_t b) const { return a/b; }
  int64_t operator()(int64_t a, int64_t b) const { return a/b; }  
};

class SubOp : public BinaryOp
{
public:
  SubOp() : BinaryOp("-") {}
  int32_t operator()(int32_t a, int32_t b) const { return a-b; }
  int64_t operator()(int64_t a, int64_t b) const { return a-b; }  
};

class AddOp : public BinaryOp
{
public:
  AddOp() : BinaryOp("+") {}
  int32_t operator()(int32_t a, int32_t b) const { return a+b; }
  int64_t operator()(int64_t a, int64_t b) const { return a+b; }  
};

class BitwiseAndOp : public BinaryOp
{
public:
  BitwiseAndOp() : BinaryOp("&") {}
  int32_t operator()(int32_t a, int32_t b) const { return a&b; }
  int64_t operator()(int64_t a, int64_t b) const { return a&b; }  
};

class BitwiseOrOp : public BinaryOp
{
public:
  BitwiseOrOp() : BinaryOp("|") {}
  int32_t operator()(int32_t a, int32_t b) const { return a|b; }
  int64_t operator()(int64_t a, int64_t b) const { return a|b; }  
};

class BitwiseXorOp : public BinaryOp
{
public:
  BitwiseXorOp() : BinaryOp("^") {}
  int32_t operator()(int32_t a, int32_t b) const { return a^b; }
  int64_t operator()(int64_t a, int64_t b) const { return a^b; }  
};

void testRecordBinaryOp(bool isNullable1, bool isNullable2, const BinaryOp& op)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, isNullable1)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt, isNullable2)));
  members.push_back(RecordMember("c", Int64Type::Get(ctxt, isNullable1)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt, isNullable2)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));

  // Result nullability
  bool isNullable = isNullable1 || isNullable2;
  
  // Simple Transfer of everything.  
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			(boost::format("a%1%b AS e, c%1%d AS f, a%1%d AS g") % 
			 op.getOp()).str());
  BOOST_CHECK_EQUAL(Int32Type::Get(ctxt, isNullable), 
		    t1.getTarget()->getMember("e").GetType());
  BOOST_CHECK_EQUAL(Int64Type::Get(ctxt, isNullable), 
		    t1.getTarget()->getMember("f").GetType());
  BOOST_CHECK_EQUAL(Int64Type::Get(ctxt, isNullable), 
		    t1.getTarget()->getMember("g").GetType());
  // Actually execute this thing.
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 7, inputBuf);
  recordType->setInt64("c", 2300, inputBuf);
  recordType->setInt64("d", 231, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(op(23,7), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) 2300, (int64_t) 231), 
		    t1.getTarget()->getInt64("f", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) 23, (int64_t) 231), 
		    t1.getTarget()->getInt64("g", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("g").isNull(outputBuf));

  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", -7, inputBuf);
  recordType->setInt64("c", 2300, inputBuf);
  recordType->setInt64("d", -231, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(op(23,-7), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) 2300, (int64_t) -231), 
		    t1.getTarget()->getInt64("f", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) 23, (int64_t) -231), 
		    t1.getTarget()->getInt64("g", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("g").isNull(outputBuf));

  recordType->setInt32("a", -23, inputBuf);
  recordType->setInt32("b", 7, inputBuf);
  recordType->setInt64("c", -2300, inputBuf);
  recordType->setInt64("d", 231, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(op(-23,7), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) -2300, (int64_t) 231),
		    t1.getTarget()->getInt64("f", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) -23, (int64_t) 231), 
		    t1.getTarget()->getInt64("g", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("g").isNull(outputBuf));

  recordType->setInt32("a", -23, inputBuf);
  recordType->setInt32("b", -7, inputBuf);
  recordType->setInt64("c", -2300, inputBuf);
  recordType->setInt64("d", -231, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(op(-23,-7), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) -2300, (int64_t) -231), 
		    t1.getTarget()->getInt64("f", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) -23, (int64_t) -231), 
		    t1.getTarget()->getInt64("g", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("g").isNull(outputBuf));
  
  if (isNullable1) {
    recordType->getFieldAddress("a").setNull(inputBuf);
    recordType->getFieldAddress("c").setNull(inputBuf);
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("g").isNull(outputBuf));
  }

  if (isNullable2) {
    recordType->getFieldAddress("a").setInt32(23, inputBuf);
    recordType->getFieldAddress("c").setInt64(9923LL, inputBuf);
    recordType->getFieldAddress("b").setNull(inputBuf);
    recordType->getFieldAddress("d").setNull(inputBuf);
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("g").isNull(outputBuf));
  }

  if (isNullable1 && isNullable2) {
    recordType->getFieldAddress("a").setNull(inputBuf);
    recordType->getFieldAddress("c").setNull(inputBuf);
    recordType->getFieldAddress("b").setNull(inputBuf);
    recordType->getFieldAddress("d").setNull(inputBuf);
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("g").isNull(outputBuf));
  }
  recordType->getFree().free(inputBuf);
  t1.getTarget()->getFree().free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLMultiplication)
{
  MulOp op;
  testRecordBinaryOp(false, false, op);
  testRecordBinaryOp(true, false, op);
  testRecordBinaryOp(false, true, op);
  testRecordBinaryOp(true, true, op);
}

BOOST_AUTO_TEST_CASE(testIQLDivide)
{
  DivOp op;
  testRecordBinaryOp(false, false, op);
  testRecordBinaryOp(true, false, op);
  testRecordBinaryOp(false, true, op);
  testRecordBinaryOp(true, true, op);
}

BOOST_AUTO_TEST_CASE(testIQLSubtract)
{
  SubOp op;
  testRecordBinaryOp(false, false, op);
  testRecordBinaryOp(true, false, op);
  testRecordBinaryOp(false, true, op);
  testRecordBinaryOp(true, true, op);
}

BOOST_AUTO_TEST_CASE(testIQLAddition)
{
  AddOp op;
  testRecordBinaryOp(false, false, op);
  testRecordBinaryOp(true, false, op);
  testRecordBinaryOp(false, true, op);
  testRecordBinaryOp(true, true, op);
}

BOOST_AUTO_TEST_CASE(testIQLBitwiseAnd)
{
  BitwiseAndOp op;
  testRecordBinaryOp(false, false, op);
  testRecordBinaryOp(true, false, op);
  testRecordBinaryOp(false, true, op);
  testRecordBinaryOp(true, true, op);
}

BOOST_AUTO_TEST_CASE(testIQLBitwiseOr)
{
  BitwiseOrOp op;
  testRecordBinaryOp(false, false, op);
  testRecordBinaryOp(true, false, op);
  testRecordBinaryOp(false, true, op);
  testRecordBinaryOp(true, true, op);
}

BOOST_AUTO_TEST_CASE(testIQLBitwiseXor)
{
  BitwiseXorOp op;
  testRecordBinaryOp(false, false, op);
  testRecordBinaryOp(true, false, op);
  testRecordBinaryOp(false, true, op);
  testRecordBinaryOp(true, true, op);
}

class UnaryOp
{
private:
  const char * mOp;
public:
  UnaryOp(const char * op)
    :
    mOp(op)
  {
  }
  virtual ~UnaryOp() {}
  const char * getOp() const { return mOp; }
  virtual int32_t operator()(int32_t a) const =0;
  virtual int64_t operator()(int64_t a) const =0;
};

class BitwiseNotOp : public UnaryOp
{
public:
  BitwiseNotOp() : UnaryOp("~") {}
  int32_t operator()(int32_t a) const { return ~a; }
  int64_t operator()(int64_t a) const { return ~a; }  
};

void testIntegerUnaryOp(bool isNullable1, const UnaryOp& op)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, isNullable1)));
  members.push_back(RecordMember("c", Int64Type::Get(ctxt, isNullable1)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));

  // Result nullability
  bool isNullable = isNullable1;
  
  // Simple Transfer of everything.  
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			(boost::format("%1%a AS e, %1%c AS f") % 
			 op.getOp()).str());
  BOOST_CHECK_EQUAL(Int32Type::Get(ctxt, isNullable), 
		    t1.getTarget()->getMember("e").GetType());
  BOOST_CHECK_EQUAL(Int64Type::Get(ctxt, isNullable), 
		    t1.getTarget()->getMember("f").GetType());
  // Actually execute this thing.
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt64("c", 2300, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(op(23), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) 2300), 
		    t1.getTarget()->getInt64("f", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("f").isNull(outputBuf));

  recordType->setInt32("a", -23, inputBuf);
  recordType->setInt64("c", -23008283445434LL, inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(op(-23), t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL(op((int64_t) -23008283445434LL), 
		    t1.getTarget()->getInt64("f", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("f").isNull(outputBuf));

  if (isNullable1) {
    recordType->getFieldAddress("a").setNull(inputBuf);
    recordType->getFieldAddress("c").setNull(inputBuf);
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
  }
}

BOOST_AUTO_TEST_CASE(testIQLBitwiseNot)
{
  BitwiseNotOp op;
  testIntegerUnaryOp(false, op);
  testIntegerUnaryOp(true, op);
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferLocalVariable)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"DECLARE tmp=a+b+c, a,c,b AS d, tmp AS e");
  t1.getTarget()->dump();

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  recordType->Print(inputBuf, std::cout);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK_EQUAL(2553, t1.getTarget()->getInt32("e", outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLCaseStatement)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"CASE WHEN a=23 THEN b ELSE c END AS d"
			", CASE WHEN a=22 THEN b ELSE c END AS e"
			", CASE WHEN a=23 THEN CASE WHEN b=230 THEN 1 ELSE 2 END ELSE c END AS f"
			", CASE WHEN a=23 THEN CASE WHEN b=231 THEN 1 ELSE 2 END ELSE c END AS g"
			", CASE WHEN a=22 THEN CASE WHEN b=231 THEN 1 ELSE 2 END ELSE CASE WHEN c=2300 THEN 3 ELSE 4 END END AS h"
			", CASE WHEN a=23 THEN CASE WHEN b=231 THEN 1 ELSE CASE WHEN c=3201 THEN 5 ELSE 6 END END ELSE CASE WHEN c=2300 THEN 3 ELSE 4 END END AS i"
			", CASE WHEN a=23 THEN 1 WHEN b=230 THEN 2 WHEN c=2300 THEN 3 ELSE 4 END AS j"
			", CASE WHEN a=22 THEN 1 WHEN b=230 THEN 2 WHEN c=2300 THEN 3 ELSE 4 END AS k"
			", CASE WHEN a=22 THEN 1 WHEN b=231 THEN 2 WHEN c=2300 THEN 3 ELSE 4 END AS l"
			", CASE WHEN a=22 THEN 1 WHEN b=231 THEN 2 WHEN c=2301 THEN 3 ELSE 4 END AS m"
			", CASE WHEN a=22 THEN 1 WHEN b=231 THEN 2 WHEN c=2300 THEN 3 END AS n"
			", CASE WHEN a=22 THEN 1 WHEN b=231 THEN 2 WHEN c=2300 THEN 3 ELSE 4.0e+00 END AS o"
			", case WHEN a=22 THEN 1 wheN b=231 Then 2 when c=2300 THEN 3 ELSE 4.0e+00 END AS p"
			", CASE WHEN a=22 THEN 1 WHEN b=231 THEN 2 WHEN c=2301 THEN 3 END AS q"
			);

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL(1, t1.getTarget()->getInt32("f", outputBuf));
  BOOST_CHECK_EQUAL(2, t1.getTarget()->getInt32("g", outputBuf));
  BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("h", outputBuf));
  BOOST_CHECK_EQUAL(6, t1.getTarget()->getInt32("i", outputBuf));
  BOOST_CHECK_EQUAL(1, t1.getTarget()->getInt32("j", outputBuf));
  BOOST_CHECK_EQUAL(2, t1.getTarget()->getInt32("k", outputBuf));
  BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("l", outputBuf));
  BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("m", outputBuf));
  BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("n", outputBuf));
  BOOST_CHECK_EQUAL(3.0, t1.getTarget()->getDouble("o", outputBuf));
  BOOST_CHECK_EQUAL(3.0, t1.getTarget()->getDouble("p", outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("q").isNull(outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLCaseStatementVarchar)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", VarcharType::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"CASE WHEN a=23 THEN 'TRUE' ELSE 'FALSE' END AS d"
			);

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setVarchar("c", "2300", inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(boost::algorithm::equals("TRUE",
				       t1.getTarget()->getVarcharPtr("d", outputBuf)->Ptr));
}

BOOST_AUTO_TEST_CASE(testIQLCaseStatementDecimal)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", DecimalType::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"CASE WHEN a=23 THEN 99823.444 ELSE 23.334 END AS d"
			", CASE WHEN b=230 THEN c WHEN a=23 THEN 99823.444 ELSE 23.334 END AS e"
			", CASE WHEN b=23 THEN c WHEN a=23 THEN CASE WHEN b=231 THEN 99823.444 WHEN b=230 THEN 777.7234 ELSE 982343.33 END ELSE 23.334 END AS f"
			);

  // Actually execute this thing.
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  ::decimal128FromString(recordType->getMemberOffset("c").getDecimalPtr(inputBuf), 
			 "123456.8234", 
			 runtimeCtxt.getDecimalContext());
  decimal128 expected;
  ::decimal128FromString(&expected, "99823.444", 
			 runtimeCtxt.getDecimalContext());
  decimal128 expected2;
  ::decimal128FromString(&expected2, "777.7234", 
			 runtimeCtxt.getDecimalContext());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  int ret=memcmp(t1.getTarget()->getMemberOffset("d").getDecimalPtr(outputBuf),
		 &expected, DECIMAL128_Bytes);
  BOOST_CHECK_EQUAL(0, ret);
  ret=memcmp(t1.getTarget()->getMemberOffset("e").getDecimalPtr(outputBuf),
	     recordType->getMemberOffset("c").getDecimalPtr(inputBuf), 
	     DECIMAL128_Bytes);
  BOOST_CHECK_EQUAL(0, ret);
  ret=memcmp(t1.getTarget()->getMemberOffset("f").getDecimalPtr(outputBuf),
	     &expected2, 
	     DECIMAL128_Bytes);
  BOOST_CHECK_EQUAL(0, ret);

}

BOOST_AUTO_TEST_CASE(testIQLCaseStatementTypePromotion)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", DecimalType::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"CASE WHEN a=23 THEN 99823 ELSE 23.334 END AS d"
			", CASE WHEN b=230 THEN c WHEN a=23 THEN 99823 ELSE 23LL END AS e"
			", CASE WHEN b=230 THEN -99 WHEN a=23 THEN 99823LL ELSE 23LL END AS f"
			);

  // Actually execute this thing.
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  ::decimal128FromString(recordType->getMemberOffset("c").getDecimalPtr(inputBuf), 
			 "123456.8234", 
			 runtimeCtxt.getDecimalContext());
  decimal128 expected;
  ::decimal128FromString(&expected, "99823", 
			 runtimeCtxt.getDecimalContext());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  int ret=memcmp(t1.getTarget()->getMemberOffset("d").getDecimalPtr(outputBuf),
		 &expected, DECIMAL128_Bytes);
  BOOST_CHECK_EQUAL(0, ret);
  ret=memcmp(t1.getTarget()->getMemberOffset("e").getDecimalPtr(outputBuf),
	     recordType->getMemberOffset("c").getDecimalPtr(inputBuf), 
	     DECIMAL128_Bytes);
  BOOST_CHECK_EQUAL(0, ret);
  BOOST_CHECK_EQUAL(-99L, t1.getTarget()->getInt64("f", outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLCaseStatementNullCondition)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"CASE WHEN a=23 THEN 99823 ELSE 23 END AS d"
			", CASE WHEN b=230 THEN c WHEN a=23 THEN 99823 ELSE 23 END AS e"
			", CASE WHEN b=230 THEN -99 WHEN a=23 THEN 99823 ELSE 23 END AS f"
			);

  // Actually execute this thing.
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 123456, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(99823, t1.getTarget()->getFieldAddress("d").getInt32(outputBuf));
  BOOST_CHECK_EQUAL(123456, t1.getTarget()->getFieldAddress("e").getInt32(outputBuf));
  BOOST_CHECK_EQUAL(-99, t1.getTarget()->getFieldAddress("f").getInt32(outputBuf));
  t1.getTarget()->getFree().free(outputBuf);

  recordType->getFieldAddress("a").setNull(inputBuf);
  recordType->setInt32("b", 231, inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getFieldAddress("d").getInt32(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getFieldAddress("e").getInt32(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getFieldAddress("f").getInt32(outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
}

void ValidateRecordAggregate(RecordTypeAggregate& a1,
			     boost::shared_ptr<RecordType> recordType,
			     const std::string& aggCol,
			     bool identityTransfer=true,
			     uint32_t numAggregates=2u,
			     bool isInitNull = true)
{
  BOOST_CHECK(NULL != a1.getTarget());
  BOOST_CHECK_EQUAL(a1.getTarget()->size(), 2u);
  BOOST_CHECK(a1.getTarget()->hasMember("a"));
  BOOST_CHECK(a1.getTarget()->hasMember("d"));
  BOOST_CHECK(NULL != a1.getAggregate());
  BOOST_CHECK_EQUAL(a1.getAggregate()->size(), numAggregates);
  BOOST_CHECK(a1.getAggregate()->hasMember("a"));
  BOOST_CHECK(a1.getAggregate()->hasMember(aggCol));
  BOOST_CHECK_EQUAL(isInitNull,
		    a1.getAggregate()->getMember(aggCol).GetType()->isNullable());
  BOOST_CHECK_EQUAL(identityTransfer, a1.isFinalTransferIdentity());

  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  InterpreterContext runtimeCtxt;
  IQLAggregateModule * m = a1.create();
  RecordBuffer aggregateBuf;
  m->executeInit(inputBuf, aggregateBuf, &runtimeCtxt);
  BOOST_CHECK(aggregateBuf != RecordBuffer());
  BOOST_CHECK_EQUAL(a1.getAggregate()->getInt32("a", aggregateBuf),
		    23);
  BOOST_CHECK_EQUAL(a1.getAggregate()->getMemberOffset(aggCol).isNull(aggregateBuf),
		    isInitNull);
  if (!isInitNull) {
    BOOST_CHECK_EQUAL(a1.getAggregate()->getInt32(aggCol, aggregateBuf),
		      0);
  }
  m->executeUpdate(inputBuf, aggregateBuf, &runtimeCtxt);
  BOOST_CHECK_EQUAL(a1.getAggregate()->getInt32("a", aggregateBuf),
		    23);
  BOOST_CHECK_EQUAL(a1.getAggregate()->getInt32(aggCol, aggregateBuf),
		    2530);
  recordType->setInt32("b", -23, inputBuf);
  recordType->setInt32("c", 25, inputBuf);
  m->executeUpdate(inputBuf, aggregateBuf, &runtimeCtxt);
  BOOST_CHECK_EQUAL(a1.getAggregate()->getInt32("a", aggregateBuf),
		    23);
  BOOST_CHECK_EQUAL(a1.getAggregate()->getInt32(aggCol, aggregateBuf),
		    2532);
  RecordBuffer transferBuf;
  m->executeTransfer(aggregateBuf, transferBuf, &runtimeCtxt);
  BOOST_CHECK_EQUAL(a1.getTarget()->getInt32("a", transferBuf),
		    23);
  BOOST_CHECK_EQUAL(a1.getTarget()->getInt32("d", transferBuf),
		    2532);
  delete m;
}

BOOST_AUTO_TEST_CASE(testIQLRecordAggregate)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  std::vector<std::string> groupKeys;
  groupKeys.push_back("a");
  RecordTypeAggregate a1(ctxt, "agg1", 
			 recordType.get(), 
			 "a, SUM(b+c) AS d",
			 groupKeys);

  ValidateRecordAggregate(a1,
			  recordType,
			  "__AggFn1__");
}

BOOST_AUTO_TEST_CASE(testIQLRecordManualAggregate)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  std::vector<std::string> groupKeys;
  groupKeys.push_back("a");
  RecordTypeAggregate a1(ctxt, "agg1", 
			 recordType.get(), 
			 "a, 0 AS d",
			 "SET d = d + b + c",
			 groupKeys);
  ValidateRecordAggregate(a1,
			  recordType,
			  "d",
			  true, 2u, false);
}

// There is some funky internal logic in IQL pertaining
// to sharing of temporaries allocated for CASE results.
// It is particularly subtle when mixed with aggregates
// so we test it here.
BOOST_AUTO_TEST_CASE(testIQLRecordAggregateWithCase1)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  std::vector<std::string> groupKeys;
  groupKeys.push_back("a");
  RecordTypeAggregate a1(ctxt, "agg1", 
			 recordType.get(), 
			 "a, SUM(CASE WHEN a = 23 THEN b+c ELSE b END) AS d",
			 groupKeys);

  ValidateRecordAggregate(a1,
			  recordType,
			  "__AggFn1__");
}
BOOST_AUTO_TEST_CASE(testIQLRecordAggregateWithCase2)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  std::vector<std::string> groupKeys;
  groupKeys.push_back("a");
  RecordTypeAggregate a1(ctxt, "agg1", 
			 recordType.get(), 
			 "a, CASE WHEN a = 23 THEN SUM(b+c) ELSE SUM(b) END AS d",
			 groupKeys);

  ValidateRecordAggregate(a1,
			  recordType,
			  "__AggFn1__",
			  false, 3);
}

BOOST_AUTO_TEST_CASE(testIQLRecordAggregateWithCase3)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  std::vector<std::string> groupKeys;
  groupKeys.push_back("a");
  RecordTypeAggregate a1(ctxt, "agg1", 
			 recordType.get(), 
			 "a, CASE WHEN a = 23 THEN SUM(CASE WHEN a > 0 THEN b+c WHEN a = 0 THEN 0 ELSE 2*b END) ELSE SUM(CASE WHEN a < 0 THEN c ELSE b END) END AS d",
			 groupKeys);

  ValidateRecordAggregate(a1,
			  recordType,
			  "__AggFn1__",
			  false, 3);
}

RecordBuffer createSimpleLogInputRecord(boost::shared_ptr<RecordType> recordType)
{
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setChar("ignore", "aaaaaa", inputBuf);
  recordType->setChar("cpcode", "bbbbbb", inputBuf);
  recordType->setChar("time", "ccccccccccc", inputBuf);
  recordType->setChar("ip_address", "dddddddd", inputBuf);
  recordType->setChar("method", "eeee", inputBuf);
  recordType->setChar("http_status", "200", inputBuf);
  recordType->setVarchar("mime_type", "image/jpeg", inputBuf);
  recordType->setVarchar("user_agent", "Mozilla/4.0%20(compatible;%20MSIE%207.0;%20Windows%20NT%205.1;%20.NET%20CLR%201.0.3705;%20.NET%20CLR%201.1.4322;%20Media%20Center%20PC%204.0;%20.NET%20CLR%202.0.50727)", inputBuf);
  recordType->setVarchar("url", "/image-pls.bestbuy.com/BestBuy_US/images/products/5965/5965164_sc.jpg", inputBuf);
  recordType->setVarchar("referrer", "http://www.tagged.com/profile.html?uid=5404238227", inputBuf);
  recordType->setVarchar("cookies", "S=i3vca4npor6c5tee4nnnjtksd0", inputBuf);
  recordType->setVarchar("custom_field", "-", inputBuf);
  return inputBuf;
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferStrings)
{
  DynamicRecordContext ctxt;
  boost::shared_ptr<RecordType> recordType = createLogInputType(ctxt);

  // A sample record.  Since we are using copy semantics,
  // the input record should be unmodified throughtout the test.
  RecordBuffer inputBuf = createSimpleLogInputRecord(recordType);
  InterpreterContext runtimeCtxt;

  {
    // Simple Transfer of everything
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), "input.*");
    t1.getTarget()->dump();
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    t1.getTarget()->Print(outputBuf, std::cout);
    t1.getTarget()->GetFree()->free(outputBuf);
  }

  // Transfer select columns with aliases
  RecordTypeTransfer t2(ctxt, "xfer2", recordType.get(), "cpcode AS cpcode, url AS url, referrer AS referrer");
  t2.getTarget()->dump();

  // Transfer select columns without aliases
  RecordTypeTransfer t3(ctxt, "xfer3", recordType.get(), "cpcode, url, referrer");
  t3.getTarget()->dump();

  {
    // Output an expression
    RecordTypeTransfer t4(ctxt, "xfer4", recordType.get(), "user_agent + url AS field1, user_agent + referrer AS field2");
    t4.getTarget()->dump();
    RecordBuffer outputBuf = t4.getTarget()->GetMalloc()->malloc();
    t4.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    t4.getTarget()->Print(outputBuf, std::cout);
    t4.getTarget()->GetFree()->free(outputBuf);
  }

  {
    // Case insensitivity of keyword
    RecordTypeTransfer t4(ctxt, "xfer4", recordType.get(), "user_agent + url aS field1, user_agent + referrer as field2");
    t4.getTarget()->dump();
    RecordBuffer outputBuf = t4.getTarget()->GetMalloc()->malloc();
    t4.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    t4.getTarget()->Print(outputBuf, std::cout);
    t4.getTarget()->GetFree()->free(outputBuf);
  }

  {
    // Output an expression and all inputs
    RecordTypeTransfer t5(ctxt, "xfer5", recordType.get(), "input.*, user_agent + url AS field1, user_agent + referrer AS field2");
    t5.getTarget()->dump();
    RecordBuffer outputBuf = t5.getTarget()->GetMalloc()->malloc();
    t5.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    t5.getTarget()->Print(outputBuf, std::cout);
    t5.getTarget()->GetFree()->free(outputBuf);
  }

  // Output an expression and all inputs
  RecordTypeTransfer t6(ctxt, "xfer6", recordType.get(), "user_agent + url AS field1, user_agent + referrer AS field2, input.*");
  t6.getTarget()->dump();

  // Clean up the input
  recordType->GetFree()->free(inputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferStringsWithMove)
{
  DynamicRecordContext ctxt;
  boost::shared_ptr<RecordType> recordType = createLogInputType(ctxt);

  // A sample record.  Since we are using move semantics,
  // the input record should be destroyed each test.
  InterpreterContext runtimeCtxt;

  {
    // Simple Transfer of everything
    RecordBuffer inputBuf = createSimpleLogInputRecord(recordType);
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), "input.*");
    t1.getTarget()->dump();
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, true);
    t1.getTarget()->Print(outputBuf, std::cout);
    t1.getTarget()->GetFree()->free(outputBuf);
    // Input should actually be cleared out and safe to free.
    recordType->GetFree()->free(inputBuf);
  }

  // // Transfer select columns with aliases
  // RecordTypeTransfer t2(ctxt, "xfer2", recordType.get(), "cpcode AS cpcode, url AS url, referrer AS referrer");
  // t2.getTarget()->dump();

  // // Transfer select columns without aliases
  // RecordTypeTransfer t3(ctxt, "xfer3", recordType.get(), "cpcode, url, referrer");
  // t3.getTarget()->dump();

  // {
  //   // Output an expression
  //   RecordTypeTransfer t4(ctxt, "xfer4", recordType.get(), "user_agent + url AS field1, user_agent + referrer AS field2");
  //   t4.getTarget()->dump();
  //   RecordBuffer outputBuf = t4.getTarget()->GetMalloc()->malloc();
  //   t4.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  //   t4.getTarget()->Print(outputBuf, std::cout);
  //   t4.getTarget()->GetFree()->free(outputBuf);
  // }

  // {
  //   // Output an expression and all inputs
  //   RecordTypeTransfer t5(ctxt, "xfer5", recordType.get(), "input.*, user_agent + url AS field1, user_agent + referrer AS field2");
  //   t5.getTarget()->dump();
  //   RecordBuffer outputBuf = t5.getTarget()->GetMalloc()->malloc();
  //   t5.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  //   t5.getTarget()->Print(outputBuf, std::cout);
  //   t5.getTarget()->GetFree()->free(outputBuf);
  // }

  // // Output an expression and all inputs
  // RecordTypeTransfer t6(ctxt, "xfer6", recordType.get(), "user_agent + url AS field1, user_agent + referrer AS field2, input.*");
  // t6.getTarget()->dump();
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferFloats)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("b", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("c", DoubleType::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), "a,c,b AS d, a+b+c AS e, b+77.21e+01 AS f, b*1.0e-01 AS g"
			", 23.334e+00 AS h"
			", 23334e+00 AS i");
  t1.getTarget()->dump();

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setDouble("a", 23.88, inputBuf);
  recordType->setDouble("b", 230.23, inputBuf);
  recordType->setDouble("c", 2300.1, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(23.88, t1.getTarget()->getDouble("a", outputBuf));
  BOOST_CHECK_EQUAL(2300.1, t1.getTarget()->getDouble("c", outputBuf));
  BOOST_CHECK_EQUAL(230.23, t1.getTarget()->getDouble("d", outputBuf));
  BOOST_CHECK_EQUAL(2554.21, t1.getTarget()->getDouble("e", outputBuf));
  BOOST_CHECK_EQUAL(1002.33, t1.getTarget()->getDouble("f", outputBuf));
  BOOST_CHECK_EQUAL(23.023, t1.getTarget()->getDouble("g", outputBuf));
  BOOST_CHECK_EQUAL(23.334, t1.getTarget()->getDouble("h", outputBuf));
  BOOST_CHECK_EQUAL(23334, t1.getTarget()->getDouble("i", outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLFunctionNamesNotAllowedAsVariables)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("b", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("c", DoubleType::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  // Take a representative list of functions; no need to be exhaustive.
  const char * funs[5] = {"log", "exp", "year", "month", "dayofyear"};
  for(int i=0; i<5; ++i) {
    try {
      RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			    (boost::format("a+b AS %1%") % funs[i]).str());
      std::cout << "Didn't get exception " << funs[i] << std::endl;
      BOOST_CHECK(false);
    } catch(std::runtime_error & ) {
    }
  }
}

BOOST_AUTO_TEST_CASE(testIQLFunctionCallDouble)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("b", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("c", DoubleType::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), "a,c,b AS d, a+b+c AS e, b+77.21e+01 AS f, b*1.0e-01 AS g, log(a) AS h, exp(a) as i");

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setDouble("a", 23.88, inputBuf);
  recordType->setDouble("b", 230.23, inputBuf);
  recordType->setDouble("c", 2300.1, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(23.88, t1.getTarget()->getDouble("a", outputBuf));
  BOOST_CHECK_EQUAL(2300.1, t1.getTarget()->getDouble("c", outputBuf));
  BOOST_CHECK_EQUAL(230.23, t1.getTarget()->getDouble("d", outputBuf));
  BOOST_CHECK_EQUAL(2554.21, t1.getTarget()->getDouble("e", outputBuf));
  BOOST_CHECK_EQUAL(1002.33, t1.getTarget()->getDouble("f", outputBuf));
  BOOST_CHECK_EQUAL(23.023, t1.getTarget()->getDouble("g", outputBuf));
  BOOST_CHECK_EQUAL(log(23.88), t1.getTarget()->getDouble("h", outputBuf));
  BOOST_CHECK_EQUAL(exp(23.88), t1.getTarget()->getDouble("i", outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLFunctionCallString)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("c", VarcharType::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a,c,b AS d, a+b+c AS e, "
			"length(a) AS f"
			", substr(c, 2, 2) as g"
			", substr(c, 2, -1) as h"
			", substr(c, 20, 1) as i"
			", trim(b) as j"
			", ltrim(b) as k"
			", rtrim(b) as l"
			", upper(c) as m"
			", lower(c) as n"
			);

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setVarchar("a", "aaa", inputBuf);
  recordType->setVarchar("b", "    bbbb dfee   ", inputBuf);
  recordType->setVarchar("c", "cdefGHIJK", inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(0, strcmp("aaa", t1.getTarget()->getVarcharPtr("a", outputBuf)->Ptr));
  BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("f", outputBuf));
  BOOST_CHECK_EQUAL(0, strcmp("ef", t1.getTarget()->getVarcharPtr("g", outputBuf)->Ptr));
  BOOST_CHECK_EQUAL(0, strcmp("efGHIJK", t1.getTarget()->getVarcharPtr("h", outputBuf)->Ptr));
  BOOST_CHECK_EQUAL(0, strcmp("", t1.getTarget()->getVarcharPtr("i", outputBuf)->Ptr));
  BOOST_CHECK_EQUAL(0, strcmp("bbbb dfee", t1.getTarget()->getVarcharPtr("j", outputBuf)->Ptr));
  BOOST_CHECK_EQUAL(0, strcmp("bbbb dfee   ", t1.getTarget()->getVarcharPtr("k", outputBuf)->Ptr));
  BOOST_CHECK_EQUAL(0, strcmp("    bbbb dfee", t1.getTarget()->getVarcharPtr("l", outputBuf)->Ptr));
  BOOST_CHECK_EQUAL(0, strcmp("CDEFGHIJK", t1.getTarget()->getVarcharPtr("m", outputBuf)->Ptr));
  BOOST_CHECK_EQUAL(0, strcmp("cdefghijk", t1.getTarget()->getVarcharPtr("n", outputBuf)->Ptr));
}

void testDatediff(bool leftNullable, bool rightNullable)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DateType::Get(ctxt, leftNullable)));
  members.push_back(RecordMember("b", DateType::Get(ctxt, rightNullable)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));

  bool resultNullable = leftNullable || rightNullable;
  
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"datediff(a,b) as c"
			);
  BOOST_CHECK_EQUAL(FieldType::INT32, t1.getTarget()->getMember("c").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(resultNullable, t1.getTarget()->getMember("c").GetType()->isNullable());

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  recordType->setDate("a", boost::gregorian::from_string("2011-02-17"), inputBuf);
  recordType->setDate("b", boost::gregorian::from_string("2011-02-15"), inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(2, t1.getTarget()->getInt32("c", outputBuf));

  recordType->setDate("a", boost::gregorian::from_string("2011-02-01"), inputBuf);
  recordType->setDate("b", boost::gregorian::from_string("2011-02-15"), inputBuf);
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(-14, t1.getTarget()->getInt32("c", outputBuf));

  if (leftNullable) {
    recordType->getFieldAddress("a").setNull(inputBuf);
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(t1.getTarget()->getFieldAddress("c").isNull(outputBuf));
  }

  if (rightNullable) {
    recordType->getFieldAddress("b").setNull(inputBuf);
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(t1.getTarget()->getFieldAddress("c").isNull(outputBuf));
  }

  recordType->getFree().free(inputBuf);
  t1.getTarget()->getFree().free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLFunctionCallDatediff)
{
  testDatediff(false, false);
  // TODO:
  // testDatediff(true, false);
  // testDatediff(false, true);
  // testDatediff(true, true);
}

BOOST_AUTO_TEST_CASE(testIQLFunctionCallDatetime)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DatetimeType::Get(ctxt)));
  members.push_back(RecordMember("b", DateType::Get(ctxt)));
  members.push_back(RecordMember("c", Int64Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a,b AS e "
			", dayofweek(b) AS f"
			", dayofmonth(b) AS g"
			", date(a) AS h"
			", dayofyear(b) AS i"
			", month(b) AS j"
			", year(b) AS k"
			", last_day(b) AS l"
			", julian_day(b) AS m"
			", unix_timestamp(a) AS n"
			", from_unixtime(c) AS o"
			);

  // Actually execute this thing.
  boost::posix_time::ptime t = boost::posix_time::time_from_string("2011-02-17 15:38:33");
  boost::posix_time::ptime t2 = boost::posix_time::time_from_string("2012-01-12 13:12:23");
  boost::gregorian::date d = boost::gregorian::from_string("2011-02-17");
  boost::gregorian::date d2 = boost::gregorian::from_string("2011-02-28");
  
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setDatetime("a", t, inputBuf);
  recordType->setDate("b", d, inputBuf);
  recordType->setInt64("c", 1326391943LL, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(5, t1.getTarget()->getInt32("f", outputBuf));
  BOOST_CHECK_EQUAL(17, t1.getTarget()->getInt32("g", outputBuf));
  BOOST_CHECK_EQUAL(d, t1.getTarget()->getFieldAddress("h").getDate(outputBuf));
  BOOST_CHECK_EQUAL(48, t1.getTarget()->getInt32("i", outputBuf));
  BOOST_CHECK_EQUAL(2, t1.getTarget()->getInt32("j", outputBuf));
  BOOST_CHECK_EQUAL(2011, t1.getTarget()->getInt32("k", outputBuf));
  BOOST_CHECK_EQUAL(d2, 
		    t1.getTarget()->getFieldAddress("l").getDate(outputBuf));
  BOOST_CHECK_EQUAL(2455610, t1.getTarget()->getInt32("m", outputBuf));
  BOOST_CHECK_EQUAL(1297975113, t1.getTarget()->getInt32("n", outputBuf));
  BOOST_CHECK_EQUAL(t2, t1.getTarget()->getFieldAddress("o").getDatetime(outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLIntervalTypes)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DatetimeType::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a "
			", INTERVAL 5 DAY AS b"
			", INTERVAL 72 SECOND AS c"
			", a + INTERVAL 72 SECOND AS d"
			", a + INTERVAL 5 DAY AS e"
			", a + INTERVAL 2 MINUTE AS f"
			", a + INTERVAL 4 HOUR AS g"
			", a + INTERVAL 2 MONTH AS h"
			", a + INTERVAL 5 YEAR AS i"
			", a + INTERVAL -2 DAY AS j"
			", a + INTERVAL -48 HOUR AS k"
			", a + INTERVAL 86402 SECOND AS l"
			", CAST('2011-01-01 23:33:17' AS DATETIME) + INTERVAL 15 DAY AS m"
			", a + interval 86402 seCOND AS n"
			);
  
  // Actually execute this thing.  
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  boost::posix_time::ptime dt = boost::posix_time::time_from_string("2011-02-17 15:38:33");
  recordType->setDatetime("a", dt, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(5, t1.getTarget()->getFieldAddress("b").getInt32(outputBuf));
  BOOST_CHECK_EQUAL(72, t1.getTarget()->getFieldAddress("c").getInt32(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-02-17 15:39:45"),
		    t1.getTarget()->getFieldAddress("d").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-02-22 15:38:33"),
		    t1.getTarget()->getFieldAddress("e").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-02-17 15:40:33"),
		    t1.getTarget()->getFieldAddress("f").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-02-17 19:38:33"),
		    t1.getTarget()->getFieldAddress("g").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-04-17 15:38:33"),
		    t1.getTarget()->getFieldAddress("h").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2016-02-17 15:38:33"),
		    t1.getTarget()->getFieldAddress("i").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-02-15 15:38:33"),
		    t1.getTarget()->getFieldAddress("j").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-02-15 15:38:33"),
		    t1.getTarget()->getFieldAddress("k").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-02-18 15:38:35"),
		    t1.getTarget()->getFieldAddress("l").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-01-16 23:33:17"),
		    t1.getTarget()->getFieldAddress("m").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-02-18 15:38:35"),
		    t1.getTarget()->getFieldAddress("n").getDatetime(outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransfer2Integers)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  members.clear();
  members.push_back(RecordMember("d", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("e", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("f", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType2(new RecordType(members));
  
  // Simple Transfer of everything.  
  std::vector<AliasedRecordType> types;
  types.push_back(AliasedRecordType("table", recordType.get()));
  types.push_back(AliasedRecordType("probe", recordType2.get()));
  RecordTypeTransfer2 t1(ctxt, "xfer1", types,
			 "table.*, probe.*");
  t1.getTarget()->dump();

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  RecordBuffer inputBuf2 = recordType2->GetMalloc()->malloc();
  recordType2->setInt32("d", 52, inputBuf2);
  recordType2->setInt32("e", 520, inputBuf2);
  recordType2->setInt32("f", 5200, inputBuf2);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, inputBuf2, outputBuf, &runtimeCtxt, false, false);
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("b", outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK_EQUAL(52, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK_EQUAL(520, t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL(5200, t1.getTarget()->getInt32("f", outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferRegex)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c1", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c3", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c34", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c4444", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("eff", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("efg", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 1, inputBuf);
  recordType->setInt32("b", 2, inputBuf);
  recordType->setInt32("c1", 3, inputBuf);
  recordType->setInt32("c3", 4, inputBuf);
  recordType->setInt32("c34", 5, inputBuf);
  recordType->setInt32("c4444", 6, inputBuf);
  recordType->setInt32("d", 7, inputBuf);
  recordType->setInt32("eff", 8, inputBuf);
  recordType->setInt32("efg", 9, inputBuf);
  
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "`c[0-9]+`");
    BOOST_CHECK_EQUAL(4U, t1.getTarget()->size());
    // Actually execute this thing.
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("c1", outputBuf));
    BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("c3", outputBuf));
    BOOST_CHECK_EQUAL(5, t1.getTarget()->getInt32("c34", outputBuf));
    BOOST_CHECK_EQUAL(6, t1.getTarget()->getInt32("c4444", outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "`c[0-9]+`, `e.*`");
    BOOST_CHECK_EQUAL(6U, t1.getTarget()->size());
    // Actually execute this thing.
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("c1", outputBuf));
    BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("c3", outputBuf));
    BOOST_CHECK_EQUAL(5, t1.getTarget()->getInt32("c34", outputBuf));
    BOOST_CHECK_EQUAL(6, t1.getTarget()->getInt32("c4444", outputBuf));
    BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("eff", outputBuf));
    BOOST_CHECK_EQUAL(9, t1.getTarget()->getInt32("efg", outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "`c[0-9]+`, `eff`");
    BOOST_CHECK_EQUAL(5U, t1.getTarget()->size());
    // Actually execute this thing.
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("c1", outputBuf));
    BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("c3", outputBuf));
    BOOST_CHECK_EQUAL(5, t1.getTarget()->getInt32("c34", outputBuf));
    BOOST_CHECK_EQUAL(6, t1.getTarget()->getInt32("c4444", outputBuf));
    BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("eff", outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "eff AS foo, `c[0-9]+`, `eff`");
    BOOST_CHECK_EQUAL(6U, t1.getTarget()->size());
    // Actually execute this thing.
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("c1", outputBuf));
    BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("c3", outputBuf));
    BOOST_CHECK_EQUAL(5, t1.getTarget()->getInt32("c34", outputBuf));
    BOOST_CHECK_EQUAL(6, t1.getTarget()->getInt32("c4444", outputBuf));
    BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("eff", outputBuf));
    BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("foo", outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "eff AS foo, `c[0-9]+`, a");
    BOOST_CHECK_EQUAL(6U, t1.getTarget()->size());
    // Actually execute this thing.
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("c1", outputBuf));
    BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("c3", outputBuf));
    BOOST_CHECK_EQUAL(5, t1.getTarget()->getInt32("c34", outputBuf));
    BOOST_CHECK_EQUAL(6, t1.getTarget()->getInt32("c4444", outputBuf));
    BOOST_CHECK_EQUAL(1, t1.getTarget()->getInt32("a", outputBuf));
    BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("foo", outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "eff AS foo, `c[0-9]+`, 34*a AS a");
    BOOST_CHECK_EQUAL(6U, t1.getTarget()->size());
    // Actually execute this thing.
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("c1", outputBuf));
    BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("c3", outputBuf));
    BOOST_CHECK_EQUAL(5, t1.getTarget()->getInt32("c34", outputBuf));
    BOOST_CHECK_EQUAL(6, t1.getTarget()->getInt32("c4444", outputBuf));
    BOOST_CHECK_EQUAL(34, t1.getTarget()->getInt32("a", outputBuf));
    BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("foo", outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "eff AS foo, `c[0-9]*3$`, 34*a AS a");
    BOOST_CHECK_EQUAL(3U, t1.getTarget()->size());
    // Actually execute this thing.
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("c3", outputBuf));
    BOOST_CHECK_EQUAL(34, t1.getTarget()->getInt32("a", outputBuf));
    BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("foo", outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "eff AS foo, `c[0-9]*3`, 34*a AS a");
    BOOST_CHECK_EQUAL(3U, t1.getTarget()->size());
    // Actually execute this thing.
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("c3", outputBuf));
    BOOST_CHECK_EQUAL(34, t1.getTarget()->getInt32("a", outputBuf));
    BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("foo", outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransfer2Regex)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c1", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c3", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c34", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 1, inputBuf);
  recordType->setInt32("b", 2, inputBuf);
  recordType->setInt32("c1", 3, inputBuf);
  recordType->setInt32("c3", 4, inputBuf);
  recordType->setInt32("c34", 5, inputBuf);

  members.clear();
  members.push_back(RecordMember("c4444", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("eff", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("efg", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType2(new RecordType(members));  
  RecordBuffer inputBuf2 = recordType2->GetMalloc()->malloc();
  recordType2->setInt32("c4444", 6, inputBuf2);
  recordType2->setInt32("d", 7, inputBuf2);
  recordType2->setInt32("eff", 8, inputBuf2);
  recordType2->setInt32("efg", 9, inputBuf2);
  // Simple Transfer of everything.  
  std::vector<AliasedRecordType> types;
  types.push_back(AliasedRecordType("table", recordType.get()));
  types.push_back(AliasedRecordType("probe", recordType2.get()));
  
  {
    RecordTypeTransfer2 t1(ctxt, "xfer1", types,
			 "`c[0-9]+`");
    BOOST_CHECK_EQUAL(4U, t1.getTarget()->size());
    // Actually execute this thing.
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, inputBuf2, outputBuf, &runtimeCtxt, false, false);
    BOOST_CHECK_EQUAL(3, t1.getTarget()->getInt32("c1", outputBuf));
    BOOST_CHECK_EQUAL(4, t1.getTarget()->getInt32("c3", outputBuf));
    BOOST_CHECK_EQUAL(5, t1.getTarget()->getInt32("c34", outputBuf));
    BOOST_CHECK_EQUAL(6, t1.getTarget()->getInt32("c4444", outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
}

BOOST_AUTO_TEST_CASE(testMemcpyCoalese)
{
  {
    std::vector<MemcpyOp> input;
    std::vector<MemcpyOp> output;
    MemcpyOp::coalesce(input, output);
    BOOST_CHECK_EQUAL(0U, output.size());
  }
  {
    std::vector<MemcpyOp> input;
    std::vector<MemcpyOp> output;
    input.push_back(MemcpyOp(0,0,4));
    input.push_back(MemcpyOp(4,4,4));
    MemcpyOp::coalesce(input, output);
    BOOST_CHECK_EQUAL(1U, output.size());
    BOOST_CHECK(FieldAddress(0)==output.front().mSourceOffset);
    BOOST_CHECK(FieldAddress(0)==output.front().mTargetOffset);
    BOOST_CHECK_EQUAL(8U, output.front().mSize);
  }
  {
    std::vector<MemcpyOp> input;
    std::vector<MemcpyOp> output;
    input.push_back(MemcpyOp(0,12,4));
    input.push_back(MemcpyOp(4,16,4));
    MemcpyOp::coalesce(input, output);
    BOOST_CHECK_EQUAL(1U, output.size());
    BOOST_CHECK(FieldAddress(0)==output.front().mSourceOffset);
    BOOST_CHECK(FieldAddress(12)==output.front().mTargetOffset);
    BOOST_CHECK_EQUAL(8U, output.front().mSize);
  }
  {
    std::vector<MemcpyOp> input;
    std::vector<MemcpyOp> output;
    input.push_back(MemcpyOp(0,12,4));
    input.push_back(MemcpyOp(4,20,4));
    MemcpyOp::coalesce(input, output);
    BOOST_CHECK_EQUAL(2U, output.size());
    BOOST_CHECK(FieldAddress(0)==output.front().mSourceOffset);
    BOOST_CHECK(FieldAddress(12)==output.front().mTargetOffset);
    BOOST_CHECK_EQUAL(4U, output.front().mSize);
    BOOST_CHECK(FieldAddress(4)==output[1].mSourceOffset);
    BOOST_CHECK(FieldAddress(20)==output[1].mTargetOffset);
    BOOST_CHECK_EQUAL(4U, output.front().mSize);
  }
  {
    std::vector<MemcpyOp> input;
    std::vector<MemcpyOp> output;
    input.push_back(MemcpyOp(0,12,4));
    input.push_back(MemcpyOp(4,20,4));
    input.push_back(MemcpyOp(8,24,4));
    MemcpyOp::coalesce(input, output);
    BOOST_CHECK_EQUAL(2U, output.size());
    BOOST_CHECK(FieldAddress(0)==output.front().mSourceOffset);
    BOOST_CHECK(FieldAddress(12)==output.front().mTargetOffset);
    BOOST_CHECK_EQUAL(4U, output.front().mSize);
    BOOST_CHECK(FieldAddress(4)==output[1].mSourceOffset);
    BOOST_CHECK(FieldAddress(20)==output[1].mTargetOffset);
    BOOST_CHECK_EQUAL(8U, output[1].mSize);
  }
  {
    std::vector<MemcpyOp> input;
    std::vector<MemcpyOp> output;
    input.push_back(MemcpyOp(0,12,4));
    input.push_back(MemcpyOp(8,16,4));
    MemcpyOp::coalesce(input, output);
    BOOST_CHECK_EQUAL(2U, output.size());
    BOOST_CHECK(FieldAddress(0)==output.front().mSourceOffset);
    BOOST_CHECK(FieldAddress(12)==output.front().mTargetOffset);
    BOOST_CHECK_EQUAL(4U, output.front().mSize);
    BOOST_CHECK(FieldAddress(8)==output[1].mSourceOffset);
    BOOST_CHECK(FieldAddress(16)==output[1].mTargetOffset);
    BOOST_CHECK_EQUAL(4U, output.front().mSize);
  }
  {
    std::vector<MemcpyOp> input;
    std::vector<MemcpyOp> output;
    input.push_back(MemcpyOp(0,12,4));
    input.push_back(MemcpyOp(8,16,4));
    input.push_back(MemcpyOp(12,20,4));
    MemcpyOp::coalesce(input, output);
    BOOST_CHECK_EQUAL(2U, output.size());
    BOOST_CHECK(FieldAddress(0)==output.front().mSourceOffset);
    BOOST_CHECK(FieldAddress(12)==output.front().mTargetOffset);
    BOOST_CHECK_EQUAL(4U, output.front().mSize);
    BOOST_CHECK(FieldAddress(8)==output[1].mSourceOffset);
    BOOST_CHECK(FieldAddress(16)==output[1].mTargetOffset);
    BOOST_CHECK_EQUAL(8U, output[1].mSize);
  }
}

void testCharCast(bool isNullable)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 6, isNullable)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt, isNullable)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, isNullable)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt, isNullable)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt, isNullable)));
  RecordType recTy(members);

  RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "123456", inputBuf);
  recTy.setVarchar("b", "abcdefghijklmnop", inputBuf);
  recTy.setInt32("c", 9923432, inputBuf);
  recTy.setInt64("d", 1239923432, inputBuf);
  recTy.setDouble("e", 8234.24344, inputBuf);

  {
    RecordTypeTransfer t1(ctxt, "xfer1", &recTy, 
			"CAST(b AS CHAR(4)) AS ret");
    BOOST_CHECK_EQUAL(4, t1.getTarget()->begin_members()->GetType()->GetSize());
    BOOST_CHECK_EQUAL(FieldType::CHAR, t1.getTarget()->begin_members()->GetType()->GetEnum());
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    char expected [] = "abcd";
    BOOST_CHECK_EQUAL(0, memcmp(&expected, t1.getTarget()->getMemberOffset("ret").getCharPtr(outputBuf), 4));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", &recTy, 
			"CAST(b AS CHAR(18)) AS ret");
    BOOST_CHECK_EQUAL(18, t1.getTarget()->begin_members()->GetType()->GetSize());
    BOOST_CHECK_EQUAL(FieldType::CHAR, t1.getTarget()->begin_members()->GetType()->GetEnum());
    BOOST_CHECK_EQUAL(isNullable, t1.getTarget()->begin_members()->GetType()->isNullable());
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    char expected [] = "abcdefghijklmnop  ";
    BOOST_CHECK_EQUAL(0, memcmp(&expected, t1.getTarget()->getMemberOffset("ret").getCharPtr(outputBuf), 18));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    RecordTypeTransfer t1(ctxt, "xfer1", &recTy, 
			"CAST(b AS CHAR(16)) AS ret");
    BOOST_CHECK_EQUAL(16, t1.getTarget()->begin_members()->GetType()->GetSize());
    BOOST_CHECK_EQUAL(FieldType::CHAR, t1.getTarget()->begin_members()->GetType()->GetEnum());
    BOOST_CHECK_EQUAL(isNullable, t1.getTarget()->begin_members()->GetType()->isNullable());
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    char expected [] = "abcdefghijklmnop";
    BOOST_CHECK_EQUAL(0, memcmp(&expected, t1.getTarget()->getMemberOffset("ret").getCharPtr(outputBuf), 16));
    t1.getTarget()->getFree().free(outputBuf);
  }
}

BOOST_AUTO_TEST_CASE(testIQLCharCast)
{
  testCharCast(false);
}

BOOST_AUTO_TEST_CASE(testIQLNullableCharCast)
{
  testCharCast(true);
}

void testDateCast(bool isNullable)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 10, isNullable)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt, isNullable)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, isNullable)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt, isNullable)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt, isNullable)));
  RecordType recTy(members);

  RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "2011-03-12", inputBuf);
  recTy.setVarchar("b", "2011-03-16", inputBuf);
  recTy.setInt32("c", 9923432, inputBuf);
  recTy.setInt64("d", 1239923432, inputBuf);
  recTy.setDouble("e", 8234.24344, inputBuf);

  {
    RecordTypeTransfer t1(ctxt, "xfer1", &recTy, 
			"CAST(b AS DATE) AS varcharToDate");
    BOOST_CHECK_EQUAL(FieldType::DATE, t1.getTarget()->begin_members()->GetType()->GetEnum());
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    boost::gregorian::date expected(2011, boost::gregorian::Mar, 16);
    BOOST_CHECK_EQUAL(expected, t1.getTarget()->getMemberOffset("varcharToDate").getDate(outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
  {
    // case insensitivity
    RecordTypeTransfer t1(ctxt, "xfer1", &recTy, 
			"cast(b as date) AS varcharToDate");
    BOOST_CHECK_EQUAL(FieldType::DATE, t1.getTarget()->begin_members()->GetType()->GetEnum());
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    boost::gregorian::date expected(2011, boost::gregorian::Mar, 16);
    BOOST_CHECK_EQUAL(expected, t1.getTarget()->getMemberOffset("varcharToDate").getDate(outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
}

BOOST_AUTO_TEST_CASE(testIQLDateCast)
{
  testDateCast(false);
}

BOOST_AUTO_TEST_CASE(testIQLNullableDateCast)
{
  testDateCast(true);
}

void testDatetimeCast(bool isNullable)
{
  namespace pt = boost::posix_time;
  namespace greg = boost::gregorian;
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 10, isNullable)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt, isNullable)));
  members.push_back(RecordMember("c", VarcharType::Get(ctxt, isNullable)));
  RecordType recTy(members);

  RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "2011-03-12", inputBuf);
  recTy.setVarchar("b", "2011-03-16 23:22:59", inputBuf);
  recTy.setVarchar("c", "2012-01-30", inputBuf);
  pt::ptime expected1 = pt::time_from_string("2011-03-16 23:22:59");
  pt::ptime expected2 = pt::time_from_string("2012-01-30 12:22:45");
  pt::ptime expected3 = pt::time_from_string("2012-01-30 00:00:00");
  {
    RecordTypeTransfer t1(ctxt, "xfer1", &recTy, 
			  "CAST(b AS DATETIME) AS varcharToDatetime"
			  ",CaST(b AS datetime) AS varcharToDatetimeCI"
			  ",CAST('2012-01-30 12:22:45' AS DATETIME) AS varcharToDatetimeLiteral"
			  ",CAST('2012-01-30' AS DATETIME) AS varcharToDateLiteral"
			  ",CAST(c AS DATETIME) AS varcharToDateVariable"
			  );
    BOOST_CHECK_EQUAL(5U, t1.getTarget()->size());
    for(RecordType::const_member_iterator m = t1.getTarget()->begin_members(),
	  e = t1.getTarget()->end_members();
	m != e; ++m) {
      BOOST_CHECK_EQUAL(FieldType::DATETIME, m->GetType()->GetEnum());
    }
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK_EQUAL(expected1, t1.getTarget()->getMemberOffset("varcharToDatetime").getDatetime(outputBuf));
    BOOST_CHECK_EQUAL(expected1, t1.getTarget()->getMemberOffset("varcharToDatetimeCI").getDatetime(outputBuf));
    BOOST_CHECK_EQUAL(expected2, t1.getTarget()->getMemberOffset("varcharToDatetimeLiteral").getDatetime(outputBuf));
    BOOST_CHECK_EQUAL(expected3, t1.getTarget()->getMemberOffset("varcharToDateLiteral").getDatetime(outputBuf));
    BOOST_CHECK_EQUAL(expected3, t1.getTarget()->getMemberOffset("varcharToDateVariable").getDatetime(outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
  recTy.getFree().free(inputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLDatetimeCast)
{
  testDatetimeCast(false);
}

void testVarcharCast(bool isNullable)
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 10, isNullable)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt, isNullable)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, isNullable)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt, isNullable)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt, isNullable)));
  members.push_back(RecordMember("f", DecimalType::Get(ctxt, isNullable)));
  members.push_back(RecordMember("g", DatetimeType::Get(ctxt, isNullable)));
  members.push_back(RecordMember("h", DateType::Get(ctxt, isNullable)));
  RecordType recTy(members);

  RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
  recTy.setChar("a", "2011-03-12", inputBuf);
  recTy.setVarchar("b", "2011-03-16", inputBuf);
  recTy.setInt32("c", 9923432, inputBuf);
  recTy.setInt64("d", 1239923432, inputBuf);
  recTy.setDouble("e", 8234.24344, inputBuf);
  decimal128 dec;
  ::decimal128FromString(&dec,
			 "123456.8234", 
			 runtimeCtxt.getDecimalContext());
  recTy.getMemberOffset("f").setDecimal(dec, inputBuf);
  boost::posix_time::ptime dt = boost::posix_time::time_from_string("2011-02-17 15:38:33");
  recTy.setDatetime("g", dt, inputBuf);
  boost::gregorian::date d = boost::gregorian::from_string("2011-02-22");
  recTy.setDate("h", d, inputBuf);

  {
    RecordTypeTransfer t1(ctxt, "xfer1", &recTy, 
			  "CAST(a AS VARCHAR) AS a"
			  ", CAST(b AS VARCHAR) AS b"
			  ", CAST(c AS VARCHAR) AS c"
			  ", CAST(d AS VARCHAR) AS d"
			  ", CAST(e AS VARCHAR) AS e"
			  ", CAST(f AS VARCHAR) AS f"
			  ", CAST(g AS VARCHAR) AS g"
			  ", CAST(h AS VARCHAR) AS h"
			  );
    for(RecordType::const_member_iterator it = t1.getTarget()->begin_members();
	it != t1.getTarget()->end_members();
	++it) {
      BOOST_CHECK_EQUAL(FieldType::VARCHAR, it->GetType()->GetEnum());
      BOOST_CHECK_EQUAL(isNullable, it->GetType()->isNullable());
    }
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(boost::algorithm::equals("2011-03-12", 
					 t1.getTarget()->getVarcharPtr("a", outputBuf)->Ptr));
    BOOST_CHECK(boost::algorithm::equals("2011-03-16", 
					 t1.getTarget()->getVarcharPtr("b", outputBuf)->Ptr));
    BOOST_CHECK(boost::algorithm::equals("9923432", 
					 t1.getTarget()->getVarcharPtr("c", outputBuf)->Ptr));
    BOOST_CHECK(boost::algorithm::equals("1239923432", 
					 t1.getTarget()->getVarcharPtr("d", outputBuf)->Ptr));
    BOOST_CHECK(boost::algorithm::equals("8234.24344", 
					 t1.getTarget()->getVarcharPtr("e", outputBuf)->Ptr));
    BOOST_CHECK(boost::algorithm::equals("123456.8234", 
					 t1.getTarget()->getVarcharPtr("f", outputBuf)->Ptr));
    BOOST_CHECK(boost::algorithm::equals("2011-02-17 15:38:33", 
					 t1.getTarget()->getVarcharPtr("g", outputBuf)->Ptr));
    BOOST_CHECK(boost::algorithm::equals("2011-02-22", 
					 t1.getTarget()->getVarcharPtr("h", outputBuf)->Ptr));
    t1.getTarget()->getFree().free(outputBuf);
  }
}

void testNullableVarcharCast()
{
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 10, true)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt, true)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt, true)));
  members.push_back(RecordMember("f", DecimalType::Get(ctxt, true)));
  members.push_back(RecordMember("g", DatetimeType::Get(ctxt, true)));
  members.push_back(RecordMember("h", DateType::Get(ctxt, true)));
  RecordType recTy(members);

  RecordBuffer inputBuf = recTy.GetMalloc()->malloc();
  recTy.getFieldAddress("a").setNull(inputBuf);
  recTy.getFieldAddress("b").setNull(inputBuf);
  recTy.getFieldAddress("c").setNull(inputBuf);
  recTy.getFieldAddress("d").setNull(inputBuf);
  recTy.getFieldAddress("e").setNull(inputBuf);
  recTy.getFieldAddress("f").setNull(inputBuf);
  recTy.getFieldAddress("g").setNull(inputBuf);
  recTy.getFieldAddress("h").setNull(inputBuf);

  {
    RecordTypeTransfer t1(ctxt, "xfer1", &recTy, 
			  "CAST(a AS VARCHAR) AS a"
			  ", CAST(b AS VARCHAR) AS b"
			  ", CAST(c AS VARCHAR) AS c"
			  ", CAST(d AS VARCHAR) AS d"
			  ", CAST(e AS VARCHAR) AS e"
			  ", CAST(f AS VARCHAR) AS f"
			  ", CAST(g AS VARCHAR) AS g"
			  ", CAST(h AS VARCHAR) AS h"
			  );
    for(RecordType::const_member_iterator it = t1.getTarget()->begin_members();
	it != t1.getTarget()->end_members();
	++it) {
      BOOST_CHECK_EQUAL(FieldType::VARCHAR, it->GetType()->GetEnum());
      BOOST_CHECK(it->GetType()->isNullable());
    }
    RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
    InterpreterContext runtimeCtxt;
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(t1.getTarget()->getFieldAddress("a").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("c").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("g").isNull(outputBuf));
    BOOST_CHECK(t1.getTarget()->getFieldAddress("h").isNull(outputBuf));
    t1.getTarget()->getFree().free(outputBuf);
  }
}

BOOST_AUTO_TEST_CASE(testIQLVarcharCast)
{
  testVarcharCast(false);
}

BOOST_AUTO_TEST_CASE(testIQLNullableVarcharCast)
{
  testVarcharCast(true);
  testNullableVarcharCast();
}

BOOST_AUTO_TEST_CASE(testIQLAddChar)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt,32)));
  members.push_back(RecordMember("c", CharType::Get(ctxt, 9)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a+c AS b"
			);
  
  // Actually execute this thing.  
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setChar("a", "4381905F306900000161B14DDCDB9911", inputBuf);
  recordType->setChar("c", "123456789", inputBuf);
  BOOST_CHECK_EQUAL(41, t1.getTarget()->begin_members()->GetType()->GetSize());
  BOOST_CHECK_EQUAL(FieldType::CHAR, t1.getTarget()->begin_members()->GetType()->GetEnum());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(0, strcmp("4381905F306900000161B14DDCDB9911123456789",t1.getTarget()->getFieldAddress("b").getCharPtr(outputBuf)));
}

BOOST_AUTO_TEST_CASE(testIQLUnaryPlusAndMinus)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("x", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("y", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"+x AS a"
			",x + + y AS b"
			",x + + -2*y AS c"
			",+ -2*y AS d"
			",+ -y AS e"
			",+ + + + y AS f"
			",- - - y AS g"
			);
  
  // Actually execute this thing.  
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("x", 32, inputBuf);
  recordType->setInt32("y", -8, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(32, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(24, t1.getTarget()->getInt32("b", outputBuf));
  BOOST_CHECK_EQUAL(48, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK_EQUAL(16, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK_EQUAL(-8, t1.getTarget()->getInt32("f", outputBuf));
  BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt32("g", outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLGetVariables)
{
  std::set<std::string> freeVariables;
  RecordTypeTransfer::getFreeVariables("CASE WHEN a = 9923 AND b = -2 THEN c ELSE 8.233 END AS score_1_1",
				       freeVariables);
  BOOST_CHECK_EQUAL(3U, freeVariables.size());
  BOOST_CHECK(freeVariables.end() != freeVariables.find("a"));
  BOOST_CHECK(freeVariables.end() != freeVariables.find("b"));
  BOOST_CHECK(freeVariables.end() != freeVariables.find("c"));
  freeVariables.clear();
  // Test multiple references to same variable only count once
  RecordTypeTransfer::getFreeVariables("CASE WHEN a = 9923 AND a*b = -2 THEN c-b ELSE 8.233 END AS score_1_1",
				       freeVariables);
  BOOST_CHECK_EQUAL(3U, freeVariables.size());
  BOOST_CHECK(freeVariables.end() != freeVariables.find("a"));
  BOOST_CHECK(freeVariables.end() != freeVariables.find("b"));
  BOOST_CHECK(freeVariables.end() != freeVariables.find("c"));
  freeVariables.clear();
  // Test reference to local variables do not count
  RecordTypeTransfer::getFreeVariables("DECLARE a = 88, CASE WHEN a = 9923 AND a*b = -2 THEN c-b ELSE 8.233 END AS score_1_1",
				       freeVariables);
  BOOST_CHECK_EQUAL(2U, freeVariables.size());
  BOOST_CHECK(freeVariables.end() != freeVariables.find("b"));
  BOOST_CHECK(freeVariables.end() != freeVariables.find("c"));
  freeVariables.clear();
  // Test multiple statements
  RecordTypeTransfer::getFreeVariables("CASE WHEN a = 9923 AND b = -2 THEN c ELSE 8.233 END AS score_1_1"
				       ", score_1_1 + d",
				       freeVariables);
  BOOST_CHECK_EQUAL(4U, freeVariables.size());
  BOOST_CHECK(freeVariables.end() != freeVariables.find("a"));
  BOOST_CHECK(freeVariables.end() != freeVariables.find("b"));
  BOOST_CHECK(freeVariables.end() != freeVariables.find("c"));
  BOOST_CHECK(freeVariables.end() != freeVariables.find("d"));
  freeVariables.clear();
}
BOOST_AUTO_TEST_CASE(testIQLGreaterLeast)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("d", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("e", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("f", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("g", DecimalType::Get(ctxt)));
  members.push_back(RecordMember("h", DecimalType::Get(ctxt)));
  members.push_back(RecordMember("i", DatetimeType::Get(ctxt)));
  members.push_back(RecordMember("j", DatetimeType::Get(ctxt)));
  members.push_back(RecordMember("k", DateType::Get(ctxt)));
  members.push_back(RecordMember("l", DateType::Get(ctxt)));
  members.push_back(RecordMember("m", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("n", VarcharType::Get(ctxt)));
  members.push_back(RecordMember("o", CharType::Get(ctxt, 10)));
  members.push_back(RecordMember("p", CharType::Get(ctxt, 8)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));

  // TODO:  Implement CHAR(N) tests
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"LEAST(a,b) AS result1"
			", GREATEST(a,b) AS result2"
			", LEAST(c,d) AS result3"
			", GREATEST(c,d) AS result4"
			", LEAST(e,f) AS result5"
			", GREATEST(e,f) AS result6"
			", LEAST(g,h) AS result7"
			", GREATEST(g,h) AS result8"
			", LEAST(i,j) AS result9"
			", GREATEST(i,j) AS result10"
			", LEAST(k,l) AS result11"
			", GREATEST(k,l) AS result12"
			", LEAST(m,n) AS result13"
			", GREATEST(m,n) AS result14"
			", LEAST(a,b,c,d) AS result15"
			", GREATEST(a,b,c,d) AS result16"
			", least(a,b,c,d) AS result17"
			", greaTEST(a,b,c,d) AS result18"
			);
  
  // Actually execute this thing.  
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 32, inputBuf);
  recordType->setInt32("b", -8882344, inputBuf);
  recordType->setInt64("c", 88823445323LL, inputBuf);
  recordType->setInt64("d", -8992LL, inputBuf);
  recordType->setDouble("e", 8882344.8344, inputBuf);
  recordType->setDouble("f", -7.23434, inputBuf);
  ::decimal128FromString(recordType->getMemberOffset("g").getDecimalPtr(inputBuf), 
			 "123456.8234", 
			 runtimeCtxt.getDecimalContext());
  ::decimal128FromString(recordType->getMemberOffset("h").getDecimalPtr(inputBuf), 
			 "-56.8234", 
			 runtimeCtxt.getDecimalContext());
  boost::posix_time::ptime dt1 = boost::posix_time::time_from_string("2011-02-17 15:38:33");
  boost::posix_time::ptime dt2 = boost::posix_time::time_from_string("2011-02-19 02:38:33");
  recordType->setDatetime("i", dt1, inputBuf);
  recordType->setDatetime("j", dt2, inputBuf);
  boost::gregorian::date d1 = boost::gregorian::from_string("2011-02-22");
  boost::gregorian::date d2 = boost::gregorian::from_string("2010-02-22");
  recordType->setDate("k", d1, inputBuf);
  recordType->setDate("l", d2, inputBuf);
  recordType->setVarchar("m", "asdfkeekefe", inputBuf);
  recordType->setVarchar("n", "asdfwefkefe", inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("result1").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(-8882344, t1.getTarget()->getInt32("result1", outputBuf));
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("result2").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(32, t1.getTarget()->getInt32("result2", outputBuf));

  BOOST_CHECK_EQUAL(FieldType::INT64, 
		    t1.getTarget()->getMember("result3").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(-8992LL, t1.getTarget()->getInt64("result3", outputBuf));
  BOOST_CHECK_EQUAL(FieldType::INT64, 
		    t1.getTarget()->getMember("result4").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(88823445323LL, t1.getTarget()->getInt64("result4", outputBuf));

  BOOST_CHECK_EQUAL(FieldType::DOUBLE, 
		    t1.getTarget()->getMember("result5").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(-7.23434, t1.getTarget()->getDouble("result5", outputBuf));
  BOOST_CHECK_EQUAL(FieldType::DOUBLE, 
		    t1.getTarget()->getMember("result6").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(8882344.8344, t1.getTarget()->getDouble("result6", outputBuf));

  BOOST_CHECK_EQUAL(FieldType::BIGDECIMAL, 
		    t1.getTarget()->getMember("result7").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(0, 
		    memcmp(
			   recordType->getMemberOffset("h").getDecimalPtr(inputBuf),
			   t1.getTarget()->getMemberOffset("result7").getDecimalPtr(outputBuf),
			   16));
  BOOST_CHECK_EQUAL(FieldType::BIGDECIMAL, 
		    t1.getTarget()->getMember("result8").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(0, 
		    memcmp(
			   recordType->getMemberOffset("g").getDecimalPtr(inputBuf),
			   t1.getTarget()->getMemberOffset("result8").getDecimalPtr(outputBuf),
			   16));

  BOOST_CHECK_EQUAL(FieldType::DATETIME, 
		    t1.getTarget()->getMember("result9").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(dt1, t1.getTarget()->getMemberOffset("result9").getDatetime(outputBuf));
  BOOST_CHECK_EQUAL(FieldType::DATETIME, 
		    t1.getTarget()->getMember("result10").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(dt2, t1.getTarget()->getMemberOffset("result10").getDatetime(outputBuf));

  BOOST_CHECK_EQUAL(FieldType::DATE, 
		    t1.getTarget()->getMember("result11").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(d2, t1.getTarget()->getMemberOffset("result11").getDate(outputBuf));
  BOOST_CHECK_EQUAL(FieldType::DATE, 
		    t1.getTarget()->getMember("result12").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(d1, t1.getTarget()->getMemberOffset("result12").getDate(outputBuf));

  BOOST_CHECK_EQUAL(FieldType::VARCHAR, 
		    t1.getTarget()->getMember("result13").GetType()->GetEnum());
  BOOST_CHECK(boost::algorithm::equals("asdfkeekefe", t1.getTarget()->getMemberOffset("result13").getVarcharPtr(outputBuf)->Ptr));
  BOOST_CHECK_EQUAL(FieldType::VARCHAR, 
		    t1.getTarget()->getMember("result14").GetType()->GetEnum());
  BOOST_CHECK(boost::algorithm::equals("asdfwefkefe", t1.getTarget()->getMemberOffset("result14").getVarcharPtr(outputBuf)->Ptr));

  BOOST_CHECK_EQUAL(FieldType::INT64, 
		    t1.getTarget()->getMember("result15").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(-8882344LL, t1.getTarget()->getInt64("result15", outputBuf));
  BOOST_CHECK_EQUAL(FieldType::INT64, 
		    t1.getTarget()->getMember("result16").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(88823445323LL, t1.getTarget()->getInt64("result16", outputBuf));
  BOOST_CHECK_EQUAL(FieldType::INT64, 
		    t1.getTarget()->getMember("result17").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(-8882344LL, t1.getTarget()->getInt64("result17", outputBuf));
  BOOST_CHECK_EQUAL(FieldType::INT64, 
		    t1.getTarget()->getMember("result18").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(88823445323LL, t1.getTarget()->getInt64("result18", outputBuf));
}

BOOST_AUTO_TEST_CASE(testIQLNegate)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("x", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("y", Int64Type::Get(ctxt)));
  members.push_back(RecordMember("z", DoubleType::Get(ctxt)));
  members.push_back(RecordMember("w", DecimalType::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"-x AS a"
			",-y AS b"
			",-z AS c"
			",-w AS d"
			);
  
  // Actually execute this thing.  
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("x", 32, inputBuf);
  recordType->setInt64("y", -8, inputBuf);
  recordType->setDouble("z", -8.999, inputBuf);
  ::decimal128FromString(recordType->getMemberOffset("w").getDecimalPtr(inputBuf), 
			 "-56.8234", 
			 runtimeCtxt.getDecimalContext());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  decimal128 expected;
  ::decimal128FromString(&expected, "56.8234", runtimeCtxt.getDecimalContext());
  BOOST_CHECK_EQUAL(-32, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt64("b", outputBuf));
  BOOST_CHECK_EQUAL(8.999, t1.getTarget()->getDouble("c", outputBuf));
  BOOST_CHECK_EQUAL(0, 
		    memcmp(
			   &expected,
			   t1.getTarget()->getMemberOffset("d").getDecimalPtr(outputBuf),
			   16));
}

BOOST_AUTO_TEST_CASE(testIQLNegateNullable)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("x", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("y", Int64Type::Get(ctxt, true)));
  members.push_back(RecordMember("z", DoubleType::Get(ctxt, true)));
  members.push_back(RecordMember("w", DecimalType::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"-x AS a"
			",-y AS b"
			",-z AS c"
			",-w AS d"
			);
  
  // Actually execute this thing.  
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("x", 32, inputBuf);
  recordType->setInt64("y", -8, inputBuf);
  recordType->setDouble("z", -8.999, inputBuf);
  decimal128 input;
  ::decimal128FromString(&input, "-56.8234", runtimeCtxt.getDecimalContext());
  recordType->getMemberOffset("w").setDecimal(input, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  decimal128 expected;
  ::decimal128FromString(&expected, "56.8234", runtimeCtxt.getDecimalContext());
  BOOST_CHECK_EQUAL(-32, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(8, t1.getTarget()->getInt64("b", outputBuf));
  BOOST_CHECK_EQUAL(8.999, t1.getTarget()->getDouble("c", outputBuf));
  BOOST_CHECK_EQUAL(0, 
		    memcmp(
			   &expected,
			   t1.getTarget()->getMemberOffset("d").getDecimalPtr(outputBuf),
			   16));
  t1.getTarget()->getFree().free(outputBuf);

  inputBuf = recordType->GetMalloc()->malloc();
  recordType->getFieldAddress("x").setNull(inputBuf);
  recordType->getFieldAddress("y").setNull(inputBuf);
  recordType->getFieldAddress("z").setNull(inputBuf);
  recordType->getFieldAddress("w").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(t1.getTarget()->getFieldAddress("a").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("c").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLAddNullableChar)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt,32, true)));
  members.push_back(RecordMember("c", CharType::Get(ctxt, 9, true)));
  // members.push_back(RecordMember("a", CharType::Get(ctxt,1, true)));
  // members.push_back(RecordMember("c", CharType::Get(ctxt, 2, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a+c AS b"
			",NULL+c AS d"
			);
  
  // Actually execute this thing.  
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setChar("a", "4381905F306900000161B14DDCDB9911", inputBuf);
  recordType->setChar("c", "123456789", inputBuf);
  BOOST_CHECK_EQUAL(41, t1.getTarget()->begin_members()->GetType()->GetSize());
  BOOST_CHECK_EQUAL(FieldType::CHAR, t1.getTarget()->begin_members()->GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->begin_members()->GetType()->isNullable());
  BOOST_CHECK_EQUAL(9, t1.getTarget()->getMember("d").GetType()->GetSize());
  BOOST_CHECK_EQUAL(FieldType::CHAR, t1.getTarget()->getMember("d").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("d").GetType()->isNullable());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(0, strcmp("4381905F306900000161B14DDCDB9911123456789",t1.getTarget()->getFieldAddress("b").getCharPtr(outputBuf)));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);

  recordType->getFieldAddress("a").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLAddNullableInt32)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a+c AS b"
			",NULL+c AS d"
			);
  
  // Actually execute this thing.  
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 777723, inputBuf);
  recordType->setInt32("c", 2343, inputBuf);
  BOOST_CHECK_EQUAL(FieldType::INT32, t1.getTarget()->begin_members()->GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->begin_members()->GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, t1.getTarget()->getMember("d").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("d").GetType()->isNullable());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(780066, t1.getTarget()->getFieldAddress("b").getInt32(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);

  recordType->getFieldAddress("a").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLAddNullableDecimal)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DecimalType::Get(ctxt, true)));
  members.push_back(RecordMember("c", DecimalType::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a+c AS b"
			",NULL+c AS d"
			);
  
  // Actually execute this thing.  
  InterpreterContext runtimeCtxt;
  decimal128 a,b,c;
  ::decimal128FromString(&a, "99234.333", runtimeCtxt.getDecimalContext());
  ::decimal128FromString(&c, "99.123", runtimeCtxt.getDecimalContext());
  ::decimal128FromString(&b, "99333.456", runtimeCtxt.getDecimalContext());
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->getFieldAddress("a").setDecimal(a, inputBuf);
  recordType->getFieldAddress("c").setDecimal(c, inputBuf);
  BOOST_CHECK_EQUAL(FieldType::BIGDECIMAL, t1.getTarget()->begin_members()->GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->begin_members()->GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::BIGDECIMAL, t1.getTarget()->getMember("d").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("d").GetType()->isNullable());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(0, memcmp(t1.getTarget()->getFieldAddress("b").getDecimalPtr(outputBuf),
			      &b, 16));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);

  recordType->getFieldAddress("a").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLAddNullableDatetime)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DatetimeType::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a + INTERVAL 72 SECOND AS d"
			);
  
  // Actually execute this thing.  
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  boost::posix_time::ptime dt = boost::posix_time::time_from_string("2011-02-17 15:38:33");
  recordType->setDatetime("a", dt, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("2011-02-17 15:39:45"),
		    t1.getTarget()->getFieldAddress("d").getDatetime(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);

  recordType->getFieldAddress("a").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);

  // TODO: Check NULL in the INTERVAL
}

BOOST_AUTO_TEST_CASE(testIQLAddNullableVarchar)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("c", VarcharType::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a+c AS b"
			);
  
  // Actually execute this thing.  
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setVarchar("a", "Mary had a ", inputBuf);
  recordType->setVarchar("c", "little lamb", inputBuf);
  BOOST_CHECK_EQUAL(FieldType::VARCHAR, t1.getTarget()->begin_members()->GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->begin_members()->GetType()->isNullable());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(0, strcmp(t1.getTarget()->getFieldAddress("b").getVarcharPtr(outputBuf)->Ptr,
			      "Mary had a little lamb"));
  t1.getTarget()->GetFree()->free(outputBuf);

  recordType->getFieldAddress("a").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);
}

void testIsNull(bool isNullable)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", VarcharType::Get(ctxt, isNullable)));
  members.push_back(RecordMember("c", VarcharType::Get(ctxt, isNullable)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a IS NULL AS b"
			",c IS NOT NULL AS d"
			);
  
  // Actually execute this thing.  
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setVarchar("a", "Mary had a ", inputBuf);
  recordType->setVarchar("c", "little lamb", inputBuf);
  BOOST_CHECK_EQUAL(FieldType::INT32, t1.getTarget()->getMember("b").GetType()->GetEnum());
  BOOST_CHECK(!t1.getTarget()->getMember("b").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, t1.getTarget()->getMember("d").GetType()->GetEnum());
  BOOST_CHECK(!t1.getTarget()->getMember("d").GetType()->isNullable());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(0, t1.getTarget()->getFieldAddress("b").getInt32(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  BOOST_CHECK_EQUAL(1, t1.getTarget()->getFieldAddress("d").getInt32(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);
  
  if (isNullable) {
    inputBuf = recordType->GetMalloc()->malloc();
    recordType->getFieldAddress("a").setNull(inputBuf);
    recordType->getFieldAddress("c").setNull(inputBuf);
    BOOST_CHECK_EQUAL(FieldType::INT32, t1.getTarget()->getMember("b").GetType()->GetEnum());
    BOOST_CHECK(!t1.getTarget()->getMember("b").GetType()->isNullable());
    BOOST_CHECK_EQUAL(FieldType::INT32, t1.getTarget()->getMember("d").GetType()->GetEnum());
    BOOST_CHECK(!t1.getTarget()->getMember("d").GetType()->isNullable());
    outputBuf = t1.getTarget()->GetMalloc()->malloc();
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
    BOOST_CHECK_EQUAL(1, t1.getTarget()->getFieldAddress("b").getInt32(outputBuf));
    BOOST_CHECK(!t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
    BOOST_CHECK_EQUAL(0, t1.getTarget()->getFieldAddress("d").getInt32(outputBuf));
    t1.getTarget()->GetFree()->free(outputBuf);
  }
}

BOOST_AUTO_TEST_CASE(testIQLIsNull)
{
  testIsNull(false);
}

BOOST_AUTO_TEST_CASE(testIQLIsNullNullable)
{
  testIsNull(true);
}

void testIsNullFunction(bool isNullable1, bool isNullable2)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, isNullable1)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, isNullable2)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  bool resultNullable = isNullable1 && isNullable2;

  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"ISNULL(a,c) AS b"
			);
  
  // Actually execute this thing.  
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 999, inputBuf);
  recordType->setInt32("c", 8234, inputBuf);
  BOOST_CHECK_EQUAL(FieldType::INT32, t1.getTarget()->getMember("b").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(resultNullable,
		    t1.getTarget()->getMember("b").GetType()->isNullable());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(999, t1.getTarget()->getFieldAddress("b").getInt32(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);

  if(isNullable1) {
    recordType->getFieldAddress("a").setNull(inputBuf);
    outputBuf = t1.getTarget()->GetMalloc()->malloc();
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
    BOOST_CHECK_EQUAL(8234, t1.getTarget()->getFieldAddress("b").getInt32(outputBuf));
    t1.getTarget()->GetFree()->free(outputBuf);
  }
  if(isNullable2) {
    recordType->setInt32("a", 999, inputBuf);
    recordType->getFieldAddress("c").setNull(inputBuf);
    outputBuf = t1.getTarget()->GetMalloc()->malloc();
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
    BOOST_CHECK_EQUAL(999, t1.getTarget()->getFieldAddress("b").getInt32(outputBuf));
    t1.getTarget()->GetFree()->free(outputBuf);
  }
  if(isNullable1 && isNullable2) {
    recordType->getFieldAddress("a").setNull(inputBuf);
    recordType->getFieldAddress("c").setNull(inputBuf);
    outputBuf = t1.getTarget()->GetMalloc()->malloc();
    t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
    BOOST_CHECK(t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
    t1.getTarget()->GetFree()->free(outputBuf);
  }
}

BOOST_AUTO_TEST_CASE(testIQLIsNullFunction)
{
  testIsNullFunction(false, false);
  testIsNullFunction(false, true);
  testIsNullFunction(true, false);
  testIsNullFunction(true, true);
}

BOOST_AUTO_TEST_CASE(testIQLIsNullFunctionTypePromotion)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int64Type::Get(ctxt, true)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, false)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"ISNULL(a,c) AS b"
			);
  
  // Actually execute this thing.  
  InterpreterContext runtimeCtxt;
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt64("a", 999LL, inputBuf);
  recordType->setInt32("c", 8234, inputBuf);
  BOOST_CHECK_EQUAL(FieldType::INT64, t1.getTarget()->getMember("b").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(false,
		    t1.getTarget()->getMember("b").GetType()->isNullable());
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(999LL, t1.getTarget()->getFieldAddress("b").getInt64(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);  

  // Check that the actual type promo works
  recordType->getFieldAddress("a").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(8234LL, t1.getTarget()->getFieldAddress("b").getInt64(outputBuf));
  t1.getTarget()->GetFree()->free(outputBuf);  
}

BOOST_AUTO_TEST_CASE(testIQLIsNullFunctionTypePromotionNegative)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("c", DatetimeType::Get(ctxt, false)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  try {
    // Simple Transfer of everything
    RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			  "ISNULL(a,c) AS b"
			  );
    BOOST_CHECK(false);
  } catch (std::exception & ex) {
    std::cout << "Received expected exception: " << ex.what() << "\n";
  }
}

void internalTestTransferOfNullableIntegers(const std::string& xfer)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), xfer);
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("a").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("a").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("c").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("c").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("d").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("d").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("e").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("e").GetType()->isNullable());

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("a").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("c").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK_EQUAL(2553, t1.getTarget()->getInt32("e", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
	      
  recordType->getFieldAddress("c").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("a").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("c").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("d", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferNullableIntegers)
{
  internalTestTransferOfNullableIntegers("a,c, a+b+c AS e,b AS d");
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferNullableIntegersWithNullableLocal)
{
  internalTestTransferOfNullableIntegers("DECLARE tmp=a+b+c,a,c,tmp AS e,b AS d");
}

void testTransferWithAtLeastOneNullable(DynamicRecordContext& ctxt,
					boost::shared_ptr<RecordType> recordType)
{
  // Simple Transfer of everything.  
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"a+b+c AS e, input.*");
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("a").GetType()->GetEnum());
  BOOST_CHECK_EQUAL(recordType->getMember("a").GetType()->isNullable(),
		    t1.getTarget()->getMember("a").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("c").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("c").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("b").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("b").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("e").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("e").GetType()->isNullable());

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("a").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("c").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("b", outputBuf));
  BOOST_CHECK_EQUAL(2553, t1.getTarget()->getInt32("e", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
	      
  recordType->getFieldAddress("c").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("a").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("c").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK(t1.getTarget()->getFieldAddress("e").isNull(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("b", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferAllNullableIntegers)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  testTransferWithAtLeastOneNullable(ctxt, recordType);
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferAllSomeNullableIntegers)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  testTransferWithAtLeastOneNullable(ctxt, recordType);
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransferNullableToNullable)
{
  // This tests a record with nullable fields (hence a bit field)
  // being transfered to a record with no nullable fields.
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt, false)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("d", Int32Type::Get(ctxt, false)));
  members.push_back(RecordMember("e", Int32Type::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  
  // Simple Transfer of everything.  
  RecordTypeTransfer t1(ctxt, "xfer1", recordType.get(), 
			"b,d,b+d AS f");
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("b").GetType()->GetEnum());
  BOOST_CHECK(!t1.getTarget()->getMember("b").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("d").GetType()->GetEnum());
  BOOST_CHECK(!t1.getTarget()->getMember("d").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("f").GetType()->GetEnum());
  BOOST_CHECK(!t1.getTarget()->getMember("f").GetType()->isNullable());

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  recordType->setInt32("d", 21, inputBuf);
  recordType->setInt32("e", 20, inputBuf);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("b", outputBuf));
  BOOST_CHECK_EQUAL(21, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK_EQUAL(251, t1.getTarget()->getInt32("f", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
	      
  recordType->getFieldAddress("a").setNull(inputBuf);
  recordType->getFieldAddress("c").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, outputBuf, &runtimeCtxt, false);
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("b").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("d").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getFieldAddress("f").isNull(outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("b", outputBuf));
  BOOST_CHECK_EQUAL(21, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK_EQUAL(251, t1.getTarget()->getInt32("f", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransfer2IntegersNullable)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  members.clear();
  members.push_back(RecordMember("d", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("e", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("f", Int32Type::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType2(new RecordType(members));
  
  // Simple Transfer of everything.  
  std::vector<AliasedRecordType> types;
  types.push_back(AliasedRecordType("table", recordType.get()));
  types.push_back(AliasedRecordType("probe", recordType2.get()));
  RecordTypeTransfer2 t1(ctxt, "xfer1", types,
			 "table.*, probe.*");
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("a").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("a").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("b").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("b").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("c").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("c").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("d").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("d").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("e").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("e").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("f").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("f").GetType()->isNullable());

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  RecordBuffer inputBuf2 = recordType2->GetMalloc()->malloc();
  recordType2->setInt32("d", 52, inputBuf2);
  recordType2->setInt32("e", 520, inputBuf2);
  recordType2->setInt32("f", 5200, inputBuf2);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, inputBuf2, outputBuf, &runtimeCtxt, false, false);
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("a").isNull(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("b", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("c").isNull(outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("d").isNull(outputBuf));
  BOOST_CHECK_EQUAL(52, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("e").isNull(outputBuf));
  BOOST_CHECK_EQUAL(520, t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("f").isNull(outputBuf));
  BOOST_CHECK_EQUAL(5200, t1.getTarget()->getInt32("f", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);

  recordType2->getMemberOffset("e").setNull(inputBuf2);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, inputBuf2, outputBuf, &runtimeCtxt, false, false);
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("a").isNull(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("b", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("c").isNull(outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("d").isNull(outputBuf));
  BOOST_CHECK_EQUAL(52, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK(t1.getTarget()->getMemberOffset("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("f").isNull(outputBuf));
  BOOST_CHECK_EQUAL(5200, t1.getTarget()->getInt32("f", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);

  recordType->getMemberOffset("b").setNull(inputBuf);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, inputBuf2, outputBuf, &runtimeCtxt, false, false);
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("a").isNull(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK(t1.getTarget()->getMemberOffset("b").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("c").isNull(outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("d").isNull(outputBuf));
  BOOST_CHECK_EQUAL(52, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK(t1.getTarget()->getMemberOffset("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("f").isNull(outputBuf));
  BOOST_CHECK_EQUAL(5200, t1.getTarget()->getInt32("f", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransfer2IntegersNullableAndNot)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("b", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("c", Int32Type::Get(ctxt)));
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  members.clear();
  members.push_back(RecordMember("d", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("e", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("f", Int32Type::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType2(new RecordType(members));
  
  // Simple Transfer of everything.  
  std::vector<AliasedRecordType> types;
  types.push_back(AliasedRecordType("table", recordType.get()));
  types.push_back(AliasedRecordType("probe", recordType2.get()));
  RecordTypeTransfer2 t1(ctxt, "xfer1", types,
			 "table.*, probe.*");
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("a").GetType()->GetEnum());
  BOOST_CHECK(!t1.getTarget()->getMember("a").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("b").GetType()->GetEnum());
  BOOST_CHECK(!t1.getTarget()->getMember("b").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("c").GetType()->GetEnum());
  BOOST_CHECK(!t1.getTarget()->getMember("c").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("d").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("d").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("e").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("e").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("f").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("f").GetType()->isNullable());

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  recordType->setInt32("a", 23, inputBuf);
  recordType->setInt32("b", 230, inputBuf);
  recordType->setInt32("c", 2300, inputBuf);
  RecordBuffer inputBuf2 = recordType2->GetMalloc()->malloc();
  recordType2->setInt32("d", 52, inputBuf2);
  recordType2->setInt32("e", 520, inputBuf2);
  recordType2->setInt32("f", 5200, inputBuf2);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, inputBuf2, outputBuf, &runtimeCtxt, false, false);
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("a").isNull(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("b", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("c").isNull(outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("d").isNull(outputBuf));
  BOOST_CHECK_EQUAL(52, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("e").isNull(outputBuf));
  BOOST_CHECK_EQUAL(520, t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("f").isNull(outputBuf));
  BOOST_CHECK_EQUAL(5200, t1.getTarget()->getInt32("f", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);

  recordType2->getMemberOffset("e").setNull(inputBuf2);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, inputBuf2, outputBuf, &runtimeCtxt, false, false);
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("a").isNull(outputBuf));
  BOOST_CHECK_EQUAL(23, t1.getTarget()->getInt32("a", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("b").isNull(outputBuf));
  BOOST_CHECK_EQUAL(230, t1.getTarget()->getInt32("b", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("c").isNull(outputBuf));
  BOOST_CHECK_EQUAL(2300, t1.getTarget()->getInt32("c", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("d").isNull(outputBuf));
  BOOST_CHECK_EQUAL(52, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK(t1.getTarget()->getMemberOffset("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("f").isNull(outputBuf));
  BOOST_CHECK_EQUAL(5200, t1.getTarget()->getInt32("f", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
}

void internalTestIQLRecordTransfer2IntegersNullableLarge(bool nullability,
							 std::size_t sz)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  for(std::size_t i=0; i<sz; i++) { 
    std::string m = (boost::format("a%1%") % i).str();
    members.push_back(RecordMember(m, Int32Type::Get(ctxt, nullability)));
  }
  boost::shared_ptr<RecordType> recordType(new RecordType(members));
  members.clear();
  members.push_back(RecordMember("d", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("e", Int32Type::Get(ctxt, true)));
  members.push_back(RecordMember("f", Int32Type::Get(ctxt, true)));
  boost::shared_ptr<RecordType> recordType2(new RecordType(members));
  
  // Simple Transfer of everything.  
  std::vector<AliasedRecordType> types;
  types.push_back(AliasedRecordType("table", recordType.get()));
  types.push_back(AliasedRecordType("probe", recordType2.get()));
  RecordTypeTransfer2 t1(ctxt, "xfer1", types,
			 "table.*, probe.*");
  for(std::size_t i=0; i<sz; i++) { 
    std::string m = (boost::format("a%1%") % i).str();
    BOOST_CHECK_EQUAL(FieldType::INT32, 
		      t1.getTarget()->getMember(m).GetType()->GetEnum());
    BOOST_CHECK_EQUAL(nullability,
		      t1.getTarget()->getMember(m).GetType()->isNullable());
  }
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("d").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("d").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("e").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("e").GetType()->isNullable());
  BOOST_CHECK_EQUAL(FieldType::INT32, 
		    t1.getTarget()->getMember("f").GetType()->GetEnum());
  BOOST_CHECK(t1.getTarget()->getMember("f").GetType()->isNullable());

  // Actually execute this thing.
  RecordBuffer inputBuf = recordType->GetMalloc()->malloc();
  for(std::size_t i=0; i<sz; i++) { 
    std::string m = (boost::format("a%1%") % i).str();
    recordType->setInt32(m, 23+i, inputBuf);
  }
  RecordBuffer inputBuf2 = recordType2->GetMalloc()->malloc();
  recordType2->setInt32("d", 52, inputBuf2);
  recordType2->setInt32("e", 520, inputBuf2);
  recordType2->setInt32("f", 5200, inputBuf2);
  RecordBuffer outputBuf = t1.getTarget()->GetMalloc()->malloc();
  InterpreterContext runtimeCtxt;
  t1.execute(inputBuf, inputBuf2, outputBuf, &runtimeCtxt, false, false);
  for(std::size_t i=0; i<sz; i++) { 
    std::string m = (boost::format("a%1%") % i).str();
    BOOST_CHECK(!t1.getTarget()->getMemberOffset(m).isNull(outputBuf));
    BOOST_CHECK_EQUAL(23+(int32_t)i, t1.getTarget()->getInt32(m, outputBuf));
  }
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("d").isNull(outputBuf));
  BOOST_CHECK_EQUAL(52, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("e").isNull(outputBuf));
  BOOST_CHECK_EQUAL(520, t1.getTarget()->getInt32("e", outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("f").isNull(outputBuf));
  BOOST_CHECK_EQUAL(5200, t1.getTarget()->getInt32("f", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);

  recordType2->getMemberOffset("e").setNull(inputBuf2);
  outputBuf = t1.getTarget()->GetMalloc()->malloc();
  t1.execute(inputBuf, inputBuf2, outputBuf, &runtimeCtxt, false, false);
  for(std::size_t i=0; i<sz; i++) { 
    std::string m = (boost::format("a%1%") % i).str();
    BOOST_CHECK(!t1.getTarget()->getMemberOffset(m).isNull(outputBuf));
    BOOST_CHECK_EQUAL(23+(int32_t)i, t1.getTarget()->getInt32(m, outputBuf));
  }
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("d").isNull(outputBuf));
  BOOST_CHECK_EQUAL(52, t1.getTarget()->getInt32("d", outputBuf));
  BOOST_CHECK(t1.getTarget()->getMemberOffset("e").isNull(outputBuf));
  BOOST_CHECK(!t1.getTarget()->getMemberOffset("f").isNull(outputBuf));
  BOOST_CHECK_EQUAL(5200, t1.getTarget()->getInt32("f", outputBuf));
  t1.getTarget()->getFree().free(outputBuf);
}

BOOST_AUTO_TEST_CASE(testIQLRecordTransfer2IntegersNullableLarge)
{
  internalTestIQLRecordTransfer2IntegersNullableLarge(true, 30);
  internalTestIQLRecordTransfer2IntegersNullableLarge(false, 30);
  internalTestIQLRecordTransfer2IntegersNullableLarge(true, 62);
  internalTestIQLRecordTransfer2IntegersNullableLarge(false, 62);
  internalTestIQLRecordTransfer2IntegersNullableLarge(true, 63);
  internalTestIQLRecordTransfer2IntegersNullableLarge(false, 63);
  internalTestIQLRecordTransfer2IntegersNullableLarge(true, 64);
  internalTestIQLRecordTransfer2IntegersNullableLarge(false, 64);
  internalTestIQLRecordTransfer2IntegersNullableLarge(true, 65);
  internalTestIQLRecordTransfer2IntegersNullableLarge(false, 65);
  internalTestIQLRecordTransfer2IntegersNullableLarge(false, 1022);
  internalTestIQLRecordTransfer2IntegersNullableLarge(false, 1023);
  internalTestIQLRecordTransfer2IntegersNullableLarge(false, 1024);
  internalTestIQLRecordTransfer2IntegersNullableLarge(false, 1025);
}


bool iql_rlike_match(std::string regex_source, std::string target) {
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;

  std::vector<RecordMember> lhsMembers;
  lhsMembers.push_back(RecordMember("rs", VarcharType::Get(ctxt)));
  RecordType lhsTy(lhsMembers);
  std::vector<RecordMember> rhsMembers;
  rhsMembers.push_back(RecordMember("t", VarcharType::Get(ctxt)));
  RecordType rhsTy(rhsMembers);

  std::vector<const RecordType*> types;
  types.push_back(&lhsTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = lhsTy.GetMalloc()->malloc();
  lhsTy.setVarchar("rs", regex_source.c_str(), lhs);
  RecordBuffer rhs = rhsTy.GetMalloc()->malloc();
  rhsTy.setVarchar("t", target.c_str(), rhs);

  RecordTypeFunction fun(ctxt, "rlike", types, "t RLIKE rs");
  bool result = fun.execute(lhs, rhs, &runtimeCtxt) != 0;
  lhsTy.GetFree()->free(lhs);
  rhsTy.GetFree()->free(rhs);
  return result;
}

bool iql_rlike_is_null(const char* regex_source, const char* target) {
  DynamicRecordContext ctxt;
  InterpreterContext runtimeCtxt;

  std::vector<RecordMember> lhsMembers;
  lhsMembers.push_back(RecordMember("rs", VarcharType::Get(ctxt, true)));
  RecordType lhsTy(lhsMembers);
  std::vector<RecordMember> rhsMembers;
  rhsMembers.push_back(RecordMember("t", VarcharType::Get(ctxt, true)));
  RecordType rhsTy(rhsMembers);

  std::vector<const RecordType*> types;
  types.push_back(&lhsTy);
  types.push_back(&rhsTy);

  RecordBuffer lhs = lhsTy.GetMalloc()->malloc();
  if (regex_source) {
    lhsTy.setVarchar("rs", regex_source, lhs);
  } else {
    lhsTy.getFieldAddress("rs").setNull(lhs);
  }
  RecordBuffer rhs = rhsTy.GetMalloc()->malloc();
  if (target) {
    rhsTy.setVarchar("t", target, rhs);
  } else {
    rhsTy.getFieldAddress("t").setNull(rhs);
  }

  RecordTypeFunction fun(ctxt, "rlike", types, "(t RLIKE rs) is NULL");
  bool result = fun.execute(lhs, rhs, &runtimeCtxt) != 0;
  lhsTy.GetFree()->free(lhs);
  rhsTy.GetFree()->free(rhs);
  return result;
}

BOOST_AUTO_TEST_CASE(testIQLRLike)
{
  BOOST_CHECK(iql_rlike_match("a+", "a"));
  BOOST_CHECK(!iql_rlike_match("a+", "b"));

  BOOST_CHECK(iql_rlike_match("(?i)a+", "AAA"));

  BOOST_CHECK(!iql_rlike_is_null("a", "a"));
  BOOST_CHECK(iql_rlike_is_null(NULL, "a"));
  BOOST_CHECK(iql_rlike_is_null("a", NULL));
  BOOST_CHECK(iql_rlike_is_null(NULL, NULL));
}


// Important test case with potentially important design
// implications is to test NULLABLE local values.
// Simple case is a NULLABLE in a transfer; bigger deal
// is a NULLABLE in an updatable context: i.e.
// DECLARE tmp INTEGER NULL;
// SET tmp = 12
// SET tmp = NULL
// BOOST_AUTO_TEST_CASE(testIQLNullableLocal)
// {
// }

// NULLABLE arrays?  Probably just protect against them.
// Test setting a non NULLABLE value in a NULLABLE variable

// Test transfer of a record with no NULL bitmap to a record with
// a NULL bitmap.  This requires proper initialization of the target
// bits.

