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

#include <list>
#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <boost/lexical_cast.hpp>

// #include <antlr3defs.h>

// #include "IQLLexer.h"
// #include "IQLParser.h"
// #include "IQLTypeCheck.h"

#include "RuntimePlan.hh"
#include "RuntimeOperator.hh"
#include "Merger.hh"
#include "QueueImport.hh"
#include "ConstantScan.hh"
#include "HdfsOperator.hh"
#include "GraphBuilder.hh"

#include <boost/date_time/posix_time/posix_time.hpp>

#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/export.hpp>

// It appears that one does not need BOOST_CLASS_EXPORT for
// non polymorphic classes.
BOOST_CLASS_EXPORT(AssignedOperatorType);
BOOST_CLASS_EXPORT(ConstrainedOperatorType);
BOOST_CLASS_EXPORT(RuntimeOperatorType);
BOOST_CLASS_EXPORT(RuntimeDevNullOperatorType);
BOOST_CLASS_EXPORT(RuntimeGenerateOperatorType);
BOOST_CLASS_EXPORT(RuntimePrintOperatorType);
BOOST_CLASS_EXPORT(RuntimeHashJoinOperatorType);
BOOST_CLASS_EXPORT(RuntimeCrossJoinOperatorType);
BOOST_CLASS_EXPORT(RuntimeSortMergeJoinOperatorType);
BOOST_CLASS_EXPORT(RuntimeHashGroupByOperatorType);
BOOST_CLASS_EXPORT(RuntimeSortGroupByOperatorType);
BOOST_CLASS_EXPORT(RuntimeSortRunningTotalOperatorType);
BOOST_CLASS_EXPORT(RuntimeHashPartitionerOperatorType);
BOOST_CLASS_EXPORT(RuntimeBroadcastPartitionerOperatorType);
BOOST_CLASS_EXPORT(RuntimeNondeterministicCollectorOperatorType);
BOOST_CLASS_EXPORT(RuntimeCopyOperatorType);
BOOST_CLASS_EXPORT(RuntimeFilterOperatorType);
BOOST_CLASS_EXPORT(RuntimeSortMergeOperatorType);
BOOST_CLASS_EXPORT(RuntimeSortOperatorType);
BOOST_CLASS_EXPORT(RuntimeHadoopEmitOperatorType);
BOOST_CLASS_EXPORT(RuntimeHdfsWriteOperatorType);
BOOST_CLASS_EXPORT(RuntimeWriteOperatorType);
BOOST_CLASS_EXPORT(RuntimeUnionAllOperatorType);
BOOST_CLASS_EXPORT(NativeInputQueueOperatorType);
BOOST_CLASS_EXPORT(InternalFileParserOperatorType<AsyncDoubleBufferStream<AsyncFileTraits<stdio_file_traits> > >);
BOOST_CLASS_EXPORT(InternalFileWriteOperatorType);
BOOST_CLASS_EXPORT(GenericParserOperatorType<ExplicitChunkStrategy>);
BOOST_CLASS_EXPORT(GenericParserOperatorType<SerialChunkStrategy>);
BOOST_CLASS_EXPORT(RuntimeConstantScanOperatorType);

/* 
   base64.cpp and base64.h

   Copyright (C) 2004-2008 René Nyffenegger

   This source code is provided 'as-is', without any express or implied
   warranty. In no event will the author be held liable for any damages
   arising from the use of this software.

   Permission is granted to anyone to use this software for any purpose,
   including commercial applications, and to alter it and redistribute it
   freely, subject to the following restrictions:

   1. The origin of this source code must not be misrepresented; you must not
      claim that you wrote the original source code. If you use this source code
      in a product, an acknowledgment in the product documentation would be
      appreciated but is not required.

   2. Altered source versions must be plainly marked as such, and must not be
      misrepresented as being the original source code.

   3. This notice may not be removed or altered from any source distribution.

   René Nyffenegger rene.nyffenegger@adp-gmbh.ch

*/

/**
 * Commented out header file
 */

//#include "base64.h"
//#include <iostream>

static const std::string base64_chars = 
             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
             "abcdefghijklmnopqrstuvwxyz"
             "0123456789+/";


static inline bool is_base64(unsigned char c) {
  return (isalnum(c) || (c == '+') || (c == '/'));
}

std::string base64_encode(unsigned char const* bytes_to_encode, unsigned int in_len) {
  std::string ret;
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];

  while (in_len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for(i = 0; (i <4) ; i++)
        ret += base64_chars[char_array_4[i]];
      i = 0;
    }
  }

  if (i)
  {
    for(j = i; j < 3; j++)
      char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; (j < i + 1); j++)
      ret += base64_chars[char_array_4[j]];

    while((i++ < 3))
      ret += '=';

  }

  return ret;

}

std::string base64_decode(std::string const& encoded_string) {
  int in_len = encoded_string.size();
  int i = 0;
  int j = 0;
  int in_ = 0;
  unsigned char char_array_4[4], char_array_3[3];
  std::string ret;

  while (in_len-- && ( encoded_string[in_] != '=') && is_base64(encoded_string[in_])) {
    char_array_4[i++] = encoded_string[in_]; in_++;
    if (i ==4) {
      for (i = 0; i <4; i++)
        char_array_4[i] = base64_chars.find(char_array_4[i]);

      char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
      char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
      char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

      for (i = 0; (i < 3); i++)
        ret += char_array_3[i];
      i = 0;
    }
  }

  if (i) {
    for (j = i; j <4; j++)
      char_array_4[j] = 0;

    for (j = 0; j <4; j++)
      char_array_4[j] = base64_chars.find(char_array_4[j]);

    char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
    char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
    char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

    for (j = 0; (j < i - 1); j++) ret += char_array_3[j];
  }

  return ret;
}

std::string PlanGenerator::serialize(boost::shared_ptr<RuntimeOperatorPlan> plan)
{
  std::ostringstream s(std::ios_base::binary | std::ios_base::out);
  boost::archive::binary_oarchive oa(s);      
  RuntimeOperatorPlan * tmp = plan.get();
  oa << BOOST_SERIALIZATION_NVP(tmp);
  
  return s.str();
}

boost::shared_ptr<RuntimeOperatorPlan> PlanGenerator::deserialize(const char * buf, std::size_t sz)
{
  boost::iostreams::stream<boost::iostreams::array_source> archiveStream(boost::iostreams::array_source(buf,sz));
  boost::archive::binary_iarchive ia(archiveStream);
  RuntimeOperatorPlan * tmp=NULL;
  ia >> BOOST_SERIALIZATION_NVP(tmp);
  return boost::shared_ptr<RuntimeOperatorPlan>(tmp);
}

std::string PlanGenerator::serialize64(boost::shared_ptr<RuntimeOperatorPlan> plan)
{
  namespace io = boost::iostreams;

  std::string compressedBuf;
  {
    // put ostream in a scope so its d'tor
    // is executed; that flushes everything
    // through the zlib filter.
    io::filtering_ostream out;
    out.push(io::zlib_compressor());
    out.push(io::back_inserter(compressedBuf));
    boost::archive::binary_oarchive oa(out);      
    RuntimeOperatorPlan * tmp = plan.get();
    oa << BOOST_SERIALIZATION_NVP(tmp);
    out.flush();
  }
  return base64_encode((const unsigned char *) &compressedBuf[0], 
		       compressedBuf.size());
}

boost::shared_ptr<RuntimeOperatorPlan> PlanGenerator::deserialize64(const char * buf, 
								    std::size_t sz)
{
  namespace io = boost::iostreams;
  std::string encoded(buf, sz);
  std::string decoded = base64_decode(encoded);

  io::filtering_istream in;
  in.push(io::zlib_decompressor());
  in.push(boost::iostreams::array_source(&decoded[0],decoded.size()));
  boost::archive::binary_iarchive ia(in);
  RuntimeOperatorPlan * tmp=NULL;
  ia >> BOOST_SERIALIZATION_NVP(tmp);
  return boost::shared_ptr<RuntimeOperatorPlan>(tmp);
}

DataflowGraphBuilder::DataflowGraphBuilder()
  :
  mPlan(new LogicalPlan()),
  mCurrentOp(NULL)
{
}

DataflowGraphBuilder::~DataflowGraphBuilder()
{
  delete mPlan;
}

void DataflowGraphBuilder::buildGraph(const std::string& str)
{
  IQLGraphBuilder::buildGraph(str, false);
}

void DataflowGraphBuilder::buildGraphFromFile(const std::string& str)
{
  if (str == "-") {
    std::stringstream ss;
    std::copy(std::istreambuf_iterator<char>(std::cin),
	      std::istreambuf_iterator<char>(),
	      std::ostreambuf_iterator<char>(ss));
    std::string tmp = ss.str();
    IQLGraphBuilder::buildGraph(tmp, false);
  } else {
    IQLGraphBuilder::buildGraph(str, true);
  }
}

void DataflowGraphBuilder::nodeStart(const char * type, 
				const char * name)
{
  LogicalOperatorFactory & f(LogicalOperatorFactory::get());
  // Case sensitive or insensitive?
  if (mOps.find(name) != mOps.end()) {
    throw std::runtime_error((boost::format("Operator with name %1% already defined") % name).str());
  }
  if (boost::algorithm::iequals("copy", type)) {
    mCurrentOp = new CopyOp();
  } else if (boost::algorithm::iequals("emit", type)) {
    mCurrentOp = new LogicalEmit();
  } else if (boost::algorithm::iequals("filter", type)) {
    mCurrentOp = new LogicalFilter();
  } else if (boost::algorithm::iequals("generate", type)) {
    mCurrentOp = new LogicalGenerate();
  } else if (boost::algorithm::iequals("group_by", type)) {
    mCurrentOp = new LogicalGroupBy(LogicalGroupBy::HYBRID);
  } else if (boost::algorithm::iequals("hash_group_by", type)) {
    mCurrentOp = new LogicalGroupBy(LogicalGroupBy::HASH);
  } else if (boost::algorithm::iequals("hash_join", type)) {
    mCurrentOp = new HashJoin(HashJoin::INNER);
  } else if (boost::algorithm::iequals("hash_right_anti_semi_join", type)) {
    mCurrentOp = new HashJoin(HashJoin::RIGHT_ANTI_SEMI);
  } else if (boost::algorithm::iequals("hash_right_outer_join", type)) {
    mCurrentOp = new HashJoin(HashJoin::RIGHT_OUTER);
  } else if (boost::algorithm::iequals("hash_right_semi_join", type)) {
    mCurrentOp = new HashJoin(HashJoin::RIGHT_SEMI);
  } else if (boost::algorithm::iequals("map", type)) {
    mCurrentOp = new LogicalInputQueue();
  } else if (boost::algorithm::iequals("merge_join", type)) {
    mCurrentOp = new SortMergeJoin(SortMergeJoin::INNER);
  } else if (boost::algorithm::iequals("merge_right_anti_semi_join", type)) {
    mCurrentOp = new SortMergeJoin(SortMergeJoin::RIGHT_ANTI_SEMI);
  } else if (boost::algorithm::iequals("merge_right_outer_join", type)) {
    mCurrentOp = new SortMergeJoin(SortMergeJoin::RIGHT_OUTER);
  } else if (boost::algorithm::iequals("merge_right_semi_join", type)) {
    mCurrentOp = new SortMergeJoin(SortMergeJoin::RIGHT_SEMI);
  } else if (boost::algorithm::iequals("read", type)) {
    mCurrentOp = new LogicalFileRead();
  } else if (boost::algorithm::iequals("reduce", type)) {
    mCurrentOp = new LogicalInputQueue();
  } else if (boost::algorithm::iequals("print", type)) {
    mCurrentOp = new LogicalPrint();
  } else if (boost::algorithm::iequals("sort", type)) {
    mCurrentOp = new LogicalSort();
  } else if (boost::algorithm::iequals("sort_group_by", type)) {
    mCurrentOp = new LogicalGroupBy(LogicalGroupBy::SORT);
  } else if (boost::algorithm::iequals("sort_merge", type)) {
    mCurrentOp = new LogicalSortMerge();
  } else if (boost::algorithm::iequals("union_all", type)) {
    mCurrentOp = new LogicalUnionAll();
  } else if (boost::algorithm::iequals("unpivot", type)) {
    mCurrentOp = new LogicalUnpivot();
  } else if (boost::algorithm::iequals("write", type)) {
    mCurrentOp = new LogicalFileWrite();
  } else if (boost::algorithm::iequals("devNull", type)) {
    mCurrentOp = new LogicalDevNull();
  } else {
    mCurrentOp = f.create(type);
  }
  mOps[name] = mCurrentOp;
  mCurrentOp->setName(name);
  mPlan->addOperator(mCurrentOp);
}

void DataflowGraphBuilder::nodeComplete()
{
  mCurrentOp = NULL;
}

void DataflowGraphBuilder::nodeAddIntegerParam(const char * name,
					  const char * val)
{
  mCurrentOp->addParam(name, LogicalOperator::param_type(boost::lexical_cast<int32_t>(val)));
}

void DataflowGraphBuilder::nodeAddStringParam(const char * name,
					 const char * val)
{
  // TODO: Properly unquotify.
  std::string strVal(val);
  strVal = strVal.substr(1, strVal.size()-2);
  mCurrentOp->addParam(name, LogicalOperator::param_type(strVal));
}

void DataflowGraphBuilder::edgeBuild(const char * from,
				const char * to)
{
  if (mOps.find(from) == mOps.end()) {
    throw std::runtime_error((boost::format("Operator %1% not defined") % from).str());
  }
  if (mOps.find(to) == mOps.end()) {
    throw std::runtime_error((boost::format("Operator %1% not defined") % to).str());
  }

  mPlan->addEdge(mOps.find(from)->second, 
		 mOps.find(to)->second);
}

boost::shared_ptr<RuntimeOperatorPlan> DataflowGraphBuilder::create(class DynamicRecordContext& ctxt,
							       int32_t numPartitions)
{
  mPlan->check();

  // If everything is OK we can apply rules to create
  // operator types.
  RuntimePlanBuilder bld;
  for(std::vector<LogicalPlan::vertex_descriptor>::iterator it = mPlan->begin_operators();
      it != mPlan->end_operators();
      ++it) {
    (*it)->create(bld);
  }
  
  boost::shared_ptr<RuntimeOperatorPlan> plan =  
    boost::make_shared<RuntimeOperatorPlan>(numPartitions, true); 
  // Stitch together the generated operators into
  // the final graph.  Add all operator types and
  // then for each logical edge, connect the corresponding
  // operator type ports.
  for(RuntimePlanBuilder::optype_iterator it = bld.begin_operator_types();
      it != bld.end_operator_types();
      ++it) {
    plan->addOperatorType(*it);
  }
  for(LogicalPlan::edge_iterator it = mPlan->begin_edges();
      it != mPlan->end_edges();
      ++it) {
    std::pair<RuntimeOperatorType*, std::size_t> s =
      bld.mapOutputPort((*it)->source(), (*it)->getSourcePort());
    std::pair<RuntimeOperatorType*, std::size_t> t =
      bld.mapInputPort((*it)->target(), (*it)->getTargetPort());

    plan->connectStraight(s.first, s.second, t.first, t.second, true, true);
  }
  for(RuntimePlanBuilder::internal_edge_iterator it = bld.begin_internal_edges();
      it != bld.end_internal_edges();
      ++it) {
    plan->connectStraight(it->Source.OpType, it->Source.Index, 
			  it->Target.OpType, it->Target.Index, 
			  it->Buffered, true);
  }

  return plan;
}


