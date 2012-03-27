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

#ifndef __GRAPHBUILDER_HH__
#define __GRAPHBUILDER_HH__

#include <boost/shared_ptr.hpp>
#include <boost/variant.hpp>
#include <map>

#include "IQLGraphBuilder.hh"

class LogicalFifo;
class LogicalOperator;
class LogicalPlan;
class RuntimeOperatorPlan;

class PlanGenerator
{
public:
  /**
   * Serialize the plan to a binary/base256 format.
   */
  static std::string serialize(boost::shared_ptr<RuntimeOperatorPlan> p);
  static boost::shared_ptr<RuntimeOperatorPlan> deserialize(const char * p,
							    std::size_t sz);
  /**
   * Serialize the plan to a base64 encoded format.
   */
  static std::string serialize64(boost::shared_ptr<RuntimeOperatorPlan> p);
  static boost::shared_ptr<RuntimeOperatorPlan> deserialize64(const char * p,
							      std::size_t sz);
  virtual ~PlanGenerator() {}
  virtual boost::shared_ptr<RuntimeOperatorPlan> create(class DynamicRecordContext& ctxt,
							int32_t numPartitions) =0;
};

/**
 * Dataflow graph building.  Not intended for production use at this point,
 * but very convenient during development/testing.
 */
class DataflowGraphBuilder : public PlanGenerator, public IQLGraphBuilder
{
private:
  std::map<std::string, LogicalOperator*> mOps;
  LogicalPlan * mPlan;
  LogicalOperator * mCurrentOp;
public:
  DataflowGraphBuilder();
  ~DataflowGraphBuilder();
  
  void buildGraph(const std::string& str);
  void buildGraphFromFile(const std::string& str);
  void nodeStart(const char * type, 
		 const char * name);
  void nodeComplete();
  void nodeAddIntegerParam(const char * name,
			   const char * val);
  void nodeAddStringParam(const char * name,
			  const char * val);
  void edgeBuild(const char * from,
		 const char * to);

  /**
   * The underlying logical plan.
   */
  LogicalPlan& getPlan()
  {
    return *mPlan;
  }

  // Create the runtime plan from the graph
  boost::shared_ptr<RuntimeOperatorPlan> create(class DynamicRecordContext& ctxt,
						int32_t numPartitions);
};


#endif
