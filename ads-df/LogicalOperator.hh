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

#ifndef __LOGICAL_OPERATOR_HH
#define __LOGICAL_OPERATOR_HH

#include <string>
#include <vector>
#include <boost/utility.hpp>
#include <boost/variant.hpp>
#include <boost/graph/graph_traits.hpp>
#include "RecordType.hh"

class LogicalOperator;
class LogicalOperatorParam;
class LogicalFifo;
class LogicalPlan;
class RuntimePlanBuilder;
class RecordType;

namespace boost {
  class mutex;
}

class SortKey
{
public:
  enum Order { ASC, DESC };
private:
  std::string mName;
  Order mOrder;
public:
  SortKey(const std::string& sortKeySpec);
  SortKey(const std::string& sortKeySpec,
	  Order o);
  ~SortKey();
  const std::string& getName() const;
  Order getOrder() const;
};

class PlanCheckContext : public DynamicRecordContext
{
public:
  /**
   * LogicalOperators should call this when they 
   * see an error that prevents them from code generating
   * but the operator can provide some information
   * that allows checking to proceed.
   */
  void logError(const LogicalOperator& op,
		const std::string& msg);
  void logError(const LogicalOperator& op,
		const LogicalOperatorParam& param,
		const std::string& msg);
  /**
   * LogicalOperators should call this when they 
   * see an error that prevents checking from proceeding.
   */
  void logFatal(const LogicalOperator& op,
		const std::string& msg);
};

class LogicalOperatorParam
{
public:
  typedef boost::variant<int32_t, std::string> param_type;
  std::string Name;
  param_type Value;
  LogicalOperatorParam()
  {
  }
  LogicalOperatorParam(const std::string& name,
		       const param_type& value)
    :
    Name(name),
    Value(value)
  {
  }

  bool equals(const std::string& name) const;
};

class LogicalOperator
{
public:
  typedef LogicalOperatorParam::param_type param_type;

private:
  std::string mName;
  std::vector<LogicalFifo*> mInputs;
  std::vector<LogicalFifo*> mOutputs;
  std::vector<LogicalOperatorParam> mParams;
  uint32_t mMinInputs;
  uint32_t mMaxInputs;
  uint32_t mMinOutputs;
  uint32_t mMaxOutputs;
protected:
  typedef std::vector<LogicalOperatorParam>::const_iterator const_param_iterator;
  const_param_iterator begin_params() const
  {
    return mParams.begin();
  }
  const_param_iterator end_params() const 
  {
    return mParams.end();
  }

  LogicalFifo* getInput(std::size_t i);
  const LogicalFifo* getInput(std::size_t i) const;
  LogicalFifo* getOutput(std::size_t i);
  const LogicalFifo* getOutput(std::size_t i) const;

  // Parameter handling helpers
  int32_t getInt32Value(PlanCheckContext& log,
			const LogicalOperatorParam& p);  
  std::string getStringValue(PlanCheckContext& log,
			     const LogicalOperatorParam& p);  
  SortKey getSortKeyValue(PlanCheckContext& log,
			  const LogicalOperatorParam& p);  
  bool getBooleanValue(PlanCheckContext& log,
		       const LogicalOperatorParam& p);  
  void checkDefaultParam(const LogicalOperatorParam& p);

  // Type checking helpers
  /**
   * Checks that each of the fields exists in the specified
   * input and makes sure each field is of sortable type.
   */

  void checkFieldsExist(PlanCheckContext& log,
			const std::vector<SortKey>& fields,
			std::size_t i);
  /**
   * Checks that each of the fields exists in the specified
   * input.
   */
  void checkFieldsExist(PlanCheckContext& log,
			const std::vector<std::string>& fields,
			std::size_t i);
  void checkIdenticalInput(PlanCheckContext& log, 
			   std::size_t i,
			   std::size_t j);
  void checkIdenticalInputs(PlanCheckContext& log);
public:
  LogicalOperator();
  LogicalOperator(uint32_t minInputs, uint32_t maxInputs,
		  uint32_t minOutputs, uint32_t maxOutputs);
  virtual ~LogicalOperator () {}
  /// @brief Check that the operator has appropriate connections
  void checkGraph(PlanCheckContext& log);
  virtual void check(PlanCheckContext& log) =0;
  virtual void create(class RuntimePlanBuilder& plan) =0;
  void setName(const std::string& name) 
  {
    mName = name;
  }
  const std::string& getName() const
  {
    return mName;
  }
  void addParam(const std::string& name,
		const param_type& value)
  {
    mParams.push_back(LogicalOperatorParam(name, value));
  }
  void addInput(LogicalFifo * fifo)
  {
    mInputs.push_back(fifo);
  }
  void addOutput(LogicalFifo * fifo)
  {
    mOutputs.push_back(fifo);
  }

  std::vector<LogicalFifo*>::iterator begin_inputs()
  {
    return mInputs.begin();
  }
  std::vector<LogicalFifo*>::iterator end_inputs()
  {
    return mInputs.end();
  }
  std::size_t size_inputs() const 
  {
    return mInputs.size();
  }
  std::vector<LogicalFifo*>::iterator begin_outputs()
  {
    return mOutputs.begin();
  }
  std::vector<LogicalFifo*>::iterator end_outputs()
  {
    return mOutputs.end();
  }
  std::size_t size_outputs() const 
  {
    return mOutputs.size();
  }
};

class LogicalFifo
{
private:
  LogicalOperator* mSource;
  LogicalOperator* mTarget;
  std::size_t mSourcePort;
  std::size_t mTargetPort;
  const RecordType * mType;
public:
  LogicalFifo(LogicalOperator * source,
	      std::size_t sourcePort,
	      LogicalOperator * target,
	      std::size_t targetPort)
    :
    mSource(source),
    mTarget(target),
    mSourcePort(sourcePort),
    mTargetPort(targetPort),
    mType(NULL)
  {
  }
  LogicalOperator* source() 
  {
    return mSource;
  }
  LogicalOperator* target()
  {
    return mTarget;
  }
  std::size_t getSourcePort() const
  {
    return mSourcePort;
  }
  std::size_t getTargetPort() const
  {
    return mTargetPort;
  }
  const RecordType * getRecordType() const
  {
    return mType;
  }
  void setRecordType(const RecordType * ty)
  {
    mType = ty;
  }
};

struct incidence_vertex_list_graph_tag : public virtual boost::incidence_graph_tag, 
					 public virtual boost::vertex_list_graph_tag
{
};

class LogicalPlan
{
private:
  std::vector<LogicalOperator*> mOperators;
  std::vector<LogicalFifo *> mFifos;
  PlanCheckContext mContext;
public:
  typedef std::vector<LogicalOperator*>::const_iterator const_operator_iterator;
  typedef std::vector<LogicalOperator*>::iterator operator_iterator;

  typedef LogicalOperator* vertex_descriptor;
  typedef LogicalFifo* edge_descriptor;
  typedef boost::directed_tag directed_category;
  typedef boost::disallow_parallel_edge_tag edge_parallel_category;
  typedef incidence_vertex_list_graph_tag traversal_category;
  typedef std::vector<vertex_descriptor>::iterator vertex_iterator;
  typedef std::vector<vertex_descriptor>::size_type vertices_size_type;
  typedef std::vector<edge_descriptor>::iterator edge_iterator;
  typedef std::vector<edge_descriptor>::iterator in_edge_iterator;
  typedef std::vector<edge_descriptor>::iterator out_edge_iterator;
  typedef std::vector<edge_descriptor>::size_type edges_size_type;
  typedef std::vector<edge_descriptor>::size_type degree_size_type;
  typedef void adjacency_iterator;

  LogicalPlan();
  ~LogicalPlan();

  std::size_t numOperators() const
  {
    return mOperators.size();
  }
  const_operator_iterator begin_operators() const
  {
    return mOperators.begin();
  }
  const_operator_iterator end_operators() const
  {
    return mOperators.end();
  }
  operator_iterator begin_operators() 
  {
    return mOperators.begin();
  }
  operator_iterator end_operators() 
  {
    return mOperators.end();
  }
  edge_iterator begin_edges() 
  {
    return mFifos.begin();
  }
  edge_iterator end_edges() 
  {
    return mFifos.end();
  }
  void addOperator(vertex_descriptor op)
  {
    mOperators.push_back(op);
  }
  void addEdge(vertex_descriptor source,
		vertex_descriptor target)
  {
    LogicalFifo * edge = new LogicalFifo(source, source->size_outputs(),
					 target, target->size_inputs());
    target->addInput(edge);
    source->addOutput(edge);
    mFifos.push_back(edge);
  }
  void check();

  /**
   * Get all operators of a particular type.
   */
  template <class _OpType>
  void getOperatorOfType(std::vector<_OpType*>& ret) {
    for(operator_iterator it = begin_operators();
	it != end_operators();
	++it) {
      _OpType * tmp = dynamic_cast<_OpType *>(*it);
      if (tmp != NULL)
	ret.push_back(tmp);
    }
  }
};

/**
 * Wrappers to adapt the LogicalPlan to the Boost Graph Library.
 * The function vertices has been placed at global scope because 
 * the concept check doesn't pass if it is in the Boost namespace.
 * I don't know why this is true and got tired of trying to figure out why.
 */
inline std::pair<LogicalPlan::vertex_iterator, LogicalPlan::vertex_iterator> vertices(const LogicalPlan& p) 
{ 
  LogicalPlan & bglHack(const_cast<LogicalPlan&>(p));
  return std::make_pair(bglHack.begin_operators(), bglHack.end_operators());
}
namespace boost
{
  inline LogicalPlan::vertices_size_type num_vertices(const LogicalPlan& p) 
  { 
    return p.numOperators(); 
  }
  inline LogicalPlan::vertex_descriptor null_vertex() 
  { 
    return NULL; 
  }
  inline LogicalPlan::vertex_descriptor source(LogicalPlan::edge_descriptor e, const LogicalPlan& p)
  {
    return e->source();
  }
  inline LogicalPlan::vertex_descriptor target(LogicalPlan::edge_descriptor e, const LogicalPlan& p)
  {
    return e->target();
  }
  inline std::pair<LogicalPlan::out_edge_iterator,LogicalPlan::out_edge_iterator> out_edges(LogicalPlan::vertex_descriptor v, const LogicalPlan& p)
  {
    return std::make_pair(v->begin_outputs(), v->end_outputs());
  }
  inline LogicalPlan::degree_size_type out_degree(LogicalPlan::vertex_descriptor v, const LogicalPlan& p)
  {
    return v->size_outputs();
  }

}

/**
 * Factory for creation of LogicalOperators.  Libraries of operators should register
 * with the singleton instance of this class.
 */
class LogicalOperatorFactory : public boost::noncopyable
{
public:
  typedef class LogicalOperator * (*CreateLogicalOperatorFn) (const std::string& opType);
private:
  boost::mutex * mGuard;
  std::map<std::string, CreateLogicalOperatorFn> mCreators;
  LogicalOperatorFactory();
  ~LogicalOperatorFactory();
public:
  static LogicalOperatorFactory& get();
  void registerCreator(const std::string& opType, 
		       CreateLogicalOperatorFn creator);
  class LogicalOperator * create(const std::string& opType);
};

#endif
