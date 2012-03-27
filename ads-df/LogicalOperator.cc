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

#include <stdexcept>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/graph/graph_concepts.hpp>
#include <boost/graph/topological_sort.hpp>
#include <boost/property_map/property_map.hpp>
#include <boost/regex.hpp>
#include <boost/thread.hpp>

#include "LogicalOperator.hh"

SortKey::SortKey(const std::string& sortKeySpec)
  :
  mOrder(ASC)
{
  boost::regex e("^\\s*([a-zA-Z_][a-zA-Z0-9_]*)(\\s+(ASC|DESC))?\\s*$",
		 boost::regex_constants::icase);
  boost::cmatch m;
  if (boost::regex_match(sortKeySpec.c_str(), m, e)) {
    BOOST_ASSERT(m.size() >= 2);
    std::string str(m[1].first, m[1].second);
    mName = str;
    if (m.size() > 2) {
      BOOST_ASSERT(m.size() == 4);
      str = std::string(m[3].first, m[3].second);
      if (boost::algorithm::iequals(str, "desc")) {
	mOrder = DESC;
      }
    }
  } else {
    throw std::runtime_error((boost::format("Invalid sort key specification "
					    "%1%") % sortKeySpec).str());
  }
}

SortKey::SortKey(const std::string& name,
		 SortKey::Order o)
  :
  mName(name),
  mOrder(o)
{
}

SortKey::~SortKey()
{
}

const std::string& SortKey::getName() const
{
  return mName;
}

SortKey::Order SortKey::getOrder() const
{
  return mOrder;
}

void PlanCheckContext::logError(const LogicalOperator& op,
				const std::string& msg)
{
  // TODO: Queue these up.
  throw std::runtime_error((boost::format("In operator %1%: %2%") %
			    op.getName() %
			    msg).str());
}

void PlanCheckContext::logError(const LogicalOperator& op,
				const LogicalOperatorParam& param,
				const std::string& msg)
{
  // TODO: Queue these up.
  throw std::runtime_error((boost::format("In operator %1%, argument %2%: %3%") %
			    op.getName() %
			    param.Name %
			    msg).str());
}

void PlanCheckContext::logFatal(const LogicalOperator& op,
				const std::string& msg)
{
  // Can't recover from this, so abort.
  throw std::runtime_error((boost::format("In operator %1%: %2%") %
			    op.getName() %
			    msg).str());
}

bool LogicalOperatorParam::equals(const std::string& name) const
{
  return boost::algorithm::iequals(Name,name);
}

LogicalOperator::LogicalOperator()
:
  mMinInputs(0),
  mMaxInputs(std::numeric_limits<uint32_t>::max()),
  mMinOutputs(0),
  mMaxOutputs(std::numeric_limits<uint32_t>::max())
{
}

LogicalOperator::LogicalOperator(uint32_t minInputs, uint32_t maxInputs,
				 uint32_t minOutputs, uint32_t maxOutputs)
  :
  mMinInputs(minInputs),
  mMaxInputs(maxInputs),
  mMinOutputs(minOutputs),
  mMaxOutputs(maxOutputs)
{
}

LogicalFifo* LogicalOperator::getInput(std::size_t i)
{
  if (i >= mInputs.size()) {
    throw std::runtime_error((boost::format("Unconnected input port %1% on operator %2%") %
			      i %
			      mName).str());
  }
  return mInputs[i];
}

const LogicalFifo* LogicalOperator::getInput(std::size_t i) const
{
  if (i >= mInputs.size()) {
    throw std::runtime_error((boost::format("Unconnected input port %1% on operator %2%") %
			      i %
			      mName).str());
  }
  return mInputs[i];
}

LogicalFifo* LogicalOperator::getOutput(std::size_t i)
{
  if (i >= mOutputs.size()) {
    throw std::runtime_error((boost::format("Unconnected output port %1% on operator %2%") %
			      i %
			      mName).str());
  }
  return mOutputs[i];
}

const LogicalFifo* LogicalOperator::getOutput(std::size_t i) const
{
  if (i >= mOutputs.size()) {
    throw std::runtime_error((boost::format("Unconnected output port %1% on operator %2%") %
			      i %
			      mName).str());
  }
  return mOutputs[i];
}

int32_t LogicalOperator::getInt32Value(PlanCheckContext& log,
				       const LogicalOperatorParam& p)
{
  try {
    return boost::get<int32_t>(p.Value);
  } catch(boost::bad_get& e) {
    log.logError(*this, p, "Expected integer argument type");
  }
  return 0;
}

std::string LogicalOperator::getStringValue(PlanCheckContext& log,
					    const LogicalOperatorParam& p)
{
  try {
    const std::string & s(boost::get<std::string>(p.Value));
    return s;
  } catch(boost::bad_get& e) {
    log.logError(*this, p, "Expected string argument type");
  }
  return "bad string argument";
}

SortKey LogicalOperator::getSortKeyValue(PlanCheckContext& log,
					 const LogicalOperatorParam& p)
{
  try {
    const std::string & s(boost::get<std::string>(p.Value));
    try {
      return SortKey(s);
    } catch(std::exception& ex) {
      log.logError(*this, p, (boost::format("Invalid sort key: %1%") %
			      s).str());
    }
  } catch(boost::bad_get& e) {
    log.logError(*this, p, "Expected sort key argument type");
  }
  return SortKey("bad key argument", SortKey::ASC);
}

bool LogicalOperator::getBooleanValue(PlanCheckContext& log,
				      const LogicalOperatorParam& p)
{
  try {
    const std::string & s(boost::get<std::string>(p.Value));
    return boost::algorithm::iequals(s, "true");
  } catch(boost::bad_get& e) {
    log.logError(*this, p, "Expected boolean argument type with "
		 "value of \"true\" or \"false\"");
  }
  return false;
}

void LogicalOperator::checkDefaultParam(const LogicalOperatorParam& p)
{
  // No defaults in place yet
  throw std::runtime_error((boost::format("Unknown argument %1% on operator %2%") %
			    p.Name %
			    mName).str());
}

void LogicalOperator::checkFieldsExist(PlanCheckContext& ctxt,
				       const std::vector<SortKey>& fields,
				       std::size_t i)
{
  // TODO: We may have non sortable types at some point.
  const RecordType * input = getInput(i)->getRecordType();
  for(std::vector<SortKey>::const_iterator it = fields.begin();
      it != fields.end();
      ++it) {
    if (!input->hasMember(it->getName())) {
      ctxt.logError(*this, (boost::format("Missing sort key "
					  "%1% on input %2%\n%3%") % 
			    it->getName() % i % input->dumpTextFormat()).str());
    }
  }
}

void LogicalOperator::checkFieldsExist(PlanCheckContext& ctxt,
				       const std::vector<std::string>& fields,
				       std::size_t i)
{
  const RecordType * input = getInput(i)->getRecordType();
  for(std::vector<std::string>::const_iterator it = fields.begin();
      it != fields.end();
      ++it) {
    if (!input->hasMember(*it)) {
      ctxt.logError(*this, (boost::format("Missing field "
					  "%1% on input %2%\n%3%") % 
			    *it % i % input->dumpTextFormat()).str());
    }
  }
}

void LogicalOperator::checkIdenticalInput(PlanCheckContext& log,
					  std::size_t i,
					  std::size_t j)
{
  const RecordType * ty1 = getInput(i)->getRecordType();
  const RecordType * ty2 = getInput(j)->getRecordType();
  if (ty1->size() != ty2->size()) {
    log.logError(*this, (boost::format("Size mismatch between input %1% and "
				       "input %2%:\n%3%\n%4%") % i % j
			 % ty1->dumpTextFormat() 
			 % ty2->dumpTextFormat()).str());
  }
  for (std::size_t k=0, sz=ty1->size(); k<sz; ++k) {
    if (ty1->GetMember(k).GetType() != ty2->GetMember(k).GetType()) {
      log.logError(*this, 
		   (boost::format("Type mismatch between input %1% and "
				  "input %2% in position %5%:\n%3%\n%4%") 
		    % i % j
		    % ty1->dumpTextFormat() % ty2->dumpTextFormat()
		    % k).str());  
    }
  }    
}

void LogicalOperator::checkIdenticalInputs(PlanCheckContext& log)
{
  for (std::size_t i=1,e=size_inputs(); i<e; ++i) {
    checkIdenticalInput(log, 0, i);
  }
}

void LogicalOperator::checkGraph(PlanCheckContext& log)
{
  if (size_inputs() < mMinInputs || size_inputs() >mMaxInputs) {
    log.logError(*this, mMinInputs == mMaxInputs ?
		 (boost::format("Operator has %1% input ports but is "
				"configured with %2% input arrows") %
		  mMinInputs % size_inputs()).str() :
		 (boost::format("Operator has between %1% and %2% input ports but is "
				"configured with %3% input arrows") %
		  mMinInputs % mMaxInputs % size_inputs()).str());
  }
  if (size_outputs() < mMinOutputs || size_outputs() >mMaxOutputs) {
    log.logError(*this, mMinOutputs == mMaxOutputs ?
		 (boost::format("Operator has %1% output ports but is "
				"configured with %2% output arrows") %
		  mMinOutputs % size_outputs()).str() :
		 (boost::format("Operator has between %1% and %2% output ports but is "
				"configured with %3% output arrows") %
		  mMinOutputs % mMaxOutputs % size_outputs()).str());
  }
}

LogicalPlan::LogicalPlan()
{
}

LogicalPlan::~LogicalPlan()
{
  for(std::vector<LogicalOperator*>::iterator it=mOperators.begin();
      it != mOperators.end();
      ++it) {
    delete *it;
  }
  for(std::vector<LogicalFifo *>::iterator it=mFifos.begin();
      it != mFifos.end();
      ++it) {
    delete *it;
  }   
}

//
// boost::topological_sort doesn't give any info
// if we don't have a DAG so we do our own DFS
//

class NotDAG 
{
public:
  LogicalPlan::edge_descriptor mEdge;
  NotDAG(LogicalPlan::edge_descriptor e)
    :
    mEdge(e)
  {
  }
};

template<typename OutputIterator>
class TopologicalSortVisitor : public boost::dfs_visitor<>
{
public:
  OutputIterator mIt;
  TopologicalSortVisitor(OutputIterator it)
    :
    mIt(it)
  {
  }

  void back_edge(const LogicalPlan::edge_descriptor& e , 
		 const LogicalPlan& ) 
  { 
    throw NotDAG(e);
  }
    
  void finish_vertex(const LogicalPlan::vertex_descriptor& u, 
		     const LogicalPlan& ) 
  {
    *mIt++ = u;
  }
};

void LogicalPlan::check()
{
  // Before anything else do basic sanity checks on 
  // whether there are too many/too few edges.
  // Do this prior to the topological sort because it
  // is possible for a misnamed operator to accidentally create
  // a cycle when in fact it is easier for a user to understand
  // a message about wrong arrow count compared to a DAG error.
  for(const_operator_iterator it = begin_operators(), 
	e = end_operators(); it != e; ++it) {
    // Are there any problems with the graph (missing or extra arrows).
    (*it)->checkGraph(mContext);
  }
  
  // Topologically sort the logical plan and check the
  // DAG condition at this point.
  typedef std::map<LogicalPlan::vertex_descriptor, boost::default_color_type> std_color_map;
  std_color_map colors;
  boost::associative_property_map< std_color_map > colorMap(colors);  
  std::vector<LogicalPlan::vertex_descriptor> v;
  typedef std::back_insert_iterator<std::vector<LogicalPlan::vertex_descriptor> > iter_type;
  typedef TopologicalSortVisitor<iter_type> visitor_type;
  visitor_type vtor(std::back_inserter(v));
  try {
    boost::depth_first_search(*this, boost::color_map(colorMap).visitor(vtor));
  } catch(NotDAG& dagEx) {
    LogicalPlan::edge_descriptor backEdge = dagEx.mEdge;
    mContext.logError(*backEdge->target(),
		      (boost::format("Arrow from port %1% of operator %2% to port %3%"
				     " creates a cycle in the dataflow graph") %
		       backEdge->source()->getName() % backEdge->getSourcePort() %
		       backEdge->getTargetPort()).str());
    return;
  }

  for(std::vector<LogicalPlan::vertex_descriptor>::reverse_iterator it = v.rbegin();
      it != v.rend();
      ++it) {
    // Type check the operator.
    for(std::vector<LogicalFifo*>::const_iterator fit = (*it)->begin_inputs(),
	  end = (*it)->end_inputs(); fit != end; ++fit) {
      if ((*fit)->getRecordType() == NULL) {
	throw std::runtime_error((boost::format("Type check error: Operator '%1%' requires input port type") %
				  (*it)->getName()).str());
      }
    }
    try {
      (*it)->check(mContext);
    } catch(std::exception & e) {
      // We want operators to use the logError API and not to throw,
      // but in any case let's handle these
      throw std::runtime_error((boost::format("In operator %1%: %2%") %
				(*it)->getName() %
				e.what()).str());
    }
    for(std::vector<LogicalFifo*>::const_iterator fit = (*it)->begin_outputs(),
	  end = (*it)->end_outputs(); fit != end; ++fit) {
      if ((*fit)->getRecordType() == NULL) {
	throw std::runtime_error((boost::format("Type check error: Operator '%1%' type check did not calculate output port type") %
				  (*it)->getName()).str());
      }
    }
  }
}

static boost::mutex sLogicalOperatorFactoryGuard;

LogicalOperatorFactory::LogicalOperatorFactory()
  :
  mGuard(NULL)
{
  mGuard = new boost::mutex();
}

LogicalOperatorFactory::~LogicalOperatorFactory()
{
  delete mGuard;
}

LogicalOperatorFactory& LogicalOperatorFactory::get()
{
  // TODO: Manage lifetime here...
  static LogicalOperatorFactory * factory = NULL;
  boost::unique_lock<boost::mutex> lock(sLogicalOperatorFactoryGuard);
  if (NULL == factory) {
    factory = new LogicalOperatorFactory();
  }
  return *factory;
}

void LogicalOperatorFactory::registerCreator(const std::string& opType, 
					CreateLogicalOperatorFn creator)
{
  boost::unique_lock<boost::mutex> lock(*mGuard);
  std::string ciOpType = boost::algorithm::to_lower_copy(opType);
  if (mCreators.find(ciOpType) != mCreators.end()) {
    throw std::runtime_error((boost::format("Error: attempt to register "
					    "operator type"
					    " %1% multiple times") %
			      opType).str());
  }
  mCreators[ciOpType] = creator;
}

LogicalOperator * LogicalOperatorFactory::create(const std::string& opType)
{
  boost::unique_lock<boost::mutex> lock(*mGuard);
  std::string ciOpType = boost::algorithm::to_lower_copy(opType);
  std::map<std::string, CreateLogicalOperatorFn>::const_iterator it = 
    mCreators.find(ciOpType);
  if (it == mCreators.end()) {
    throw std::runtime_error((boost::format("Unknown operator "
					    "type: %1%") %
			      opType).str());
  }

  return it->second(opType);
}

