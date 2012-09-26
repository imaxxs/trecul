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

#ifndef __RUNTIMEPROCESS_H
#define __RUNTIMEPROCESS_H

#include <vector>
#include <string>
#include <map>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/shared_ptr.hpp>

#include "DataflowRuntime.hh"

class RuntimeOperator;
class RuntimeOperatorType;
class RuntimeOperatorPlan;
class AssignedOperatorType;
class DataflowScheduler;
class IntraProcessFifoSpec;
class InterProcessFifoSpec;
class InProcessFifo;
// The following are all MPI specific (or at least related
// to the simple flow control protocol we layer on top of MPI).
class RuntimeMessageDemuxOperatorType;
class RuntimeBufferPoolOperatorType;
class RuntimeDataAvailableOperatorType;
class RuntimeMessageReceiverOperatorType;
class RuntimeSendAvailableOperatorType;
class RuntimeMessageSendOperatorType;
class RemoteReceiveFifo;
class RuntimeProcess;

namespace boost {
  class thread;
}

class Timer
{
private:
  boost::posix_time::ptime mTick;
  int32_t mPartition;
public:
  Timer(int32_t partition);
  ~Timer();
};

/**
 * The following methods are implemented for a remoting strategy.
 * Default implementation is to throw an exception
 * since the default doesn't support remoting.
 */
class ProcessRemoting
{
public:
  virtual ~ProcessRemoting() {}
  virtual void addSource(const InterProcessFifoSpec& spec, 
			 int32_t sourcePartition, 
			 int32_t sourcePartitionConstraintIndex);
  virtual void addTarget(const InterProcessFifoSpec& spec, 
			 int32_t targetPartition, 
			 int32_t targetPartitionConstraintIndex);
  virtual void runRemote(std::vector<boost::shared_ptr<boost::thread> >& threads) {}
};

/**
 * Virtual constructor for remoting strategy
 */
class ProcessRemotingFactory
{
public:
  virtual ~ProcessRemotingFactory() {}
  virtual ProcessRemoting* create(RuntimeProcess& p)
  {
    return new ProcessRemoting();
  }
};

/**
 * A runtime operator process contains a collection of partitions that
 * are executed by one or more threads.  
 */
class RuntimeProcess
{
private:
  // The partitions this process is managing.
  int32_t mPartitionStart;
  int32_t mPartitionEnd;
  int32_t mNumPartitions;

  // The dataflow scheduler for a partition
  std::map<int32_t, DataflowScheduler*> mSchedulers;

  // As operators are created we need to be able to lookup the operator
  // the operator type and partition number.
  std::map<const RuntimeOperatorType *, std::map<int32_t, RuntimeOperator *> > mTypePartitionIndex;
  std::map<int32_t, std::vector<RuntimeOperator*> > mPartitionIndex;
  std::vector<RuntimeOperator * > mAllOperators;
  // Channels connecting operators that both live in this process
  std::vector<InProcessFifo *> mChannels;
  // State for remote execution
  boost::shared_ptr<ProcessRemoting> mRemoteExecution;

  /**
   * Create all of the operators in the required partitions.
   */
  void createOperators(const AssignedOperatorType& ty);

  const std::vector<RuntimeOperator*>& getOperators(int32_t partition);

  void connectStraightLine(const IntraProcessFifoSpec& spec);
  void connectCrossbar(const InterProcessFifoSpec& spec);

  void init(int32_t partitionStart, 
	    int32_t partitionEnd,
	    int32_t numPartitions,
	    const RuntimeOperatorPlan& plan,
	    ProcessRemotingFactory& remoting);

  // Helper shim for running a scheduler.
  // TODO: Extra stuff to get any exceptions back.
  static void run(DataflowScheduler& s);
  /**
   * Validate graph prior to running.
   */
  void validateGraph();
public:
  RuntimeProcess(int32_t partitionStart, 
		 int32_t partitionEnd,
		 int32_t numPartitions,
		 const RuntimeOperatorPlan& plan,
		 ProcessRemotingFactory& remoting);
  RuntimeProcess(int32_t partitionStart, 
		 int32_t partitionEnd,
		 int32_t numPartitions,
		 const RuntimeOperatorPlan& plan);
  virtual ~RuntimeProcess();

  /**
   * Get all operators of a particular type.
   */
  template <class _OpType>
  void getOperatorOfType(std::vector<_OpType*>& ret) {
    for(std::vector<RuntimeOperator * >::iterator it = mAllOperators.begin();
	it != mAllOperators.end();
	++it) {
      _OpType * tmp = dynamic_cast<_OpType *>(*it);
      if (tmp != NULL)
	ret.push_back(tmp);
    }
  }

  /**
   * Run the dataflow and return on completion.
   */
  void run();

  /**
   * Initialize the dataflow for cooperative multitask running within an
   * existing ambient thread.
   * In this model of execution, operators have the option of yielding to
   * the scheduler for a reason other than reading or writing to a port (e.g.
   * waiting on a socket managed outside of dataflow).  The scheduler then exits
   * back to the controlling thread by returning from runSome().  It is then 
   * incumbent upon the controlling thread to pass an event to appropriate 
   * operators to enable them to be unblocked.  At that point, the controller
   * can invoke runSome() again.
   */
  void runInit();
  DataflowScheduler::RunCompletion runSome(int64_t maxIterations);
  bool runSome();
  void runComplete();

  /**
   * The following methods are available for a remoting derived class
   * to modify the process dataflow graph.
   */

  /**
   * Connect ports of two operators in the same process (but perhaps
   * different threads.
   */
  void connectInProcess(RuntimeOperator & source, int32_t outputPort, int32_t sourcePartition,
			RuntimeOperator & target, int32_t inputPort, int32_t targetPartition, bool buffered);
  void connectInProcess(RuntimeOperator & source, 
			int32_t outputPort, 
			DataflowScheduler & sourceScheduler,
			RuntimeOperator & target, 
			int32_t inputPort, 
			DataflowScheduler & targetScheduler, 
			bool buffered);
  /**
   * Create an operator in a single partition managed by this process and update
   * all indexes on the operator collection.
   */
  RuntimeOperator * createOperator(const RuntimeOperatorType * ty, int32_t partition);

  /**
   * Get the operator created from a type in a particular partition.
   */
  RuntimeOperator * getOperator(const RuntimeOperatorType* ty, int32_t partition);

  /**
   * Test whether the process contains the partition in question.
   */
  bool hasPartition(std::size_t partition)
  {
    return partition <= (std::size_t) std::numeric_limits<int32_t>::max() &&
      mPartitionStart <= (int32_t) partition && mPartitionEnd >= (int32_t) partition;
  }

  /**
   * Get the partitions for a given operator type.
   */
  void getPartitions(const AssignedOperatorType * opType,
		     boost::dynamic_bitset<> & result);

  /**
   * Get the dataflow scheduler for a partition.
   */
  DataflowScheduler& getScheduler(int32_t partition);
};

class AdsDfSpeculativeExecution
{
public:
  enum Type { BOTH, NONE, MAP, REDUCE };
private:
  Type mType;
public:
  AdsDfSpeculativeExecution();
  AdsDfSpeculativeExecution(const std::string& str);
  const char * isMapEnabledString() const;
  const char * isReduceEnabledString() const;
};

class PlanRunner
{
private:
  static void createSerialized64PlanFromFile(const std::string& f,
					     int32_t partitions,
					     std::string& plan);
  static void createSerialized64MapPlan(const std::string& f,
					int32_t partitions,
					std::string& emitFormat,
					std::string& plan);
  static void createSerialized64ReducePlan(const std::string& f,
					   int32_t partitions,
					   const std::string& defaultReduceFormat,
					   std::string& plan);
public:
  static int runMapReduceJob(const std::string& mapProgram,
			     const std::string& reduceProgram,
			     const std::string& inputDir,
			     const std::string& outputDir,
			     bool useHp);
  static int runMapReduceJob(const std::string& mapProgram,
			     const std::string& reduceProgram,
			     const std::string& inputDir,
			     const std::string& outputDir,
			     int32_t numReduces,
			     bool jvmReuse,
			     bool useHp);
  static int runMapReduceJob(const std::string& mapProgram,
			     const std::string& reduceProgram,
			     const std::string& name,
			     const std::string& jobQueue,
			     const std::string& inputDir,
			     const std::string& outputDir,
			     int32_t numReduces,
			     bool jvmReuse,
			     bool useHp,
			     AdsDfSpeculativeExecution speculative,
			     int32_t timeout);
  static int run(int argc, char ** argv);
};

/**
 * Interface into the Hadoop installation.
 */
class HadoopSetup
{
public:
  /**
   * Get the Hadoop installation directory.
   */
  static std::string hadoopHome();
  /**
   * Set appropriate environment variables required for Hadoop
   * and HDFS functionality (e.g. CLASSPATH).
   */
  static void setEnvironment();
};

/**
 * Some classes for process management.
 * These classes are culled together from
 * ideas and code that were floated about during
 * the Boost.Process review in February 2011 (the proposed
 * library was soundly rejected and this attempts to
 * incorporate some of the ideas thrown out during the
 * review).
 */
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/shared_ptr.hpp>

/**
 * A unidirectional communication channel between a parent
 * and child process.
 */
class ProcessPipe
{
public:
  typedef boost::iostreams::file_descriptor_source source_type;
  typedef boost::iostreams::file_descriptor_sink sink_type;

  sink_type mSink;
  source_type mSource;

  ProcessPipe();
};

/**
 * A Posix process initializer.
 *
 * The idea of an initializer is that it configures the parent
 * and child processes very generically (e.g. everything from
 * simply specifying arguments to setting up redirection of
 * file descriptors in the child).  These configurations may
 * unfold as a sequence of actions in both the parent and child
 * and callbacks provide for the hooks to drive the initializer
 * specific behavior (a bit like implementing external methods
 * via a visitor pattern).
 *
 * This is structured as an event handler that responds to
 * phases in process creation.
 * What makes this Posix specific are the fact that the
 * events correspond to Unix-style fork/exec (e.g. as opposed
 * to Win32 CreateProcess).
 *
 * We considered two approaches to initializers: a simple OO
 * inheritence one and a modern C++ metaprogramming (e.g. Boost
 * fusion) one.  We opt for the former although the Boost
 * discussion was using the latter.  To me the use of modern C++
 * here seems like overkill given the lack of performance sensitivity
 * in process creation (what do a few virtual function calls really
 * matter) and the trade off in template code bloat and compile time
 * overhead.
 */
class PosixProcessInitializer
{
public:
  virtual ~PosixProcessInitializer() {}
  virtual void onPreForkParent(class PosixProcessFactory& parent) {}
  virtual void onFailedForkParent(class PosixProcessFactory& parent) {}
  virtual void onPostForkParent(class PosixProcessFactory& parent) {}
  virtual void onPostForkChild(class PosixProcessFactory& parent) {}
  virtual void onFailedExecChild(class PosixProcessFactory& parent) {}
};

class PosixProcessFactory
{
public:
  typedef boost::shared_ptr<PosixProcessInitializer> init_type;
  typedef pid_t pid_type;

  class PosixStringArray
  {
  protected:
    // The first slot here is executable name,
    // the last always a NULL ptr.
    std::vector<char*> mArgsVec;
    PosixStringArray(std::size_t sz)
      :
      mArgsVec(sz, NULL)
    {
    }
  public:
    void push_back(char * arg)
    {
      BOOST_ASSERT(mArgsVec.back() == NULL);
      mArgsVec.back() = arg;
      mArgsVec.push_back(NULL);
    }
    char * const * get() const { return &mArgsVec[0]; }
  };

  class PosixEnv : public PosixStringArray
  {
  public:
    PosixEnv()
      :
      PosixStringArray(1)
    {
    }
  };

  class PosixArgs : public PosixStringArray
  {
  public:
    PosixArgs()
      :
      PosixStringArray(2)
    {
    }
    void setExe(char * e)
    {
      mArgsVec.front() = e;
    }
  };
  boost::filesystem::path mExe;
  PosixEnv mEnvVars;
  pid_type mPid;
  PosixArgs mArgs;

private:
  typedef std::vector<init_type> inits_type;
  typedef std::vector<init_type>::const_iterator inits_iterator_type;
  void onPreForkParent(const inits_type& initializers);
  void onPostForkParent(const inits_type& initializers);
  void onPostForkChild(const inits_type& initializers);
  void onFailedForkParent(const inits_type& initializers);
  void onFailedExecChild(const inits_type& initializers);
public:
  PosixProcessFactory();

  // TODO: Templatize on range or iterator
  void create(const std::vector<init_type>& initializers);

  /**
   * Synchronously wait for the process to complete.
   * Note that this does not return if the process is stopped/continued.
   * Returns the exit status of the process if the process exited cleanly.
   * Returns the 256+signal number of a process that was terminated.
   * Returns -1 and sets ec if there is an error waiting.
   */
  int32_t waitForCompletion(boost::system::error_code & ec);
  int32_t waitForCompletion();
};

class PosixPath : public PosixProcessInitializer
{
private:
  boost::filesystem::path mPath;
  std::string mPathChars;
public:
  PosixPath(const boost::filesystem::path& exe);
  void onPreForkParent(PosixProcessFactory& parent);
};

class PosixArgument : public PosixProcessInitializer
{
private:
  std::string mArg;
public:
  PosixArgument(const std::string& arg);
  void onPreForkParent(PosixProcessFactory& parent);
};

class PosixArguments : public PosixProcessInitializer
{
private:
  typedef std::vector<PosixArgument> args_type;
  args_type mArgs;
public:
  PosixArguments(const std::string& arg);
  PosixArguments(const std::string& arg1, 
		 const std::string& arg2);
  PosixArguments(const std::string& arg1, 
		 const std::string& arg2, 
		 const std::string& arg3);
  PosixArguments& operator() (const PosixArgument& arg);
  void onPreForkParent(PosixProcessFactory& parent);
};

class StandardInTo : public PosixProcessInitializer
{
private:
  ProcessPipe& mPipe;
public:
  StandardInTo(ProcessPipe& p);
  void onPostForkParent(PosixProcessFactory& parent);
  void onPostForkChild(PosixProcessFactory& parent);
};

class StandardInFrom : public PosixProcessInitializer
{
private:
  ProcessPipe& mPipe;
public:
  StandardInFrom(ProcessPipe& p);
  void onPostForkParent(PosixProcessFactory& parent);
  void onPostForkChild(PosixProcessFactory& parent);
};

class StandardOutTo : public PosixProcessInitializer
{
private:
  ProcessPipe& mPipe;
  bool mIsStdErr;
public:
  StandardOutTo(ProcessPipe& p, bool isStdErr=false);
  void onPostForkParent(PosixProcessFactory& parent);
  void onPostForkChild(PosixProcessFactory& parent);
};

class PosixParentEnvironment : public PosixProcessInitializer
{
public:
  PosixParentEnvironment();
  void onPreForkParent(PosixProcessFactory& parent);
};

class PosixEnvironmentVariable : public PosixProcessInitializer
{
private:
  std::string mEnvVar;
public:
  PosixEnvironmentVariable(const std::string& nm, const std::string& val);
  void onPreForkParent(PosixProcessFactory& parent);
};

class GdbStackTrace
{
typedef boost::shared_ptr<PosixProcessInitializer> ppiptr;
  std::vector<ppiptr> mInitializers;
public:
  GdbStackTrace();
  ~GdbStackTrace();
  int32_t generate();
};

#endif
