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

#include <unistd.h>
#include <fcntl.h>

#include "md5.h"
#include "RuntimeProcess.hh"
#include "RuntimeOperator.hh"
#include "RuntimePlan.hh"
#include "DataflowRuntime.hh"
#include "QueueImport.hh"
#include "SuperFastHash.h"
#include "GraphBuilder.hh"
#include "HdfsOperator.hh"

#include <fstream>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/constants.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/program_options.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>
#include <boost/regex.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#if defined(__APPLE__) || defined(__APPLE_CC__)
#include <sys/param.h>
#include <mach-o/dyld.h>
#if !defined(environ)
extern char ** environ;
#endif
#endif

namespace po = boost::program_options;

static void checkRegularFileExists(const std::string& filename)
{
  boost::filesystem::path p (filename);
  if (!boost::filesystem::exists(p)) {
    throw std::runtime_error((boost::format("%1% does not exist") %
			      filename).str());
  }
  if (!boost::filesystem::is_regular_file(p)) {
    throw std::runtime_error((boost::format("%1% is not a regular file") %
			      filename).str());
  }  
}

static void readInputFile(const std::string& filename, 
			  std::string& contents)
{
  checkRegularFileExists(filename);
  std::stringstream ostr;
  std::fstream mapFile(filename.c_str(), std::ios_base::in);
  std::copy(std::istreambuf_iterator<char>(mapFile),
	    std::istreambuf_iterator<char>(),
	    std::ostreambuf_iterator<char>(ostr));
  contents = ostr.str();
}

class Executable
{
public:
  /**
   * Get full path to the executable that is running.
   */
  static boost::filesystem::path getPath();
};

boost::filesystem::path Executable::getPath()
{
#if defined(linux) || defined(__linux) || defined(__linux__)
  // Linux we may query /proc
  char buf[PATH_MAX+1];
  ssize_t len=0;
  if((len = ::readlink("/proc/self/exe", &buf[0], PATH_MAX)) != -1)
    buf[len] = 0;
  else {
    int err = errno;
    throw std::runtime_error((boost::format("Failed to resolve executable path: %1%") %
			      err).str());
  }
  return buf;
#elif defined(__APPLE__) || defined(__APPLE_CC__)
  char buf[MAXPATHLEN+1];
  uint32_t pathLen = MAXPATHLEN;
  int ret = _NSGetExecutablePath(&buf[0], &pathLen);
  if (0 == ret) {
    return buf;
  } else if (-1 == ret) {
    char * dynbuf = new char [pathLen];
    ret = _NSGetExecutablePath(dynbuf, &pathLen);
    if (0 == ret) {
      return dynbuf;
    } else {
      return "/";
    }
  } else {
    return "/";
  }
#else
#error "Unsupported platform"
#endif
}

void ProcessRemoting::addSource(const InterProcessFifoSpec& spec, 
				int32_t sourcePartition, 
				int32_t sourcePartitionConstraintIndex)
{
  throw std::runtime_error("Standard dataflow process does not support repartitioning/shuffle");
}

void ProcessRemoting::addTarget(const InterProcessFifoSpec& spec, 
				int32_t targetPartition, 
				int32_t targetPartitionConstraintIndex)
{
  throw std::runtime_error("Standard dataflow process does not support repartitioning/shuffle");
}

RuntimeProcess::RuntimeProcess(int32_t partitionStart, 
			       int32_t partitionEnd,
			       int32_t numPartitions,
			       const RuntimeOperatorPlan& plan)
  :
  mPartitionStart(partitionStart),
  mPartitionEnd(partitionEnd),
  mNumPartitions(numPartitions)
{
  ProcessRemotingFactory remoting;
  init(partitionStart, partitionEnd, numPartitions, plan, remoting);
}

RuntimeProcess::RuntimeProcess(int32_t partitionStart, 
			       int32_t partitionEnd,
			       int32_t numPartitions,
			       const RuntimeOperatorPlan& plan,
			       ProcessRemotingFactory& remoting)
  :
  mPartitionStart(partitionStart),
  mPartitionEnd(partitionEnd),
  mNumPartitions(numPartitions)
{
  init(partitionStart, partitionEnd, numPartitions, plan, remoting);
}

void RuntimeProcess::init(int32_t partitionStart, 
			  int32_t partitionEnd,
			  int32_t numPartitions,
			  const RuntimeOperatorPlan& plan,
			  ProcessRemotingFactory& remoting)
{
  mRemoteExecution = boost::shared_ptr<ProcessRemoting>(remoting.create(*this));
  if (partitionStart < 0 || partitionEnd >= numPartitions)
    throw std::runtime_error("Invalid partition allocation to process");
  for(int32_t i=partitionStart; i<=partitionEnd; i++) {
    mSchedulers[i] = new DataflowScheduler(i, numPartitions);
  }

  for(RuntimeOperatorPlan::operator_const_iterator it = plan.operator_begin();
      it != plan.operator_end();
      ++it) {
    createOperators(*it->get());
  }
  for(RuntimeOperatorPlan::intraprocess_fifo_const_iterator it = plan.straight_line_begin();
      it != plan.straight_line_end();
      ++it) {
    connectStraightLine(*it);
  }
  for(RuntimeOperatorPlan::interprocess_fifo_const_iterator it = plan.crossbar_begin();
      it != plan.crossbar_end();
      ++it) {
    connectCrossbar(*it);
  }

  for(std::vector<InProcessFifo *>::iterator channel = mChannels.begin();
      channel != mChannels.end();
      ++channel) {
    if ((*channel)->getTarget()->getOperator().getNumInputs() == 1)
      (*channel)->setBuffered(false);
  }
}

RuntimeProcess::~RuntimeProcess()
{
  for(std::map<int32_t, DataflowScheduler*>::iterator it = mSchedulers.begin();
      it != mSchedulers.end();
      ++it) {
    delete it->second;
  }
  for(std::vector<RuntimeOperator * >::iterator opit = mAllOperators.begin();
      opit != mAllOperators.end();
      ++opit) {
    delete *opit;
  }  
  for(std::vector<InProcessFifo *>::iterator chit = mChannels.begin();
      chit != mChannels.end();
      ++chit) {
    delete *chit;
  }
}

void RuntimeProcess::connectInProcess(RuntimeOperator & source, int32_t outputPort, int32_t sourcePartition,
				      RuntimeOperator & target, int32_t inputPort, int32_t targetPartition,
				      bool buffered)
{
  connectInProcess(source, outputPort, *mSchedulers[sourcePartition], 
		   target, inputPort, *mSchedulers[targetPartition], buffered);
}

void RuntimeProcess::connectInProcess(RuntimeOperator & source, 
				      int32_t outputPort, 
				      DataflowScheduler & sourceScheduler,
				      RuntimeOperator & target, 
				      int32_t inputPort, 
				      DataflowScheduler & targetScheduler,
				      bool buffered)
{
  mChannels.push_back(new InProcessFifo(sourceScheduler, targetScheduler, buffered));
  source.setOutputPort(mChannels.back()->getSource(), outputPort);
  mChannels.back()->getSource()->setOperator(source);
  target.setInputPort(mChannels.back()->getTarget(), inputPort);
  mChannels.back()->getTarget()->setOperator(target);    
  
}

void RuntimeProcess::connectStraightLine(const IntraProcessFifoSpec& spec)
{
  std::vector<int32_t> spartitions;
  spec.getSourceOperator()->getPartitions(mPartitionStart,
					  mPartitionEnd,
					  spartitions);
  std::vector<int32_t> tpartitions;
  spec.getTargetOperator()->getPartitions(mPartitionStart, 
					  mPartitionEnd,
					  tpartitions);
  if (spartitions != tpartitions)
    throw std::runtime_error("Invalid plan: straight line connection specified on operators that are not in the same partitions.");
  for(std::vector<int32_t>::const_iterator i=spartitions.begin();
      i != spartitions.end();
      ++i) {
    RuntimeOperator * sourceOp = getOperator(spec.getSourceOperator()->Operator, *i);
    if (sourceOp==NULL) throw std::runtime_error("Operator not created");
    RuntimeOperator * targetOp = getOperator(spec.getTargetOperator()->Operator, *i);
    if (targetOp==NULL) throw std::runtime_error("Operator not created");
    connectInProcess(*sourceOp, spec.getSourcePort(), *i,
		     *targetOp, spec.getTargetPort(), *i, spec.getBuffered());
  }  
}

void RuntimeProcess::connectCrossbar(const InterProcessFifoSpec& spec)
{
  // Get the partitions within this process for each operator.
  std::vector<int32_t> spartitions;
  spec.getSourceOperator()->getPartitions(mPartitionStart, mPartitionEnd, spartitions);
  std::vector<int32_t> tpartitions;
  spec.getTargetOperator()->getPartitions(mPartitionStart, mPartitionEnd, tpartitions);

  // To calculate MPI tags in crossbars, we need to know the index/position
  // of a partition within the vector of partitions the operator lives on.
  for(std::vector<int32_t>::const_iterator i=spartitions.begin();
      i != spartitions.end();
      ++i) {
    mRemoteExecution->addSource(spec, *i, 
				spec.getSourceOperator()->getPartitionPosition(*i));
  } 
  for(std::vector<int32_t>::const_iterator i=tpartitions.begin();
      i != tpartitions.end();
      ++i) {
    mRemoteExecution->addTarget(spec, *i, 
				spec.getTargetOperator()->getPartitionPosition(*i));
  } 
}

RuntimeOperator * RuntimeProcess::createOperator(const RuntimeOperatorType * ty, int32_t partition)
{
  std::map<int32_t, DataflowScheduler*>::const_iterator sit = mSchedulers.find(partition);
  if (sit == mSchedulers.end())
    throw std::runtime_error((boost::format("Internal Error: failed to create scheduler for data partition %1%") % partition).str());
  RuntimeOperator * op = ty->create(*sit->second);
  mAllOperators.push_back(op);
  mPartitionIndex[partition].push_back(op);
  mTypePartitionIndex[ty][partition] = op;
  return op;
}

void RuntimeProcess::createOperators(const AssignedOperatorType& ty)
{
  std::vector<int32_t> partitions;
  ty.getPartitions(mPartitionStart, mPartitionEnd, partitions);
  for(std::vector<int32_t>::const_iterator i=partitions.begin();
      i != partitions.end();
      ++i) {
    createOperator(ty.Operator, *i);
  }
}

DataflowScheduler& RuntimeProcess::getScheduler(int32_t partition)
{
  return *mSchedulers[partition];
}

const std::vector<RuntimeOperator*>& RuntimeProcess::getOperators(int32_t partition)
{
  std::map<int32_t, std::vector<RuntimeOperator*> >::const_iterator it = mPartitionIndex.find(partition);
  return it->second;
}

RuntimeOperator * RuntimeProcess::getOperator(const RuntimeOperatorType* ty, int32_t partition)
{
  std::map<const RuntimeOperatorType *, std::map<int32_t, RuntimeOperator *> >::const_iterator it1=mTypePartitionIndex.find(ty);
  if (it1==mTypePartitionIndex.end()) return NULL;
  std::map<int32_t, RuntimeOperator *>::const_iterator it2=it1->second.find(partition);
  if (it2==it1->second.end()) return NULL;
  return it2->second;
}

void RuntimeProcess::getPartitions(const AssignedOperatorType * opType,
				   boost::dynamic_bitset<> & result)
{
  opType->getPartitions(mNumPartitions, result);
}

void RuntimeProcess::run(DataflowScheduler& s)
{
  try {
    // TODO: Signal and exception handling.
    s.run();
    s.cleanup();
  } catch(std::exception& ex) {
    std::cerr << "Failure in scheduler thread: " << ex.what() << std::endl;
  }
}

class DataflowSchedulerThreadRunner 
{
private:
  bool mFailed;
  std::string mMessage;
  DataflowScheduler& mScheduler;
public:
  DataflowSchedulerThreadRunner(DataflowScheduler& s);
  bool isFailed() const { return mFailed; }
  const std::string& getMessage() const { return mMessage; }
  void run();
};

DataflowSchedulerThreadRunner::DataflowSchedulerThreadRunner(DataflowScheduler & s)
  :
  mFailed(false),
  mScheduler(s)
{
}

void DataflowSchedulerThreadRunner::run()
{
  try {
    // TODO: genericize to accept a functor argument.
    mScheduler.run();
    mScheduler.cleanup();
  } catch(std::exception& ex) {
    mMessage = ex.what();
    mFailed = true;
  }
}

void RuntimeProcess::validateGraph()
{
  // Sanity check that all of the operator ports are properly configured.
  for(std::vector<RuntimeOperator * >::iterator opit = mAllOperators.begin();
      opit != mAllOperators.end();
      ++opit) {
    for(RuntimeOperator::input_port_iterator pit = (*opit)->input_port_begin();
	pit != (*opit)->input_port_end();
	++pit) {
      if (*opit != (*pit)->getOperatorPtr()) {
	throw std::runtime_error("Internal Error: Incorrectly configured input port");
      }
    }
    for(RuntimeOperator::input_port_iterator pit = (*opit)->input_port_begin();
	pit != (*opit)->input_port_end();
	++pit) {
      if (*opit != (*pit)->getOperatorPtr()) {
	throw std::runtime_error("Internal Error: Incorrectly configured output port");
      }
    }
  }
}

void RuntimeProcess::runInit()
{
  validateGraph();
  for(std::map<int32_t, DataflowScheduler*>::iterator it = mSchedulers.begin();
      it != mSchedulers.end();
      ++it) {
    it->second->setOperators(mPartitionIndex[it->first]);
  }
  BOOST_ASSERT(mSchedulers.size() == 1);
  mSchedulers.begin()->second->init();
}

DataflowScheduler::RunCompletion RuntimeProcess::runSome(int64_t maxIterations)
{
  BOOST_ASSERT(mSchedulers.size() == 1);
  return mSchedulers.begin()->second->runSome(maxIterations);
}

bool RuntimeProcess::runSome()
{
  BOOST_ASSERT(mSchedulers.size() == 1);
  return mSchedulers.begin()->second->runSome();
}

void RuntimeProcess::runComplete()
{
  BOOST_ASSERT(mSchedulers.size() == 1);
  mSchedulers.begin()->second->complete();
  mSchedulers.begin()->second->cleanup();
}

void RuntimeProcess::run()
{
  validateGraph();
  // Simple model: one thread per scheduler.
  std::vector<boost::shared_ptr<boost::thread> > threads;
  std::vector<boost::shared_ptr<DataflowSchedulerThreadRunner> > runners;

  // Start any threads necessary for remote execution
  mRemoteExecution->runRemote(threads);

  // Now start schedulers for each partition.
  for(std::map<int32_t, DataflowScheduler*>::iterator it = mSchedulers.begin();
      it != mSchedulers.end();
      ++it) {
    it->second->setOperators(mPartitionIndex[it->first]);
    runners.push_back(boost::shared_ptr<DataflowSchedulerThreadRunner>(new DataflowSchedulerThreadRunner(*it->second)));
    threads.push_back(boost::shared_ptr<boost::thread>(new boost::thread(boost::bind(&DataflowSchedulerThreadRunner::run, runners.back()))));
  }

  // // Print out the state of channels every now and then
  // for (int k=0; k<10; ++k) {
  //   for(std::vector<InProcessFifo *>::const_iterator channel = mChannels.begin();
  // 	channel != mChannels.end();
  // 	++channel) {
  //     std::cout << "Channel[" << 
  // 	(*channel)->getSource()->getOperatorPtr()->getName().c_str() <<
  // 	"," <<
  // 	(*channel)->getTarget()->getOperatorPtr()->getName().c_str() <<
  // 	"] Size=" << (*channel)->getSize() << "; Source Buffer Size=" <<
  // 	(*channel)->getSource()->getLocalBuffer().getSize() << "; Target Buffer Size=" <<
  // 	(*channel)->getTarget()->getLocalBuffer().getSize() << std::endl;
  //   }
  //   boost::this_thread::sleep(boost::posix_time::milliseconds(5000));
  //   std::cout << "=================================================" << std::endl;
  // }
  
  // Wait for workers to complete.
  for(std::vector<boost::shared_ptr<boost::thread> >::iterator it = threads.begin();
      it != threads.end();
      ++it) {
    (*it)->join();
  }

  // Check for errors and rethrow.
  int32_t numThreadErrors=0;
  std::stringstream errorMessages;
  for(std::vector<boost::shared_ptr<DataflowSchedulerThreadRunner> >::iterator it = runners.begin();
      it != runners.end();
      ++it) {
    if ((*it)->isFailed()) {
      numThreadErrors += 1;
      errorMessages << ((*it)->getMessage().size() == 0 ? "No message detail" : (*it)->getMessage().c_str()) << "\n";
    }
  }
  if (numThreadErrors) {
    if (numThreadErrors > 1) {
      std::cerr << "Failures";
    } else {
      std::cerr << "Failure"; 
    }
    std::cerr << " in scheduler thread: " << errorMessages.str().c_str() << std::endl;
    throw std::runtime_error(errorMessages.str());
  }
}

Timer::Timer(int32_t partition)
  :
  mPartition(partition)
{
  mTick = boost::posix_time::microsec_clock::universal_time();
}

Timer::~Timer()
{
  boost::posix_time::ptime tock = boost::posix_time::microsec_clock::universal_time();
  std::cout << "ExecutionTime:\t" << mPartition << "\t" << (tock - mTick) << std::endl;
}

class TempFile
{
private:
  boost::filesystem::path mFileName;
public:
  TempFile(const std::string& contents);
  ~TempFile();
  std::string getName() const;
};

TempFile::TempFile(const std::string& contents)
{
  boost::filesystem::path tmpDir((boost::format("/ghostcache/hadoop/temp/%1%")
				  % ::getenv("USER")).str());
  // For backward compatibility try this if we don't have ghostcache set up
  if (!boost::filesystem::exists(tmpDir))
    tmpDir = boost::filesystem::path("/usr/local/akamai/tmp");

  // Retry in case we get a file name that collides
  for(int32_t i=0;i<2;i++) {
    boost::filesystem::path tmpStr(FileSystem::getTempFileName());
    boost::filesystem::path tmpPath(tmpDir / tmpStr);
    int fd = ::open(tmpPath.file_string().c_str(), 
		    O_CREAT | O_EXCL | O_WRONLY,
		    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (-1 == fd) {
      if (errno == EEXIST) {
	continue;
      } else {
	break;
      }
    }
    ssize_t sz = ::write(fd, contents.c_str(), contents.size());
    ::close(fd);
    if (sz != (ssize_t) contents.size()) {
      continue;
    }
    mFileName = tmpPath;
    return;
  }
  throw std::runtime_error((boost::format("Failed to create temporary file in "
					  "directory %1%") % 
			    tmpDir.directory_string()).str());
}

TempFile::~TempFile()
{
  if (!mFileName.empty()) {
    boost::filesystem::remove(mFileName);
  }
}

std::string TempFile::getName() const
{
  return mFileName.directory_string();
}

std::string HadoopSetup::hadoopHome()
{
  std::string hadoopHome("/usr/local/akamai/third-party/hadoop");
  if (getenv("HADOOP_HOME")) {
    hadoopHome = getenv("HADOOP_HOME");
  } 
  return hadoopHome;
}

void HadoopSetup::setEnvironment()
{
  std::string myHome(hadoopHome());  
  std::string myHadoopEnv(myHome + "/conf/hadoop-env.sh");
  std::string myHadoopConfig(myHome + "/bin/hadoop-config.sh");

  // Don't do anything if we don't have a valid Hadoop
  // install
  if (!boost::filesystem::exists(myHome) ||
      !boost::filesystem::is_directory(myHome) ||
      !boost::filesystem::exists(myHadoopEnv) ||
      !boost::filesystem::is_regular_file(myHadoopEnv) ||
      !boost::filesystem::exists(myHadoopConfig) ||
      !boost::filesystem::is_regular_file(myHadoopConfig))
    return;

  // If Hadoop home exists then grab environment variables set
  // up by conf scripts.
  PosixProcessFactory p;
  std::vector<boost::shared_ptr<PosixProcessInitializer> > v;
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new PosixPath("/bin/bash")));
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new PosixArgument("-c")));
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new PosixArgument("source " + myHadoopEnv + ";\n"
									   "source " + myHadoopConfig + ";\n"
									   "echo \"CLASSPATH=$CLASSPATH\"")));
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new PosixParentEnvironment()));

  ProcessPipe stdOutPipe;
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new StandardOutTo(stdOutPipe)));
  p.create(v);
  boost::iostreams::stream<ProcessPipe::source_type> fromChild(stdOutPipe.mSource);

  // Grab the environment from stdout.
  typedef std::map<std::string,std::string> string_map;
  string_map envVars;
  std::string buf;
  while(std::getline(fromChild, buf)) {
    std::vector<std::string> s;
    boost::algorithm::split(s, buf, boost::algorithm::is_any_of("="), boost::algorithm::token_compress_on);
    envVars[s[0]] = s[1];
  }
  p.waitForCompletion();
  for(string_map::iterator it = envVars.begin();
      it != envVars.end();
      ++it) {
    ::setenv(it->first.c_str(), it->second.c_str(), 1);
  }
  
  ::setenv("LIBHDFS_OPTS", "-Xmx100m", 0);
}

/**
 * Run an ads-df dataflow using Hadoop pipes.
 *
 * An ads-df job is run in Hadoop using pipes.  This is
 * accomplished by having ads-df compile the dataflow plans
 * and pass them to a slave executable ads-df-pipes that 
 * runs the plans as a map reduce task.
 * 
 * To make upgrades of ads-df easy we have every version
 * of ads-df upload an appropriate version of ads-df-pipes into
 * Hadoop.  This makes it possible for multiple versions of
 * ads-df to coexist on a cluster.  At a minimum this is 
 * crucial while a cluster is being upgraded; without this
 * it would be possible to have a version mismatch between the
 * ads-df that serialized a dataflow plan and an ads-df-pipes
 * slave that deserializes and executes it.  Much easier than
 * demanding backward/forward compatibility is to just keep
 * these versions separate from one another.
 *
 * We identify versions of ads-df by taking a checksum of
 * the ads-df-pipes executable.  This has the advantage that
 * one doesn't have to manage arbitrary versioning schemes 
 * and that one is protected from incompatible changes during
 * a development cycle that don't correspond to a change in any
 * official release version number.  The big disadvantage of this
 * upload policy is that developer machines will accumulate a large
 * quantity of ads-df-pipes executable versions in HDFS and it will
 * be necessary to purge these on occasion.  An alternative that was
 * tried was just having ads-df always upload a private "one use
 * only" copy of ads-df-pipes to HDFS.  It appears that the massively
 * scalable Hadoop core is incapable of handling this much traffic
 * in its distributed cache as our entire cluster was crippled after
 * a couple of days with this upload policy.
 */
class AdsPipesJobConf
{
private:
  std::string mJobDir;
  std::string mMapper;
  std::string mReducer;
  std::string mName;
  std::string mJobQueue;
  int32_t mNumReducers;
  bool mJvmReuse;
  boost::filesystem::path mLocalPipesPath;
  std::string mLocalPipesChecksum;

  /**
   * Name of pipes executable in HDFS.  It goes into the
   * distributed cache from here.
   */
  std::string getPipesExecutableName() const;
  /**
   * Name of map plan in HDFS.  It goes into the
   * distributed cache from here.
   */
  std::string getMapPlanFileName() const;
  /**
   * Name of reduce plan in HDFS.  It goes into the
   * distributed cache from here.
   */
  std::string getReducePlanFileName() const;
  /**
   * Copy plan from local file system to HDFS.
   */
  void copyPlanFileToHDFS(const std::string& plan,
			  const std::string& planFileName);
  /**
   * Copy ads-df-pipes from local file system to HDFS.
   */
  void copyPipesExecutableToHDFS();

  /**
   * MD5 checksum of ads-df-pipes executable on local disk.
   */
  static std::string getPipesExecutableChecksum(const boost::filesystem::path & p);

public:
  AdsPipesJobConf(const std::string& jobDir);
  void setMapper(const std::string& m);
  void setReducer(const std::string& r, int32_t numReducers);
  void setName(const std::string& name);
  void setNumReducers(int32_t numReducers);
  void setJvmReuse(bool jvmReuse);
  void setJobQueue(const std::string& jobQueue);
  std::string get() const;
  void copyFilesToHDFS();
  static int32_t copyFromLocal(const std::string& localPath,
			       const std::string& remotePath);
};

AdsPipesJobConf::AdsPipesJobConf(const std::string& jobDir)
  :
  mJobDir(jobDir),
  mNumReducers(0),
  mJvmReuse(true)
{
  // We assume that ads-df-pipes is in the same directory
  // as this exe
  mLocalPipesPath =
    Executable::getPath().parent_path()/boost::filesystem::path("ads-df-pipes");
  if (!boost::filesystem::exists(mLocalPipesPath))
    throw std::runtime_error((boost::format("Couldn't find ads-df-pipes "
					    "executable: %1%.  "
					    "Check installation") % mLocalPipesPath.file_string()).str());
  mLocalPipesChecksum = getPipesExecutableChecksum(mLocalPipesPath);
}

void AdsPipesJobConf::setMapper(const std::string& m)
{
  mMapper = m;
}

void AdsPipesJobConf::setReducer(const std::string& r, int32_t numReducers)
{
  mReducer = r;
  mNumReducers = numReducers;
}

void AdsPipesJobConf::setName(const std::string& name)
{
  mName = name;
}

void AdsPipesJobConf::setNumReducers(int32_t numReducers)
{
  mNumReducers = numReducers;
}

void AdsPipesJobConf::setJvmReuse(bool jvmReuse)
{
  mJvmReuse = jvmReuse;
}
 
void AdsPipesJobConf::setJobQueue(const std::string& jobQueue)
{
  mJobQueue = jobQueue;
}

std::string AdsPipesJobConf::get() const
{
  boost::format openBoilerPlateFormat(
			      "<?xml version=\"1.0\"?>\n"
			      "<configuration>\n"
			      "%2%"
			      "  <property>\n"
			      "    <name>hadoop.pipes.executable</name>\n"
			      "    <value>%1%</value>\n"
			      "  </property>\n"
			      "  <property>\n"
			      "    <name>hadoop.pipes.java.recordreader</name>\n"
			      "    <value>true</value>\n"
			      "  </property>\n"
			      "  <property>\n"
			      "    <name>hadoop.pipes.java.recordwriter</name>\n"
			      "    <value>true</value>\n"
			      "  </property>\n"
			      "  <property>\n"
			      "    <name>mapred.job.shuffle.input.buffer.percent</name>\n"
			      "    <value>0.5</value>\n"
			      "  </property>\n"
			      );
  boost::format mapperFormat(
			     "  <property>\n"
			     "    <name>com.akamai.ads.dataflow.mapper.plan</name>\n"
			     "    <value>%1%</value>\n"
			     "  </property>\n"
			     );
  boost::format numReducersFormat(
			      "  <property>\n"
			      "    <name>mapred.reduce.tasks</name>\n"
			      "    <value>%1%</value>\n"
			      "  </property>\n"
				  );
  boost::format reducerFormat(
			      "  <property>\n"
			      "    <name>com.akamai.ads.dataflow.reducer.plan</name>\n"
			      "    <value>%1%</value>\n"
			      "  </property>\n"
			      );
  boost::format jobName(
			"  <property>\n"
			"     <name>mapred.job.name</name>\n"
			"     <value>%1%</value>\n"
			"  </property>\n"
			);
  boost::format jobQueue(
			 "  <property>\n"
			 "     <name>mapreduce.job.queuename</name>\n"
			 "     <value>%1%</value>\n"
			 "  </property>\n"
			 );
  // We used to put plans in the job conf and let them get pushed
  // out through the distributed cache by virtue of that. When we 
  // had "large" plans such as those generated from scoring runs
  // this caused memory pressure on various JVMs (despite the 
  // fact that we were only talking about 10MB or so).  To avoid
  // any such problems in the future we put plans into the 
  // distributed cache ourselves so that Java doesn't have to touch them
  // during JobConf processing.
  // Note that we set the symlink option so that Hadoop will create
  // links to these files in the working directory of each map
  // reduce task.  This is the only way to make the location of
  // these file predictable when one doesn't have access to the
  // distributed cache API (which pipes doesn't provide).
  boost::format distCacheFormat(
			     "  <property>\n"
			     "    <name>mapreduce.job.cache.files</name>\n"
			     "    <value>%1%</value>\n"
			     "  </property>\n"
			     "  <property>\n"
			     "    <name>mapreduce.job.cache.symlink.create</name>\n"
			     "    <value>yes</value>\n"
			     "  </property>\n"
				);

  std::string closeBoilerPlate(
			       "</configuration>\n"
			       );


  std::string jvmReuseProperty(mJvmReuse ?
			       "  <property>\n"
			       "    <name>mapreduce.job.jvm.numtasks</name>\n"
			       "    <value>-1</value>\n"
			       "  </property>\n" :			       
			       "  <property>\n"
			       "    <name>mapreduce.job.jvm.numtasks</name>\n"
			       "    <value>1</value>\n"
			       "  </property>\n" 
			       );

  std::string ret = (openBoilerPlateFormat % getPipesExecutableName() % jvmReuseProperty).str();
  std::string distCacheFiles;
  ret += (jobName % mName).str();
  if (mJobQueue.size()) {
    ret += (jobQueue % mJobQueue).str();
  }
  // The URI fragment on dist cache names is used
  // to determine the name of the symlink that will 
  // created.  Pass the fragment in the job conf so
  // the map reduce tasks know how to find their plans.
  if (mMapper.size()) {
    const char * planFileLocal = "map.plan.txt";
    ret += (mapperFormat % planFileLocal).str();
    distCacheFiles += getMapPlanFileName();
    distCacheFiles += "#";
    distCacheFiles += planFileLocal;
  }
  ret += (numReducersFormat % mNumReducers).str();
  if (mReducer.size()) {
    const char * planFileLocal = "reduce.plan.txt";
    ret += (reducerFormat % planFileLocal).str();
    if (distCacheFiles.size()) {
      distCacheFiles += ",";
    }
    distCacheFiles += getReducePlanFileName();
    distCacheFiles += "#";
    distCacheFiles += planFileLocal;
  }
  if (distCacheFiles.size()) {
    ret += (distCacheFormat % distCacheFiles).str();
  }
  ret += closeBoilerPlate;

  return ret;
}

std::string AdsPipesJobConf::getPipesExecutableName() const
{
  return "/a/bin/ads-df-pipes-" + mLocalPipesChecksum;
}

std::string AdsPipesJobConf::getMapPlanFileName() const
{
  return mJobDir + "/plans/map.plan.txt";
}

std::string AdsPipesJobConf::getReducePlanFileName() const
{
  return mJobDir + "/plans/reduce.plan.txt";
}

int32_t AdsPipesJobConf::copyFromLocal(const std::string& localPath,
				       const std::string& remotePath)
{
  // Just shell out to hadoop to upload file
  typedef boost::shared_ptr<PosixProcessInitializer> ppiptr;
  PosixProcessFactory hadoopJob;
  std::vector<ppiptr> v;
  v.push_back(ppiptr(new PosixPath(HadoopSetup::hadoopHome() + "/bin/hadoop")));     
  v.push_back(ppiptr(new PosixArgument("fs")));
  v.push_back(ppiptr(new PosixArgument("-copyFromLocal")));
  v.push_back(ppiptr(new PosixArgument(localPath)));
  v.push_back(ppiptr(new PosixArgument(remotePath)));
  hadoopJob.create(v);
  return hadoopJob.waitForCompletion();
}

void AdsPipesJobConf::copyPlanFileToHDFS(const std::string& plan,
					 const std::string& planFileName)
{
  TempFile tmp(plan);
  int32_t ret = copyFromLocal(tmp.getName(), planFileName);
  if (ret != 0) {
    throw std::runtime_error((boost::format("Failed to upload plan "
					    "file from %1% to %2%") % 
			      tmp.getName() % planFileName).str());
  }
}

void AdsPipesJobConf::copyPipesExecutableToHDFS()
{
  // If file doesn't yet exist in HDFS, copy it up.
  HdfsFileSystem fs ("hdfs://default:0");
  std::string pipesPath=getPipesExecutableName();
  PathPtr pipesUri (Path::get(fs.getRoot(), pipesPath));
  if (fs.exists(pipesUri)) {
    return;
  }

  int32_t ret = copyFromLocal(mLocalPipesPath.file_string(),
			      pipesPath);
  // We might fail because another process has uploaded the file.
  // Only fail if the file doesn't exist.
  // We could retry some number of times (or not).
  if (ret != 0 && !fs.exists(pipesUri)) {
    throw std::runtime_error("Failed to upload pipes executable ads-df-pipes to HDFS");
  }
}

void AdsPipesJobConf::copyFilesToHDFS()
{
  copyPipesExecutableToHDFS();
  if (mMapper.size()) {
    copyPlanFileToHDFS(mMapper, getMapPlanFileName());
  }
  if (mReducer.size()) {
    copyPlanFileToHDFS(mReducer, getReducePlanFileName());
  }
}

std::string AdsPipesJobConf::getPipesExecutableChecksum(const boost::filesystem::path & p)
{
  static const int32_t bufSz(4096);
  static const int32_t digestSz(16);
  uint8_t buf[bufSz];
  stdio_file_traits::file_type f = 
    stdio_file_traits::open_for_read(p.file_string().c_str(), 0, 
				     std::numeric_limits<uint64_t>::max());
  md5_byte_t md5digest[digestSz];
  md5_state_t md5sum;
  ::md5_init(&md5sum);
  int32_t numRead = 0;
  do {
    numRead = stdio_file_traits::read(f, &buf[0], bufSz);
    ::md5_append(&md5sum, (const md5_byte_t *) &buf[0], numRead);
  } while(numRead == bufSz);
  ::md5_finish(&md5sum, md5digest);
  stdio_file_traits::close(f);

  static const char hexDigits [] = {'0','1','2','3','4','5','6','7',
				    '8','9','a','b','c','d','e','f'};
  char output[2*digestSz];
  for(int32_t i=0; i<digestSz; i++) {
    output[2*i] = hexDigits[(md5digest[i]&0xf0) >> 4];
    output[2*i+1] = hexDigits[md5digest[i]&0x0f];
  }
  
  return std::string(&output[0], &output[32]);
}

void PlanRunner::createSerialized64PlanFromFile(const std::string& f,
						int32_t partitions,
						std::string& p)
{
  DynamicRecordContext ctxt;
  DataflowGraphBuilder gb;
  gb.buildGraphFromFile(f);  
  boost::shared_ptr<RuntimeOperatorPlan> plan = gb.create(ctxt, partitions);
  p = PlanGenerator::serialize64(plan);
}

void PlanRunner::createSerialized64MapPlan(const std::string& f,
					   int32_t partitions,
					   std::string& emitFormat,
					   std::string& p)
{
  DynamicRecordContext ctxt;
  DataflowGraphBuilder gb;
  gb.buildGraph(f);  
  boost::shared_ptr<RuntimeOperatorPlan> plan = gb.create(ctxt, partitions);
  p = PlanGenerator::serialize64(plan);
  // Now that plan is checked we can scrape up the output
  // format of any emit operator so that it can be fed to
  // a reducer.
  std::vector<LogicalEmit *> emitOp;
  gb.getPlan().getOperatorOfType(emitOp);
  if (emitOp.size() > 1) {
    throw std::runtime_error("Must be at most one emit operator in a map plan.");
  }
  if (emitOp.size() == 1) {
    emitFormat = emitOp[0]->getStringFormat();
  }
}

void PlanRunner::createSerialized64ReducePlan(const std::string& f,
					      int32_t partitions,
					      const std::string& defaultReduceFormat,
					      std::string& p)
{
  DynamicRecordContext ctxt;
  DataflowGraphBuilder gb;
  gb.buildGraph(f);  
  if (defaultReduceFormat.size()) {
    std::vector<LogicalInputQueue *> reduceOp;
    gb.getPlan().getOperatorOfType(reduceOp);
    if (reduceOp.size() != 1) {
      throw std::runtime_error("Must be exactly one reduce operator in a reduce plan.");
    }
    if (reduceOp[0]->getStringFormat().size() == 0) {
      reduceOp[0]->setStringFormat(defaultReduceFormat);
    }
  }
  boost::shared_ptr<RuntimeOperatorPlan> plan = gb.create(ctxt, partitions);
  p = PlanGenerator::serialize64(plan);
}

static bool checkRequiredArgs(const po::variables_map& vm,
			      const std::vector<std::pair<std::string, std::string> >& args)
{
  typedef std::vector<std::pair<std::string, std::string> > pairs;
  bool ok=true;
  for(pairs::const_iterator p = args.begin();
      p != args.end();
      ++p) {
    if (0==vm.count(p->first) && 0 < vm.count(p->second)) {
      std::cerr << (boost::format("Cannot use \"%1%\" option without a \"%2%\" option") %
		    p->second % p->first).str().c_str() << "\n";
      ok = false;
    }
  }
  return ok;
}

int PlanRunner::runMapReduceJob(const std::string& mapProgram,
				const std::string& reduceProgram,
				const std::string& inputDirArg,
				const std::string& outputDirArg,
				bool useHp)
{
  return runMapReduceJob(mapProgram, reduceProgram, "", "",
			 inputDirArg, outputDirArg,
			 reduceProgram.size() ? 1 : 0, true, useHp);
}

int PlanRunner::runMapReduceJob(const std::string& mapProgram,
				const std::string& reduceProgram,
				const std::string& inputDirArg,
				const std::string& outputDirArg,
				int32_t numReduces,
				bool jvmReuse,
				bool useHp)
{
  return runMapReduceJob(mapProgram, reduceProgram, "", "", inputDirArg, 
			 outputDirArg, numReduces, true, useHp);
}

int PlanRunner::runMapReduceJob(const std::string& mapProgram,
				const std::string& reduceProgram,
				const std::string& name,
				const std::string& jobQueue,
				const std::string& inputDirArg,
				const std::string& outputDirArg,
				int32_t numReduces,
				bool jvmReuse,
				bool useHp)
{
  // Create a temporary work space for this job
  boost::shared_ptr<HdfsDelete> cleanup;
  std::string jobDir = FileSystem::getTempFileName();
  jobDir = "/tmp/ads_df/" + jobDir;
  std::string uri("hdfs://default:0");
  uri += jobDir;
  cleanup = boost::make_shared<HdfsDelete>(uri);
  
  std::string inputDir;
  if (inputDirArg.size() == 0) {
    inputDir = "/1_2048/serials";
  } else {
    inputDir = inputDirArg;
  }
  std::string outputDir;
  if (outputDirArg.size() == 0) {
    outputDir = jobDir + "/output";
  } else {
    outputDir = outputDirArg;
  }

  // Set up job conf and fork/exec hadoop to run
  AdsPipesJobConf jobConf(jobDir);
  jobConf.setName(name);
  jobConf.setJobQueue(jobQueue);

  std::string mapBuf;
  std::string emitFormat;
  // TODO: Infer the number of serials from a conf file or from
  // the input directory?
  static const int32_t serials(2048);
  createSerialized64MapPlan(mapProgram, serials, emitFormat, mapBuf);
  jobConf.setMapper(mapBuf);
  if(reduceProgram.size()) {    
    std::string reduceBuf;
    createSerialized64ReducePlan(reduceProgram, numReduces, emitFormat, reduceBuf);
    jobConf.setReducer(reduceBuf,numReduces);
  }
  jobConf.setJvmReuse(jvmReuse);

  // Make sure we have all necessary files uploaded to HDFS
  jobConf.copyFilesToHDFS();

  // Create the job configuration temp file and run Hadoop against it.
  TempFile jobConfFile(jobConf.get());
  typedef boost::shared_ptr<PosixProcessInitializer> ppiptr;
  PosixProcessFactory hadoopJob;
  std::vector<ppiptr> v;
  if (!useHp) {
    v.push_back(ppiptr(new PosixPath(HadoopSetup::hadoopHome() + "/bin/hadoop")));
  } else {
    v.push_back(ppiptr(new PosixPath("/usr/local/akamai/bin/ads-hp-client")));       
    v.push_back(ppiptr(new PosixArgument("job")));
    v.push_back(ppiptr(new PosixArgument("hadoop")));
  }
  v.push_back(ppiptr(new PosixArgument("pipes")));
  v.push_back(ppiptr(new PosixArgument("-conf")));
  v.push_back(ppiptr(new PosixArgument(jobConfFile.getName())));
  v.push_back(ppiptr(new PosixArgument("-input")));
  v.push_back(ppiptr(new PosixArgument(inputDir)));
  v.push_back(ppiptr(new PosixArgument("-output")));
  v.push_back(ppiptr(new PosixArgument(outputDir)));
  v.push_back(ppiptr(new PosixParentEnvironment()));
  //TODO: Support some command line config of what to do with stdin/out/err.
  hadoopJob.create(v);
  return hadoopJob.waitForCompletion();
}

GdbStackTrace::GdbStackTrace()
{
  // Find gdb64 or gdb
  std::string gdb64("/usr/bin/gdb64");
  std::string gdb("/usr/bin/gdb");
  if(boost::filesystem::exists(gdb64) &&
     boost::filesystem::is_regular_file(gdb64)) {
    mInitializers.push_back(ppiptr(new PosixPath(gdb64)));
  } else if(boost::filesystem::exists(gdb) &&
	    boost::filesystem::is_regular_file(gdb)) {
    mInitializers.push_back(ppiptr(new PosixPath(gdb)));
  }
  if (mInitializers.size()) {
    mInitializers.push_back(ppiptr(new PosixArgument("--batch")));
    mInitializers.push_back(ppiptr(new PosixArgument("-ex")));
    mInitializers.push_back(ppiptr(new PosixArgument("set pagination off")));
    mInitializers.push_back(ppiptr(new PosixArgument("-ex")));
    mInitializers.push_back(ppiptr(new PosixArgument("thread apply all bt full")));
    mInitializers.push_back(ppiptr(new PosixArgument("-ex")));
    mInitializers.push_back(ppiptr(new PosixArgument("detach")));
    mInitializers.push_back(ppiptr(new PosixArgument("-ex")));
    mInitializers.push_back(ppiptr(new PosixArgument("quit")));
    mInitializers.push_back(ppiptr(new PosixArgument("-s")));
    mInitializers.push_back(ppiptr(new PosixArgument(Executable::getPath().file_string())));
    mInitializers.push_back(ppiptr(new PosixArgument("-p")));
    mInitializers.push_back(ppiptr(new PosixArgument(boost::lexical_cast<std::string>(::getpid()))));
  }
}

GdbStackTrace::~GdbStackTrace()
{
}

int32_t GdbStackTrace::generate()
{
  PosixProcessFactory f;
  boost::system::error_code ec;
  if (mInitializers.size()) {
    f.create(mInitializers);
    return f.waitForCompletion(ec);
  } else {
    return -1;
  }
}

static GdbStackTrace st;
static void onSigsegv(int )
{
  st.generate();
  abort();
}

int PlanRunner::run(int argc, char ** argv)
{
  // Install signal handlers
  struct sigaction action;
  action.sa_flags = SA_RESTART;
  sigemptyset(&action.sa_mask);
  action.sa_handler = &onSigsegv;
  if (sigaction(SIGSEGV, &action, NULL) < 0) {
    std::cerr << "Sigaction: " << strerror(errno) << ". Exiting." << std::endl;
    return 1;
  }
  
  // Make sure this symbol can be dlsym'd
  // TODO: I think a better way to do this is to export a pointer to the function
  // from the module.
  int dummy=9923;
  SuperFastHash((char *) &dummy, sizeof(int), sizeof(int));
  
  // If a Hadoop installation is present, then setup appropriate env.
  HadoopSetup::setEnvironment();

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("compile", "generate dataflow plan but don't run")
    ("serial", po::value<int32_t>(), "specific partition against which to run a dataflow")
    ("partitions", po::value<int32_t>(), "number of partitions for the flow")
    ("plan", "run dataflow from a compiled plan")
    ("nojvmreuse", "suppress JVM reuse in map reduce programs")
    ("file", po::value<std::string>(), "input script file to be run in process")
    ("name", po::value<std::string>(), "job name to run as")
    ("map", po::value<std::string>(), "input mapper script file for jobs run through Hadoop pipes")
    ("reduce", po::value<std::string>(), "input reducer script file for jobs run through Hadoop pipes")
    ("numreduces", po::value<int32_t>(), "number of reducers")
    ("input", po::value<std::string>(), "input directory for jobs run through Hadoop pipes")
    ("output", po::value<std::string>(), "output directory for jobs run through Hadoop pipes")
    ("jobqueue", po::value<std::string>(), "job queue for jobs run through Hadoop pipes")
    ("proxy", "use proxy ads-hp-client for jobs run through Hadoop pipes")
    ;

  po::variables_map vm;        
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);    
  
  if (vm.count("help") ||
      (0 == vm.count("file") && 0 == vm.count("map"))) {
    std::cerr << desc << "\n";
    return 1;
  }

  // Validation steps
  if (vm.count("file") && (vm.count("map") || vm.count("reduce"))) {
    std::cerr << "Cannot use both \"file\" and \"map\"/\"reduce\" options" << std::endl;
    std::cerr << desc << std::endl;
    return 1;    
  }
  std::vector<std::pair<std::string,std::string> > pairs;
  pairs.push_back(std::make_pair("file", "compile"));
  pairs.push_back(std::make_pair("file", "plan"));
  pairs.push_back(std::make_pair("map", "reduce"));
  pairs.push_back(std::make_pair("map", "input"));
  pairs.push_back(std::make_pair("map", "output"));
  pairs.push_back(std::make_pair("map", "proxy"));
  pairs.push_back(std::make_pair("map", "nojvmreuse"));
  pairs.push_back(std::make_pair("map", "jobqueue"));
  if (!checkRequiredArgs(vm, pairs)) {
    std::cerr << desc << std::endl;
    return 1;    
  }

  if (vm.count("compile")) {
    std::string inputFile(vm["file"].as<std::string>());
    int32_t partitions=1;
    if (vm.count("partitions")) {
      partitions = vm["partitions"].as<int32_t>();
    }
    std::string buf;
    createSerialized64PlanFromFile(inputFile, partitions, buf);
    std::cout << buf.c_str();
    return 0;
  } else if (vm.count("plan")) {
    Timer t(0);
    std::string inputFile(vm["file"].as<std::string>());
    checkRegularFileExists(inputFile);
    int32_t partition=0;
    if (vm.count("serial")) {
      partition = vm["serial"].as<int32_t>();
    }
    int32_t partitions=1;
    if (vm.count("partitions")) {
      partitions = vm["partitions"].as<int32_t>();
    }
    std::size_t sz = boost::filesystem::file_size(inputFile);
    std::ifstream istr(inputFile.c_str());
    std::vector<char> encoded(sz);
    istr.read(&encoded[0], sz);

    boost::shared_ptr<RuntimeOperatorPlan> tmp = PlanGenerator::deserialize64(&encoded[0] ,
									      encoded.size());
    RuntimeProcess p(partition,partition,partitions,*tmp.get());
    p.run();
    return 0;
  } else if (vm.count("map")) {    
    bool useHp(vm.count("proxy") > 0);
    bool jvmReuse(vm.count("nojvmreuse") == 0);
    std::string inputDir(vm.count("input") ? 
			 vm["input"].as<std::string>().c_str() : 
			 "/1_2048/serials");
    std::string outputDir(vm.count("output") ? 
			  vm["output"].as<std::string>().c_str() : 
			  "");
    std::string jobQueue(vm.count("jobqueue") ? 
			 vm["jobqueue"].as<std::string>().c_str() : 
			 "");

    std::string mapProgram;
    readInputFile(vm["map"].as<std::string>(), mapProgram);

    std::string reduceProgram;
    int32_t reduces=0;
    if(vm.count("reduce")) {
      readInputFile(vm["reduce"].as<std::string>(), reduceProgram);
      if (vm.count("numreduces")) {
	reduces = vm["numreduces"].as<int32_t>();
      } else {
	reduces = 1;
      }
    }

    std::string name;
    if (vm.count("name")) {
      name = vm["name"].as<std::string>();
    } else {
      std::string map = vm["map"].as<std::string>();
      name += map + "-mapper";
      if (vm.count("reduce")) {
	name += "," + vm["reduce"].as<std::string>() + "-reducer";
      }
    }

    return runMapReduceJob(mapProgram,
			   reduceProgram,
			   name,
			   jobQueue,
			   inputDir,
			   outputDir,
			   reduces,
			   jvmReuse,
			   useHp);
  } else {
    std::string inputFile(vm["file"].as<std::string>());
    checkRegularFileExists(inputFile);
    int32_t partitions=1;
    if (vm.count("partitions")) {
      partitions = vm["partitions"].as<int32_t>();
    }
    int32_t partition=0;
    if (vm.count("serial")) {
      partition = vm["serial"].as<int32_t>();
    }
    // Make sure partitions is at least as large as partition+1;
    if (partition >= partitions)
      partitions = partition+1;
    DataflowGraphBuilder gb;
    gb.buildGraphFromFile(inputFile);
    DynamicRecordContext ctxt;
    boost::shared_ptr<RuntimeOperatorPlan> plan = gb.create(ctxt, partitions);
    RuntimeProcess p(partition,partition,partitions,*plan.get());
    p.run();
    return 0;
  }
}

#include <sys/wait.h>

ProcessPipe::ProcessPipe()
{
  int pipe_fd[2];
  int error = ::pipe(pipe_fd);
  if (error == -1) {
    boost::system::system_error err(boost::system::error_code(errno,
							      boost::system::get_system_category()),
				    "ProcessPipe::ProcessPipe: pipe failed");
  }

  // Create the fd devices and have them close the
  // fd on d'tor.
  mSink.open(pipe_fd[1], true);
  mSource.open(pipe_fd[0], true);
}

PosixProcessFactory::PosixProcessFactory()
{
}

void PosixProcessFactory::onPreForkParent(const inits_type& initializers) 
{
  for(inits_iterator_type i = initializers.begin(); 
      i != initializers.end(); 
      ++i) {
    (*i)->onPreForkParent(*this);
  }
}  

void PosixProcessFactory::onPostForkParent(const inits_type& initializers) 
{
  for(inits_iterator_type i = initializers.begin(); 
      i != initializers.end(); 
      ++i) {
    (*i)->onPostForkParent(*this);
  }
}  

void PosixProcessFactory::onPostForkChild(const inits_type& initializers) 
{
  for(inits_iterator_type i = initializers.begin(); 
      i != initializers.end(); 
      ++i) {
    (*i)->onPostForkChild(*this);
  }

  // TODO: Possibly add PATH search.
  ::execve(mExe.file_string().c_str(), mArgs.get(), mEnvVars.get());
  // Only get here with failed exec.
  onFailedExecChild(initializers);
}  

void PosixProcessFactory::onFailedForkParent(const inits_type& initializers) 
{
  for(inits_iterator_type i = initializers.begin(); 
      i != initializers.end(); 
      ++i) {
    (*i)->onFailedForkParent(*this);
  }
}  

void PosixProcessFactory::onFailedExecChild(const inits_type& initializers) 
{
  for(inits_iterator_type i = initializers.begin(); 
      i != initializers.end(); 
      ++i) {
    (*i)->onFailedExecChild(*this);
  }
}  

void PosixProcessFactory::create(const std::vector<init_type>& initializers)
{
  onPreForkParent(initializers);
  switch(mPid = ::fork()) {
  case 0: 
    onPostForkChild(initializers); 
    return;
  case -1: 
    onFailedForkParent(initializers); 
    return;
  default:
    onPostForkParent(initializers); 
    return;
  }
}

int32_t PosixProcessFactory::waitForCompletion(boost::system::error_code & ec)
{
  do {
    int status;
    int err = ::waitpid(mPid, &status, 0);
    if (err == mPid) {
      if (WIFEXITED(status)) {
	ec.clear();
	return WEXITSTATUS(status);
      } else if (WIFSIGNALED(status)) {
	ec.clear();
	return 256 + WTERMSIG(status);
      } else {
	// stop or continue of process.  keep waiting.
	continue;
      }
    } else if (err == -1 && errno == EINTR) {
      continue;
    } else if (err == -1 && errno == ECHILD) {
      // Ugh.  Someone must have SIG_IGN'd SIGCHLD so
      // the OS isn't keeping children around for us to reap.
      // In this case, it's a crap shoot what the exit
      // status of the child is.      
      // Here we're going to be pessimistic and assume
      // failure because we really want the developer to
      // track down what is going on here...
      return -773433;
    } else {
      // Bad news:  waitpid failed some bad reason.
      ec = boost::system::error_code(errno,
				     boost::system::get_system_category());
      return -1;
    }
  } while(true);
  
  return -1;
}

int32_t PosixProcessFactory::waitForCompletion()
{
  boost::system::error_code ec;
  int32_t ret = waitForCompletion(ec);
  if (ec) {
    boost::system::system_error e(ec);
    BOOST_THROW_EXCEPTION(e);
  }
  return ret;
}

PosixPath::PosixPath(const boost::filesystem::path& exe)
  :
  mPath(exe),
  mPathChars(exe.file_string())
{
}

void PosixPath::onPreForkParent(PosixProcessFactory& parent)
{
  parent.mExe = mPath;
  parent.mArgs.setExe(&mPathChars[0]);
}

PosixArgument::PosixArgument(const std::string& arg)
  :
  mArg(arg)
{
}

void PosixArgument::onPreForkParent(PosixProcessFactory& parent)
{
  parent.mArgs.push_back(&mArg[0]);
}

PosixArguments::PosixArguments(const std::string& arg)
  :
  mArgs(1, arg)
{
}

PosixArguments::PosixArguments(const std::string& arg1,
			       const std::string& arg2)
{
  mArgs.push_back(arg1);
  mArgs.push_back(arg2);
}

PosixArguments::PosixArguments(const std::string& arg1,
			       const std::string& arg2,
			       const std::string& arg3)
{
  mArgs.push_back(arg1);
  mArgs.push_back(arg2);
  mArgs.push_back(arg3);
}

PosixArguments& PosixArguments::operator() (const PosixArgument& arg)
{
  mArgs.push_back(arg);
  return *this;
}

void PosixArguments::onPreForkParent(PosixProcessFactory& parent)
{
  for(args_type::iterator a = mArgs.begin();
      a != mArgs.end();
      ++a) {
    a->onPreForkParent(parent);
  }
}

StandardInFrom::StandardInFrom(ProcessPipe& p)
  :
  mPipe(p)
{
}

void StandardInFrom::onPostForkParent(PosixProcessFactory& parent)
{
  // Parent cannot read from this.
  mPipe.mSource.close();
}

void StandardInFrom::onPostForkChild(PosixProcessFactory& parent)
{
  // Child reads from this on stdin but cannot write to it.
  ::dup2(mPipe.mSource.handle(), STDIN_FILENO);
  mPipe.mSink.close();
}

StandardOutTo::StandardOutTo(ProcessPipe& p, bool isStdErr)
  :
  mPipe(p),
  mIsStdErr(isStdErr)
{
}

void StandardOutTo::onPostForkParent(PosixProcessFactory& parent)
{
  // Parent can't write to this
  mPipe.mSink.close();
}

void StandardOutTo::onPostForkChild(PosixProcessFactory& parent)
{
  // Child writes to this on stdout/err but cannot read from it.
  ::dup2(mPipe.mSink.handle(), mIsStdErr ? STDERR_FILENO : STDOUT_FILENO);
  mPipe.mSource.close();
}

PosixParentEnvironment::PosixParentEnvironment()
{
}

void PosixParentEnvironment::onPreForkParent(PosixProcessFactory& parent)
{
  for(char ** var = environ; *var != NULL; ++var) {
    parent.mEnvVars.push_back(*var);
  }
}

PosixEnvironmentVariable::PosixEnvironmentVariable(const std::string& nm,
						   const std::string& val)
{
  mEnvVar = nm+"="+val;
}

void PosixEnvironmentVariable::onPreForkParent(PosixProcessFactory& parent)
{
  parent.mEnvVars.push_back(const_cast<char*>(mEnvVar.c_str()));
}
