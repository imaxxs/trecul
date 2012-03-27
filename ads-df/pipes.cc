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

#include <iostream>
#include <stdexcept>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/tokenizer.hpp>
#include "Pipes.hh"
#include "TemplateFactory.hh"
#include "StringUtils.hh"
#include "SerialUtils.hh"
#include "RuntimeProcess.hh"
#include "HdfsOperator.hh"
#include "QueueImport.hh"
#include "GraphBuilder.hh"
#include "FileSystem.hh"

class DataflowMapReducer
{
private:
  boost::shared_ptr<RuntimeProcess> mProcess;
  NativeInputQueueOperator * mInput;
  int32_t mRecordsQueued;

  /**
   * Have the dataflow scheduler yield back to us
   * after this many iterations.  We need it to do
   * this so we can ping the Java mapper to let it
   * know we are alive.  In practice this number causes
   * the scheduler to yield every few seconds which is
   * enough to amortize overhead but more than enough to
   * keep us alive (heck a ping every 10 minutes would
   * be enough for that).
   */
  static const int64_t YIELD_AFTER = 100000;
public:
  DataflowMapReducer(int32_t partition,
		     int32_t numPartitions,
		     const RuntimeOperatorPlan& plan,
		     HadoopPipes::TaskContext & context,
		     bool isMap);
  void map(HadoopPipes::TaskContext& ctxt,
	   const std::string& val);
  void close();
  /**
   * Read a plan file (either mapper or reducer).  We have
   * arranged for Hadoop to symlink these into the current
   * working directory of the map reduce task.
   */
  static void getEncodedPlan(const std::string& planFileName,
			     std::string& encoded);
};

DataflowMapReducer::DataflowMapReducer(int32_t partition,
				       int32_t numPartitions,
				       const RuntimeOperatorPlan& plan,
				       HadoopPipes::TaskContext & context,
				       bool isMap)
  : 
  mInput(NULL), 
  mRecordsQueued(0)
{
  mProcess = boost::make_shared<RuntimeProcess>(partition,partition,
						numPartitions, plan);

  // Find the input operator (if any) for the plan
  std::vector<NativeInputQueueOperator*> ops;
  mProcess->getOperatorOfType<>(ops);

  // Find the input operator to feed data to.
  static const char * reduceMsg = "Invalid plan.  Dataflow reduce plans must "
    "contain exactly one reduce operator";
  static const char * mapMsg = "Invalid plan.  Dataflow map plans must "
    "contain at most one map operator";
  if (ops.size() > 1) {
    throw std::runtime_error(isMap ? mapMsg : reduceMsg);
  } else if (ops.size() == 1) {
    mInput = ops[0];
  } else if (!isMap) {
    throw std::runtime_error(reduceMsg);
  }

  // Attach all emitters to pipes output context
  std::vector<RuntimeHadoopEmitOperator*> emitOps;
  mProcess->getOperatorOfType<>(emitOps);
  if (emitOps.size() > 1) {
    throw std::runtime_error("Invalid plan.  Dataflow map/reduce plans "
			     "can have at most one emit operator");
  } else if (emitOps.size() == 1) {
    emitOps[0]->setContext(&context);
  }

  std::cout << "Calling RuntimeProcess::runInit" << std::endl;
  mProcess->runInit();
  std::cout << "Finished calling RuntimeProcess::runInit" << std::endl;
}

void DataflowMapReducer::getEncodedPlan(const std::string& planFileName,
					std::string& encoded)
{
  // Map and reduce plans come down to us thorough the
  // distributed cache.  We tell Hadoop to create symlinks
  // so the files are in a predictable place.
  // I'm not particularly happy about the assumption that
  // things will be in the current working directory but
  // that is in fact how Hadoop documents the behavior of
  // symlinking the distributed cache and it doesn't seem to
  // put the directory in the job configuration anywhere.
  std::string planFile((boost::format("file://%1%/%2%") %
			boost::filesystem::current_path().directory_string() 
			% planFileName).str());
  PathPtr planPath = Path::get(planFile);
  AutoFileSystem fs(planPath->getUri());
  if (!fs->exists(planPath)) {
    throw std::runtime_error((boost::format("Dataflow plan %1% not found") 
			      % planPath->toString()).str());
  }
  fs->readFile(planPath->getUri(), encoded);
}

void DataflowMapReducer::map(HadoopPipes::TaskContext & ctxt,
			     const std::string& val) 
{
  typedef NativeInputQueueOperatorType::field_importer_const_iterator iterator;
  if (mInput) {
    StringDataBlock blk;
    blk.bindString(val);
    const NativeInputQueueOperatorType& opTy(mInput->getLogParserType());
    RecordBuffer buf = opTy.mMalloc.malloc();
    for(iterator it = opTy.begin_importers();
	it != opTy.end_importers();
	++it) {
      if (! it->Import(blk, buf)) {
	throw std::runtime_error((boost::format("Invalid input record to %1%:"
						"\n%2%\n") 
				  % "map" % val.c_str()).str());
      }
    }
    mInput->getQueue().push(buf);
    mRecordsQueued += 1;
    if (mRecordsQueued >= 140) {
      // std::cout << "Calling RuntimeProcess::runSome(); recordsQueued=" << 
      //   mRecordsQueued << std::endl;
      mRecordsQueued = 0;
      mInput->onEvent(NULL);
      bool ret = mProcess->runSome();
      // runSome should only return when input queue is exhausted
      if (ret || !mInput->getQueue().empty())
	throw std::runtime_error((boost::format("Logic error reduce: "
						"Q.size()=%1%; ret=%2%") %
				  mInput->getQueue().Q.size() %
				  ret).str());
      // std::cout << "Finished calling RuntimeProcess::runSome" << std::endl;
    }
  } else if (mProcess.get() != NULL) {
    // Ignore the inputs (they are just being used to kick off
    // an appropriate number of mappers).  In this case, the
    // dataflow is probably adapting itself to the partition number
    // of the map task.
    // In this case, make sure that we provide liveness notification
    // back to Hadoop since otherwise it will kill us after 10 minutes.
    std::cout << "Calling RuntimeProcess::runSome()" << std::endl;
    DataflowScheduler::RunCompletion ret;
    do {
      ret = mProcess->runSome(YIELD_AFTER);
      ctxt.progress();
    } while(ret == DataflowScheduler::MAX_ITERATIONS_REACHED);
    std::cout << "Calling RuntimeProcess::runComplete()" << std::endl;
    mProcess->runComplete();
    std::cout << "Finished calling RuntimeProcess::runComplete()" << std::endl;
    mProcess.reset();
  }
}

void DataflowMapReducer::close()
{
  if (mInput && mProcess.get() != NULL) {
    std::cout << "Calling RuntimeProcess::runSome() close" << std::endl;
    // Send EOS into the flow and then join with the dataflow thread.
    mInput->getQueue().push(RecordBuffer());
    mInput->onEvent(NULL);
    bool ret = mProcess->runSome();
    std::cout << "Finished calling RuntimeProcess::runSome" << std::endl;
    // runSome should only return when input queue is exhausted
    // in this case return should be true to signal that the flow
    // has completed (since we sent in EOS).
    if (!ret || !mInput->getQueue().empty())
      throw std::runtime_error((boost::format("Logic error close: "
					      "Q.size()=%1%; ret=%2%") %
				mInput->getQueue().Q.size() %
				ret).str());
    std::cout << "Calling RuntimeProcess::runComplete() close" << std::endl;
    mProcess->runComplete();
    std::cout << "Finished calling RuntimeProcess::runComplete() close" << std::endl;
  }
}

class DataflowMap: public HadoopPipes::Mapper {
private:
  static int64_t readLong(const uint8_t * buf) {
    return (int64_t) (
		      (((uint64_t) buf[0]) << 56) |
		      (((uint64_t) buf[1]) << 48) |
		      (((uint64_t) buf[2]) << 40) |
		      (((uint64_t) buf[3]) << 32) |
		      (((uint64_t) buf[4]) << 24) |
		      (((uint64_t) buf[5]) << 16) |
		      (((uint64_t) buf[6]) << 8) |
		      (((uint64_t) buf[7])) 
		      );
  }
  boost::shared_ptr<RuntimeOperatorPlan> tmp;
  boost::shared_ptr<DataflowMapReducer> mMapReducer;

  static bool hasInput(boost::shared_ptr<RuntimeOperatorPlan> p)
  {
    // Find the input operator (if any) for the plan
    std::vector<const NativeInputQueueOperatorType*> ops;
    p->getOperatorOfType<>(ops);
    return ops.size() > 0;
  }
public:
  
  DataflowMap(HadoopPipes::MapContext& context) {
    std::string filename;

    // Deserialize a FileSplit
    HadoopUtils::StringInStream stream(context.getInputSplit());
    HadoopUtils::deserializeString(filename, stream);
    // TODO: I don't think these start/end values for the split are actually
    // coming in.  This may be a change in 0.21.0 but I am not
    // sure.  We don't need them so it isn't a big deal but I am
    // curious.
    // uint8_t buf[8];
    // stream.read(&buf[0], 8);
    // start = readLong(&buf[0]);
    // stream.read(&buf[0], 8);
    // length = readLong(&buf[0]);
    
    // A dataflow mapper must have a plan 
    static const char * planProperty = "com.akamai.ads.dataflow.mapper.plan";
    static const char * numMapsProperty = "mapreduce.job.maps";
    const std::string& strNumPartitions(context.getJobConf()->get(numMapsProperty));
    int32_t numPartitions = boost::lexical_cast<int32_t>(strNumPartitions);    
    if (!context.getJobConf()->hasKey(planProperty)) {
      throw std::runtime_error("Dataflow map job requires a com.akamai.ads.dataflow.mapper.plan in job configuration");
    }

    const std::string& planFile(context.getJobConf()->get(planProperty));

    std::string encoded;
    DataflowMapReducer::getEncodedPlan(planFile, encoded);

    tmp = PlanGenerator::deserialize64(&encoded[0] ,
				       encoded.size());

    int32_t partition = 0;
    if (hasInput(tmp)) {
      static const char * taskPartitionProperty = "mapreduce.task.partition";
      const std::string& strTaskPartition(context.getJobConf()->get(taskPartitionProperty));
      partition = boost::lexical_cast<int32_t>(strTaskPartition);    
    } else {
      // Filename is an HDFS URI.  The file will have underscore delimited
      // structure and the trailing component is a 5 digit number (padded
      // with leading zeros).  This final number
      // tells us what serial number we are processing.
      // TODO: Should we be more demanding about the format of this 
      // file?
      // TODO: Encapsulate serial structure in to a class.
      URI serialFile(filename.c_str());
      boost::filesystem::path fsPath(serialFile.getPath());
      typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
      boost::char_separator<char> sep("_");
      tokenizer tok(fsPath.filename(), sep);
      if (tok.begin() == tok.end()) {
	throw std::runtime_error((boost::format("Invalid serial file name: %1%") %
				  filename).str());
      }
      // Tokenizer iterator has little to offer in functionality, 
      // so just find the last token by scanning and then cast.
      std::string last;
      for(tokenizer::iterator tokIt = tok.begin();
	  tokIt != tok.end();
	  ++tokIt) {
	last = *tokIt;
      }
      try {
	partition = boost::lexical_cast<int32_t>(last);    
      } catch(boost::bad_lexical_cast& ) {
	throw std::runtime_error((boost::format("Invalid serial file name: %1%") %
				  filename).str());
      }
    }
    
    DataflowMapReducer * raw = new DataflowMapReducer(partition, 
						      numPartitions,
						      *tmp.get(),
						      context,
						      true);
    mMapReducer = boost::shared_ptr<DataflowMapReducer>(raw);
  }
  
  void map(HadoopPipes::MapContext& context) {
    mMapReducer->map(context, context.getInputValue());
  }

  void close() {
    mMapReducer->close();
  }
};

class DataflowReduce: public HadoopPipes::Reducer {
private:
  boost::shared_ptr<RuntimeOperatorPlan> tmp;
  boost::shared_ptr<DataflowMapReducer> mMapReducer;
public:
  DataflowReduce(HadoopPipes::TaskContext& context) {
    // A dataflow mapper must have a plan 
    static const char * planProperty = "com.akamai.ads.dataflow.reducer.plan";
    static const char * numReducesProperty = "mapreduce.job.reduces";
    static const char * taskPartitionProperty = "mapreduce.task.partition";
    
    if (!context.getJobConf()->hasKey(planProperty)) {
      throw std::runtime_error("Dataflow reduce job requires a com.akamai.ads.dataflow.reducer.plan in job configuration");
    }
    const std::string& planFile(context.getJobConf()->get(planProperty));
    const std::string& strNumReduces(context.getJobConf()->get(numReducesProperty));
    const std::string& strTaskPartition(context.getJobConf()->get(taskPartitionProperty));
    
    std::string encoded;
    DataflowMapReducer::getEncodedPlan(planFile, encoded);
    std::cout << "============ Encoded Plan ==============";    
    std::cout << encoded.c_str() << std::endl;
    std::cout << "==========================" << std::endl;
    std::cout << "NumReduces=" << strNumReduces.c_str() << std::endl;
    std::cout << "TaskPartition=" << strTaskPartition.c_str() << std::endl;

    int32_t partition = boost::lexical_cast<int32_t>(strTaskPartition);
    int32_t numPartitions = boost::lexical_cast<int32_t>(strNumReduces);

    tmp = PlanGenerator::deserialize64(&encoded[0] ,
				       encoded.size());
    DataflowMapReducer * raw = new DataflowMapReducer(partition, 
						      numPartitions,
						      *tmp.get(),
						      context,
						      false);
    mMapReducer = boost::shared_ptr<DataflowMapReducer>(raw);
  }

  void reduce(HadoopPipes::ReduceContext& context) {
    while (context.nextValue()) {
      mMapReducer->map(context, context.getInputValue());
    }   
  }
  void close() {
    mMapReducer->close();
  }
};

static GdbStackTrace st;
static void onSigsegv(int )
{
  st.generate();
  abort();
}

int main(int argc, char *argv[]) {
  // Install signal handlers
  struct sigaction action;
  action.sa_flags = SA_RESTART;
  sigemptyset(&action.sa_mask);
  action.sa_handler = &onSigsegv;
  if (sigaction(SIGSEGV, &action, NULL) < 0) {
    std::cerr << "Sigaction: " << strerror(errno) << ". Exiting." << std::endl;
    return 1;
  }

  ::setenv("LIBHDFS_OPTS", "-Xmx100m", 0);
  try {
    return HadoopPipes::runTask(HadoopPipes::TemplateFactory<DataflowMap, 
				DataflowReduce>());
  } catch (HadoopUtils::Error & e) {
    std::cerr << e.getMessage().c_str() << std::endl;
    throw e;
  }
}

