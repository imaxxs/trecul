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

#ifndef __HDFSOPERATOR_H
#define __HDFSOPERATOR_H

#include <boost/shared_ptr.hpp>
#include "FileSystem.hh"
#include "RuntimeOperator.hh"
#include "FileWriteOperator.hh"

class FileChunk;
namespace HadoopPipes {
  class TaskContext;
};

/**
 * An operation to delete an HDFS file on destruction.
 */
class HdfsDelete
{
private:
  std::string mPath;
public:
  HdfsDelete(const std::string& path);
  ~HdfsDelete();
};

class HdfsFileSystem : public FileSystem
{
private:
  // Use pimpl idiom to hide HDFS interface.
  class HdfsFileSystemImpl * mImpl;
public:
  HdfsFileSystem(const std::string& uri);
  HdfsFileSystem(UriPtr uri);
  ~HdfsFileSystem();

  /**
   * Glob a file and try to distribute the results 
   * evently among numPartitions partitions.
   */
  void expand(std::string pattern,
	      int32_t numPartitions,
	      std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files);

  /**
   * Get the root of the file system.
   * Should this be a URI or a PathPtr?
   */
  PathPtr getRoot();

  /**
   * Get information about a path.
   */
  virtual boost::shared_ptr<FileStatus> getStatus(PathPtr p);

  /**
   * Does a path exists?
   */
  virtual bool exists(PathPtr p);

  /**
   * Recursively delete a path.
   */
  virtual bool removeAll(PathPtr p);

  /**
   * Get a directory listing of a path that isDirectory.
   */
  virtual void list(PathPtr p,
		    std::vector<boost::shared_ptr<FileStatus> >& result);

  /**
   * Read the contents of a file into a std::string.
   */
  virtual void readFile(UriPtr uri, std::string& out);
};

class hdfs_file_traits
{
public: 
  typedef class hdfs_file_handle * file_type;

  // Split into desired number of partitions.
  static void expand(std::string pattern, 
		     int32_t numPartitions,
		     std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files);
  static file_type open_for_read(const char * filename, uint64_t beginOffset, uint64_t endOffset);
  static void close(file_type f);
  static int32_t read(file_type f, uint8_t * buf, int32_t bufSize);
  static bool isEOF(file_type f);
};

class RuntimeHdfsWriteOperatorType : public RuntimeOperatorType
{
  friend class RuntimeHdfsWriteOperator;
private:
  RecordTypePrint mPrint;
  RecordTypeFree mFree;
  std::string mHdfsHost;
  int32_t mHdfsPort;
  std::string mHdfsFile;
  int32_t mBufferSize;
  int32_t mReplicationFactor;
  int32_t mBlockSize;
  std::string mHeader;
  std::string mHeaderFile;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mPrint);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mHdfsHost);
    ar & BOOST_SERIALIZATION_NVP(mHdfsPort);
    ar & BOOST_SERIALIZATION_NVP(mHdfsFile);
    ar & BOOST_SERIALIZATION_NVP(mBufferSize);
    ar & BOOST_SERIALIZATION_NVP(mReplicationFactor);
    ar & BOOST_SERIALIZATION_NVP(mBlockSize);
    ar & BOOST_SERIALIZATION_NVP(mHeader);
    ar & BOOST_SERIALIZATION_NVP(mHeaderFile);
  }
  RuntimeHdfsWriteOperatorType()
  {
  }
public:
  RuntimeHdfsWriteOperatorType(const std::string& opName,
			       const RecordType * ty, 
			       const std::string& hdfsHost, 
			       int32_t port, 
			       const std::string& hdfsFile,
			       const std::string& header,
			       const std::string& headerFile,
			       int32_t bufferSize=0, 
			       int32_t replicationFactor=0, 
			       int32_t blockSize=0)
    :
    RuntimeOperatorType(opName.c_str()),
    mPrint(ty->getPrint()),
    mFree(ty->getFree()),
    mHdfsHost(hdfsHost),
    mHdfsPort(port),
    mHdfsFile(hdfsFile),
    mBufferSize(bufferSize),
    mReplicationFactor(replicationFactor),
    mBlockSize(blockSize),
    mHeader(header),
    mHeaderFile(headerFile)
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& services) const;
};

class LogicalEmit : public LogicalOperator
{
private:
  std::string mKey;
public:
  LogicalEmit();
  ~LogicalEmit();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
  std::string getStringFormat() const 
  {
    if (getInput(0) == NULL ||
	getInput(0)->getRecordType() == NULL) {
      throw std::runtime_error("getStringFormat requires LogicalEmit::check is called");
    }
    return getInput(0)->getRecordType()->dumpTextFormat();
  }
};

class RuntimeHadoopEmitOperatorType : public RuntimeOperatorType
{
  friend class RuntimeHadoopEmitOperator;
private:
  RecordTypePrint mPrint;
  RecordTypeFree mFree;
  RecordTypePrint mKey;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mPrint);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mKey);
  }
  RuntimeHadoopEmitOperatorType()
  {
  }
public:
  RuntimeHadoopEmitOperatorType(const std::string& opName,
				const RecordType * ty, 
				const std::string& keyField)
    :
    RuntimeOperatorType(opName.c_str()),
    mPrint(ty->getPrint()),
    mFree(ty->getFree()),
    mKey(TaggedFieldAddress(ty->getFieldAddress(keyField),
			    ty->getMember(keyField).GetType()->GetEnum()))
  {
  }
  RuntimeOperator * create(RuntimeOperator::Services& services) const;
};

class RuntimeHadoopEmitOperator : public RuntimeOperator
{
public:
  void writeToHdfs(RecordBuffer input, bool isEOS);  

private:
  enum State { START, READ };
  State mState;
  const RuntimeHadoopEmitOperatorType &  getHadoopEmitType()
  {
    return *reinterpret_cast<const RuntimeHadoopEmitOperatorType *>(&getOperatorType());
  }
  HadoopPipes::TaskContext * mContext;
  RuntimePrinter mKeyPrinter;
  RuntimePrinter mValuePrinter;
public:
  RuntimeHadoopEmitOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType);
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();

  /**
   * set the pipes context so that the operator can emit.
   * This must be called before the flow is started.
   */
  void setContext(HadoopPipes::TaskContext * ctxt)
  {
    mContext = ctxt;
  }
};

#endif
