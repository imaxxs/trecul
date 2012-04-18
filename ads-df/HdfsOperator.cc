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

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include "hdfs.h"
#include "Pipes.hh"
#include "HdfsOperator.hh"
#include "RecordParser.hh"

class hdfs_file_handle
{
public:
  hdfsFS FileSystem;
  hdfsFile File;
  uint64_t End;
  hdfs_file_handle()
    :
    FileSystem(NULL),
    File(NULL),
    End(std::numeric_limits<uint64_t>::max())
  {
  }

  static int32_t write(hdfsFS fs, hdfsFile f, const void * buf, int32_t sz)
  {
    tSize ret = ::hdfsWrite(fs, f, buf, sz);
    if (ret == -1) {
      throw std::runtime_error("hdfsWrite failed");
    }
    return (int32_t) ret;
  }
  static void flush(hdfsFS fs, hdfsFile f)
  {
    int ret = ::hdfsFlush(fs, f);
    if (ret == -1) {
      throw std::runtime_error("hdfsFlush failed");
    }
  }
  static void close(hdfsFS fs, hdfsFile f)
  {
    int ret = ::hdfsCloseFile(fs, f);
    if (ret == -1) {
      throw std::runtime_error("hdfsCloseFile failed");
    }
  }
  static void close(hdfsFS fs, hdfsFile f, int& ret)
  {
    ret = ::hdfsCloseFile(fs, f);
  }
};

class HdfsFileSystemImpl
{
private:
  PathPtr mUri;
  hdfsFS mFileSystem;
  
  boost::shared_ptr<FileStatus> createFileStatus(hdfsFileInfo& fi);
  PathPtr transformDefaultUri(PathPtr p);
public:
  HdfsFileSystemImpl(UriPtr uri);
  ~HdfsFileSystemImpl();

  PathPtr getRoot();

  boost::shared_ptr<FileStatus> getStatus(PathPtr p);
  bool exists(PathPtr p);
  bool removeAll(PathPtr p);

  void list(PathPtr p,
	    std::vector<boost::shared_ptr<FileStatus> >& result);
  void readFile(UriPtr uri, std::string& out);
};

PathPtr HdfsFileSystemImpl::transformDefaultUri(PathPtr p)
{
  UriPtr uri = p->getUri();
  if (boost::algorithm::equals("default", uri->getHost()) &&
      0 == uri->getPort()) {
    hdfsFileInfo * info = hdfsGetPathInfo(mFileSystem, "/");
    std::string baseUri(info->mName);
    if(uri->getPath().size() > 1)
      baseUri += uri->getPath().substr(1);
    uri = boost::shared_ptr<URI>(new URI(baseUri.c_str()));
    hdfsFreeFileInfo(info, 1);
  }
  return Path::get(uri);
}

HdfsFileSystemImpl::HdfsFileSystemImpl(UriPtr uri)
  :
  mFileSystem(NULL)
{
  mFileSystem = hdfsConnect(uri->getHost().c_str(), uri->getPort());
  if (mFileSystem == NULL) {
    throw std::runtime_error((boost::format("Failed to connect hdfs://%1%:%2%") %
			      uri->getHost() %
			      uri->getPort()).str());
  }
  mUri = transformDefaultUri(Path::get(uri));
}

HdfsFileSystemImpl::~HdfsFileSystemImpl()
{
}

boost::shared_ptr<FileStatus> HdfsFileSystemImpl::createFileStatus(hdfsFileInfo& fi)
{
  std::string pathStr(fi.mName);
  if(fi.mKind == kObjectKindDirectory &&
     pathStr[pathStr.size()-1] != '/') 
    pathStr += std::string("/");
  return boost::make_shared<FileStatus>(Path::get(pathStr),
					fi.mKind == kObjectKindFile,
					fi.mKind == kObjectKindDirectory,
					(std::size_t) fi.mSize);
}

PathPtr HdfsFileSystemImpl::getRoot()
{
  return mUri;
}

boost::shared_ptr<FileStatus> HdfsFileSystemImpl::getStatus(PathPtr p)
{
  p = transformDefaultUri(p);
  hdfsFileInfo * fileInfo = ::hdfsGetPathInfo(mFileSystem, 
					      p->toString().c_str());
  if (fileInfo == NULL) {
    throw std::runtime_error((boost::format("File %1% does not exist") %
			      p->toString()).str());
  }
  boost::shared_ptr<FileStatus> tmp = createFileStatus(*fileInfo);
  hdfsFreeFileInfo(fileInfo, 1);
  return tmp;
}

bool HdfsFileSystemImpl::exists(PathPtr p)
{
  p = transformDefaultUri(p);
  hdfsFileInfo * fileInfo = ::hdfsGetPathInfo(mFileSystem, 
					      p->toString().c_str());
  if (fileInfo != NULL) {
    ::hdfsFreeFileInfo(fileInfo, 1);
    return true;
  } else {
    return false;
  }
}

bool HdfsFileSystemImpl::removeAll(PathPtr p)
{
  p = transformDefaultUri(p);
  int ret = ::hdfsDelete(mFileSystem, p->toString().c_str(), 1);	    
  return ret==0;
}

void HdfsFileSystemImpl::list(PathPtr p,
			      std::vector<boost::shared_ptr<FileStatus> >& result)
{
  int numEntries=-1;
  p = transformDefaultUri(p);
  hdfsFileInfo * tmp = ::hdfsListDirectory(mFileSystem, 
					   p->toString().c_str(), 
					   &numEntries);
  if (NULL == tmp && numEntries != 0) {
    throw std::runtime_error((boost::format("No such file or directory: %1%") %
  			      p->toString()).str());
  }
  for(int i=0; i<numEntries; ++i) {
    result.push_back(createFileStatus(tmp[i]));
  }
  hdfsFreeFileInfo(tmp, numEntries);
}

void HdfsFileSystemImpl::readFile(UriPtr uri, std::string& out)
{
  if (boost::algorithm::iequals("hdfs", uri->getScheme())) {
    std::stringstream sstr;
    hdfs_file_traits::file_type hdfsFile = 
      hdfs_file_traits::open_for_read(uri->toString().c_str(),
				      0,
				      std::numeric_limits<uint64_t>::max());
    while(!hdfs_file_traits::isEOF(hdfsFile)) {
      uint8_t buf[4096];
      int32_t sz = hdfs_file_traits::read(hdfsFile, &buf[0], 4096);
      sstr.write((const char *) &buf[0], sz);
    }
    out = sstr.str();
  } else {
    throw std::runtime_error((boost::format("Invalid HDFS URL %1%") %
			      uri->toString()).str());
  }
}

HdfsFileSystem::HdfsFileSystem(const std::string& uri)
  :
  mImpl(new HdfsFileSystemImpl(boost::make_shared<URI>(uri.c_str())))
{
}

class HdfsFileSystemRegistrar
{
public:
  HdfsFileSystemRegistrar();
  static FileSystem * create(UriPtr uri);
};

HdfsFileSystemRegistrar::HdfsFileSystemRegistrar()
{
  FileSystemFactory & factory(FileSystemFactory::get());
  factory.registerCreator("hdfs", &create);
}

FileSystem * HdfsFileSystemRegistrar::create(UriPtr uri)
{
  return new HdfsFileSystem(uri);
}

// Static to register file system
static HdfsFileSystemRegistrar registrar;

HdfsFileSystem::HdfsFileSystem(UriPtr uri)
  :
  mImpl(new HdfsFileSystemImpl(uri))
{
}

HdfsFileSystem::~HdfsFileSystem()
{
  delete mImpl;
}
  
void HdfsFileSystem::expand(std::string pattern,
			      int32_t numPartitions,
			      std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files)
{
  hdfs_file_traits::expand(pattern, numPartitions, files);
}

PathPtr HdfsFileSystem::getRoot()
{
  return mImpl->getRoot();
}

boost::shared_ptr<FileStatus> HdfsFileSystem::getStatus(PathPtr p)
{
  return mImpl->getStatus(p);
}

bool HdfsFileSystem::exists(PathPtr p)
{
  return mImpl->exists(p);
}

bool HdfsFileSystem::removeAll(PathPtr p)
{
  return mImpl->removeAll(p);
}

void HdfsFileSystem::list(PathPtr p,
			    std::vector<boost::shared_ptr<FileStatus> >& result)
{
  mImpl->list(p, result);
}

void HdfsFileSystem::readFile(UriPtr uri, std::string& out)
{
  mImpl->readFile(uri, out);
}

HdfsDelete::HdfsDelete(const std::string& path)
{
  URI uri(path.c_str());
  if (!boost::algorithm::iequals(uri.getScheme(), "hdfs")) {
    throw std::runtime_error((boost::format("HdfsDelete::HdfsDelete path "
					    "argument %1% must be a valid HDFS"
					    " URL") % path).str());
  }
  mPath = path;
}

HdfsDelete::~HdfsDelete()
{
  if (mPath.size()) {
    HdfsFileSystem fs (mPath);
    fs.removeAll(Path::get(mPath));
  }
}

static bool canBeSplit(hdfsFileInfo& file)
{
  // Gzip compressed files cannot be split.  Others can.
  int len = strlen(file.mName);
  return  len < 3 || !boost::algorithm::iequals(".gz", &file.mName[len-3]);
}


class PartitionCapacity {
public:
  std::size_t mPartition;
  std::size_t mCapacity;
  PartitionCapacity(std::size_t partition,
		    std::size_t capacity)
    :
    mPartition(partition),
    mCapacity(capacity)
  {
  }

  bool operator < (const PartitionCapacity& pc) const
  {
    return 
      mCapacity < pc.mCapacity || 
      (mCapacity==pc.mCapacity && mPartition < pc.mPartition);
  }
};

void hdfs_file_traits::expand(std::string pattern, 
			      int32_t numPartitions,
			      std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files)
{
  files.resize(numPartitions);
  // TODO: Handle globbing 
  URI path(pattern.c_str());
  hdfsFS fs = hdfsConnect(path.getHost().c_str(), path.getPort());
  if (fs == NULL) {
    throw std::runtime_error((boost::format("Failed to connect hdfs://%1%:%2%") %
			      path.getHost() %
			      path.getPort()).str());
  }
  int numEntries = 1;
  hdfsFileInfo * fileInfo = hdfsGetPathInfo(fs, path.getPath().c_str());
  if (fileInfo == NULL) {
    throw std::runtime_error((boost::format("File %1% does not exist") %
			      path.getPath()).str());
  }
  if (fileInfo->mKind == kObjectKindDirectory) {
    hdfsFileInfo * tmp = hdfsListDirectory(fs, path.getPath().c_str(), &numEntries);
    hdfsFreeFileInfo(fileInfo, 1);
    fileInfo = tmp;
  }

  // TODO: Should we sort the files on size????
  // Get size of all files.
  std::vector<uint64_t> cumulativeSizes;
  uint64_t totalBlocks=0;
  for(int i=0; i<numEntries; i++) {
    cumulativeSizes.push_back(fileInfo[i].mSize + (i==0 ? 0 : cumulativeSizes.back()));
    totalBlocks += (fileInfo[i].mSize + fileInfo[i].mBlockSize - 1)/fileInfo[i].mBlockSize;
  }
  uint64_t totalFileSize=cumulativeSizes.back();

  // Check that numPartitions agrees with JobServers
  // if ((std::size_t) numPartitions != PlanRunner::JobServers.size()) {
  //   throw std::runtime_error((boost::format("Mismatch between size of job server list=%1% and requested num partitions = %2%") % PlanRunner::JobServers.size() % numPartitions).str());
  // }

  // // This code is for a negative test: what happens if we
  // // use 0% local reads from HDFS.
  // std::vector<PartitionCapacity> q;
  // for(int32_t p = 0; p < numPartitions; ++p) {
  //   q.push_back(PartitionCapacity(p,0));
  // }
  // // Assign blocks to servers.
  // // Use greedy algorithm in which we figure out which servers have
  // // the fewest local blocks and assign those first.
  // std::size_t numAssigned = 0;
  // std::map<int, std::map<tOffset, bool> > blockAssignment;
  // for(int i=0; i<numEntries; i++) {
  //   char *** hosts = hdfsGetHosts(fs, 
  // 				  fileInfo[i].mName, 
  // 				  0, 
  // 				  fileInfo[i].mSize);
  //   int blockIdx=0;
  //   tOffset offset=0;
  //   for(; offset<fileInfo[i].mSize; offset += fileInfo[i].mBlockSize, blockIdx++) {
  //     // Mark block as unassigned
  //     blockAssignment[i][offset] = false;

  //     // Pick the local server with smallest number
  //     // of assigned blocks.
  //     std::set<std::string> local;
  //     for(char ** blockHosts = hosts[blockIdx]; *blockHosts != NULL; ++blockHosts) {
  // 	local.insert(*blockHosts);
  //     }
  //     for(std::vector<PartitionCapacity>::iterator pcit = q.begin();
  // 	  pcit != q.end();
  // 	  ++pcit) {
  // 	if (local.end() ==
  // 	    local.find(PlanRunner::JobServers[pcit->mPartition])) {
  // 	  boost::shared_ptr<FileChunk> chunk(new FileChunk(fileInfo[i].mName,
  // 							   offset,
  // 							   offset+fileInfo[i].mBlockSize));
  // 	  // TODO: Choose the partition with fewest chunks.
  // 	  files[pcit->mPartition].push_back(chunk);
  // 	  pcit->mCapacity += 1;
  // 	  // Suboptimal.  This is test code!
  // 	  std::sort(q.begin(), q.end());
  // 	  numAssigned += 1;
  // 	  break;
  // 	} 
  //     }
  //   }
  // }  

  ///////////////////////////////////////////////////////////
  // The following code was used during benchmarking to
  // to implement load balancing across an HDFS file
  //////////////////////////////////////////////////////////
  // // Assign blocks to servers.
  // // Use greedy algorithm in which we figure out which servers have
  // // the fewest local blocks and assign those first.
  // std::map<int, std::map<tOffset, bool> > blockAssignment;
  // for(int i=0; i<numEntries; i++) {
  //   char *** hosts = hdfsGetHosts(fs, 
  // 				  fileInfo[i].mName, 
  // 				  0, 
  // 				  fileInfo[i].mSize);
  //   int blockIdx=0;
  //   tOffset offset=0;
  //   for(; offset<fileInfo[i].mSize; offset += fileInfo[i].mBlockSize, blockIdx++) {
  //     // Mark block as unassigned
  //     blockAssignment[i][offset] = false;

  //     // Pick the local server with smallest number
  //     // of assigned blocks.
  //     std::size_t minHost = 0;
  //     std::size_t minHostLoad = std::numeric_limits<std::size_t>::max();

  //     for(char ** blockHosts = hosts[blockIdx]; *blockHosts != NULL; ++blockHosts) {
  // 	std::map<std::string, std::vector<std::size_t> >::const_iterator it = 
  // 	  PlanRunner::JobServerIndex.find(*blockHosts);
  // 	if(PlanRunner::JobServerIndex.end() != it) {
  // 	  for(std::vector<std::size_t>::const_iterator pit = it->second.begin();
  // 	      pit != it->second.end();
  // 	      ++pit) {
  // 	    if (files[*pit].size() < minHostLoad) {
  // 	      minHostLoad = files[*pit].size();
  // 	      minHost = *pit;
  // 	    }
  // 	  }
  // 	}  	
  //     }

  //     if (minHostLoad != std::numeric_limits<std::size_t>::max()) {
  // 	// Attach allocation to partition
  // 	boost::shared_ptr<FileChunk> chunk(new FileChunk(fileInfo[i].mName,
  // 							 offset,
  // 							 offset+fileInfo[i].mBlockSize));
  // 	// TODO: Choose the partition with fewest chunks.
  // 	files[minHost].push_back(chunk);
  // 	blockAssignment[i][offset] = true;
  //     }
  //   }
  //   hdfsFreeHosts(hosts);
  // }

  // // Take a pass through and assign all blocks that didn't have a local server
  // // TODO: It would be good to load balance here.
  // uint64_t numLocalBlocks=0;
  // uint64_t numRemoteBlocks=0;
  // for(std::map<int, std::map<tOffset, bool> >::const_iterator it = blockAssignment.begin();
  //     it != blockAssignment.end();
  //     ++it) {
  //   for(std::map<tOffset, bool>::const_iterator iit = it->second.begin();
  // 	iit != it->second.end();
  // 	++iit) {
  //     if (iit->second) {
  // 	numLocalBlocks +=1 ;
  // 	continue; 
  //     }
  //     numRemoteBlocks += 1;
  //     int i = it->first;
  //     tOffset offset = iit->first;
  //     boost::shared_ptr<FileChunk> chunk(new FileChunk(fileInfo[i].mName,
  // 						       offset,
  // 						       offset+fileInfo[i].mBlockSize));
  //     files[rand() % numPartitions].push_back(chunk);    
  //   }
  // }
  // std::cout << "HdfsRead with numLocalBlocks=" << numLocalBlocks << ", numRemoteBlocks=" << numRemoteBlocks << std::endl;
  // for(std::size_t i=0; i<files.size(); i++) {
  //   std::cout << PlanRunner::JobServers[i] << " assigned " << files[i].size() << " blocks\n";
  // }
  ///////////////////////////////////////////////////////////
  // End of benchmarking code
  //////////////////////////////////////////////////////////

  // Our goal is to assign even amounts to partitions.
  uint64_t partitionSize = (totalFileSize+numPartitions-1)/numPartitions;
  // Partition we are assigning files to.
  int32_t currentPartition=0;
  // Amount to try to allocate to the current partition.
  // Make sure that the last partition can hold whatever remains.
  uint64_t  currentPartitionRemaining = currentPartition+1 == numPartitions ? 
    std::numeric_limits<uint64_t>::max() :
    partitionSize;
  for(int currentFile=0; currentFile<numEntries; ++currentFile)  {
    // We don't want to assign a small part of a file to a partition however
    // This is an arbitrary number at this point; don't know if it is sensible.
    // It should probably be determined by the block size and the size of the file
    // itself.
    const uint64_t minimumFileAllocationSize = 1024*1024;
    uint64_t currentFileRemaining = fileInfo[currentFile].mSize;
    while(currentFileRemaining) {
      uint64_t currentFilePosition = fileInfo[currentFile].mSize - currentFileRemaining;
      uint64_t fileAllocation = 0;
      bool splittable = canBeSplit(fileInfo[currentFile]);
      if (!splittable ||
  	  currentFileRemaining < currentPartitionRemaining ||
  	  currentFileRemaining-currentPartitionRemaining < minimumFileAllocationSize) {
  	// This file cannot be broken up (e.g. gzip compression).  Have no
  	// choice but to assign completely to 1 partition.
  	// or
  	// If what remains of the file is small before or after our target allocation,
  	// just go ahead and
  	// put the remainder of the file in this partition even if it overflows a bit.
  	fileAllocation = currentFileRemaining;
      } else {
  	// Fill up this partition.  
  	fileAllocation = currentPartitionRemaining;
      }
      // Attach allocation to partition
      boost::shared_ptr<FileChunk> chunk(new FileChunk(fileInfo[currentFile].mName,
  						       currentFilePosition,
						       splittable ?
  						       currentFilePosition+fileAllocation :
						       std::numeric_limits<uint64_t>::max()));
      files[currentPartition].push_back(chunk);
      // Update state of the partition and move on to next if necessary.
      // If what remains in the partition is "small" then close out the partition
      // even if it is not technically full.
      if(minimumFileAllocationSize <= currentPartitionRemaining &&
	 fileAllocation <= currentPartitionRemaining-minimumFileAllocationSize) {
  	currentPartitionRemaining -= fileAllocation;
      } else {
  	currentPartition += 1;
  	// Make sure that the last partition can hold whatever remains.
  	currentPartitionRemaining = currentPartition+1 == numPartitions ? 
  	  std::numeric_limits<uint64_t>::max() :
  	  partitionSize;
      }
      // Anymore file to be allocated?
      currentFileRemaining -= fileAllocation;	
    }
  }
  hdfsFreeFileInfo(fileInfo, numEntries);
}

static boost::mutex fsCacheGuard;
static std::map<std::string, hdfsFS> fsCache;

static hdfsFS connectToHDFS(URI & path)
{
  hdfsFS fs = NULL;
  // Note that if we were connecting to many different
  // hosts it might not be acceptable to hold the lock
  // during the connect call, but in this case it is 
  // probably better to be pessimistic and block other
  // threads since they are likely connecting to the
  // same HDFS instance.
  boost::unique_lock<boost::mutex> lk(fsCacheGuard);
  
  std::map<std::string, hdfsFS>::iterator it = fsCache.find(path.getHost());
  if (it == fsCache.end()) {
    fs = hdfsConnect(path.getHost().c_str(), path.getPort());
    if (fs == NULL) {
      throw std::runtime_error((boost::format("Couldn't open HDFS filesystem. host=%1%; port=%2%") %
				path.getHost() %
				path.getPort()).str());
    }
    fsCache[path.getHost()] = fs;
  } else {
    fs = it->second;
  }
  return fs;
}

hdfs_file_traits::file_type hdfs_file_traits::open_for_read(const char * filename, 
							    uint64_t beginOffset,
							    uint64_t endOffset)
{
  URI path(filename);

  file_type f = new hdfs_file_handle();
  // Connect to the file system
  f->FileSystem = connectToHDFS(path);

  // Check and save the size of the file.
  hdfsFileInfo * fi = hdfsGetPathInfo(f->FileSystem, path.getPath().c_str());
  if (fi == NULL) {
    throw std::runtime_error((boost::format("File %1% does not exist") %
			      filename).str());
    
  }
  f->End = (uint64_t) fi->mSize;
  hdfsFreeFileInfo(fi, 1);
    
  f->File = hdfsOpenFile(f->FileSystem, 
			 path.getPath().c_str(), 
			 O_RDONLY,
			 0, 0, 0);
  
  if (f->File == NULL) {
    throw std::runtime_error((boost::format("Couldn't open HDFS file %1%") %
			      filename).str());
  }

  // Seek to appropriate offset.
  hdfsSeek(f->FileSystem, f->File, beginOffset);
    
  return f;
} 

void hdfs_file_traits::close(hdfs_file_traits::file_type f)
{
  hdfsCloseFile(f->FileSystem, f->File);
  // Don't close since we have the caching hack.
  //hdfsDisconnect(f->FileSystem);
}

int32_t hdfs_file_traits::read(hdfs_file_traits::file_type f, uint8_t * buf, int32_t bufSize)
{
  tSize tmp = hdfsRead(f->FileSystem, f->File, buf, bufSize);
  if (tmp == -1)
    throw std::runtime_error("Failed reading from HDFS file");
  return tmp;
}

bool hdfs_file_traits::isEOF(hdfs_file_traits::file_type f)
{
  uint64_t filePos = (uint64_t) hdfsTell(f->FileSystem, f->File);
  if (filePos >= f->End) 
    return true;
  else
    return false;
}

class HdfsDataBlockRegistrar
{
public:
  HdfsDataBlockRegistrar();
  static DataBlock * create(const char * filename,
			    int32_t targetBlockSize,
			    uint64_t begin,
			    uint64_t end);
};

HdfsDataBlockRegistrar::HdfsDataBlockRegistrar()
{
  DataBlockFactory & factory(DataBlockFactory::get());
  factory.registerCreator("hdfs", &create);
}

DataBlock * HdfsDataBlockRegistrar::create(const char * filename,
					    int32_t targetBlockSize,
					    uint64_t begin,
					    uint64_t end)
{
  typedef BlockBufferStream<hdfs_file_traits> hdfs_block;
  typedef BlockBufferStream<zlib_file_traits<hdfs_block> > zlib_hdfs_block;
  URI uri(filename);
  bool compressed = uri.getPath().size() > 3 &&
    boost::algorithm::iequals(".gz", 
			      uri.getPath().substr(uri.getPath().size()-3));
  if (compressed) {
    return new zlib_hdfs_block(filename, targetBlockSize, begin, end);
  } else {
    return new hdfs_block(filename, targetBlockSize, begin, end);
  }
}

// Static to register file system
static HdfsDataBlockRegistrar dataBlockRegistrar;

/**
 * We write data to HDFS by first writing to a temporary
 * file and then renaming that temporary file to a permanent
 * file name.
 * In a program that writes multiple files, it is crucial that
 * the files be renamed in a deterministic order.  The reason for
 * this is that multiple copies of the program may be running
 * (e.g. due to Hadoop speculative execution) and we demand that
 * exactly one of the copies succeeds.  If we do not have a deterministic
 * order of file renames then it is possible for a "deadlock"-like
 * scenario to occur in which all copies fail (think of renaming
 * a file as being equivalent to taking a write lock on a resource
 * identified by the file name).
 */
class HdfsFileCommitter
{
private:
  std::vector<boost::shared_ptr<class HdfsFileRename> >mActions;
  std::string mError;
  /**
   * Number of actions that have requested commit
   */
  std::size_t mCommits;

  // TODO: Don't use a singleton here, have the dataflow
  // manage the lifetime.
  static HdfsFileCommitter * sCommitter;
  static int32_t sRefCount;
  static boost::mutex sGuard;
public:  
  static HdfsFileCommitter * get();
  static void release(HdfsFileCommitter *);
  HdfsFileCommitter();
  ~HdfsFileCommitter();
  void track (const std::string& from, const std::string& to,
	      hdfsFS fileSystem);
  bool commit();
  const std::string& getError() const 
  {
    return mError;
  }
};


/**
 * Implements a 2-phase commit like protocol for committing
 * a rename.
 */
class HdfsFileRename
{
private:
  std::string mFrom;
  std::string mTo;
  hdfsFS mFileSystem;
public:
  HdfsFileRename(const std::string& from,
		 const std::string& to,
		 hdfsFS fileSystem);
  ~HdfsFileRename();
  bool prepare(std::string& err);
  void commit();
  void rollback();
  void dispose();
  static bool renameLessThan (boost::shared_ptr<HdfsFileRename> lhs, 
			      boost::shared_ptr<HdfsFileRename> rhs);
};


HdfsFileCommitter * HdfsFileCommitter::sCommitter = NULL;
int32_t HdfsFileCommitter::sRefCount = 0;
boost::mutex HdfsFileCommitter::sGuard;

HdfsFileCommitter * HdfsFileCommitter::get()
{
  boost::unique_lock<boost::mutex> lock(sGuard);
  if (sRefCount++ == 0) {
    sCommitter = new HdfsFileCommitter();
  }
  return sCommitter;
}

void HdfsFileCommitter::release(HdfsFileCommitter *)
{
  boost::unique_lock<boost::mutex> lock(sGuard);
  if(--sRefCount == 0) {
    delete sCommitter;
    sCommitter = NULL;
  }
}

HdfsFileCommitter::HdfsFileCommitter()
  :
  mCommits(0)
{
}

HdfsFileCommitter::~HdfsFileCommitter()
{
}

void HdfsFileCommitter::track (const std::string& from, 
			       const std::string& to,
			       hdfsFS fileSystem)
{
  mActions.push_back(boost::shared_ptr<HdfsFileRename>(new HdfsFileRename(from, to, fileSystem)));
}

bool HdfsFileCommitter::commit()
{
  std::cout << "HdfsFileCommitter::commit; mCommits = " << mCommits << std::endl;
  if (++mCommits == mActions.size()) {      
    // Sort the actions to make a deterministic order.
    std::sort(mActions.begin(), mActions.end(), 
	      HdfsFileRename::renameLessThan);
    for(std::size_t i = 0; i<mActions.size(); ++i) {
      if (!mActions[i]->prepare(mError)) {
	// Failed to commit delete temp files that haven't
	// been dealt with.  Note that we don't rollback
	// since we are no longer assuming a single process
	// writes all of the files.  If we did rollback then we
	// might be undoing work that another process that has
	// succeeded is assuming is in the filesystem. That would
	// appear to the user as job success when some files
	// have not been written.
	// See CR 1459063 for more details.
	for(std::size_t j = 0; j<mActions.size(); ++j) {
	  mActions[j]->dispose();
	}
	mActions.clear();
	mCommits = 0;
	return false;
      }
    }
    // Everyone voted YES, so commit (essentially a noop).
    for(std::size_t i = 0; i<mActions.size(); ++i) {
      mActions[i]->commit();
    }
    mActions.clear();
    mCommits = 0;
  }
  return true;
}

bool HdfsFileRename::renameLessThan (boost::shared_ptr<HdfsFileRename> lhs, 
				     boost::shared_ptr<HdfsFileRename> rhs)
{
  return strcmp(lhs->mTo.c_str(), rhs->mTo.c_str()) < 0;
}

HdfsFileRename::HdfsFileRename(const std::string& from,
			       const std::string& to,
			       hdfsFS fileSystem)
  :
  mFrom(from),
  mTo(to),
  mFileSystem(fileSystem)
{
  if (mFrom.size() == 0 || mTo.size() == 0)
    throw std::runtime_error("HdfsFileRename::HdfsFileRename "
			     "expects non-empty filenames");
}

HdfsFileRename::~HdfsFileRename()
{
}

bool HdfsFileRename::prepare(std::string& err)
{
  if (mFrom.size()) {
    BOOST_ASSERT(mTo.size() != 0);
    std::cout << "HdfsFileRename::prepare renaming " << mFrom.c_str() << " to " <<
      mTo.c_str() << std::endl;
    int ret = ::hdfsRename(mFileSystem, mFrom.c_str(), mTo.c_str());
    if (ret != 0) {
      std::string msg = (boost::format("Failed to rename HDFS file %1% to %2%") %
			 mFrom % mTo).str();
      std::cout << msg.c_str() << std::endl;
      // Check whether the file already exists.  If so and it is
      // the same size as what we just wrote, then assume idempotence
      // and return success.
      hdfsFileInfo * fileInfo = ::hdfsGetPathInfo(mFileSystem, 
						  mTo.c_str());
      if (fileInfo == NULL) {
	err = (boost::format("Rename failed and target file %1% does not exist") %
	       mTo).str();      
	std::cout << err.c_str() << std::endl;
	return false;
      }
      hdfsFileInfo * fromFileInfo = ::hdfsGetPathInfo(mFileSystem, 
						      mFrom.c_str());
      if (fromFileInfo == NULL) {
	err = (boost::format("Rename failed, target file %1% exists but failed to "
			     "get status of temporary file %2%") %
	       mTo % mFrom).str();      
	std::cout << err.c_str() << std::endl;
	::hdfsFreeFileInfo(fileInfo, 1);
	return false;
      }
      // This is an interesting check but with compression enabled
      // it isn't guaranteed to hold.  In particular, in a reducer
      // to which we have emitted with a non-unique key there is
      // non-determinism in the order of the resulting stream (depending
      // on the order in which map files are processed for example).
      // If the order winds up being sufficiently different between two
      // files, then the compression ratio may differ and the resulting
      // file sizes won't match.
      if (fileInfo->mSize != fromFileInfo->mSize) {
	msg = (boost::format("Rename failed: target file %1% already exists and has "
			     "size %2%, "
			     "temporary file %3% has size %4%; "
			     "ignoring rename failure and continuing") %
	       mTo % fileInfo->mSize % mFrom % fromFileInfo->mSize).str();      
      } else {
	msg = (boost::format("Both %1% and %2% have the same size; ignoring "
			     "rename failure and continuing") %
	       mFrom % mTo).str();
      }
      std::cout << msg.c_str() << std::endl;
      ::hdfsFreeFileInfo(fileInfo, 1);
      ::hdfsFreeFileInfo(fromFileInfo, 1);
    } 
    mFrom = "";      
    return true;
  } else {
    return false;
  }
}

void HdfsFileRename::commit()
{
  if (mFrom.size() == 0 && mTo.size() != 0) {
    std::cout << "HdfsFileRename::commit " << mTo.c_str() << std::endl;
    // Only commit if we prepared.
    mTo = "";
  }
}

void HdfsFileRename::rollback()
{
  if (mFrom.size() == 0 && mTo.size() != 0) {
    // Only rollback if we prepared.
    std::cout << "Rolling back permanent file: " << mTo.c_str() << std::endl;
    ::hdfsDelete(mFileSystem, mTo.c_str(), 0);
    mTo = "";
  }
}

void HdfsFileRename::dispose()
{
  if (mFrom.size() != 0) {
    std::cout << "Removing temporary file: " << mFrom.c_str() << std::endl;
    ::hdfsDelete(mFileSystem, mFrom.c_str(), 0);
    mFrom = "";
  }
}

class RuntimeHdfsWriteOperator : public RuntimeOperator
{
public:
  void writeToHdfs(RecordBuffer input, bool isEOS);  
private:
  enum State { START, READ };
  State mState;
  const RuntimeHdfsWriteOperatorType &  getHdfsWriteType() 
  {
    return *reinterpret_cast<const RuntimeHdfsWriteOperatorType *>(&getOperatorType());
  }

  hdfsFS mFileSystem;
  hdfsFile mFile;
  RuntimePrinter mPrinter;
  ZLibCompress mCompressor;
  AsyncWriter<RuntimeHdfsWriteOperator> mWriter;
  boost::thread * mWriterThread;
  HdfsFileCommitter * mCommitter;
  std::string mError;

  void renameTempFile();
  /**
   * Is this operator writing an inline header?
   */
  bool hasInlineHeader() 
  {
    return getHdfsWriteType().mHeader.size() != 0 && 
      getHdfsWriteType().mHeaderFile.size()==0;
  }
  /**
   * Is this operator writing a header file?
   */
  bool hasHeaderFile()
  {
    return getPartition() == 0 && 
      getHdfsWriteType().mHeader.size() != 0 && 
      getHdfsWriteType().mHeaderFile.size()!=0;  
  }
  
public:
  RuntimeHdfsWriteOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType);
  ~RuntimeHdfsWriteOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

RuntimeHdfsWriteOperator::RuntimeHdfsWriteOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mFileSystem(NULL),
  mFile(NULL),
  mPrinter(getHdfsWriteType().mPrint),
  mWriter(*this),
  mWriterThread(NULL),
  mCommitter(NULL)
{
}

RuntimeHdfsWriteOperator::~RuntimeHdfsWriteOperator()
{
  delete mWriterThread;
  if (mFile) {
    // Abnormal shutdown
    int ret;
    hdfs_file_handle::close(mFileSystem, mFile, ret);
    mFile = NULL;
  }
  if (mCommitter) {
    HdfsFileCommitter::release(mCommitter);
    mCommitter = NULL;
  }
  if (mFileSystem) {
    // Since we are not calling hdfsConnectNewInstance
    // we are sharing references to the same underlying
    // Java class instance.  Explicitly closing will
    // mess up other instances floating around.
    // hdfsDisconnect(mFileSystem);
    mFileSystem = NULL;
  }
}

void RuntimeHdfsWriteOperator::start()
{
  // Connect to the file system
  mFileSystem = hdfsConnect(getHdfsWriteType().mHdfsHost.c_str(), getHdfsWriteType().mHdfsPort);
  if (mFileSystem == NULL) {
    throw std::runtime_error("Couldn't open HDFS filesystem");
  }
  // TODO: Check if file exists
  // TODO: Make sure file is cleaned up in case of failure.
  // We create a temporary file name and write to that.  When
  // complete we'll rename the file.  This should make things safe
  // in case multiple copies of the operator are trying to write
  // to the same file (e.g. running in Hadoop with speculative execution).
  // The temporary file name must have the pattern serial_ddddd
  // so that it uses the appropriate block placement policy.
  std::string tmpStr = FileSystem::getTempFileName();

  std::stringstream str;
  str << getHdfsWriteType().mHdfsFile << "/" << tmpStr << "_serial_" << 
    std::setw(5) << std::setfill('0') << getPartition() <<
    ".gz";
  std::string tempFile = str.str();
  std::stringstream permFile;
  permFile << getHdfsWriteType().mHdfsFile + "/serial_" << 
    std::setw(5) << std::setfill('0') << getPartition() <<
    ".gz";  
  mCommitter = HdfsFileCommitter::get();
  mCommitter->track(tempFile, permFile.str(), mFileSystem);
  mFile = hdfsOpenFile(mFileSystem, 
		       tempFile.c_str(),
  		       O_WRONLY|O_CREAT,
  		       getHdfsWriteType().mBufferSize,
  		       getHdfsWriteType().mReplicationFactor,
  		       /*getHdfsWriteType().mBlockSize*/2000000000);
  if (mFile == NULL) {
    throw std::runtime_error("Couldn't create HDFS file");
  }
  if (hasInlineHeader()) {
    // We write in-file header for every partition
    mCompressor.put((const uint8_t *) getHdfsWriteType().mHeader.c_str(), 
		    getHdfsWriteType().mHeader.size(), 
		    false);
    while(!mCompressor.run()) {
      uint8_t * output;
      std::size_t outputLen;
      mCompressor.consumeOutput(output, outputLen);
      hdfs_file_handle::write(mFileSystem, mFile, output, outputLen);
    }
  } else if (hasHeaderFile()) {
    // Extract path from URI
    URI uri(getHdfsWriteType().mHeaderFile.c_str());
    std::string tmpHeaderStr = FileSystem::getTempFileName();
    std::string tmpHeaderFile("/tmp/headers/" + tmpHeaderStr);
    mCommitter->track(tmpHeaderFile, uri.getPath(), mFileSystem);
    hdfsFile headerFile = hdfsOpenFile(mFileSystem, 
				       tmpHeaderFile.c_str(),
				       O_WRONLY|O_CREAT,
				       getHdfsWriteType().mBufferSize,
				       getHdfsWriteType().mReplicationFactor,
				       getHdfsWriteType().mBlockSize);
    if (headerFile == NULL) {
      throw std::runtime_error("Couldn't create header file");
    }
    hdfs_file_handle::write(mFileSystem, headerFile, 
			    &getHdfsWriteType().mHeader[0], 
			    (tSize) getHdfsWriteType().mHeader.size());
    hdfs_file_handle::close(mFileSystem, headerFile);
  }

  // Start a thread that will write
  mWriterThread = 
    new boost::thread(boost::bind(&AsyncWriter<RuntimeHdfsWriteOperator>::run, 
				  boost::ref(mWriter)));
  mState = START;
  onEvent(NULL);
}

void RuntimeHdfsWriteOperator::renameTempFile()
{
  // Put the file in its final place.
  if(!mCommitter->commit()) {
    mError = mCommitter->getError();
  } 

  if (0 == mError.size() && hasHeaderFile()) {
    // Commit the header file as well.
    if(!mCommitter->commit()) {
      mError = mCommitter->getError();
    } 
  }
  // Since we are not calling hdfsConnectNewInstance
  // we are sharing references to the same underlying
  // Java class instance.  Explicitly closing will
  // mess up other instances floating around.
  // hdfsDisconnect(mFileSystem);
  mFileSystem = NULL;
}

void RuntimeHdfsWriteOperator::writeToHdfs(RecordBuffer input, bool isEOS)
{
  if (!isEOS) {
    mPrinter.print(input);
    getHdfsWriteType().mFree.free(input);
    mCompressor.put((const uint8_t *) mPrinter.c_str(), mPrinter.size(), false);
    while(!mCompressor.run()) {
      uint8_t * output;
      std::size_t outputLen;
      mCompressor.consumeOutput(output, outputLen);
      hdfs_file_handle::write(mFileSystem, mFile, output, outputLen);
    }
    mPrinter.clear();
  } else {
    // Flush data through the compressor.
    mCompressor.put(NULL, 0, true);
    while(true) {
      mCompressor.run();
      uint8_t * output;
      std::size_t outputLen;
      mCompressor.consumeOutput(output, outputLen);
      if (outputLen > 0) {
	hdfs_file_handle::write(mFileSystem, mFile, output, outputLen);
      } else {
	break;
      }
    }    
    // Flush data to disk
    hdfs_file_handle::flush(mFileSystem, mFile);
    // Clean close of file and file system
    hdfs_file_handle::close(mFileSystem, mFile);
    mFile = NULL;
  }
}

void RuntimeHdfsWriteOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      {
	RecordBuffer input;
	read(port, input);
	bool isEOS = RecordBuffer::isEOS(input);
	//writeToHdfs(input, isEOS);
	mWriter.enqueue(input);
	if (isEOS) {
	  // Wait for writers to flush; perhaps this should be done is shutdown
	  mWriterThread->join();
	  // See if there was an error within the writer that we need
	  // to throw out.  Errors here are thow that would have happened
	  // after we enqueued EOS.
	  std::string err;
	  mWriter.getError(err);
	  if (err.size()) {
	    throw std::runtime_error(err);
	  }
	  // Do the rename of the temp file in the main dataflow
	  // thread because this makes the order in which files are renamed
	  // across the entire dataflow deterministic (at least for the map-reduce
	  // case where there is only a single dataflow thread and all collectors/reducers
	  // are sort-merge).
	  renameTempFile();
	  break;
	}
      }
    }
  }
}

void RuntimeHdfsWriteOperator::shutdown()
{
  if (mFile) {
    int ret;
    hdfs_file_handle::close(mFileSystem, mFile, ret);
    mFile = NULL;
  }
  if (mFileSystem) {
    // Since we are not calling hdfsConnectNewInstance
    // we are sharing references to the same underlying
    // Java class instance.  Explicitly closing will
    // mess up other instances floating around.
    // hdfsDisconnect(mFileSystem);
    mFileSystem = NULL;
  }
  if (mError.size() != 0) {
    throw std::runtime_error(mError);
  }
}

RuntimeOperator * RuntimeHdfsWriteOperatorType::create(RuntimeOperator::Services& services) const
{
  return new RuntimeHdfsWriteOperator(services, *this);
}

LogicalEmit::LogicalEmit()
{
}

LogicalEmit::~LogicalEmit()
{
}

void LogicalEmit::check(PlanCheckContext& log)
{
  // Validate the parameters
  std::string predicate;
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "key")) {
      mKey = boost::get<std::string>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }

  if (!getInput(0)->getRecordType()->hasMember(mKey)) {
    log.logError(*this, std::string("Missing field: ") + mKey);
  }
}

void LogicalEmit::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimeHadoopEmitOperatorType("RuntimeHadoopEmitOperatorType",
				      getInput(0)->getRecordType(),
				      mKey);
  
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
}

RuntimeHadoopEmitOperator::RuntimeHadoopEmitOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mContext(NULL),
  mKeyPrinter(getHadoopEmitType().mKey),
  mValuePrinter(getHadoopEmitType().mPrint)
{
}

void RuntimeHadoopEmitOperator::start()
{
  // Somebody needs to set the context!
  if (mContext == NULL) {
    throw std::runtime_error("Must set RuntimeHadoopEmitOperator::mContext");
  }
  mState = START;
  onEvent(NULL);
}

void RuntimeHadoopEmitOperator::writeToHdfs(RecordBuffer input, bool isEOS)
{
  if (!isEOS) {
    mValuePrinter.print(input, false);
    mKeyPrinter.print(input, false);
    getHadoopEmitType().mFree.free(input);
    mContext->emit(mKeyPrinter.str(), mValuePrinter.str());
    mKeyPrinter.clear();
    mValuePrinter.clear();
  } 
}

void RuntimeHadoopEmitOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      {
	RecordBuffer input;
	read(port, input);
	bool isEOS = RecordBuffer::isEOS(input);
	writeToHdfs(input, isEOS);
	if (isEOS) {
	  break;
	}
      }
    }
  }
}

void RuntimeHadoopEmitOperator::shutdown()
{
}

RuntimeOperator * RuntimeHadoopEmitOperatorType::create(RuntimeOperator::Services& services) const
{
  return new RuntimeHadoopEmitOperator(services, *this);
}

