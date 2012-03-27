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

#include <iomanip>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/make_shared.hpp>
#include <boost/thread.hpp>

#include "RecordParser.hh"

void stdio_file_traits::expand(std::string pattern, 
		     int32_t numPartitions,
		     std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files)
{
   gzip_file_traits::expand(pattern, numPartitions, files);
}

stdio_file_traits::file_type stdio_file_traits::open_for_read(const char * filename, uint64_t beginOffset, uint64_t endOffset)
{
  stdio_file_segment * seg = new stdio_file_segment();
  seg->mFile = ::open(filename, O_RDONLY);
  if (seg->mFile < 0) {
    int err = errno;    
    throw std::runtime_error((boost::format("Failed opening file %1%: errno=%2%") % filename % err).str());
  }
  seg->mEndOffset = endOffset;
  ssize_t sz = lseek(seg->mFile, 0, SEEK_END);
  if (sz < seg->mEndOffset)
    seg->mEndOffset = lseek(seg->mFile, 0, SEEK_END);
  ::lseek(seg->mFile, beginOffset, SEEK_SET);

#ifdef IQL_HAS_FADVISE  
  ::posix_fadvise(seg->mFile, beginOffset, sz-beginOffset, POSIX_FADV_SEQUENTIAL);
#endif

  return seg;
}

stdio_file_traits::file_type stdio_file_traits::open_for_write(const char * filename)
{
  stdio_file_segment * seg = new stdio_file_segment();
  seg->mFile = ::open(filename,
		      O_WRONLY|O_CREAT|O_TRUNC,
		      S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP);
  if (seg->mFile == -1) {
    throw std::runtime_error((boost::format("Couldn't create file: %1%") % filename).str());
  }
  seg->mEndOffset = 0;
  return seg;
}

void stdio_file_traits::close(stdio_file_traits::file_type f)
{
  ::close (f->mFile);
  delete f;
}

void stdio_file_traits::remove(const char * filename)
{
  ::unlink(filename);
}

int32_t stdio_file_traits::read(stdio_file_traits::file_type f, uint8_t * buf, int32_t bufSize)
{
  // Initiate prefetch of next chunk.
  ssize_t pos = ::lseek(f->mFile, 0, SEEK_CUR);
#ifdef IQL_HAS_FADVISE  
  ::posix_fadvise(f->mFile, pos+bufSize, bufSize, POSIX_FADV_WILLNEED);
#endif
  // Read what I am supposed to.
  int sz = ::read(f->mFile, buf, bufSize);
  return sz;
}

int32_t stdio_file_traits::write(stdio_file_traits::file_type f, uint8_t * buf, int32_t bufSize)
{
  for(;;) {
    ssize_t ret = ::write(f->mFile, buf, bufSize);
    if (ret < 0) {
      if (errno == EINTR)
	continue;
      throw errno;
    }
    return (int32_t) ret;
  }
}

bool stdio_file_traits::isEOF(stdio_file_traits::file_type f)
{
  return ::lseek(f->mFile, 0, SEEK_CUR) >= f->mEndOffset;  
}

void gzip_file_traits::expand(std::string pattern, 
			      int32_t numPartitions,
			      std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files)
{
  AutoFileSystem fs(URI::get("file:///"));
  fs->expand(pattern, numPartitions, files);  
}

int32_t gzip_file_traits::read(file_type f, uint8_t * buf, int32_t bufSize)
{
  int tmp = ::gzread(f->mFile, buf, bufSize);
  if (tmp < 0) {
    int err = errno;
    throw std::runtime_error((boost::format("Failure reading from compressed file: error = %1%") % err).str());
  }
  return tmp;
}

bool gzip_file_traits::isEOF(file_type f)
{
  return 0 != ::gzeof(f->mFile) || ::gztell(f->mFile) >= f->mEndOffset;
}

static boost::mutex sDataBlockFactoryGuard;

DataBlockFactory::DataBlockFactory()
  :
  mGuard(NULL)
{
  mGuard = new boost::mutex();
}

DataBlockFactory::~DataBlockFactory()
{
  delete mGuard;
}

DataBlockFactory& DataBlockFactory::get()
{
  // TODO: Manage lifetime here...
  static DataBlockFactory * factory = NULL;
  boost::unique_lock<boost::mutex> lock(sDataBlockFactoryGuard);
  if (NULL == factory) {
    factory = new DataBlockFactory();
  }
  return *factory;
}

void DataBlockFactory::registerCreator(const std::string& uriScheme, 
					CreateDataBlockFn creator)
{
  boost::unique_lock<boost::mutex> lock(*mGuard);
  std::string ciScheme = boost::algorithm::to_lower_copy(uriScheme);
  if (mCreators.find(ciScheme) != mCreators.end()) {
    throw std::runtime_error((boost::format("Error: attempt to register "
					    "URI scheme"
					    " %1% multiple times") %
			      uriScheme).str());
  }
  mCreators[ciScheme] = creator;
}

DataBlock * DataBlockFactory::create(const char * filename,
				     int32_t targetBlockSize,
				     uint64_t begin,
				     uint64_t end)
{
  boost::unique_lock<boost::mutex> lock(*mGuard);
  UriPtr uri = UriPtr(new URI(filename));
  std::string ciScheme = boost::algorithm::to_lower_copy(uri->getScheme());
  if (0 == ciScheme.size()) {
    // Default scheme
    ciScheme = URI::getDefaultScheme();
  }
  std::map<std::string, CreateDataBlockFn>::const_iterator it = 
    mCreators.find(ciScheme);
  if (it == mCreators.end()) {
    throw std::runtime_error((boost::format("Error: attempt to create "
					    "data block with unknown "
					    "scheme %1%") %
			      uri->getScheme()).str());
  }

  return it->second(filename, targetBlockSize, begin, end);
}

DataBlock * DataBlock::get(const char * filename,
			   int32_t targetBlockSize,
			   uint64_t begin,
			   uint64_t end)
{
  DataBlockFactory & f(DataBlockFactory::get());
  return f.create(filename, targetBlockSize, begin, end);
}

class StdioDataBlockRegistrar
{
public:
  StdioDataBlockRegistrar();
  static DataBlock * create(const char * filename,
			    int32_t targetBlockSize,
			    uint64_t begin,
			    uint64_t end);
};

StdioDataBlockRegistrar::StdioDataBlockRegistrar()
{
  DataBlockFactory & factory(DataBlockFactory::get());
  factory.registerCreator("file", &create);
}

DataBlock * StdioDataBlockRegistrar::create(const char * filename,
					    int32_t targetBlockSize,
					    uint64_t begin,
					    uint64_t end)
{
  typedef BlockBufferStream<stdio_file_traits> file_block;
  typedef BlockBufferStream<zlib_file_traits<file_block> > zlib_file_block;
  URI uri(filename);
  bool compressed = uri.getPath().size() > 3 &&
    boost::algorithm::iequals(".gz", 
			      uri.getPath().substr(uri.getPath().size()-3));
  if (compressed) {
    return new zlib_file_block(uri.getPath().c_str(), 
			       targetBlockSize, begin, end);
  } else {
    return new file_block(uri.getPath().c_str(), 
			  targetBlockSize, begin, end);
  }
}

// Static to register file system
static StdioDataBlockRegistrar dataBlockRegistrar;

class MemoryMappedPrefetch
{
private:
  boost::thread * mThread;
  boost::mutex mGuard;
  boost::condition_variable mCondVar;
  const uint8_t * mBegin;
  const uint8_t * mEnd;
  // Put this here to avoid having the 
  // reader optimized away.
  uint8_t mLast;
  bool mStop;

  void run();

public:
  MemoryMappedPrefetch();
  ~MemoryMappedPrefetch();
  /**
   * Start reading the region.
   */
  void start(const uint8_t * start, const uint8_t * end);
  /**
   * Stop reading the region.
   */
  void stop();
  /**
   * Kill the tread and wait for termination.
   */
  void shutdown();
};

MemoryMappedPrefetch::MemoryMappedPrefetch()
  :
  mThread(NULL),
  mBegin(NULL),
  mEnd(NULL),
  mLast(0),
  mStop(false)
{
  mThread = new boost::thread(boost::bind(&MemoryMappedPrefetch::run, this));
}

MemoryMappedPrefetch::~MemoryMappedPrefetch()
{
  shutdown();
}

void MemoryMappedPrefetch::run()
{
  while(true) {
    {
      boost::unique_lock<boost::mutex> lock(mGuard);
      while(mBegin == NULL && !mStop) {
	mCondVar.wait(lock);
      }
    }
    if (mStop) break;
    for(const uint8_t * it = mBegin;
	it < mEnd;
	it += 8*1024) {
      mLast = *it;
    }
    {
      boost::unique_lock<boost::mutex> lock(mGuard);
      mBegin = mEnd = NULL;
      mCondVar.notify_one();
    }
  }
}

void MemoryMappedPrefetch::start(const uint8_t * begin, const uint8_t * end)
{
  BOOST_ASSERT(mBegin == NULL);
  BOOST_ASSERT(mEnd == NULL);
  boost::unique_lock<boost::mutex> lock(mGuard);
  mBegin = begin;
  mEnd = end;
  mCondVar.notify_one();
}

void MemoryMappedPrefetch::stop()
{
  boost::unique_lock<boost::mutex> lock(mGuard);
  while(mBegin != 0) {
    std::cout << "Waiting on prefetcher..." << std::endl;
    mCondVar.wait(lock);
  }
}

void MemoryMappedPrefetch::shutdown()
{
  boost::unique_lock<boost::mutex> lock(mGuard);
  mStop = true;
  mCondVar.notify_one();
  if (mThread) {
    mThread->join();
    delete mThread;
    mThread = NULL;
  }
}

MemoryMappedFileBuffer::MemoryMappedFileBuffer(const char * file,
					       int32_t blockSize,
					       uint64_t beginOffset,
					       uint64_t endOffset)
  :
  mBlockSize(blockSize),
  mFileSize(0),
  mStreamEnd(endOffset),
  mPrefetcher(NULL)
{
  // We need to know the file size so we don't read too far.
  mFileSize = boost::filesystem::file_size(file);
  // We shouldn't read too far past this (to a record boundary if necessary).
  mStreamEnd = std::min(mFileSize, (uintmax_t) mStreamEnd);

  mMapping = boost::shared_ptr<boost::interprocess::file_mapping>(new boost::interprocess::file_mapping(file,
													boost::interprocess::read_only
													));
  mRegion = boost::shared_ptr<boost::interprocess::mapped_region>(new boost::interprocess::mapped_region(*mMapping.get(),
													boost::interprocess::read_only,
													beginOffset,
													mBlockSize));
  mCurrentBlockPtr = reinterpret_cast<uint8_t *>(mRegion->get_address());
  mCurrentBlockEnd = mCurrentBlockPtr + std::min(std::size_t(mFileSize-mRegion->get_offset()), 
				     mRegion->get_size());
  if (mRegion->get_size() <= 0)
    throw std::runtime_error("Invalid memory mapped region");
}

MemoryMappedFileBuffer::~MemoryMappedFileBuffer()
{
  if (mPrefetcher) {
    mPrefetcher->shutdown();
    delete mPrefetcher;
  }
}

void MemoryMappedFileBuffer::openWindow(std::size_t sz)
{
  // Are we done with the file?
  if (mFileSize == uintmax_t(mRegion->get_offset() + (mCurrentBlockPtr - reinterpret_cast<uint8_t*>(mRegion->get_address())))) return;

  // Make sure the prefetcher isn't still referencing the window we are about to
  // unmap.
  // if(mPrefetcher == NULL)
  //   mPrefetcher = new MemoryMappedPrefetch();
  // mPrefetcher->stop();

  // Figure out where we have to start from.  If there is a mark then we have to account
  // for the mark in the requsted window size.
  uint8_t * keep = mCurrentBlockMark ? mCurrentBlockMark : mCurrentBlockPtr;
  std::size_t ptrOffset = std::size_t(mCurrentBlockPtr - keep);
  sz += ptrOffset;
  // Round up sz to nearest page
  sz = (sz + boost::interprocess::mapped_region::get_page_size() - 1)/boost::interprocess::mapped_region::get_page_size();

  // Open up a block of at least size sz starting at keep.
  mRegion = boost::shared_ptr<boost::interprocess::mapped_region>(
								 new boost::interprocess::mapped_region(*mMapping.get(),
													boost::interprocess::read_only,
													std::size_t(mRegion->get_offset() + 
														    (keep-reinterpret_cast<uint8_t *>(mRegion->get_address()))),
													std::max(sz, mBlockSize)));
  uint8_t * newRegionStart = reinterpret_cast<uint8_t *>(mRegion->get_address());
  mCurrentBlockMark = mCurrentBlockMark ? newRegionStart : NULL;
  mCurrentBlockPtr = newRegionStart + ptrOffset;  
  mCurrentBlockEnd = newRegionStart + std::min(std::size_t(mFileSize-mRegion->get_offset()), 
					 mRegion->get_size());
  if (mRegion->get_size() <= 0)
    throw std::runtime_error("Invalid memory mapped region");

  // // Let OS know we'll be scanning this puppy
  // int pg = getpagesize();
  // uint8_t * firstPage = (uint8_t *) (pg*(std::size_t(newRegionStart)/pg));
  // madvise(firstPage, std::size_t(mRegionEnd - firstPage), MADV_WILLNEED | MADV_SEQUENTIAL);

  // Hand off to the prefetcher
  // mPrefetcher->start(newRegionStart, mRegionEnd);
}

bool MemoryMappedFileBuffer::isEOF() 
{
  uintmax_t filePointer = uintmax_t(mRegion->get_offset() + 
				    (mCurrentBlockPtr - reinterpret_cast<uint8_t*>(mRegion->get_address())));
  return mStreamEnd <= filePointer;
}

ExplicitChunkStrategy::ExplicitChunkStrategy()
{
}

ExplicitChunkStrategy::~ExplicitChunkStrategy()
{
}

void ExplicitChunkStrategy::expand(const std::string& file,
				   int32_t numPartitions)
{
  FileSystem * fs = FileSystem::get(boost::make_shared<URI>(file.c_str()));
  // Expand file name globbing
  fs->expand(file, numPartitions, mFile);
  FileSystem::release(fs);
  fs = NULL;

  // At least one match for the file/file pattern
  if (mFile.size() == 0)
    throw std::runtime_error((boost::format("Could not find any file %1%") % file).str());
}

void ExplicitChunkStrategy::getFilesForPartition(int32_t partition,
						 std::vector<boost::shared_ptr<FileChunk> >& files) const
{
  files = mFile[partition];
}

SerialChunkStrategy::SerialChunkStrategy()
{
}

SerialChunkStrategy::~SerialChunkStrategy()
{
}

void SerialChunkStrategy::expand(const PathPtr & uri,
				 int32_t numPartitions)
{
  // No expansion
  mUri = uri;
}

void SerialChunkStrategy::getFilesForPartition(int32_t partition,
					       std::vector<boost::shared_ptr<FileChunk> >& files) const
{
  // get file that matches the serial.
  std::ostringstream ss;
  ss << "serial_" << std::setw(5) << std::setfill('0') << partition;
  std::string sn(ss.str());
  typedef std::vector<boost::shared_ptr<FileStatus> > fstats;
  fstats ls;
  {
    // use scope to hide variable fs.
    // Get the file system for the uri
    FileSystem * fs = FileSystem::get(mUri->getUri());
    fs->list(mUri, ls);
    FileSystem::release(fs);
  }
  for(fstats::iterator fit = ls.begin();
      fit != ls.end();
      ++fit) {
    const std::string& fname((*fit)->getPath()->toString());
    // Extract out the filename from the full path and check what
    // serial it is.
    boost::filesystem::path fsPath((*fit)->getPath()->getUri()->getPath());
    std::string stm = fsPath.stem();
    if (stm.size() >= sn.size() &&
	boost::algorithm::equals(sn, stm.substr(0, sn.size()))) {
      files.push_back(boost::make_shared<FileChunk>(fname, 
						    0,
						    std::numeric_limits<uint64_t>::max()));
    }
  }    
}

