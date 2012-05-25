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

#include <fstream>
#include <iomanip>
#include <sstream>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "uriparser/Uri.h"
// TODO: GCC specific.  Replace.
#include "glob.h"

#include "RecordType.hh"
#include "IQLInterpreter.hh"

#include "FileSystem.hh"

UriPtr URI::get(const char * filename)
{
  return boost::make_shared<URI>(filename);
}

URI::URI(const char * filename) 
  :
  mBuffer(filename)
{
  // Parse the filename
  UriUriA uri;
  UriParserStateA parserState;
  parserState.uri = &uri;
  if (uriParseUriA(&parserState, mBuffer.c_str()) != URI_SUCCESS) {
    uriFreeUriMembersA(&uri);
    throw std::runtime_error((boost::format("Failed to parse HDFS filename: %1%") % filename).str());
  }
  init(&uri);

  // Done with URI
  uriFreeUriMembersA(&uri);
}
  
URI::URI(const URI& parent, const URI& child)
{
  // Reparse uri strings into Uri objects then resolve.
  UriUriA uri;
  UriUriA p;
  UriUriA c;
  UriParserStateA parserState;
  parserState.uri = &p;
  if (uriParseUriA(&parserState, parent.toString().c_str()) != URI_SUCCESS) {
    uriFreeUriMembersA(&p);
    throw std::runtime_error((boost::format("Failed to parse URI: %1%") % 
  			      parent.toString()).str());
  }
  parserState.uri = &c;
  if (uriParseUriA(&parserState, child.toString().c_str()) != URI_SUCCESS) {
    uriFreeUriMembersA(&p);
    uriFreeUriMembersA(&c);
    throw std::runtime_error((boost::format("Failed to parse URI: %1%") % 
  			      child.toString()).str());
  }
  
  if (uriAddBaseUriA(&uri, &c, &p) != URI_SUCCESS) {
    uriFreeUriMembersA(&p);
    uriFreeUriMembersA(&c);
    uriFreeUriMembersA(&uri);
    throw std::runtime_error((boost::format("Failure resolving relative URI %1% against base %2%") %
			      child.toString() % parent.toString()).str());
  }
  // All done resolving
  uriFreeUriMembersA(&p);
  uriFreeUriMembersA(&c);

  init(&uri);

  // Create a string for the URI that this class owns
  int charsRequired=0;
  if (uriToStringCharsRequiredA(&uri, &charsRequired) != URI_SUCCESS) {
    throw std::runtime_error("Failed to allocate memory");
  }
  // This actually allocates the extra character for end of string
  mBuffer.resize(charsRequired);
  // Increment to let uriparser know about the extra space.
  charsRequired++;
  uriToStringA(&mBuffer[0], &uri, charsRequired, NULL);

  // Done with URI
  uriFreeUriMembersA(&uri);
}

URI::~URI()
{
}

void URI::init(UriUriA * uri)
{
  mScheme.assign(uri->scheme.first, uri->scheme.afterLast);

  
  if (uri->hostText.first) {
    std::string hdfsHost(uri->hostText.first, uri->hostText.afterLast);
    mHost = hdfsHost;
  }
  mPort = 0;
  if (uri->portText.first) {
    mPort = boost::lexical_cast<int32_t>(std::string(uri->portText.first, 
							 uri->portText.afterLast));
  }
    
  // Put the path segments back together again.
  // I don't believe that these are contiguous.
  for(UriPathSegmentA * segment = uri->pathHead;
      segment != NULL;
      segment = segment->next) {
    if (mScheme.size() || uri->absolutePath || segment != uri->pathHead)
      mFile += "/";
    mFile += std::string(segment->text.first,
			 segment->text.afterLast);
  }
}

const std::string& URI::toString() const
{
  return mBuffer;
}

const char * URI::getDefaultScheme()
{
  return "file";
}

Path::Path(const std::string& str)
  :
  mUri(boost::make_shared<URI>(str.c_str()))
{
}

Path::Path(UriPtr uri)
  :
  mUri(uri)
{
}

Path::Path(PathPtr parent, PathPtr child)
  :
  mUri(boost::make_shared<URI>(*parent->mUri.get(), *child->mUri.get()))
{
}

PathPtr Path::get(const std::string& p)
{
  return boost::shared_ptr<Path>(new Path(p));
}

PathPtr Path::get(UriPtr p)
{
  return boost::shared_ptr<Path>(new Path(p));
}

PathPtr Path::get(PathPtr parent, PathPtr child)
{
  return boost::shared_ptr<Path>(new Path(parent, child));
}

PathPtr Path::get(PathPtr parent,
		  const std::string& child)
{
  return get(parent, get(child));
}

PathPtr Path::get(const std::string& parent,
		  const std::string& child)
{
  return get(get(parent), get(child));
}

const std::string& Path::toString()
{
  return mUri->toString();
}

static boost::mutex sFileSystemFactoryGuard;

FileSystemFactory::FileSystemFactory()
  :
  mGuard(NULL)
{
  mGuard = new boost::mutex();
}

FileSystemFactory::~FileSystemFactory()
{
  delete mGuard;
}

FileSystemFactory& FileSystemFactory::get()
{
  // TODO: Manage lifetime here...
  static FileSystemFactory * factory = NULL;
  boost::unique_lock<boost::mutex> lock(sFileSystemFactoryGuard);
  if (NULL == factory) {
    factory = new FileSystemFactory();
  }
  return *factory;
}

void FileSystemFactory::registerCreator(const std::string& uriScheme, 
					CreateFileSystemFn creator)
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

FileSystem * FileSystemFactory::create(UriPtr uri)
{
  boost::unique_lock<boost::mutex> lock(*mGuard);
  std::string ciScheme = boost::algorithm::to_lower_copy(uri->getScheme());
  if (0 == ciScheme.size()) {
    // Default scheme
    ciScheme = URI::getDefaultScheme();
  }
  std::map<std::string, CreateFileSystemFn>::const_iterator it = 
    mCreators.find(ciScheme);
  if (it == mCreators.end()) {
    throw std::runtime_error((boost::format("Error: attempt to create "
					    "filesystem with unknown "
					    "scheme %1%") %
			      uri->getScheme()).str());
  }

  return it->second(uri);
}

FileSystem * FileSystem::get(UriPtr uri)
{
  FileSystemFactory & f(FileSystemFactory::get());
  return f.create(uri);
}

std::string FileSystem::readFile(const std::string& uri)
{
  return readFile(boost::make_shared<URI>(uri.c_str()));
}

std::string FileSystem::readFile(UriPtr uri)
{
  std::string tmp;
  AutoFileSystem fs(uri);
  fs->readFile(uri, tmp);
  return tmp;
}

void FileSystem::release(FileSystem * fs)
{
  delete fs;
}

static std::string base16_encode(const uint8_t * in,
				 std::size_t bufSz)
{
  const char * lut = "0123456789ABCDEFG";
  std::string tmpStr(2*bufSz, ' ');
  char * tmpIt = &tmpStr[0];
  for(std::size_t i = 0; i<bufSz; i++) {
    *tmpIt++ = lut[(in[i] & 0xF0) >> 4];
    *tmpIt++ = lut[in[i] & 0x0F];    
  }  
  return tmpStr;
}

std::string FileSystem::getTempFileName()
{
  boost::uuids::uuid tmp = boost::uuids::random_generator()();
  return base16_encode((const uint8_t *) &tmp, tmp.size());
}

class LocalFileSystemImpl
{
private:
  PathPtr mRoot;
  boost::shared_ptr<FileStatus> createFileStatus(const boost::filesystem::path & fsPath);
public:
  LocalFileSystemImpl();
  LocalFileSystemImpl(const char * root);
  LocalFileSystemImpl(UriPtr uri);
  ~LocalFileSystemImpl();
  
  /**
   * Get the root of the file system.
   * Should this be a URI or a PathPtr?
   */
  PathPtr getRoot();

  /**
   * Get information about a path.
   */
  boost::shared_ptr<FileStatus> getStatus(PathPtr p);

  /**
   * Does this path exist in the file system?
   */
  bool exists(PathPtr p);

  /**
   * Get a directory listing of a path that isDirectory.
   */
  void list(PathPtr p,
	    std::vector<boost::shared_ptr<FileStatus> >& result);

  /**
   * Read the contents of a file into a std::string.
   */
  void readFile(UriPtr uri, std::string& out);
};

LocalFileSystemImpl::LocalFileSystemImpl()
  :
  mRoot(Path::get(std::string("file:///")))
{
}

LocalFileSystemImpl::LocalFileSystemImpl(const char * root)
  :
  mRoot(Path::get(std::string(root)))
{
}

LocalFileSystemImpl::LocalFileSystemImpl(UriPtr uri)
  :
  mRoot(Path::get(uri))
{
}

LocalFileSystemImpl::~LocalFileSystemImpl()
{
}

PathPtr LocalFileSystemImpl::getRoot()
{
  return mRoot;
}

boost::shared_ptr<FileStatus> LocalFileSystemImpl::createFileStatus(const boost::filesystem::path & fsPath)
{
  boost::filesystem::file_status s = boost::filesystem::status(fsPath);
  std::string pathStr = fsPath.string();
  if(boost::filesystem::is_directory(s) &&
     pathStr[pathStr.size()-1] != '/') 
    pathStr += std::string("/");
    
  return boost::make_shared<FileStatus>(Path::get(getRoot(), pathStr),
					boost::filesystem::is_regular_file(s), 
					boost::filesystem::is_directory(s), 
					boost::filesystem::is_regular_file(s) ?
					boost::filesystem::file_size(fsPath) :
					0);
}

boost::shared_ptr<FileStatus> LocalFileSystemImpl::getStatus(PathPtr p)
{
  // Validate that this is a file URI.
  if (!boost::algorithm::iequals("file", p->getUri()->getScheme()))
    throw std::runtime_error((boost::format("Invalid URI used with local file system: %1%") %
			      p->toString()).str());
  boost::filesystem::path fsPath(p->getUri()->getPath());
  return createFileStatus(fsPath);
}

bool LocalFileSystemImpl::exists(PathPtr p)
{
  // Validate that this is a file URI.
  if (!boost::algorithm::iequals("file", p->getUri()->getScheme()))
    throw std::runtime_error((boost::format("Invalid URI used with local file system: %1%") %
			      p->toString()).str());
  boost::filesystem::path fsPath(p->getUri()->getPath());
  return boost::filesystem::exists(fsPath);
}

void LocalFileSystemImpl::list(PathPtr p,
			  std::vector<boost::shared_ptr<FileStatus> >& result)
{
  if (!boost::algorithm::iequals("file", p->getUri()->getScheme()))
    throw std::runtime_error((boost::format("Invalid URI used with local file system: %1%") %
			      p->toString()).str());
  boost::filesystem::path fsPath(p->getUri()->getPath());
  boost::filesystem::directory_iterator end;
  for(boost::filesystem::directory_iterator it(fsPath);
      it != end;
      ++it) {
    result.push_back(createFileStatus(it->path()));
  }
}

void LocalFileSystemImpl::readFile(UriPtr uri, std::string& out)
{
  std::stringstream sstr;
  std::fstream f(uri->getPath().c_str(), std::ios_base::in);
  std::copy(std::istreambuf_iterator<char>(f),
	    std::istreambuf_iterator<char>(),
	    std::ostreambuf_iterator<char>(sstr));
  out = sstr.str();
}

// Class to register the LocalFileSystem class.
// Yes, there could be a template here but it
// would require a template indirection to 
// pass the string as a template arg.  A little
// cut and paste doesn't seem so bad by comparison.
class LocalFileSystemRegistrar
{
public:
  LocalFileSystemRegistrar();
  static FileSystem * create(UriPtr uri);
};

LocalFileSystemRegistrar::LocalFileSystemRegistrar()
{
  FileSystemFactory & factory(FileSystemFactory::get());
  factory.registerCreator("file", &create);
}

FileSystem * LocalFileSystemRegistrar::create(UriPtr uri)
{
  return new LocalFileSystem2(uri);
}

// Static to register file system
static LocalFileSystemRegistrar registrar;

LocalFileSystem2::LocalFileSystem2()
  :
  mImpl(new LocalFileSystemImpl())
{
}

LocalFileSystem2::LocalFileSystem2(const char * root)
  :
  mImpl(new LocalFileSystemImpl(root))
{
}

LocalFileSystem2::LocalFileSystem2(UriPtr uri)
  :
  mImpl(new LocalFileSystemImpl(uri))
{
}

LocalFileSystem2::~LocalFileSystem2()
{
  delete mImpl;
}
  
static bool canBeSplit(const std::string& file)
{
  // Gzip compressed files cannot be split.  Others can.
  std::size_t len = file.size();
  return  len < 3 || !boost::algorithm::iequals(".gz", &file[len-3]);
}

class FileInfo
{
public:
  std::string mName;
  uintmax_t mSize;
  FileInfo(const std::string& name, uintmax_t sz)
    :
    mName(name),
    mSize(sz)
  {
  }
};

void LocalFileSystem2::expand(std::string pattern,
			      int32_t numPartitions,
			      std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files)
{
  // Expand the pattern and then collect the size of each file.
  std::vector<std::string> fileNames;
  Glob::expand(pattern, fileNames);
  // No match.  File doesn't exist.
  if (fileNames.size() == 0) return;

  // TODO: Putting the required info into an acceptable container for a generic
  // algorithm that should be factored out.
  std::vector<FileInfo> fileInfo;
  for(std::vector<std::string>::const_iterator it = fileNames.begin();
      it != fileNames.end();
      ++it) {
    fileInfo.push_back(FileInfo(*it, boost::filesystem::file_size(*it)));
  }
  int numEntries = (int) fileNames.size();

  // TODO: Should we sort the files on size????
  // Get size of all files.
  std::vector<uint64_t> cumulativeSizes;
  for(int i=0; i<numEntries; i++) {
    cumulativeSizes.push_back(fileInfo[i].mSize + (i==0 ? 0 : cumulativeSizes.back()));
  }
  uint64_t totalFileSize=cumulativeSizes.back();

  // Allocate a list of chunks for each partition.
  files.resize(numPartitions);
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
      bool splittable = canBeSplit(fileInfo[currentFile].mName);
      if (!splittable ||
	  currentFileRemaining <= currentPartitionRemaining ||
	  currentFileRemaining-currentPartitionRemaining <= minimumFileAllocationSize) {
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
      // Attach allocation to partition. Also pretend
      // the file is infinitely large if compressed otherwise it will confuse the code that
      // tries to limit reading to a subchunk of a file due to the difference
      // in size between compressed and uncompressed sizes.
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
}

PathPtr LocalFileSystem2::getRoot()
{
  return mImpl->getRoot();
}

boost::shared_ptr<FileStatus> LocalFileSystem2::getStatus(PathPtr p)
{
  return mImpl->getStatus(p);
}

bool LocalFileSystem2::exists(PathPtr p)
{
  return mImpl->exists(p);
}

void LocalFileSystem2::list(PathPtr p,
			    std::vector<boost::shared_ptr<FileStatus> >& result)
{
  mImpl->list(p, result);
} 

void LocalFileSystem2::readFile(UriPtr uri, std::string& out)
{
  mImpl->readFile(uri, out);
}

SerialOrganizedTable::SerialOrganizedTable()
{
}  

SerialOrganizedTable::SerialOrganizedTable(int32_t commonVersion,
					   int32_t tableMajorVersion,
					   const std::string& tableName,
					   const char * pred)
  :
  mCommonVersion(commonVersion),
  mTableMajorVersion(tableMajorVersion),
  mTableName(tableName),
  mContext(NULL),
  mRecordType(NULL),
  mPredicate(NULL),
  mMinorVersionField(0),
  mDateField(1),
  mRuntimeContext(NULL)
{
  // These are the path components of the table.
  // We assume that it is possible to write
  // predicates against all of these.
  mPathComponents.push_back("MinorNumber");
  mPathComponents.push_back("Date");
  mPathComponents.push_back("BatchId");
  
  // Create a record type against which we evaluate predicates.
  mContext = new DynamicRecordContext();
  std::vector<RecordMember> members;
  members.push_back(RecordMember("MinorNumber", VarcharType::Get(*mContext)));
  members.push_back(RecordMember("Date", VarcharType::Get(*mContext)));
  members.push_back(RecordMember("BatchId", VarcharType::Get(*mContext)));
  mRecordType = RecordType::get(*mContext, members);

  mFields.push_back(mRecordType->getFieldAddress("MinorNumber"));
  mFields.push_back(mRecordType->getFieldAddress("Date"));
  mFields.push_back(mRecordType->getFieldAddress("BatchId"));
  mMinorVersionField = 0;
  mDateField = 1;

  std::vector<RecordMember> emptyMembers;
  const RecordType * emptyTy = RecordType::get(*mContext, emptyMembers);
  std::vector<const RecordType *> types;
  types.push_back(mRecordType);
  types.push_back(emptyTy);

  if (pred) {
    mPredicate = new RecordTypeFunction(*mContext, "chareq", types, pred);
    mRuntimeContext = new InterpreterContext();
  }
}

SerialOrganizedTable::~SerialOrganizedTable()
{
  delete mContext;
  delete mPredicate;
  // Note: mRecordType owned by mContext.
  delete mRuntimeContext;
}

void SerialOrganizedTable::bindComponent(FileSystem * fs,
					 std::size_t level,
					 PathPtr p)
{
  std::vector<boost::shared_ptr<FileStatus> > ls;
  fs->list(p, ls);
  if (level + 1 == mPathComponents.size()) {
    for(std::vector<boost::shared_ptr<FileStatus> >::iterator pit = ls.begin();
	pit != ls.end();
	++pit) {
      // Extract the path elements and evaluate predicate against them.
      // TODO: Support more that VARCHAR data type.
      boost::filesystem::path fsPath((*pit)->getPath()->getUri()->getPath());
      if (std::size_t(std::distance(fsPath.begin(), fsPath.end())) < 
	  mPathComponents.size()) {
	throw std::runtime_error("Invalid table path.  Too short to evaluate predicate");
      }
      // Set the last path components into the fields in reverse order.
      boost::filesystem::path::iterator comp = fsPath.end();
      --comp;
      // We expect to have a trailing slash in the URI hence a trailing dot in
      // the Boost filesystem path.  Skip it if it is there.
      if (*comp == ".") {
	--comp;
      }
      RecordBuffer buf = mRecordType->getMalloc().malloc();
      for(std::vector<FieldAddress>::const_reverse_iterator field = mFields.rbegin();
	  field != mFields.rend();
	  ++field) {
	std::string tmp = *comp;
	field->SetVariableLengthString(buf, comp->c_str(), comp->size());
	--comp;
      }
      bool good = mPredicate ?
	mPredicate->execute(buf, RecordBuffer(), mRuntimeContext) : true;
      const char * minorVersionStr = mFields[mMinorVersionField].getVarcharPtr(buf)->Ptr;
      int32_t minorVersion = boost::lexical_cast<int32_t>(minorVersionStr);
      std::string dateStr(mFields[mDateField].getVarcharPtr(buf)->Ptr);
      mRecordType->getFree().free(buf);
      if (good)
	mSerialPaths.push_back(SerialOrganizedTableFile::get(minorVersion, dateStr,
							     Path::get(p, (*pit)->getPath())));
    }
  } else {
    for(std::vector<boost::shared_ptr<FileStatus> >::iterator pit = ls.begin();
	pit != ls.end();
	++pit) {
      // TODO: For predicates that don't reference all path components
      // evaluate as soon as possible to avoid directory expansion.
      // Such logic could be very fancy and extract portions of a single
      // predicate that could be evaluated at each level...
      if ((*pit)->isDirectory())
	bindComponent(fs, level+1, Path::get(p, (*pit)->getPath()));
    }
  }
}

void SerialOrganizedTable::bind(FileSystem * fs)
{
  // First we locate a directory named /CommonVersion_*
  // Then we have the table root.
  std::vector<boost::shared_ptr<FileStatus> > ls;
  fs->list(fs->getRoot(), ls);

  // Find the pattern /CommonVersion, should not be more than one match.
  // We look for the pattern relative to the URI base of the filesystem.
  std::string match = boost::lexical_cast<std::string>(mCommonVersion);
  match += "_"; 
  PathPtr rootPath;
  for(std::vector<boost::shared_ptr<FileStatus> >::iterator it=ls.begin();
      it != ls.end();
      ++it) {
    boost::filesystem::path fsPath((*it)->getPath()->getUri()->getPath());
    // Get the last component of the fsPath. Skip a dot (standing for trailing
    // slash) if present.
    boost::filesystem::path::iterator comp = fsPath.end();
    --comp;
    if (boost::algorithm::equals(*comp, ".")) {
      --comp;
    }
    if (comp->size() >= match.size() && 
	boost::algorithm::equals(match, comp->substr(0,match.size()))) {
      rootPath = (*it)->getPath();
      break;
    }
  }
  if (rootPath == PathPtr()) {
    throw std::runtime_error("Could not resolve table base");
  }
  PathPtr nextPath = Path::get(rootPath,
			       mTableName + "/");
  mTableRoot = Path::get(nextPath,
			 boost::lexical_cast<std::string>(mTableMajorVersion) + "/");

  // We recursively descend into the table root 
  // First layer is minor version
  // Second layer is date
  // Third layer is batch id.
  // Last layer is actual files.
  bindComponent(fs, 0, mTableRoot);
}

void SerialOrganizedTable::getSerialFiles(FileSystem * fs,
					  int32_t serialNumber,
					  std::vector<boost::shared_ptr<FileChunk> >& files) const
{
  std::ostringstream ss;
  ss << "serial_" << std::setw(5) << std::setfill('0') << serialNumber << ".gz";
  std::string sn(ss.str());
  for(std::vector<SerialOrganizedTableFilePtr>::const_iterator it = getSerialPaths().begin();
      it != getSerialPaths().end();
      ++it) {
    typedef std::vector<boost::shared_ptr<FileStatus> > fstats;
    fstats ls;
    fs->list((*it)->getPath(), ls);
    for(fstats::iterator fit = ls.begin();
	fit != ls.end();
	++fit) {
      const std::string& fname((*fit)->getPath()->toString());
      if (fname.size() >= sn.size() &&
	  boost::algorithm::equals(sn, fname.substr(fname.size()-sn.size()))) {
	files.push_back(boost::make_shared<FileChunk>(fname, 
						      0,
						      std::numeric_limits<uint64_t>::max()));
      }
    }
  }  
}

// GNU glob interface.  
// TODO: For portability we want an independent implementation.
void Glob::expand(std::string pattern, std::vector<std::string>& files)
{
  // // Boost filesystem doesn't handle ~
  // if (pattern[0] == '~')
  //   pattern = std::string(getenv("HOME")) + pattern.substr(1);
  // // Can we apply this to the glob?
  // boost::filesystem::path fullPath = boost::filesystem::system_complete(pattern);
  // // Break off the file from the directory.
  // std::string filename = fullPath.filename();
  // boost::filesystem::path dir = fullPath.parent_path();
  // // Make the glob into a regex
  // std::string rexStr = ::glob_to_regex(filename);
  // boost::regex rex (rexStr);
  // // Iterate over current directory looking for matches
  // boost::filesystem::directory_iterator end_iter;
  // for (boost::filesystem::directory_iterator dir_itr( dir );
  //      dir_itr != end_iter;
  //      ++dir_itr ) {
  //   boost::filesystem::path tmpPath = dir_itr->path();
  //   std::string tmpFileName = tmpPath.filename();

  //   if(boost::filesystem::is_regular_file(dir_itr->status()) &&
  //      boost::regex_match(tmpPath.filename(), rex)) {
  //     files.push_back(tmpPath.file_string());
  //   }
  // }
  glob_t result;
  glob(pattern.c_str(), GLOB_TILDE, NULL, &result);
  for(std::size_t i=0; i<result.gl_pathc; ++i) {
    files.push_back(result.gl_pathv[i]);
  }
  globfree(&result);
}

