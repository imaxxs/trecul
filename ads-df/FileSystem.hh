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

#ifndef __FILESYSTEM_HH__
#define __FILESYSTEM_HH__

#include <stdint.h>
#include <string>
#include <vector>
#include <map>
#include <boost/utility.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>

/** 
 * Forward declarations
 */
namespace boost {
  class mutex;
}

/**
 * A contiguous piece of a file for processing.  This is a collection of bytes
 * and is not guaranteed to be aligned with any particular record format.
 */
class FileChunk
{
private:
  std::string mFilename;
  uint64_t mBegin;
  uint64_t mEnd;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mFilename);
    ar & BOOST_SERIALIZATION_NVP(mBegin);
    ar & BOOST_SERIALIZATION_NVP(mEnd);
  }
  FileChunk()
  {
  }  
public:
  FileChunk(const std::string& filename,
	    uint64_t begin,
	    uint64_t end)
    :
    mFilename(filename),
    mBegin(begin),
    mEnd(end)
  {
  }
  const std::string& getFilename() const { return mFilename; }
  uint64_t getBegin() const { return mBegin; }
  uint64_t getEnd() const { return mEnd; }
};

typedef boost::shared_ptr<class URI> UriPtr;

class URI : boost::noncopyable
{
private:
  std::string mBuffer;
  std::string mScheme;
  std::string mHost;
  int32_t mPort;
  std::string mFile;

  void init(struct UriUriStructA * uri);

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mBuffer);
    ar & BOOST_SERIALIZATION_NVP(mScheme);
    ar & BOOST_SERIALIZATION_NVP(mHost);
    ar & BOOST_SERIALIZATION_NVP(mPort);
    ar & BOOST_SERIALIZATION_NVP(mFile);
  }
  URI()
  {
  }  
public:
  static UriPtr get(const char * filename);
  URI(const char * filename) ;
  URI(const URI& parent, const URI& child);
  ~URI();

  const std::string & getScheme() const { return mScheme; }
  const std::string & getHost() const { return mHost; }
  int32_t getPort() const { return mPort; }
  const std::string & getPath() const { return mFile; }
  const std::string & toString() const;

  static const char * getDefaultScheme();
};

typedef boost::shared_ptr<class Path> PathPtr;

class Path : boost::noncopyable
{
private:
  UriPtr mUri;
  Path(const std::string& str);
  Path(UriPtr uri);
  Path(PathPtr parent, PathPtr child);
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mUri);
  }
  Path()
  {
  }  
public:
  static PathPtr get(const std::string& p);
  static PathPtr get(UriPtr uri);
  static PathPtr get(PathPtr parent, PathPtr child);
  static PathPtr get(PathPtr parent,
		     const std::string& child);
  static PathPtr get(const std::string& parent,
		     const std::string& child);

  UriPtr getUri() const { return mUri; }
  const std::string& toString();
};

class FileStatus
{
private:
  uintmax_t mSize;
  PathPtr mPath;
  bool mIsRegularFile;
  bool mIsDirectory;
public:
  FileStatus(PathPtr p,
	     bool isRegularFile,
	     bool isDirectory,
	     uintmax_t sz)
    :
    mSize(sz),
    mPath(p),
    mIsRegularFile(isRegularFile),
    mIsDirectory(isDirectory)
  {
  }
  virtual ~FileStatus() {}
  bool isRegularFile() const 
  {
    return mIsRegularFile;
  }
  bool isDirectory() const 
  {
    return mIsDirectory;
  }
  uintmax_t size() const 
  {
    return mSize;
  }
  PathPtr getPath() const
  {
    return mPath;
  }
};

class FileSystemFactory : public boost::noncopyable
{
public:
  typedef class FileSystem * (*CreateFileSystemFn) (UriPtr);
private:
  boost::mutex * mGuard;
  std::map<std::string, CreateFileSystemFn> mCreators;
  FileSystemFactory();
  ~FileSystemFactory();
public:
  static FileSystemFactory& get();
  void registerCreator(const std::string& uriScheme, 
		       CreateFileSystemFn creator);
  class FileSystem * create(UriPtr uri);
};

class FileSystem
{
public:
  virtual ~FileSystem() {}
  /**
   * Get the file system associated with the URI.
   */
  static FileSystem * get(UriPtr uri);
  /**
   * Read an entire file into a string.
   */
  static std::string readFile(const std::string& uri);
  static std::string readFile(UriPtr uri);
  static void release(FileSystem * fs);

  /**
   * Get a temporary file name.
   */
  static std::string getTempFileName();

  virtual void expand(std::string pattern,
		      int32_t numPartitions,
		      std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files) = 0;
  /**
   * Get the root of the file system.
   */
  virtual PathPtr getRoot() =0;

  /**
   * Get information about a path.
   */
  virtual boost::shared_ptr<FileStatus> getStatus(PathPtr p) =0;

  /**
   * Does the path exist
   */
  virtual bool exists(PathPtr p) =0;

  /**
   * Get a directory listing of a path that isDirectory.
   */
  virtual void list(PathPtr p,
		    std::vector<boost::shared_ptr<FileStatus> >& result) =0;

  /**
   * Read the contents of a file into a std::string.
   */
  virtual void readFile(UriPtr uri, std::string& out) =0;
};

/**
 * autoptr for FileSystem
 */
class AutoFileSystem : boost::noncopyable
{
private:
  FileSystem * mFS;
public:
  AutoFileSystem(UriPtr uri)
    :
    mFS(FileSystem::get(uri))
  {
  }
  ~AutoFileSystem()
  {
    if (mFS) {
      FileSystem::release(mFS);
    }
  }
  FileSystem * operator->()
  {
    return mFS;
  }
};

class LocalFileSystem2 : public FileSystem
{
private:
  class LocalFileSystemImpl * mImpl;
public:
  LocalFileSystem2();
  LocalFileSystem2(const char * root);
  LocalFileSystem2(UriPtr uri);
  ~LocalFileSystem2();

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
   * Does the path exist
   */
  virtual bool exists(PathPtr p);

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

/**
 * A path to a file storing data for a serial organized table.
 * A path has attributes such as MinorVersion.  
 * TODO: We want path components of serial files to be metadata
 * driven (e.g. partitioning keys).  Integrate that functionality here.
 */
typedef boost::shared_ptr<class SerialOrganizedTableFile> SerialOrganizedTableFilePtr;

class SerialOrganizedTableFile
{
private:
  int32_t mMinorVersion;
  std::string mDate;
  PathPtr mPath;
public:
  SerialOrganizedTableFile(int32_t minorVersion,
			   const std::string& dateField,
			   PathPtr path)
    :
    mMinorVersion(minorVersion),
    mDate(dateField),
    mPath(path)
  {
  }

  int32_t getMinorVersion() const 
  {
    return mMinorVersion;
  }
  const std::string& getDate() const 
  {
    return mDate;
  }
  PathPtr getPath() const
  {
    return mPath;
  }

  static SerialOrganizedTableFilePtr get(int32_t minorVersion, 
					 const std::string& dateField,
					 PathPtr path)
  {
    return SerialOrganizedTableFilePtr(new SerialOrganizedTableFile(minorVersion, dateField, path));
  }
};

/**
 * A table that is hash partitioned on akid 
 * and range partitioned on date.
 * This class encapsulates the directory structure that is used
 * for storing such data.
 *
 * The format for serial organized tables URIs:
 * /CommonVersion_SerialCount/TableName/DBVersionNumber/MinorNumber/Date/BatchId/FileName
 * where FileName is of the form serial_ddddd
 * 
 * To name a table requires the triple:
 * CommonVersion
 * TableName
 * DBVersionNumber
 *
 * The scanning operators can accept limiting predicates on the date.
 * The serial number is determined by the serial/Hadoop input split associated 
 * with the map job in which the operator is executing.
 *
 * We want to be able to support reading serial organized tables out of
 * a local file system as well as HDFS (or other cluster files systems in
 * the future).  Thus we use a file system abstraction beneath the path 
 * manipulations.
 *
 */
class SerialOrganizedTable
{
private:
  int32_t mCommonVersion;
  int32_t mTableMajorVersion;
  std::string mTableName;
  // The path to /CommonVersion_SerialCount/TableName/DBVersionNumber
  PathPtr mTableRoot;
  // Beneath the table root we have
  // a number of path components.
  // Optionally we can handle evaluating
  // predicates against these path components
  // (e.g. date ranges).
  std::vector<std::string> mPathComponents;

  // These are the paths to directories containing
  // serials that obey our predicates.
  // TODO: For our application to event_d we want the
  // date to be part of the table schema.  We should
  // probably store it here.
  std::vector<SerialOrganizedTableFilePtr> mSerialPaths;

  // Support for evaluating predicates against the
  // directory structure.
  class DynamicRecordContext * mContext;
  const class RecordType * mRecordType;
  class RecordTypeFunction * mPredicate;
  std::vector<class FieldAddress> mFields;
  std::size_t mMinorVersionField;
  std::size_t mDateField;
  class InterpreterContext * mRuntimeContext;
  
  // Recurse down directory path to get to serials.
  void bindComponent(FileSystem * fs, 
		     std::size_t level, PathPtr p);

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mSerialPaths);
  }
  SerialOrganizedTable();
public:
  SerialOrganizedTable(int32_t commonVersion,
		       int32_t tableMajorVersion,
		       const std::string& tableName,
		       const char * pred = NULL);
  ~SerialOrganizedTable();
  void bind(FileSystem * fs);
  const std::vector<SerialOrganizedTableFilePtr>& getSerialPaths() const
  {
    return mSerialPaths;
  }
  /**
   * Get the files from filesystem associated with this serial number
   * and table.
   */
  void getSerialFiles(FileSystem * fs,
		      int32_t serialNumber,
		      std::vector<boost::shared_ptr<FileChunk> >& files) const;
};

class Glob
{
public:
  static void expand(std::string pattern, std::vector<std::string>& files);
};

#endif
