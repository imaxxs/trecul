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

#ifndef __RECORDPARSER_HH
#define __RECORDPARSER_HH

#include <string>
#include <vector>
#include <iostream>
#include <boost/shared_ptr.hpp>
#include <boost/assert.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/serialization/shared_ptr.hpp>

#include "zlib.h"

#include "RecordType.hh"
#include "RuntimeOperator.hh"
#include "FileSystem.hh"

namespace boost {
  namespace interprocess {
    class file_mapping;
    class mapped_region;
  }
}

// As I am thinking about buffer abstractions it seems that
// I want to factor out into some kind of policy objects the
// behavior where buffer memory comes from.
// For example we want the ZLib interface to play nicely as a
// filter in the buffer model but we don't want to force any
// buffer/memory management policy on the zlib interface.
// I think the _InputBuffer concept works fine for this but
// I don't think I have a workable _OutputBuffer concept that
// works as well.  Here we punt by using the 
// read() interface (that is to say we don't abstract the notion
// of _OutputBuffer rather we create a notion of how an output
// buffer would be filled in a pull fashion).  Note that we DON'T
// want the read() interface to actually be an _InputBuffer because
// input buffers must provide memory!
template <class _InputBuffer>
class ZLibDecompress
{
private:
  _InputBuffer mInput;
  z_stream mStream;
  std::size_t mSize;

public:
  ZLibDecompress(const char * filename)
    :
    mInput(filename, 64*1024),
    mSize(0)
  {
    mStream.zalloc = Z_NULL;
    mStream.zfree = Z_NULL;
    mStream.opaque = Z_NULL;
    ::inflateInit2(&mStream, 31);
    mStream.avail_in = 0;
    mStream.next_in = NULL;
  }
  ~ZLibDecompress()
  {
    ::inflateEnd(&mStream);
  }
  int32_t read(uint8_t * buf, int32_t bufSize)
  {
    mStream.avail_out = bufSize;
    mStream.next_out = buf;
    do {
      if (mStream.avail_in == 0) {
	// Try to open a full window but no worry if we get a short read.
	mSize = 64*1024;
	mInput.open(mSize, mStream.next_in);
	mStream.avail_in = (uInt) mSize;
	if (mSize == 0) {
	  if (mInput.isEOF()) {
	    break;
	  } else {
	    // We actually got a zero read without being at EOF
	    throw std::runtime_error("Error reading compressed file");
	  }
	}
      }
      int ret = ::inflate(&mStream, 1);
      if (mStream.avail_in == 0) {
	// We've exhausted input.  Try to get more next time
	// around.
	mInput.consume(mSize);
	mSize = 0;
      }
      if (mStream.avail_out == 0) {
	break;
      } else {
	// If there wasn't a problem, the either mStream.avail_in == 0 ||
	// mStream.avail_out == 0.
	if (mStream.avail_in != 0) {
	  throw std::runtime_error((boost::format("Error decompressing file: inflate returned %1%") %
				    ret).str());
	}
      }
    } while(true);

    // Done trying to read.
    return mStream.next_out ? mStream.next_out - buf : bufSize;
  }
  bool isEOF()
  {
    return mStream.avail_in == 0 && mInput.isEOF();
  }
};

template <class _InputBuffer>
class zlib_file_traits
{
public:
  typedef typename _InputBuffer::file_system_type file_system_type;
  typedef ZLibDecompress<_InputBuffer> * file_type;
  // Split into desired number of partitions.
  static void expand(std::string pattern, 
		     int32_t numPartitions,
		     std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files)
  {
    file_system_type::expand(pattern, numPartitions, files);
  }
  static file_type open_for_read(const char * filename, uint64_t beginOffset, uint64_t endOffset)
  {
    return file_type(new ZLibDecompress<_InputBuffer> (filename));
  }
  static void close(file_type f)
  {
    delete f;
  }
  static int32_t read(file_type f, uint8_t * buf, int32_t bufSize)
  {
    return f->read(buf, bufSize);
  }  
  static bool isEOF(file_type f)
  {
    return f->isEOF();
  }
};

class stdio_file_traits
{
  struct stdio_file_segment
  {
    int mFile;
    uint64_t mEndOffset;
    stdio_file_segment()
      :
      mFile(0),
      mEndOffset(std::numeric_limits<uint64_t>::max())
    {
    }
  };
public: 
  typedef stdio_file_segment * file_type;

  // Split into desired number of partitions.
  static void expand(std::string pattern, 
		     int32_t numPartitions,
		     std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files);
  static file_type open_for_read(const char * filename, uint64_t beginOffset, uint64_t endOffset);
  static file_type open_for_write(const char * filename);
  static void close(file_type f);
  static void remove(const char * filename);
  static int32_t read(file_type f, uint8_t * buf, int32_t bufSize);
  static int32_t write(file_type f, uint8_t * buf, int32_t bufSize);
  static bool isEOF(file_type f);
};

class gzip_file_traits
{
  struct gzip_file_segment
  {
    gzFile mFile;
    uint64_t mEndOffset;
    gzip_file_segment()
      :
      mFile(NULL),
      mEndOffset(std::numeric_limits<uint64_t>::max())
    {
    }
  };
public: 
  typedef gzip_file_segment * file_type;

  // Split into desired number of partitions.
  static void expand(std::string pattern, 
		     int32_t numPartitions,
		     std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files);

  static file_type open_for_read(const char * filename)
  {
    return open_for_read(filename, 0, std::numeric_limits<uint64_t>::max());
  }
  static file_type open_for_read(const char * filename, uint64_t beginOffset, uint64_t endOffset)
  {
    gzip_file_segment * tmp = new gzip_file_segment();
    tmp->mEndOffset = endOffset;
    tmp->mFile = ::gzopen(filename, "rb");
    if (tmp->mFile == NULL) {
      int save = errno;
      throw std::runtime_error((boost::format("Failed opening file %1% for reading: errno=%2%; message=%3%") % 
				filename %
				save %
				strerror(save)).str());
    }
    if (beginOffset) {
      // Doing this is a bad idea from a performance
      // perspective but we won't disallow it.
      ::gzseek(tmp->mFile, (z_off_t) beginOffset, SEEK_SET);
    }
    return tmp;
  }
  static file_type open_for_write(const char * filename)
  {
    gzip_file_segment * tmp = new gzip_file_segment();
    tmp->mFile = ::gzopen(filename, "wb");
    if (tmp->mFile == NULL)
      throw std::runtime_error((boost::format("Failed opening file %1% for writing") % filename).str());
    return tmp;
  } 
  static void close(file_type f)
  {
    ::gzclose(f->mFile);
  }
  static int32_t read(file_type f, uint8_t * buf, int32_t bufSize);
  static int32_t write(file_type f, uint8_t * buf, int32_t bufSize)
  {
    int tmp = ::gzwrite(f->mFile, buf, (unsigned) bufSize);
    if (tmp == 0) {
      int err = errno;
      throw std::runtime_error((boost::format("Failure writing to compressed file: error = %1%") % err).str());
    }
    return tmp;
  }
  static bool isEOF(file_type f);
};

template <class _InputBuffer>
class AutoMark
{
private:
  _InputBuffer & mBuffer;
public:
  AutoMark(_InputBuffer & buffer)
    :
    mBuffer(buffer)
  {
    mBuffer.setMark();
  }
  ~AutoMark()
  {
    mBuffer.releaseMark();
  }
};

template <class _InputBuffer>
class FieldImporter {
public:
  static void createDefaultImport(const RecordType * recordType,
				  const RecordType * baseRecordType,
				  char fieldDelim,
				  char recordDelim,
				  std::vector<FieldImporter<_InputBuffer> >& importers)
  {
    typedef FieldImporter<_InputBuffer> field_importer_type;
    // Apply default rules for inferring physical layout from
    // logical record definition.
    for(RecordType::const_member_iterator mit = baseRecordType->begin_members();
	mit != baseRecordType->end_members();
	++mit) {
      // TODO: Last field allow either record or field terminator so that
      // we can support ignoring trailing fields.
      char delim = mit+1 == baseRecordType->end_members() ? recordDelim : fieldDelim;
      if (recordType->hasMember(mit->GetName())) {
	const RecordMember& member(recordType->getMember(mit->GetName()));
	FieldAddress offset = recordType->getMemberOffset(mit->GetName());
	// Get the importer to set up.
	importers.push_back(field_importer_type());
	field_importer_type& fit(importers.back());
	// Dispatch on in-memory type.
	switch(member.GetType()->GetEnum()) {
	case FieldType::VARCHAR:
	  fit.InitVariableLengthTerminatedString(offset, delim, 
						 member.GetType()->isNullable());
	  break;
	case FieldType::CHAR:
	  fit.InitFixedLengthString(offset, 
				    member.GetType()->GetSize(), 
				    member.GetType()->isNullable());
	  break;
	case FieldType::BIGDECIMAL:
	  fit.InitDelimitedDecimal(offset, delim, member.GetType()->isNullable());
	  break;
	case FieldType::INT32:
	  fit.InitDecimalInt32(offset, 
			       member.GetType()->isNullable());
	  break;
	case FieldType::INT64:
	  fit.InitDecimalInt64(offset, 
			       member.GetType()->isNullable());
	  break;
	case FieldType::DOUBLE:
	  fit.InitDouble(offset, delim, 
			 member.GetType()->isNullable());
	  break;
	case FieldType::DATETIME:
	  fit.InitDelimitedDatetime(offset, 
				    member.GetType()->isNullable());
	  break;
	case FieldType::DATE:
	  fit.InitDelimitedDate(offset, 
				member.GetType()->isNullable());
	  break;
	case FieldType::FUNCTION:
	  throw std::runtime_error("Importing function types not supported");
	  break;
	default:
	  throw std::runtime_error("Importing unknown field type");
	}
      }
      importers.push_back(field_importer_type());
      importers.back().InitConsumeTerminatedString(delim);
    }
    // Optimize this a bit.  Iterate backward to find the
    // last importer that generates a value (not just consume).
    // Make one more importer consuming till the end of the line.
    for(std::size_t i=1; i<=importers.size(); ++i) {
      if (!importers[importers.size()-i].isConsumeOnly()) {
	if (i > 2) {
	  // Last generator is at size - i.  So make the last
	  // element a consumer at size - i + 1.  Thus new size
	  // is size-i+2.
	  importers.resize(importers.size() - i + 2);
	  importers.back().InitConsumeTerminatedString(recordDelim);
	}
	break;
      }
    }
  }

private:
  typedef bool (FieldImporter::*ImportFunc) (_InputBuffer& source, RecordBuffer target) const;
  std::size_t mSourceSize;
  FieldAddress mTargetOffset;
  ImportFunc mImporter;

  static const std::vector<ImportFunc>& availableImporters()
  {
    static std::vector<ImportFunc> funcs;
    if (funcs.size() == 0) {
      funcs.push_back(&FieldImporter::ImportFixedLengthString);
      funcs.push_back(&FieldImporter::ImportFixedLengthTerminatedString);
      funcs.push_back(&FieldImporter::ImportVariableLengthTerminatedString);
      funcs.push_back(&FieldImporter::ImportVariableLengthString);
      funcs.push_back(&FieldImporter::ConsumeTerminatedString);
      funcs.push_back(&FieldImporter::ImportDecimalInt32);
      funcs.push_back(&FieldImporter::ImportDouble);
      funcs.push_back(&FieldImporter::ImportDelimitedDatetime);
      funcs.push_back(&FieldImporter::ImportDelimitedDecimal);
      funcs.push_back(&FieldImporter::ImportDecimalInt64);
      funcs.push_back(&FieldImporter::ImportDelimitedDate);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportFixedLengthString>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportFixedLengthTerminatedString>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportVariableLengthTerminatedString>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportVariableLengthString>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ConsumeTerminatedString>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportDecimalInt32>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportDouble>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportDelimitedDatetime>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportDelimitedDecimal>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportDecimalInt64>);
      funcs.push_back(&FieldImporter::template ImportNullableField<&FieldImporter::ImportDelimitedDate>);
    }
    return funcs;
  }

  int getImporterIndex() const
  {
    if(mImporter == NULL) return -1;
    const std::vector<ImportFunc>& funcs(availableImporters());
    for(std::size_t i=0; i<funcs.size(); ++i) {
      if (mImporter == funcs[i]) {
	return i;
      }
    }
    throw std::runtime_error("Unknown importer");
  }
  ImportFunc getImporter(int idx)
  {
    if (idx==-1) return NULL;
    const std::vector<ImportFunc>& funcs(availableImporters());
    if (idx < 0 || (std::size_t) idx >= funcs.size()) {
      throw std::runtime_error("Invalid importer index");
    }
    return funcs[idx];
  }
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    int tmp = getImporterIndex();
    ar & BOOST_SERIALIZATION_NVP(mSourceSize);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(tmp);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    int tmp;
    ar & BOOST_SERIALIZATION_NVP(mSourceSize);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(tmp);
    mImporter =  getImporter(tmp);
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()

  static int32_t PrefixStringLength(const char * prefixLength)
  {
    int32_t val = (*prefixLength++ - '0') * 1000;
    val += (*prefixLength++ - '0') * 100;
    val += (*prefixLength++ - '0') * 10;
    val += (*prefixLength++ - '0');
    return val;
  }

  bool ImportFixedLengthString(_InputBuffer& source, RecordBuffer target) const
  {
    uint8_t * s = source.open(mSourceSize);
    if (s != NULL) {
      mTargetOffset.SetFixedLengthString(target, (const char *) s, mSourceSize);
      source.consume(mSourceSize);
      return true;
    }
    return false;
  }

  bool ImportFixedLengthTerminatedString(_InputBuffer& source, RecordBuffer target) const
  {
    uint8_t term = ((const FixedLengthPad *) &mSourceSize)->TermChar;
    uint8_t * s = source.open(1);
    AutoMark<_InputBuffer> m(source);
    while(true) {
      if (!s) {
	return false;
      }
      if (*s != term) {
	source.consume(1);
	s = source.open(1);
      } else {
	source.consume(1);
	break;
      }
    }

    // Done reading.  Now the mark is stable.
    uint8_t * mark = source.getMark();
    // Check that length isn't too long...
    if (mark + ((const FixedLengthPad *) &mSourceSize)->Size < s) {
      return false;
    }

    // Set the values we have passing in the pad char and the number we need appended.
    return true;    
  }

  bool ImportVariableLengthString(_InputBuffer& source, RecordBuffer target) const
  {
    uint8_t * s = source.open(5);
    if (s != NULL) {
      int32_t len = PrefixStringLength((char *) s);
      source.consume(5);
      s = source.open(len);
      if (s != NULL) {
	mTargetOffset.SetVariableLengthString(target, (const char *) s, len);
	source.consume(len);
	return true;
      }
    }
    return false;    
  }

  bool ImportVariableLengthTerminatedString(_InputBuffer& source, RecordBuffer target) const
  {
    // HACK: stuff terminator in mSize since it isn't used otherwise
    uint8_t term = (uint8_t) mSourceSize;
    uint8_t * s = source.open(1);
    AutoMark<_InputBuffer> m(source);
    while(true) {
      if (!s) return false;
      if (*s != term) {
	source.consume(1);
	s = source.open(1);
      } else {
	break;
      }
    }
    mTargetOffset.SetVariableLengthString(target, (const char *) source.getMark(), s-source.getMark());
    return true;
  }

  bool ConsumeTerminatedString(_InputBuffer& source, RecordBuffer target) const
  {
    // HACK: stuff terminator in mSize since it isn't used otherwise
    uint8_t term = (uint8_t) mSourceSize;
    while(true) {      
      // Open a window bigger than what we might need
      // to tighten up the inner loop.
      std::size_t sz = 64;
      uint8_t * start;
      source.open(sz, start);
      if (sz == 0) return false;
      uint8_t * end = start + sz;
      uint8_t * s = start;
      while(s != end) {
	if (*s++ == term) {
	  source.consume(std::size_t(s - start));
	  return true;    
	}
      }
      source.consume(sz);
    }
  }

  bool ImportDecimalInt32(_InputBuffer & source, RecordBuffer target) const
  {
    bool neg = false;
    uint8_t * s = source.open(1);
    if (s == NULL) 
      return false;
    if (*s == '-') {
      neg = true;
      source.consume(1);
    } else if (*s == '+') {
      source.consume(1);
    }
    int32_t val = 0;
    while(true) {
      s = source.open(1);
      if (s == NULL) {
	return false;
      }
      if (*s > '9' || *s < '0')  {
	// TODO: Right now assuming and not validating a single delimiter character
	// TODO: Protect against overflow
	mTargetOffset.setInt32(neg ? -val : val, target);
	return true;
      }
      val = val * 10 + (*s - '0');
      source.consume(1);
    }
  }
  
  bool ImportDouble(_InputBuffer & source, RecordBuffer target) const
  {
    uint8_t term = (uint8_t) mSourceSize;
    uint8_t * s = source.open(1);
    AutoMark<_InputBuffer> m(source);
    while(true) {
      if (!s) return false;
      if (*s != term) {
	source.consume(1);
	s = source.open(1);
      } else {
	break;
      }
    }

    mTargetOffset.setDouble(atof((char *) source.getMark()), target);
    return true;    
  }
  
  bool ImportDelimitedDatetime(_InputBuffer & source, RecordBuffer target) const
  {
    // Importing a fixed format of 19 chars
    // YYYY-MM-DD hh:mm:ss
    static const int32_t formatSz = 19;
    uint8_t * s = source.open(formatSz);
    if (s == NULL) return false;
    unsigned short nums[6];
    nums[0] = 0;
    for(int i=0; i<4; i++) {
      if (*s > '9' || *s < '0')  {
	return false;
      }
      nums[0] = nums[0] * 10 + (*s - '0');
      s++;
    }

    // Trust the compiler to unroll these loops
    // appropriately (or trust that it won't do it
    // if not beneficial).
    for(int j=1; j<6; j++) {
      // TODO: Validate delimiter;
      s++;
      nums[j] = 0;
      for(int i=0; i<2; i++) {
	if (*s > '9' || *s < '0')  {
	  return false;
	}
	nums[j] = nums[j] * 10 + (*s - '0');
	s++;
      }
    }
    
    source.consume(formatSz);

    typedef boost::posix_time::ptime ptime;
    typedef boost::posix_time::time_duration time_duration;
    typedef boost::gregorian::date date;
    // This call profiles to be pretty slow.
    // Can this be improved without losing too much
    // safety?
    ptime t(date(nums[0], nums[1], nums[2]),
    	    time_duration(nums[3],nums[4],nums[5]));
    mTargetOffset.setDatetime(t, target);
    return true;    
  }
  
  bool ImportDelimitedDate(_InputBuffer & source, RecordBuffer target) const
  {
    // Importing a fixed format of 10 chars
    // YYYY-MM-DD
    static const int32_t formatSz = 10;
    uint8_t * s = source.open(formatSz);
    if (s == NULL) return false;
    unsigned short nums[3];
    nums[0] = 0;
    for(int i=0; i<4; i++) {
      if (*s > '9' || *s < '0')  {
	return false;
      }
      nums[0] = nums[0] * 10 + (*s - '0');
      s++;
    }

    // Trust the compiler to unroll these loops
    // appropriately (or trust that it won't do it
    // if not beneficial).
    for(int j=1; j<3; j++) {
      // TODO: Validate delimiter;
      s++;
      nums[j] = 0;
      for(int i=0; i<2; i++) {
	if (*s > '9' || *s < '0')  {
	  return false;
	}
	nums[j] = nums[j] * 10 + (*s - '0');
	s++;
      }
    }
    
    source.consume(formatSz);

    typedef boost::gregorian::date date;
    // This call profiles to be pretty slow.
    // Can this be improved without losing too much
    // safety?
    date t(date(nums[0], nums[1], nums[2]));
    mTargetOffset.setDate(t, target);
    return true;    
  }
  
  bool ImportDelimitedDecimal(_InputBuffer & source, RecordBuffer target) const
  {
    // decNumber requires null terminated string.
    // Cost of import is high enough the extra copy into
    // doesn't appear to be a big deal.
    char buf[DECIMAL128_String];
    char * bufptr = &buf[0];
    char * bufend = &buf[DECIMAL128_String];
    uint8_t term = (uint8_t) mSourceSize;
    uint8_t * s = source.open(1);
    while(true) {
      if (!s) return false;
      if (*s != term &&
	  bufptr != bufend) {
	*bufptr++ = *s;
	source.consume(1);
	s = source.open(1);
      } else {
	break;
      }
    }
    // Decimal too long!
    // TODO: Should be more forgiving here due to 
    // leading and trailing zeros.
    if (bufptr == bufend)
      return false;

    *bufptr = 0;

    decContext decCtxt;
    decContextDefault(&decCtxt, DEC_INIT_DECIMAL128); // no traps, please
    decimal128FromString(mTargetOffset.getDecimalPtr(target),
			 buf,
			 &decCtxt);
    mTargetOffset.clearNull(target);
    return decCtxt.status == 0;
  }

  bool ImportDecimalInt64(_InputBuffer & source, RecordBuffer target) const
  {
    bool neg = false;
    uint8_t * s = source.open(1);
    if (s == NULL) 
      return false;
    if (*s == '-') {
      neg = true;
      source.consume(1);
    } else if (*s == '+') {
      source.consume(1);
    }
    int64_t val = 0;
    while(true) {
      s = source.open(1);
      if (s == NULL) {
	return false;
      }
      if (*s > '9' || *s < '0')  {
	// TODO: Right now assuming and not validating a single delimiter character
	// TODO: Protect against overflow
	mTargetOffset.setInt64(neg ? -val : val, target);
	return true;
      }
      val = val * 10LL + (*s - '0');
      source.consume(1);
    }
  }
  
  template <ImportFunc PMF>
  bool ImportNullableField(_InputBuffer & source, RecordBuffer target) const
  {
    // Check for null marker.  Set mark so we can roll back if need be.
    source.setMark();
    uint8_t * s = source.open(2);
    if (s == NULL) {
      source.releaseMark();
      return false;
    }
    if (s[0] == '\\' && s[1] == 'N') {
      source.consume(2);
      source.releaseMark();
      mTargetOffset.setNull(target);
      return true;
    } else {
      source.rewindMark();
      return (this->*PMF)(source, target);
    }
  }

  struct FixedLengthPad
  {
    uint16_t Size;
    uint8_t TermChar;
    uint8_t PadChar;
  };

public:
  FieldImporter() 
    :
    mSourceSize(0),
    mTargetOffset(0),
    mImporter(NULL)
  {
  }

  void InitFixedLengthString(const FieldAddress& targetOffset, std::size_t sz, bool nullable = false) 
  {
    mSourceSize = sz;
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportFixedLengthString>;
    } else {
      mImporter = &FieldImporter::ImportFixedLengthString;
    }
  }

  void InitFixedLengthTerminatedString(const FieldAddress& targetOffset, 
				       uint8_t terminator, 
				       uint8_t padChar, 
				       uint16_t sz,
				       bool nullable = false) 
  {
    // Use a union
    FixedLengthPad * hack = (FixedLengthPad *) &mSourceSize;
    hack->Size = sz;
    hack->TermChar = terminator;
    hack->PadChar =padChar;
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportFixedLengthTerminatedString>;
    } else {
      mImporter = &FieldImporter::ImportFixedLengthTerminatedString;
    }
  }

  void InitVariableLengthString(const FieldAddress& targetOffset, bool nullable=false)
  {
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportVariableLengthString>;
    } else {
      mImporter = &FieldImporter::ImportVariableLengthString;
    }
  }

  void InitVariableLengthTerminatedString(const FieldAddress& targetOffset, char terminator, bool nullable=false)
  {
    mSourceSize = (uint8_t) terminator;
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportVariableLengthTerminatedString>;
    } else {
      mImporter = &FieldImporter::ImportVariableLengthTerminatedString;
    }
  }

  void InitConsumeTerminatedString(char terminator)
  {
    mSourceSize = (uint8_t) terminator;
    mImporter = &FieldImporter::ConsumeTerminatedString;
  }

  void InitDecimalInt32(const FieldAddress& targetOffset, bool nullable = false)
  {
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportDecimalInt32>;
    } else {
      mImporter = &FieldImporter::ImportDecimalInt32;
    }
  }

  void InitDouble(const FieldAddress& targetOffset, char terminator, bool nullable = false)
  {
    mSourceSize = (uint8_t) terminator;
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportDouble>;
    } else {
      mImporter = &FieldImporter::ImportDouble;
    }
  }

  void InitDelimitedDatetime(const FieldAddress& targetOffset, bool nullable = false)
  {
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportDelimitedDatetime>;
    } else {
      mImporter = &FieldImporter::ImportDelimitedDatetime;
    }
  }

  void InitDelimitedDate(const FieldAddress& targetOffset, bool nullable = false)
  {
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportDelimitedDate>;
    } else {
      mImporter = &FieldImporter::ImportDelimitedDate;
    }
  }

  void InitDelimitedDecimal(const FieldAddress& targetOffset, char terminator, 
			    bool nullable)
  {
    mSourceSize = (uint8_t) terminator;
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportDelimitedDecimal>;
    } else {
      mImporter = &FieldImporter::ImportDelimitedDecimal;
    }
  }

  void InitDecimalInt64(const FieldAddress& targetOffset, bool nullable=false)
  {
    mTargetOffset = targetOffset;
    if (nullable) {
      mImporter = &FieldImporter::template ImportNullableField<&FieldImporter::ImportDecimalInt64>;
    } else {
      mImporter = &FieldImporter::ImportDecimalInt64;
    }
  }
  
  bool isConsumeOnly() const 
  {
    return mImporter == &FieldImporter::ConsumeTerminatedString;
  }

  bool Import(_InputBuffer & source, RecordBuffer target) const
  {
    if (mImporter == NULL) {
      throw std::runtime_error("Importer not initialized");
    }
    return (this->*mImporter)(source, target);
  }
};
  
// Factory for creating DataBlock instances.
class DataBlockFactory : public boost::noncopyable
{
public:
  typedef class DataBlock * 
  (*CreateDataBlockFn) (const char *, int32_t, uint64_t, uint64_t);
private:
  boost::mutex * mGuard;
  std::map<std::string, CreateDataBlockFn> mCreators;
  DataBlockFactory();
  ~DataBlockFactory();
public:
  static DataBlockFactory& get();
  void registerCreator(const std::string& uriScheme, 
		       CreateDataBlockFn creator);
  class DataBlock * create(const char * file, 
			   int32_t targetBlockSize, 
			   uint64_t beginOffset,
			   uint64_t endOffset);

};

class DataBlock
{
protected:
  // Mark for saving data in block
  uint8_t * mCurrentBlockMark;
  // Start of the data block
  uint8_t * mCurrentBlockStart;
  // End of valid data in the block
  uint8_t * mCurrentBlockEnd;
  // Current position within block
  uint8_t * mCurrentBlockPtr;
  /**
   * DataBlock calls this when it needs to increase
   * available data.
   */
  virtual void openWindow(std::size_t windowSize) =0;
public:
  DataBlock()
    :
    mCurrentBlockMark(NULL),
    mCurrentBlockStart(NULL),
    mCurrentBlockEnd(NULL),
    mCurrentBlockPtr(NULL)
  {
  }
    
  virtual ~DataBlock()
  {
    // Note that DataBlock doesn't assume that
    // it has ownership of the underlying pointer(s)
    // hence does NOT delete.  A derived class must
    // handle that.
  }

  static DataBlock * get(const char * filename,
			 int32_t targetBlockSize,
			 uint64_t begin,
			 uint64_t end);
			 
  /**
   * Open a window on a buffer and return NULL if a 
   * window of that size cannot be opened.
   */
  uint8_t * open(std::size_t windowSize)
  {
    if (mCurrentBlockPtr + windowSize <= mCurrentBlockEnd) 
      return mCurrentBlockPtr;
    openWindow (windowSize);
    return mCurrentBlockPtr + windowSize <= mCurrentBlockEnd ? mCurrentBlockPtr : NULL;
  }
  /**
   * Open a buffer and accept short reads.
   */
  void open(std::size_t& windowSize, uint8_t *& buf)
  {
    if (mCurrentBlockPtr + windowSize <= mCurrentBlockEnd) {
      buf = mCurrentBlockPtr;
      return;
    }
    openWindow (windowSize);
    buf = mCurrentBlockPtr;
    windowSize = std::min(windowSize, std::size_t(mCurrentBlockEnd - mCurrentBlockPtr));
  }
  void consume(std::size_t bytes)
  {
    mCurrentBlockPtr += bytes;
  }

  uint8_t * read(std::size_t sz)
  {
    uint8_t * ptr = open(sz);
    if (ptr) consume(sz);
    return ptr;
  }

  uint8_t * getMark()
  {
    return mCurrentBlockMark;
  }
  void setMark()
  {
    BOOST_ASSERT(mCurrentBlockMark == NULL);
    mCurrentBlockMark = mCurrentBlockPtr;
  }
  void rewindMark()
  {
    BOOST_ASSERT(mCurrentBlockMark != NULL);
    mCurrentBlockPtr = mCurrentBlockMark;
    mCurrentBlockMark = NULL;
  }
  void releaseMark()
  {
    BOOST_ASSERT (mCurrentBlockMark != NULL);
    mCurrentBlockMark = NULL;
  }
  // TODO: Should I be concerned about
  // the virtual function call overhead?
  virtual bool isEOF() =0;
};

/**
 * Implement a open/consume stream interface over a block provider.
 * Don't assume random access on the underlying provider: just a minimal
 * forward iteration over blocks.  Doesn't assume that the underlying
 * blocks are constant size (although we implement a minimum block
 * request size).
 * TODO: Make this compatible with async IO and check that it will
 * work over a socket.
 */
template <class _FileTraits>
class BlockBufferStream : public DataBlock
{
public:
  typedef _FileTraits file_system_type;
private:
  std::size_t mMinimumBlockRequestSize;
  // Total size of the buffer in block.
  std::size_t mBufferCapacity;
  // Where is mCurrentBlockStart in the stream?
  uint64_t mBlockOffset;
  // How far are we allowed to read?
  uint64_t mStreamEnd;
  typename _FileTraits::file_type mFile;

protected:
  void openWindow(std::size_t windowSize)
  {
    // See how much unused we have in current buffer.  Unused may take account
    // of a mark as well as the current ptr.  If any then move it to the 
    // front of the buffer and fill up the rest.
    // We may have to grow the buffer if the requested window is bigger than
    // the current buffer of if the amount to read is less than our commitment.
    if (mCurrentBlockMark && (mCurrentBlockPtr < mCurrentBlockMark))
      throw std::runtime_error("Internal error mCurrentBlockPtr < mCurrentBlockMark");
    uint8_t * keepStart = mCurrentBlockMark ? mCurrentBlockMark : mCurrentBlockPtr;
    std::size_t amtConsumed = std::size_t(keepStart - mCurrentBlockStart);
    std::size_t ptrOffset = std::size_t(mCurrentBlockPtr - keepStart);
    std::size_t bufUse = std::size_t(mCurrentBlockEnd - keepStart);
    std::size_t bufAvail = mBufferCapacity - bufUse;
    std::size_t amtToRead = std::max(windowSize, mMinimumBlockRequestSize);
    if (bufAvail < amtToRead) {
      mBufferCapacity = amtToRead + bufUse;
      uint8_t * newBuffer = new uint8_t [mBufferCapacity];
      if(bufUse)
	memcpy(newBuffer, keepStart, bufUse);
      delete [] mCurrentBlockStart;
      mCurrentBlockStart = newBuffer;
    } else {
      memmove(mCurrentBlockStart, keepStart, bufUse);
    }
    // Reset all pointers
    mCurrentBlockMark = mCurrentBlockMark ? mCurrentBlockStart : NULL;
    mCurrentBlockPtr = mCurrentBlockStart + ptrOffset;
    mCurrentBlockEnd = mCurrentBlockStart + bufUse;
    mCurrentBlockEnd += _FileTraits::read(mFile, mCurrentBlockEnd, amtToRead);
    mBlockOffset += amtConsumed;
  }

public:
  BlockBufferStream(const char * file, 
		    int32_t targetBlockSize, 
		    uint64_t beginOffset=0,
		    uint64_t endOffset=0xffffffffffffffffULL)
    :
    mMinimumBlockRequestSize(targetBlockSize),
    mBufferCapacity(0),
    mBlockOffset(beginOffset),
    mStreamEnd(endOffset)
  {
    // Start out with a fresh "empty" block. The first read
    // attempt with allocate and fill appropriately.
    mFile = _FileTraits::open_for_read(file, beginOffset, endOffset);
  }

  ~BlockBufferStream()
  {
    _FileTraits::close(mFile);
    delete [] mCurrentBlockStart;
  }
  bool isEOF()
  {
    // EOF is when I've read past the end of my assigned file chunk or I've exhausted
    // the last block in the file.
    return 
      mBlockOffset + (mCurrentBlockPtr - mCurrentBlockStart) >= mStreamEnd || 
      (mCurrentBlockPtr >= mCurrentBlockEnd && _FileTraits::isEOF(mFile));
  }
  // The following are for testing purposes and not part of the
  // buffer concept.
  uint8_t * start() const { return mCurrentBlockStart; }
  uint8_t * end() const { return mCurrentBlockEnd; }
};

class MemoryMappedFileBuffer : public DataBlock
{
public:
  typedef stdio_file_traits file_system_type;
private:
  boost::shared_ptr<boost::interprocess::file_mapping> mMapping;
  boost::shared_ptr<boost::interprocess::mapped_region> mRegion;
  // Block size for mapping regions
  std::size_t mBlockSize;
  // File size
  uintmax_t mFileSize;
  // End of file we're opened on.
  // Note that this is approximate since
  // for delimited files, we actually can actually read
  // past this to find a delimiter/record boundary.
  uint64_t mStreamEnd;
  // An optional background thread that prefetches
  // an opened window by reading bytes
  class MemoryMappedPrefetch * mPrefetcher;

  void openWindow(std::size_t windowSize);

public:
  MemoryMappedFileBuffer(const char * file, 
			 int32_t blockSize,
			 uint64_t beginOffset=0,
			 uint64_t endOffset=0xffffffffffffffffULL);

  ~MemoryMappedFileBuffer();

  bool isEOF();
};

class ExplicitChunkStrategy
{
public:
  typedef std::string file_input;
private:
  // What file(s) am I parsing?
  std::vector<std::vector<boost::shared_ptr<FileChunk> > > mFile;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mFile);
  }
public:
  ExplicitChunkStrategy();

  ~ExplicitChunkStrategy();

  void expand(const std::string& file,
	      int32_t numPartitions);

  void getFilesForPartition(int32_t partition,
			    std::vector<boost::shared_ptr<FileChunk> >& files) const;
};

class SerialChunkStrategy
{
public:
  typedef PathPtr file_input;
private:
  // What file system am I reading from
  PathPtr mUri;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mUri);
  }
public:
  SerialChunkStrategy();

  ~SerialChunkStrategy();

  void expand(const PathPtr & uri,
	      int32_t numPartitions);

  void getFilesForPartition(int32_t partition,
			    std::vector<boost::shared_ptr<FileChunk> >& files) const;
};

template<class _OpType>
class GenericParserOperator : public RuntimeOperator
{
private:
  typedef _OpType operator_type;
  typedef typename _OpType::input_buffer_type input_buffer_type;

  enum State { START, WRITE, WRITE_EOF };
  State mState;

  // The list of files from which I read; retrieved
  // by calling my operator type.
  std::vector<boost::shared_ptr<FileChunk> > mFiles;
  // Which file am I working on?
  std::vector<boost::shared_ptr<FileChunk> >::const_iterator mFileIt;
  // Input buffer for the file.
  DataBlock * mInputBuffer;
  // Record buffer I am importing into
  RecordBuffer mOutput;
  // Records imported
  uint64_t mRecordsImported;
  // Is there a comment line configured
  int32_t mCommentLineSz;

  bool isComment()
  {
    const uint8_t * line = mInputBuffer->open(mCommentLineSz);
    return line && 0 == memcmp(line, getLogParserType().mCommentLine.c_str(), 
			       mCommentLineSz);
  }

  void skipComments()
  {
    if (mCommentLineSz) {
      while(isComment()) {
	RecordBuffer nullRecord;
	getLogParserType().mSkipImporter.Import(*mInputBuffer, nullRecord);
      }
    }
  }

  /**
   * Parse until a valid record.  
   */
  bool parseRecord(RecordBuffer buf)
  {
    skipComments();

    for(typename operator_type::field_importer_const_iterator it = getLogParserType().mImporters.begin();
	it != getLogParserType().mImporters.end();
	++it) {
      if (! it->Import(*mInputBuffer, buf)) {
	return false;
      }
    }
    // Done cause we had good record
    return true;
  }

  const operator_type & getLogParserType()
  {
    return *static_cast<const operator_type *>(&getOperatorType());
  }

public:
  GenericParserOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
    :
    RuntimeOperator(services, opType),
    mInputBuffer(NULL),
    mRecordsImported(0)
  {
    std::size_t sz = getLogParserType().mCommentLine.size();
    if (sz > std::numeric_limits<int32_t>::max()) {
      throw std::runtime_error("Comment line too large");
    }
    mCommentLineSz = (int32_t) sz;
  }

  ~GenericParserOperator()
  {
    delete mInputBuffer;
  }

  /**
   * intialize.
   */
  void start()
  {
    mFiles.clear();
    // What file(s) am I parsing?
    typename _OpType::chunk_strategy_type chunkFiles;
    // Expand file name globbing, then get files for this
    // partition.
    chunkFiles.expand(getLogParserType().mFileInput, getNumPartitions());
    chunkFiles.getFilesForPartition(getPartition(), mFiles);
    mState = START;
    mRecordsImported = 0;
    onEvent(NULL);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      for(mFileIt = mFiles.begin();
	  mFileIt != mFiles.end();
	  ++mFileIt) {
	BOOST_ASSERT(mInputBuffer == NULL);
	// Allocate a new input buffer for the file in question.
	if ((*mFileIt)->getBegin() > 0) {
	  mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
					64*1024,
					(*mFileIt)->getBegin()-1,
					(*mFileIt)->getEnd());
	  RecordBuffer nullRecord;
	  getLogParserType().mSkipImporter.Import(*mInputBuffer, nullRecord);
	} else {
	  mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
					64*1024,
					(*mFileIt)->getBegin(),
					(*mFileIt)->getEnd());
	  if (getLogParserType().mSkipHeader) {
	    RecordBuffer nullRecord;
	    getLogParserType().mSkipImporter.Import(*mInputBuffer, nullRecord);
	  }
	}
	// Try to read a byte so we get a valid report
	// on EOF for empty files.
	mInputBuffer->open(1);
	// Read all of the record in the file.
	while(!mInputBuffer->isEOF()) {
	  // This is our actual record.
	  mOutput = getLogParserType().mMalloc.malloc();
	  if (parseRecord(mOutput)) {
	    mRecordsImported += 1;
	    requestWrite(0);
	    mState = WRITE;
	    return;
	  case WRITE:
	    write(port, mOutput, false);
	  } else {
	    // Done with this file
	    getLogParserType().mFree.free(mOutput);
	    mOutput = RecordBuffer();
	    break;
	  }
	}
	// Either EOF or parse failure.  In either
	// case done with this file.
	delete mInputBuffer;
	mInputBuffer = NULL;
      }
      // Done with the last file so output EOS.
      requestWrite(0);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer::create(), true);
      return;
    }
  }

  void shutdown()
  {
  }
};

template <class _ChunkStrategy = ExplicitChunkStrategy >
class GenericParserOperatorType : public RuntimeOperatorType
{
  // Don't really know how to do friends between templates.
public:
  typedef _ChunkStrategy chunk_strategy_type;
  typedef typename _ChunkStrategy::file_input file_input_type;
  typedef FieldImporter<DataBlock> field_importer_type;
  typedef typename std::vector<FieldImporter<DataBlock> >::const_iterator field_importer_const_iterator;
  typedef DataBlock input_buffer_type;

  // What file(s) am I parsing?
  file_input_type mFileInput;
  // Importer instructions
  std::vector<field_importer_type> mImporters;
  // Importer to read to end of line (when skipping over non "r" log lines).
  field_importer_type mSkipImporter;
  // Create new records
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  // What am I importing
  const RecordType * mRecordType;
  // Is there a header to skip?
  bool mSkipHeader;
  // Skip lines starting with this.
  std::string mCommentLine;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFileInput);
    ar & BOOST_SERIALIZATION_NVP(mImporters);
    ar & BOOST_SERIALIZATION_NVP(mSkipImporter);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mSkipHeader);
    ar & BOOST_SERIALIZATION_NVP(mCommentLine);
  }
  GenericParserOperatorType()
  {
  }  

public:
  GenericParserOperatorType(const typename _ChunkStrategy::file_input& file,
			    char fieldSeparator,
			    char recordSeparator,
			    const RecordType * recordType,
			    const RecordType * baseRecordType=NULL,
			    const char * commentLine = "")
    :
    RuntimeOperatorType("GenericParserOperatorType"),
    mFileInput(file),
    mRecordType(recordType),
    mSkipHeader(false),
    mCommentLine(commentLine)
  {
    mMalloc = mRecordType->getMalloc();
    mFree = mRecordType->getFree();

    // Records have tab delimited fields and newline delimited records
    field_importer_type::createDefaultImport(recordType, 
					     baseRecordType ? baseRecordType : recordType,
					     fieldSeparator, 
					     recordSeparator, mImporters);
    
    // To skip a line we just parse to newline and discard.
    // We need this when syncing to the middle of a file.
    mSkipImporter.InitConsumeTerminatedString(recordSeparator);
  }

  ~GenericParserOperatorType()
  {
  }

  const RecordType * getOutputType()
  {
    return mRecordType;
  }

  void setSkipHeader(bool value) 
  {
    mSkipHeader = value;
  }

  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

template <class _ChunkStrategy>
RuntimeOperator * GenericParserOperatorType<_ChunkStrategy>::create(RuntimeOperator::Services & services) const
{
  return new GenericParserOperator<GenericParserOperatorType<_ChunkStrategy> >(services, *this);
}

#endif
