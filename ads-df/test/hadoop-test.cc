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

// DBTODO
// #include "HdfsSequenceFile.hh"
#include "RuntimeProcess.hh"

#define BOOST_TEST_MODULE HadoopTests
#include <boost/test/unit_test.hpp>

class HadoopTest
{
public:
  HadoopTest()
  {
    static bool inited = false;
    // Don't worry about threads at this point.
    if (!inited) {
      HadoopSetup::setEnvironment();
      inited = true;
    }
  }
};

// BOOST_AUTO_TEST_CASE(testSequenceFileParse)
// {
//   HdfsSequenceFile<MemoryMappedFileBuffer> sf;
//   MemoryMappedFileBuffer buf((boost::format("%1%/logs/sampleSequenceFile") % getenv("HOME")).str().c_str(),
// 			     128*1024);
//   sf.parseHeader(buf);
// }

// BOOST_AUTO_TEST_CASE(testSerialOrganizedTableHdfs)
// {
//   std::cout << "testSerialOrganizedTableHdfs" << std::endl;
//   HadoopTest ht;
//   HdfsFileSystem fs ("hdfs://bos-lpunw:11390/");
//   SerialOrganizedTable t(1,1,"event_d");
//   t.bind(&fs);
//   for(std::vector<SerialOrganizedTableFilePtr>::const_iterator it = t.getSerialPaths().begin();
//       it != t.getSerialPaths().end();
//       ++it) {
//     std::cout << (*it)->getPath()->toString().c_str() << std::endl;
//   }
// }

// BOOST_AUTO_TEST_CASE(testHadoopListNotExists)
// {
//   std::cout << "testHadoopListNotExists" << std::endl;
//   HadoopTest ht;
//   FileSystem * fs = FileSystem::get(UriPtr(new URI("hdfs://default:0/")));
//   std::vector<boost::shared_ptr<FileStatus> > ret;
//   try {
//     fs->list(Path::get("hdfs://default:0/idontexist9999999"), ret);
//     BOOST_CHECK(false);
//   } catch (std::runtime_error & ex) {
//     std::cout << "Received expected exception: " << ex.what() << std::endl;
//   }
//   FileSystem::release(fs);
// }
