#
# Copyright (c) 2012, Akamai Technologies
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
#   Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
# 
#   Redistributions in binary form must reproduce the above
#   copyright notice, this list of conditions and the following
#   disclaimer in the documentation and/or other materials provided
#   with the distribution.
# 
#   Neither the name of the Akamai Technologies nor the names of its
#   contributors may be used to endorse or promote products derived
#   from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
#

# - Find Hadoop
# Find C++ interface components to Hadoop.  Determines where the 
# include and library directories are and resolves the names
# of libraries when found.  Uses the value of HADOOP_HOME environment
# variable when set to find the prebuilt libraries that are included
# with Hadoop distributions.  Looks for the hdfs, pipes and utils 
# libraries.
# Sets the following variables:
# HADOOP_FOUND
# HADOOP_LIBRARIES
# HADOOP_INCLUDE_DIRS
# HADOOP_HDFS_LIBRARY
# HADOOP_HDFS_INCLUDE_DIR
# HADOOP_PIPES_LIBRARY
# HADOOP_PIPES_INCLUDE_DIR
# HADOOP_UTILS_LIBRARY
# HADOOP_UTILS_INCLUDE_DIR
#

FILE (TO_CMAKE_PATH "$ENV{HADOOP_HOME}" _HADOOP_HOME)

# Start with empty
SET(HADOOP_LIBRARY_DIRECTORIES "${_HADOOP_HOME}/c++/lib")

# Hadoop 0.20 and 0.21 ship with prebuilt libraries for
# Linux that go in these oddly named directories.  Prepend
# these to the search path
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
  IF(CMAKE_SYSTEM_PROCESSOR MATCHES "^i[3-9]86$")
    SET(HADOOP_LIBRARY_DIRECTORIES "${_HADOOP_HOME}/c++/Linux-i386-32/lib" ${HADOOP_LIBRARY_DIRECTORIES})
  ELSEIF(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    SET(HADOOP_LIBRARY_DIRECTORIES "${_HADOOP_HOME}/c++/Linux-amd64-64/lib" ${HADOOP_LIBRARY_DIRECTORIES})
  ELSE(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    MESSAGE(STATUS "Processor type ${CMAKE_SYSTEM_PROCESSOR} doesn't support prebuilt Hadoop binaries")
  ENDIF(CMAKE_SYSTEM_PROCESSOR MATCHES "^i[3-9]86$")  
ELSE (CMAKE_SYSTEM_NAME MATCHES "Linux")
  MESSAGE(STATUS "System type ${CMAKE_SYSTEM_NAME} doesn't support prebuilt Hadoop binaries")
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

SET(HADOOP_INCLUDE_DIRECTORIES 
  ${_HADOOP_HOME}/hdfs/src/c++/libhdfs)

# Why does Hadoop pipes put its headers in architecture
# specific directories?  In case they are arch specific,
# prepend them to the path
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
  IF(CMAKE_SYSTEM_PROCESSOR MATCHES "^i[3-9]86$")
    SET(HADOOP_INCLUDE_DIRECTORIES "${_HADOOP_HOME}/c++/Linux-i386-32/include" ${HADOOP_INCLUDE_DIRECTORIES})
  ELSEIF(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    SET(HADOOP_INCLUDE_DIRECTORIES "${_HADOOP_HOME}/c++/Linux-amd64-64/include" ${HADOOP_INCLUDE_DIRECTORIES})
  ENDIF(CMAKE_SYSTEM_PROCESSOR MATCHES "^i[3-9]86$")  
ENDIF (CMAKE_SYSTEM_NAME MATCHES "Linux")

# At this point, we don't have any guesses as standard locations
# of Hadoop installs.  Perhaps we should look at Cloudera's 
# distros?

# Look for libhdfs
FIND_PATH(HADOOP_HDFS_INCLUDE_DIR NAMES hdfs.h PATHS ${HADOOP_INCLUDE_DIRECTORIES})
MARK_AS_ADVANCED(HADOOP_HDFS_INCLUDE_DIR)
FIND_LIBRARY(HADOOP_HDFS_LIBRARY NAMES hdfs PATHS ${HADOOP_LIBRARY_DIRECTORIES})
MARK_AS_ADVANCED(HADOOP_HDFS_LIBRARY)

# Look for libhadooppipes
FIND_PATH(HADOOP_PIPES_INCLUDE_DIR NAMES Pipes.hh PATHS ${HADOOP_INCLUDE_DIRECTORIES})
MARK_AS_ADVANCED(HADOOP_PIPES_INCLUDE_DIR)
FIND_LIBRARY(HADOOP_PIPES_LIBRARY NAMES hadooppipes PATHS ${HADOOP_LIBRARY_DIRECTORIES})
MARK_AS_ADVANCED(HADOOP_PIPES_LIBRARY)

# Look for libhadooputils
FIND_PATH(HADOOP_UTILS_INCLUDE_DIR NAMES SerialUtils.hh PATHS ${HADOOP_INCLUDE_DIRECTORIES})
MARK_AS_ADVANCED(HADOOP_UTILS_INCLUDE_DIR)
FIND_LIBRARY(HADOOP_UTILS_LIBRARY NAMES hadooputils PATHS ${HADOOP_LIBRARY_DIRECTORIES})
MARK_AS_ADVANCED(HADOOP_UTILS_LIBRARY)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(HADOOP DEFAULT_MSG HADOOP_HDFS_LIBRARY HADOOP_HDFS_INCLUDE_DIR HADOOP_PIPES_LIBRARY HADOOP_PIPES_INCLUDE_DIR HADOOP_UTILS_LIBRARY HADOOP_UTILS_INCLUDE_DIR)

IF (HADOOP_FOUND)
  SET(HADOOP_LIBRARIES ${HADOOP_HDFS_LIBRARY} ${HADOOP_PIPES_LIBRARY} ${HADOOP_UTILS_LIBRARY})
  SET(HADOOP_INCLUDE_DIRS ${HADOOP_HDFS_INCLUDE_DIR} ${HADOOP_PIPES_INCLUDE_DIR} ${HADOOP_UTILS_INCLUDE_DIR})
ELSE (HADOOP_FOUND)
  SET(HADOOP_LIBRARIES)
  SET(HADOOP_INCLUDE_DIRS)
ENDIF (HADOOP_FOUND)


