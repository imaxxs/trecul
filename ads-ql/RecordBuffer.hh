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

#ifndef __RECORDBUFFER_HH
#define __RECORDBUFFER_HH

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

class RecordBuffer
{
public:
  uint8_t * Ptr;
  operator bool() { return Ptr != NULL; }
  static RecordBuffer create() {
    RecordBuffer buf (NULL);
    return buf;
  }
  static RecordBuffer malloc(std::size_t sz) {
    RecordBuffer buf((uint8_t *) ::malloc(sz));
    if (buf.Ptr == NULL)
      throw std::runtime_error("Allocation failure");
    memset(buf.Ptr, 0, sz);
    return buf;
  }
  static void free(RecordBuffer & buf) {
    if (NULL != buf.Ptr) ::free(buf.Ptr);
    buf.Ptr = NULL;
  }
  static bool isEOS(RecordBuffer & buf) {
    return buf.Ptr == NULL;
  }
  RecordBuffer(uint8_t * ptr)
    :
    Ptr(ptr)
  {
  }
  RecordBuffer()
    :
    Ptr(NULL)
  {
  }
};

#endif
