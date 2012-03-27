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

#ifndef __IQLGETVARIABLESPASS_H__
#define __IQLGETVARIABLESPASS_H__

/**
 * C interface to a pass to gather free variables from an IQL expression
 * list.  Doesn't do any kind of type checking or calculation, only 
 * identifies the names of variables.
 */

#ifdef __cplusplus
extern "C" {
#endif
  
  typedef struct IQLGetVariablesContextStruct * IQLGetVariablesContextRef;

  void IQLGetVariablesAddField(IQLGetVariablesContextRef ctxt, const char * name);
  void IQLGetVariablesBuildLocal(IQLGetVariablesContextRef ctxt, const char * name);
  void IQLGetVariablesBuildVariableReference(IQLGetVariablesContextRef ctxt, 
					     const char * name);
  void IQLGetVariablesBuildArrayReference(IQLGetVariablesContextRef ctxt, 
					  const char * name);
  void IQLGetVariablesBeginAggregateFunction(IQLGetVariablesContextRef ctxt);
  void IQLGetVariablesBuildAggregateFunction(IQLGetVariablesContextRef ctxt);
#ifdef __cplusplus
}

#include <string>
#include <set>

class GetVariablesContext
{
private:
  std::set<std::string> mFreeVariables;
  std::set<std::string> mLocalVariables;
public:
  GetVariablesContext();
  ~GetVariablesContext();
  const std::set<std::string>& getFreeVariables() const;
  const std::set<std::string>& getLocalVariables() const;
  void addField(const char * name);
  void buildLocal(const char * name);
  void buildVariableReference(const char * name);
  void buildArrayReference(const char * name);
  void beginAggregateFunction();
  void buildAggregateFunction();
};

class GetVariablesContext * unwrap(IQLGetVariablesContextRef ctxt);
IQLGetVariablesContextRef wrap(class GetVariablesContext *);

#endif

#endif
