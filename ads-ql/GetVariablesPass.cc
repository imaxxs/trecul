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

#include <stdexcept>
#include "GetVariablesPass.h"

GetVariablesContext::GetVariablesContext()
{
}

GetVariablesContext::~GetVariablesContext()
{
}

const std::set<std::string>& GetVariablesContext::getFreeVariables() const
{
  return mFreeVariables;
}
const std::set<std::string>& GetVariablesContext::getLocalVariables() const
{
  return mLocalVariables;
}

void GetVariablesContext::addField(const char * name)
{
  buildLocal(name);
}

void GetVariablesContext::buildLocal(const char * name)
{
  if (mLocalVariables.find(name) == mLocalVariables.end())
    mLocalVariables.insert(name);
}

void GetVariablesContext::buildVariableReference(const char * name)
{
  if (mLocalVariables.find(name) == mLocalVariables.end() &&
      mFreeVariables.find(name) == mFreeVariables.end())
    mFreeVariables.insert(name);
}

void GetVariablesContext::buildArrayReference(const char * name)
{
  buildVariableReference(name);
}

void GetVariablesContext::beginAggregateFunction()
{
  throw std::runtime_error("Not yet implemented");
}

void GetVariablesContext::buildAggregateFunction()
{
  throw std::runtime_error("Not yet implemented");
}

class GetVariablesContext * unwrap(IQLGetVariablesContextRef ctxt)
{
  return reinterpret_cast<class GetVariablesContext *>(ctxt);
}

IQLGetVariablesContextRef wrap(class GetVariablesContext * val)
{
  return reinterpret_cast<IQLGetVariablesContextRef>(val);  
}


void IQLGetVariablesAddField(IQLGetVariablesContextRef ctxtRef, const char * name)
{
  GetVariablesContext * ctxt = unwrap(ctxtRef);
  ctxt->addField(name);
}

void IQLGetVariablesBuildLocal(IQLGetVariablesContextRef ctxtRef, const char * name)
{
  GetVariablesContext * ctxt = unwrap(ctxtRef);
  ctxt->buildLocal(name);
}

void IQLGetVariablesBuildVariableReference(IQLGetVariablesContextRef ctxtRef, 
					   const char * name)
{
  GetVariablesContext * ctxt = unwrap(ctxtRef);
  ctxt->buildVariableReference(name);
}

void IQLGetVariablesBuildArrayReference(IQLGetVariablesContextRef ctxtRef, 
					const char * name)
{
  GetVariablesContext * ctxt = unwrap(ctxtRef);
  ctxt->buildArrayReference(name);
}

void IQLGetVariablesBeginAggregateFunction(IQLGetVariablesContextRef ctxtRef)
{
  GetVariablesContext * ctxt = unwrap(ctxtRef);
  ctxt->beginAggregateFunction();
}

void IQLGetVariablesBuildAggregateFunction(IQLGetVariablesContextRef ctxtRef)
{
  GetVariablesContext * ctxt = unwrap(ctxtRef);
  ctxt->buildAggregateFunction();
}


