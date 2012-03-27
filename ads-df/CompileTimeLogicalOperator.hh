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

#ifndef __COMPILE_TIME_LOGICAL_OPERATOR_HH
#define __COMPILE_TIME_LOGICAL_OPERATOR_HH

#include "LogicalOperator.hh"

/**
 * An operator that supports an option for compile time execution.
 */
class CompileTimeLogicalOperator : public LogicalOperator
{
protected:
  /**
   * True if this operator should be executed at compile time
   * and its results transmitted at runtime as a constant scan.
   */
  bool mConstantScan;

  /**
   * Subclasses override this to implement their create method.
   */
  virtual void internalCreate(class RuntimePlanBuilder& plan) = 0;
  
  /**
   * Create the operator with desired initialization of constantScan.
   */
  CompileTimeLogicalOperator(bool constantScan=false);

public:

  /**
   * Either create an instance of the operator type or a constant
   * scan representing the compile time result if requested.
   */
  void create(class RuntimePlanBuilder& plan);  
};

#endif
