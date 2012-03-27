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

#if !defined(__IQLINTERPRETER_HH)
#define __IQLINTERPRETER_HH

#include <string>
#include <vector>
#include <set>
#include <map>
#include <boost/dynamic_bitset.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/regex.hpp>

// decNumber
extern "C" {
#include "decimal128.h"
}

#include "RecordType.hh"

// Forward decl
class CodeGenerationContext;

class RecordType;

namespace llvm {
  class Type;
  class Value;
  class ExecutionEngine;
  class FunctionPassManager;
  class MachineRelocation;
}

#include "RecordBuffer.hh"

class InterpreterContext {
private:
  decContext mDecimalContext;
  std::set<void *> mToFree;

  typedef std::map<std::string, boost::regex> regex_cache_type;
  static const size_t MAX_REGEX_CACHE = 20;
  regex_cache_type mRegexCache; 
public:
  InterpreterContext();
  ~InterpreterContext();
  decContext * getDecimalContext() {
    return &mDecimalContext;
  }
  void * malloc(size_t sz);
  void erase(void * ptr);
  void clear();
  bool regex_match(const char* regex, const char* string);
};

class LLVMBase
{
private:
  llvm::ExecutionEngine * TheExecutionEngine;

protected:
  class CodeGenerationContext * mContext;
  llvm::FunctionPassManager * mFPM;
  /**
   * Index from address to symbol for external functions.
   * This is needed so we can recognize the relocations of
   * function calls to external functions.
   */
  std::map<void*, std::string> mExternalFunctionsIdx;

  void InitializeLLVM();
  void ConstructFunction(const std::string& funName, const std::vector<std::string>& recordArgs);
  void ConstructFunction(const std::string& funName, const std::vector<std::string>& recordArgs, llvm::Type * returnType);
  void CreateMemcpyIntrinsic();  
  void CreateMemsetIntrinsic();  
  void CreateMemcmpIntrinsic();  
  void createTransferFunction(const std::string& funName,
			      const RecordType * input,
			      const RecordType * output);
  void createTransferFunction(const std::string & mFunName,
			      const std::vector<const RecordType *>& mSources,
			      const std::vector<boost::dynamic_bitset<> >& masks,
			      const RecordType * output);
  void createUpdate(const std::string & mFunName,
		    const std::vector<const RecordType *>& mSources,
		    const std::vector<boost::dynamic_bitset<> >& masks);
  
public:
  LLVMBase();
  virtual ~LLVMBase();
  llvm::Value * LoadAndValidateExternalFunction(const char * externalFunctionName, 
						llvm::Type * funTy);
};

class IQLRecordBufferMethod
{
private:
  typedef void (*LLVMFuncType)(char*, class InterpreterContext *);
  LLVMFuncType mFunction;
  class IQLRecordBufferMethodHandle * mImpl;
public:
  IQLRecordBufferMethod(const std::vector<unsigned char>& bitcode, const std::string& functionName);
  ~IQLRecordBufferMethod();
  void Execute(char * buf, class InterpreterContext * ctxt) const
  {
    (*mFunction)(buf, ctxt);
  }
};

class IQLUpdateModule
{
private:
  std::string mFunName;
  std::vector<unsigned char> mBitcode;
  // TODO: change this to support more than two inputs (using char** presumably)
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
  LLVMFuncType mFunction;
  class IQLRecordBufferMethodHandle * mImpl;

  // Create the LLVM module from the bitcode.
  void initImpl();

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mBitcode);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mBitcode);

    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLUpdateModule()
    :
    mFunction(NULL),
    mImpl(NULL)
  {
  }
public:
  IQLUpdateModule(const std::string& funName, 
		    const std::vector<unsigned char>& bitcode);
  ~IQLUpdateModule();
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt) const;
};

/**
 * Execute a list of IQL statements in place on a collection
 * of records.  This supports imperative programming and thus
 * is really useful for things like updating aggregates in
 * group by calculations.
 */
class RecordTypeInPlaceUpdate : public LLVMBase
{
private:
  std::vector<const RecordType *> mSources;
  std::string mFunName;
  std::string mStatements;
  std::vector<unsigned char> mBitcode;

  // TODO: change this to support more than two inputs (using char** presumably)
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
  LLVMFuncType mUpdateFunction;
  class IQLRecordBufferMethodHandle * mImpl;

  void init(class DynamicRecordContext& recCtxt, 
	    const std::string & funName, 
	    const std::vector<const RecordType *>& sources, 
	    const std::vector<boost::dynamic_bitset<> >& sourceMasks,
	    const std::string& statements);
public:
  /**
   * Compile a sequence of statements to execute against a collection of
   * record buffers.
   * Right now this will throw if there are any name collisions among
   * the inputs.  TODO: Support aliasing or multi-part names to disambiguate.
   */
  RecordTypeInPlaceUpdate(class DynamicRecordContext& recCtxt, 
			  const std::string & funName, 
			  const std::vector<const RecordType *>& sources, 
			  const std::string& statements);

  RecordTypeInPlaceUpdate(class DynamicRecordContext& recCtxt, 
			  const std::string & funName, 
			  const std::vector<const RecordType *>& sources, 
			  const std::vector<boost::dynamic_bitset<> >& sourceMasks,
			  const std::string& statements);

  ~RecordTypeInPlaceUpdate();
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt) const;

  /**
   * Create serializable update function.
   */
  IQLUpdateModule * create() const;
};

/**
 * X86MethodInfo represents a JIT'd function together with relocations.
 *
 * If marked as valid, this function is relocatable.  The relocation 
 * is implemented by the Boost serialization methods.
 * The point in having these relocatable methods is that code generation
 * in LLVM isn't too fast and there are some important cases for us (e.g.
 * scoring) in which we have LOTS (megabytes) of code.  It make much
 * more sense for us to compile & code gen once at plan compilation time
 * and then farm out X86 code rather than LLVM bitcode.  Doing so means that
 * we need to be able to perform relocations (in scoring we need relocations
 * specifically for floating point constants that live in the constant pool).
 * This is specific to LLVM 2.8 & X86 and will need to track carefully
 * to subsequent LLVM versions!  Hopefully when the MCJIT project is
 * complete this code will no longer be necessary.
 */
class X86MethodInfo
{
public:
  /**
   * These are the different kinds of relocations supported
   * by LLVM for the X86 Target.  This enum is defined in
   * the LLVM code base in lib/Target/X86/X86Relocations.h
   * but is NOT part of the official LLVM API.  This definition
   * is correct for LLVM 2.8 and 2.9 but may change in the future.
   */
  enum RelocationType {
    reloc_pcrel_word = 0,
    reloc_picrel_word = 1,
    reloc_absolute_word = 2,
    reloc_absolute_word_sext = 3,
    reloc_absolute_dword = 4
  };
  /**
   * A serializable representation for an important subset
   * of X86 relocations: those which are absolute offsets
   * to addresses within the method segments.
   * MachineCodeOffset is an offset from MethodBegin.
   * ResultPtrOffset is an offset relative to MethodBegin.
   */
  class Relocation
  {
  public:
    intptr_t MachineCodeOffset;
    intptr_t ResultPtrOffset;
    std::string Symbol;
    Relocation()
      :
      MachineCodeOffset(0),
      ResultPtrOffset(0)
    {
    }
    Relocation(intptr_t machineCodeOffset,
	       intptr_t resultPtrOffset)
      :
      MachineCodeOffset(machineCodeOffset),
      ResultPtrOffset(resultPtrOffset)
    {
    }
    Relocation(intptr_t machineCodeOffset,
	       const std::string& symbol)
      :
      MachineCodeOffset(machineCodeOffset),
      Symbol(symbol)
    {
    }
    // Serialization
    template <class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
      ar & BOOST_SERIALIZATION_NVP(MachineCodeOffset);
      ar & BOOST_SERIALIZATION_NVP(ResultPtrOffset);
      ar & BOOST_SERIALIZATION_NVP(Symbol);
    }
  };

private:
  uint8_t * mMethodBegin;
  std::size_t mMethodSize;
  std::size_t mCodeOffset;
  std::vector<Relocation> mRelocations;
  bool mIsValid;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    // How is the method region aligned?  Preserving this
    // mod 16 is important because of SSE extensions and their
    // 128-bit types.  If we have packed doubles in the constant pool
    // whose addresses are used with SSE2 then you'll fail with
    // segfault if alignment isn't preserved.
    uint8_t alignMod16 = (uint8_t) ((uintptr_t) mMethodBegin)&0x0f;
    ar & BOOST_SERIALIZATION_NVP(mMethodSize);
    ar & BOOST_SERIALIZATION_NVP(mCodeOffset);
    ar & BOOST_SERIALIZATION_NVP(mRelocations);
    ar & BOOST_SERIALIZATION_NVP(mIsValid);
    ar & BOOST_SERIALIZATION_NVP(alignMod16);
    ar << boost::serialization::make_array(mMethodBegin, mMethodSize);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    uint8_t alignMod16;
    ar & BOOST_SERIALIZATION_NVP(mMethodSize);
    ar & BOOST_SERIALIZATION_NVP(mCodeOffset);
    ar & BOOST_SERIALIZATION_NVP(mRelocations);
    ar & BOOST_SERIALIZATION_NVP(mIsValid);
    ar & BOOST_SERIALIZATION_NVP(alignMod16);
    mMethodBegin = allocateRWX(mMethodSize, alignMod16);
    ar >> boost::serialization::make_array(mMethodBegin, mMethodSize);
    relocate();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()

  /**
   * Allocate a writable, executable memory region for
   * deserialized JIT'd function.
   */
  uint8_t * allocateRWX(std::size_t sz, uint8_t alignMod16);
  /**
   * Apply relocations to code after deserialization.
   */
  void relocate();
public:
  X86MethodInfo()
    :
    mMethodBegin(NULL),
    mMethodSize(0),
    mIsValid(true)
  {
  }

  void setMethod(void * methodBegin,
		 void * methodEnd,
		 void * codeBegin);
  void addRelocation(const llvm::MachineRelocation& reloc,
		     const std::map<void*, std::string>& externalFunctions);

  void * getCodeBegin() 
  {
    if (!mIsValid)
      throw std::runtime_error("X86 Method has invalid relocations");
    return mMethodBegin + mCodeOffset;
  }
};

class IQLTransferModule
{
private:
  RecordTypeMalloc mMalloc;
  std::string mCopyFunName;
  std::string mMoveFunName;
  std::vector<unsigned char> mBitcode;
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
  LLVMFuncType mCopyFunction;
  LLVMFuncType mMoveFunction;
  class IQLRecordBufferMethodHandle * mImpl;
  X86MethodInfo * mInfo;
  bool mIsPIC;

  // Create the LLVM module from the bitcode.
  void initImpl(const std::map<void*,std::string>& externalFunctions);

  // Used for serializing/deserializing PIC code.
  X86MethodInfo * getFunInfo(const std::string& str) const;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mIsPIC);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    if (!mIsPIC) {
      ar & BOOST_SERIALIZATION_NVP(mCopyFunName);
      ar & BOOST_SERIALIZATION_NVP(mMoveFunName);
      ar & BOOST_SERIALIZATION_NVP(mBitcode);
    } else {
      // Serialize the copy function info
      X86MethodInfo * info = getFunInfo(mCopyFunName);
      ar & BOOST_SERIALIZATION_NVP(info);      
    }
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mIsPIC);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    if (!mIsPIC) {
      ar & BOOST_SERIALIZATION_NVP(mCopyFunName);
      ar & BOOST_SERIALIZATION_NVP(mMoveFunName);
      ar & BOOST_SERIALIZATION_NVP(mBitcode);
      // No need for a valid map of external functions
      // since we are not trying to extract relocations.
      std::map<void*,std::string> externalFunctions;
      initImpl(externalFunctions);
    } else {
      // Grab the method without a method handle, so
      // directly the X86 code.
      X86MethodInfo * info = NULL;
      ar & BOOST_SERIALIZATION_NVP(info);      
      mInfo = info;
      mCopyFunction = (LLVMFuncType) mInfo->getCodeBegin();
    }
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLTransferModule()
    :
    mCopyFunction(NULL),
    mMoveFunction(NULL),
    mImpl(NULL),
    mInfo(NULL),
    mIsPIC(false)
  {
  }
public:
  IQLTransferModule(const RecordTypeMalloc & targetMalloc,
		    const std::string& copyFunName, 
		    const std::string& moveFunName, 
		    const std::vector<unsigned char>& bitcode,
		    const std::map<void*,std::string>& externalFunctions,
		    bool isPIC = false);
  ~IQLTransferModule();
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt, bool isSourceMove) const;
};

/**
 * Transform an input record into an output.
 * This assumes we are provided with a list of
 * expressions each of which generates an output field.
 * It does not support statements/procedural logic.
 */
class RecordTypeTransfer : public LLVMBase
{
private:
  const RecordType * mSource;
  const RecordType * mTarget;
  std::string mFunName;
  std::string mTransfer;
  std::vector<unsigned char> mBitcode;

  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
  LLVMFuncType mCopyFunction;
  LLVMFuncType mMoveFunction;
  class IQLRecordBufferMethodHandle * mImpl;
  bool mIsIdentity;
public:
  /**
   * Get the list of free variables in the transfer.
   */
  static void getFreeVariables(const std::string& xfer,
			       std::set<std::string>& freeVariables);
  /**
   * Perform a transfer of the source record to a target following the specification
   * provided in the program transfer.
   */
  RecordTypeTransfer(class DynamicRecordContext& recCtxt, 
		     const std::string & funName, 
		     const RecordType * source, 
		     const std::string& transfer);

  ~RecordTypeTransfer();

  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & source, RecordBuffer target, class InterpreterContext * ctxt, bool isSourceMove) const;

  /**
   * The target type of the transfer.
   */
  const RecordType * getTarget() const
  {
    return mTarget;
  }

  /**
   * Is this an identity transfer?
   */
  bool isIdentity() const 
  {
    return mIsIdentity;
  }

  /** 
   * create a serializable IQLTransferModule that implements the program.
   */
  IQLTransferModule * create(bool isPIC=false) const;
};

class AliasedRecordType
{
private:
  std::string mAlias;
  const RecordType * mType;
public:
  // Compiler generated copy c'tor and assignment OK.
  AliasedRecordType(const std::string& alias, const RecordType * ty)
    :
    mAlias(alias),
    mType(ty)
  {
  }
  ~AliasedRecordType()
  {
  }
  const std::string& getAlias() const { return mAlias; }
  const RecordType * getType() const { return mType; }
};

class IQLTransferModule2
{
private:
  RecordTypeMalloc mMalloc;
  std::string mCopyFunName;
  std::string mMoveFunName;
  std::vector<unsigned char> mBitcode;
  // TODO: Must genericize
  typedef void (*LLVMFuncType)(char*, char*, char *, class InterpreterContext *);
  LLVMFuncType mCopyFunction;
  LLVMFuncType mMoveFunction;
  class IQLRecordBufferMethodHandle * mImpl;

  // Create the LLVM module from the bitcode.
  void initImpl();

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mCopyFunName);
    ar & BOOST_SERIALIZATION_NVP(mMoveFunName);
    ar & BOOST_SERIALIZATION_NVP(mBitcode);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mCopyFunName);
    ar & BOOST_SERIALIZATION_NVP(mMoveFunName);
    ar & BOOST_SERIALIZATION_NVP(mBitcode);

    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLTransferModule2()
    :
    mCopyFunction(NULL),
    mMoveFunction(NULL),
    mImpl(NULL)
  {
  }
public:
  IQLTransferModule2(const RecordTypeMalloc & recordMalloc,
		     const std::string& copyFunName, 
		     const std::string& moveFunName, 
		     const std::vector<unsigned char>& bitcode);
  ~IQLTransferModule2();
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer & sourceA, 
	       RecordBuffer & sourceB,
	       RecordBuffer & target, 
	       class InterpreterContext * ctxt, 
	       bool isSourceAMove,
	       bool isSourceBMove) const
  {
    RecordBuffer sources[2] = { sourceA, sourceB };
    bool isSourceMove [2] = { isSourceAMove, isSourceBMove };
    execute(&sources[0], &isSourceMove[0], 2, target, ctxt);
  }
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer * sources, 
	       bool * isSourceMove,
	       int32_t numSources,
	       RecordBuffer & target, 
	       class InterpreterContext * ctxt) const;
};

class RecordTypeTransfer2 : public LLVMBase
{
private:
  std::vector<AliasedRecordType> mSources;
  const RecordType * mTarget;
  std::string mFunName;
  std::string mTransfer;
  std::vector<unsigned char> mBitcode;

  // TODO: Must genericize
  typedef void (*LLVMFuncType)(char*, char*, char *, class InterpreterContext *);
  LLVMFuncType mCopyFunction;
  LLVMFuncType mMoveFunction;
  class IQLRecordBufferMethodHandle * mImpl;
public:
  /**
   * Perform a transfer of the source record to a target following the specification
   * provided in the program transfer.
   */
  RecordTypeTransfer2(class DynamicRecordContext& recCtxt, 
		      const std::string & funName, 
		      const std::vector<AliasedRecordType>& source, 
		      const std::string& transfer);

  ~RecordTypeTransfer2();

  void execute(RecordBuffer & sourceA, 
	       RecordBuffer & sourceB,
	       RecordBuffer target, 
	       class InterpreterContext * ctxt, 
	       bool isSourceAMove,
	       bool isSourceBMove) const
  {
    RecordBuffer sources[2] = { sourceA, sourceB };
    bool isSourceMove [2] = { isSourceAMove, isSourceBMove };
    execute(&sources[0], &isSourceMove[0], 2, target, ctxt);
  }
  /**
   * Copy or move the contents of source to target depending on the value
   * of flag isSourceMove.
   */
  void execute(RecordBuffer * sources, 
	       bool * isSourceMove,
	       int32_t numSources,
	       RecordBuffer target, 
	       class InterpreterContext * ctxt) const;

  /**
   * The target type of the transfer.
   */
  const RecordType * getTarget() const
  {
    return mTarget;
  }

  /**
   * Get a serializable module corresponding to this transfer.
   */
  IQLTransferModule2 * create() const;
};

class IQLFunctionModule
{
public:
  typedef void (*LLVMFuncType)(char*, char*, int32_t *, class InterpreterContext *);
private:
  std::string mFunName;
  std::vector<unsigned char> mBitcode;
  LLVMFuncType mFunction;
  class IQLRecordBufferMethodHandle * mImpl;

  // Create the LLVM module from the bitcode.
  void initImpl();

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mBitcode);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mFunName);
    ar & BOOST_SERIALIZATION_NVP(mBitcode);

    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLFunctionModule()
    :
    mFunction(NULL),
    mImpl(NULL)
  {
  }
public:
  IQLFunctionModule(const std::string& funName, 
		    const std::vector<unsigned char>& bitcode);
  ~IQLFunctionModule();
  /**
   * Execute the method.
   */
  int32_t execute(RecordBuffer sourceA, RecordBuffer sourceB, class InterpreterContext * ctxt) const;
  /**
   * For those who want to make a copy of the function pointer into another
   * data structure...
   */
  LLVMFuncType getRawFunction () const { return mFunction; }
};

/**
 * Evaluate an integer value expression against a set of input records.
 * TODO: Templatize on the output parameter.  Gracefully handle an
 * arbitrary number of input records (currently must be one or two).
 */
class RecordTypeFunction : public LLVMBase
{
public:
  // TODO: change this to support more than two inputs (using char** presumably)
  typedef void (*LLVMFuncType)(char*, char*, int32_t *, class InterpreterContext *);
private:
  std::vector<const RecordType *> mSources;
  std::string mFunName;
  std::string mStatements;
  std::vector<unsigned char> mBitcode;

  LLVMFuncType mFunction;
  class IQLRecordBufferMethodHandle * mImpl;
public:
  /**
   * Compile an expression against a set of input sources.
   * Right now this will throw if there are any name collisions among
   * the inputs.  TODO: Support aliasing or multi-part names to disambiguate.
   */
  RecordTypeFunction(class DynamicRecordContext& recCtxt, 
		     const std::string & funName, 
		     const std::vector<const RecordType *> sources, 
		     const std::string& statements,
		     const std::vector<std::string> * prefixes = NULL);

  ~RecordTypeFunction();
  /**
   * Evaluate and return.
   */
  int32_t execute(RecordBuffer sourceA, RecordBuffer sourceB, class InterpreterContext * ctxt) const;

  /**
   * For those who want to make a copy of the function pointer into another
   * data structure...
   */
  LLVMFuncType getRawFunction () const { return mFunction; }
  
  /**
   * Create a serializable representation of the function that can be sent on the wire.
   */
  IQLFunctionModule * create() const;
};

class IQLAggregateModule
{
private:
  RecordTypeMalloc mAggregateMalloc;
  RecordTypeMalloc mTransferMalloc;
  std::string mInitName;
  std::string mUpdateName;
  std::string mTransferName;
  std::vector<unsigned char> mBitcode;
  // TODO: change this to support more than two inputs (using char** presumably)
  typedef void (*LLVMFuncType)(char*, char*, class InterpreterContext *);
  typedef void (*LLVMFuncType2)(char*, char*, char*, class InterpreterContext *);
  LLVMFuncType mInitFunction;
  LLVMFuncType mUpdateFunction;
  LLVMFuncType mTransferFunction;
  class IQLRecordBufferMethodHandle * mImpl;
  bool mIsTransferIdentity;

  // Create the LLVM module from the bitcode.
  void initImpl();

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void save(Archive & ar, const unsigned int version) const
  {
    ar & BOOST_SERIALIZATION_NVP(mInitName);
    ar & BOOST_SERIALIZATION_NVP(mUpdateName);
    ar & BOOST_SERIALIZATION_NVP(mTransferName);
    ar & BOOST_SERIALIZATION_NVP(mBitcode);
    ar & BOOST_SERIALIZATION_NVP(mAggregateMalloc);
    ar & BOOST_SERIALIZATION_NVP(mTransferMalloc);
    ar & BOOST_SERIALIZATION_NVP(mIsTransferIdentity);
  }
  template <class Archive>
  void load(Archive & ar, const unsigned int version) 
  {
    ar & BOOST_SERIALIZATION_NVP(mInitName);
    ar & BOOST_SERIALIZATION_NVP(mUpdateName);
    ar & BOOST_SERIALIZATION_NVP(mTransferName);
    ar & BOOST_SERIALIZATION_NVP(mBitcode);
    ar & BOOST_SERIALIZATION_NVP(mAggregateMalloc);
    ar & BOOST_SERIALIZATION_NVP(mTransferMalloc);
    ar & BOOST_SERIALIZATION_NVP(mIsTransferIdentity);

    initImpl();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
  IQLAggregateModule()
    :
    mInitFunction(NULL),
    mUpdateFunction(NULL),
    mTransferFunction(NULL),
    mImpl(NULL),
    mIsTransferIdentity(false)
  {
  }
public:
  IQLAggregateModule(const RecordTypeMalloc& aggregateMalloc,
		     const RecordTypeMalloc& targetMalloc,
		     const std::string& initName, 
		     const std::string& updateName,
		     const std::string& transferName,
		     const std::vector<unsigned char>& bitcode,
		     bool isTransferIdentity);
  ~IQLAggregateModule();

  /**
   * Is executeTransfer an identity mapping?
   */
  bool getIsTransferIdentity() const 
  {
    return mIsTransferIdentity; 
  }
  /**
   * Execute methods for aggregate functions.
   */
  void executeInit(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt) const;
  void executeUpdate(RecordBuffer source, RecordBuffer target, class InterpreterContext * ctxt) const;
  void executeTransfer(RecordBuffer & source, RecordBuffer & target, class InterpreterContext * ctxt) const;
  void executeTransfer(RecordBuffer & source1, RecordBuffer& source2, RecordBuffer & target, class InterpreterContext * ctxt) const;
};

/**
 * Transform an input record into an output.
 * This assumes we are provided with a list of
 * expressions each of which generates an output field.
 * It does not support statements/procedural logic.
 */
class RecordTypeAggregate : public LLVMBase
{
private:
  const RecordType * mSource;
  const RecordType * mAggregate;
  const RecordType * mTarget;
  std::string mInitializeFun;
  std::string mUpdateFun;
  std::string mTransferFun;
  std::vector<unsigned char> mBitcode;

  // Is the transfer the identity?
  bool mIsIdentity;

  void createUpdateFunction(const std::vector<std::string>& groupKeys);
public:
  void init(class DynamicRecordContext& recCtxt, 
	    const std::string & funName, 
	    const RecordType * source, 
	    const std::string& init,
	    const std::string& update,
	    const std::vector<std::string>& groupKeys,
	    bool isOlap=false);
  /**
   * Perform an aggregate transfer of an input record.
   */
  RecordTypeAggregate(class DynamicRecordContext& recCtxt, 
		      const std::string & funName, 
		      const RecordType * source, 
		      const std::string& transfer,
		      const std::vector<std::string>& groupKeys,
		      bool isOlap=false);

  /**
   * Perform an aggregate transfer of an input record.
   */
  RecordTypeAggregate(class DynamicRecordContext& recCtxt, 
		      const std::string & funName, 
		      const RecordType * source, 
		      const std::string& init,
		      const std::string& update,
		      const std::vector<std::string>& groupKeys,
		      bool isOlap=false);

  ~RecordTypeAggregate();

  /**
   * The target type of the transfer.
   * The target record is created not from the source but
   * rather from the aggregate record.  In those cases in which
   * the transfer from aggregate to final record is an identity
   * use the method isFinalTransferIdentity.
   */
  const RecordType * getTarget() const
  {
    return mTarget;
  }

  /**
   * The aggregate type of the transfer.
   * The aggregate type contains the group fields as well as 
   * one field for each aggregate function expression.  The
   * initialize function is used to create the initial values
   * in the aggregate record based on an source record.  The update
   * function takes an aggregate record and a source record and
   * updates the values of the aggregate in response to the source.
   */
  const RecordType * getAggregate() const
  {
    return mAggregate;
  }

  /**
   * Is this the transfer from aggregate to final an
   * identity?
   */
  bool isFinalTransferIdentity() const 
  {
    return mIsIdentity;
  }

  /** 
   * create a serializable IQLTransferModule that implements the program.
   */
  IQLAggregateModule * create() const;
};

#endif
