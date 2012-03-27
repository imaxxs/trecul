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

#include "CodeGenerationContext.hh"
#include "LLVMGen.h"
#include "RecordType.hh"

IQLToLLVMValue::IQLToLLVMValue (LLVMValueRef val, 
				IQLToLLVMValue::ValueType globalOrLocal)
  :
  mValue(val),
  mIsNull(NULL),
  mValueType(globalOrLocal)
{
}
  
IQLToLLVMValue::IQLToLLVMValue (LLVMValueRef val, llvm::Value * isNull, 
				IQLToLLVMValue::ValueType globalOrLocal)
  :
  mValue(val),
  mIsNull(isNull),
  mValueType(globalOrLocal)
{
}

LLVMValueRef IQLToLLVMValue::getValue() const 
{ 
  return mValue; 
}

llvm::Value * IQLToLLVMValue::getNull() const 
{ 
  return mIsNull; 
}

void IQLToLLVMValue::setNull(llvm::Value * nv) 
{ 
  mIsNull = nv; 
}

bool IQLToLLVMValue::isLiteralNull() const 
{ 
  return mValue == NULL; 
}

IQLToLLVMValue::ValueType IQLToLLVMValue::getValueType() const 
{ 
  return mValueType; 
}

LLVMTypeRef IQLToLLVMValue::getVariableType(CodeGenerationContext * ctxt,
					    const FieldType * ft)
{
  LLVMTypeRef ty = wrap(ft->LLVMGetType(ctxt));
  if (!isValueType(ty)) {
    ty = ::LLVMPointerType(ty,0);
  }
  return ty;
}

bool IQLToLLVMValue::isValueType(LLVMTypeRef ty) 
{
  LLVMContextRef c = LLVMGetTypeContext(ty);
  return ty == LLVMInt32TypeInContext(c) ||
    ty == LLVMInt64TypeInContext(c) ||
    ty == LLVMDoubleTypeInContext(c);
}

bool IQLToLLVMValue::isPointerToValueType(LLVMTypeRef ty) 
{
  if (LLVMGetTypeKind(ty) != LLVMPointerTypeKind)
    return false;
  ty = LLVMGetElementType(ty);
  LLVMContextRef c = LLVMGetTypeContext(ty);
  return ty == LLVMInt32TypeInContext(c) ||
    ty == LLVMInt64TypeInContext(c) ||
    ty == LLVMDoubleTypeInContext(c);
}

bool IQLToLLVMValue::isValueType() const 
{
  LLVMTypeRef ty = LLVMTypeOf(mValue);
  return isValueType(ty);
}

bool IQLToLLVMValue::isPointerToValueType() const 
{
  LLVMTypeRef ty = LLVMTypeOf(mValue);
  return isPointerToValueType(ty);
}

bool IQLToLLVMValue::isReferenceType() const
{
  return !isValueType();
}

IQLToLLVMValueRef IQLToLLVMValue::get(CodeGenerationContext * ctxt, 
				      LLVMValueRef val, 
				      IQLToLLVMValue::ValueType globalOrLocal)
{
  return get(ctxt, val, NULL, globalOrLocal);
}

IQLToLLVMValueRef IQLToLLVMValue::get(CodeGenerationContext * ctxt, 
				      LLVMValueRef val,
				      llvm::Value * nv,
				      IQLToLLVMValue::ValueType globalOrLocal)
{
  ctxt->ValueFactory.push_back(new IQLToLLVMValue(val, nv, globalOrLocal));
  return reinterpret_cast<IQLToLLVMValueRef>(ctxt->ValueFactory.back());
}

IQLToLLVMField::IQLToLLVMField(CodeGenerationContext * ctxt,
			       const RecordType * recordType,
			       const std::string& memberName,
			       const std::string& recordName)
  :
  mMemberName(memberName),
  mBasePointer(NULL),
  mRecordType(recordType)
{
  mBasePointer = llvm::unwrap(ctxt->lookupValue(recordName.c_str())->getValue());
}

IQLToLLVMField::IQLToLLVMField(const RecordType * recordType,
			       const std::string& memberName,
			       llvm::Value * basePointer)
  :
  mMemberName(memberName),
  mBasePointer(basePointer),
  mRecordType(recordType)
{
}

IQLToLLVMField::~IQLToLLVMField() 
{
}

void IQLToLLVMField::setNull(CodeGenerationContext *ctxt, bool isNull) const
{
  mRecordType->LLVMMemberSetNull(mMemberName, ctxt, mBasePointer, isNull);
}

IQLToLLVMValueRef IQLToLLVMField::getValuePointer(CodeGenerationContext * ctxt) const
{
  llvm::Value * outputVal = mRecordType->LLVMMemberGetPointer(mMemberName, 
							      ctxt, 
							      mBasePointer,
							      false);
  // Don't worry about Nullability, it is dealt separately  
  IQLToLLVMValueRef val = 
    IQLToLLVMValue::get(ctxt, 
			llvm::wrap(outputVal), 
			NULL, 
			IQLToLLVMValue::eGlobal);  
  return val;
}

const IQLToLLVMValue * 
IQLToLLVMField::getEntirePointer(CodeGenerationContext * ctxt) const
{
  llvm::Value * outputVal = mRecordType->LLVMMemberGetPointer(mMemberName, 
							      ctxt, 
							      mBasePointer,
							      false);
  llvm::Value * nullVal = NULL;
  if (isNullable()) {
    nullVal = mRecordType->LLVMMemberGetNull(mMemberName,
					     ctxt,
					     mBasePointer);
  }
  IQLToLLVMValueRef val = 
    IQLToLLVMValue::get(ctxt, 
			llvm::wrap(outputVal), 
			nullVal,
			IQLToLLVMValue::eGlobal);  
  return unwrap(val);
}

bool IQLToLLVMField::isNullable() const
{
  const FieldType * outputTy = mRecordType->getMember(mMemberName).GetType();
  return outputTy->isNullable();
}

IQLToLLVMLocal::IQLToLLVMLocal(const IQLToLLVMValue * val,
			       llvm::Value * nullBit)
  :
  mValue(val),
  mNullBit(nullBit)
{
}

IQLToLLVMLocal::~IQLToLLVMLocal()
{
}

IQLToLLVMValueRef IQLToLLVMLocal::getValuePointer(CodeGenerationContext * ctxt) const
{
  return wrap(mValue);
}

const IQLToLLVMValue * 
IQLToLLVMLocal::getEntirePointer(CodeGenerationContext * ctxt) const
{
  if(NULL == mNullBit) {
    return mValue;
  } else {
    llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
    return unwrap(IQLToLLVMValue::get(ctxt, mValue->getValue(),
				      b->CreateLoad(mNullBit),
				      mValue->getValueType()));
  }
}

llvm::Value * IQLToLLVMLocal::getNullBitPointer() const
{
  return mNullBit;
}

void IQLToLLVMLocal::setNull(CodeGenerationContext * ctxt, bool isNull) const
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  b->CreateStore(isNull ? b->getTrue() : b->getFalse(), mNullBit);
}

bool IQLToLLVMLocal::isNullable() const
{
  return mNullBit != NULL;
}

SymbolTable::SymbolTable()
{
}

SymbolTable::~SymbolTable()
{
  for(table_type::iterator it = mSymbols.begin();
      it != mSymbols.end();
      ++it) {
    delete it->second;
  }
}

IQLToLLVMLValue * SymbolTable::lookup(const char * nm) const
{
  table_type::const_iterator it = mSymbols.find(nm);
  if (it == mSymbols.end() )
    return NULL;
  else
    return it->second;
}

void SymbolTable::add(const char * nm, IQLToLLVMLValue * value)
{
  // Don't bother worrying about overwriting a symbol table entry
  // this should be safe by virtue of type check. 
  // TODO: We shouldn't even be managing a symbol table during
  // code generation all names should be resolved during type
  // checking.
  // table_type::const_iterator it = mSymbols.find(nm);
  // if (it != mSymbols.end() )
  //   throw std::runtime_error((boost::format("Variable %1% already defined")
  // 			      % nm).str());
  mSymbols[nm] = value;
}

void SymbolTable::clear()
{
  mSymbols.clear();
}

void SymbolTable::dump() const
{
  // for(table_type::const_iterator it = tab.begin();
  //     it != tab.end();
  //     ++it) {
  //   std::cerr << it->first.c_str() << ":";
  //   llvm::unwrap(unwrap(it->second)->getValue())->dump();
  // }
}

CodeGenerationFunctionContext::CodeGenerationFunctionContext()
  :
  Builder(NULL),
  mSymbolTable(NULL),
  Function(NULL),
  RecordArguments(NULL),
  OutputRecord(NULL),
  AllocaCache(NULL)
{
}

CodeGenerationContext::CodeGenerationContext()
  :
  mOwnsModule(true),
  mSymbolTable(NULL),
  LLVMContext(NULL),
  LLVMModule(NULL),
  LLVMBuilder(NULL),
  LLVMDecContextPtrType(NULL),
  LLVMDecimal128Type(NULL),
  LLVMVarcharType(NULL),
  LLVMDatetimeType(NULL),
  LLVMFunction(NULL),
  IQLRecordArguments(NULL),
  IQLOutputRecord(NULL),
  LLVMMemcpyIntrinsic(NULL),
  LLVMMemsetIntrinsic(NULL),
  LLVMMemcmpIntrinsic(NULL),
  IQLMoveSemantics(0),
  IsIdentity(true),
  AggFn(0),
  AllocaCache(NULL)
{
  LLVMContext = ::LLVMContextCreate();
  LLVMModule = ::LLVMModuleCreateWithNameInContext("my cool JIT", 
						   LLVMContext);
}

CodeGenerationContext::~CodeGenerationContext()
{
  typedef std::vector<IQLToLLVMValue *> factory;
  for(factory::iterator it = ValueFactory.begin();
      it != ValueFactory.end();
      ++it) {
    delete *it;
  }

  if (LLVMBuilder) {
    LLVMDisposeBuilder(LLVMBuilder);
    LLVMBuilder = NULL;
  }
  if (mSymbolTable) {
    delete mSymbolTable;
    mSymbolTable = NULL;
  }  
  delete unwrap(IQLRecordArguments);
  if (mOwnsModule && LLVMModule) {
    LLVMDisposeModule(LLVMModule);
    LLVMModule = NULL;
  }
  if (LLVMContext) {
    LLVMContextDispose(LLVMContext);
    LLVMContext = NULL;
  }
  while(IQLCase.size()) {
    delete IQLCase.top();
    IQLCase.pop();
  }
}

void CodeGenerationContext::disownModule()
{
  mOwnsModule = false;
}

void CodeGenerationContext::defineVariable(const char * name,
					   llvm::Value * val,
					   llvm::Value * nullVal,
					   IQLToLLVMValue::ValueType globalOrLocal)
{
  IQLToLLVMValueRef tmp = IQLToLLVMValue::get(this, wrap(val), 
					      NULL, globalOrLocal);
  IQLToLLVMLocal * local = new IQLToLLVMLocal(unwrap(tmp),
					      nullVal);
  mSymbolTable->add(name, local);
}

void CodeGenerationContext::defineFieldVariable(llvm::Value * basePointer,
						const char * memberName,
						const char * prefixedMemberName,
						const RecordType * recordType)
{
  IQLToLLVMField * field = new IQLToLLVMField(recordType,
					      memberName,
					      basePointer);
  mSymbolTable->add(prefixedMemberName, field);
}

const IQLToLLVMLValue * 
CodeGenerationContext::lookup(const char * name)
{
  IQLToLLVMLValue * lval = mSymbolTable->lookup(name);
  return lval;
}

const IQLToLLVMValue * 
CodeGenerationContext::lookupValue(const char * name)
{
  IQLToLLVMLValue * lval = mSymbolTable->lookup(name);
  return lval->getEntirePointer(this);
}

LLVMValueRef CodeGenerationContext::getContextArgumentRef()
{
  return lookupValue("__DecimalContext__")->getValue();
}

void CodeGenerationContext::reinitializeForTransfer()
{
  delete (local_cache *) AllocaCache;
  delete mSymbolTable;
  mSymbolTable = new SymbolTable();
  AllocaCache = new local_cache();
}

void CodeGenerationContext::reinitialize()
{
  // Reinitialize and create transfer
  mSymbolTable->clear();
  LLVMFunction = NULL;
  unwrap(IQLRecordArguments)->clear();
}

void CodeGenerationContext::createFunctionContext()
{
  LLVMBuilder = LLVMCreateBuilderInContext(LLVMContext);
  mSymbolTable = new SymbolTable();
  LLVMFunction = NULL;
  IQLRecordArguments = wrap(new std::map<std::string, std::pair<std::string, const RecordType*> >());
  IQLOutputRecord = NULL;
  AllocaCache = new local_cache();
}

void CodeGenerationContext::dumpSymbolTable()
{
  mSymbolTable->dump();
}

void CodeGenerationContext::restoreAggregateContext(CodeGenerationFunctionContext * fCtxt)
{
  this->LLVMBuilder = fCtxt->Builder;
  this->mSymbolTable = fCtxt->mSymbolTable;
  this->LLVMFunction = fCtxt->Function;
  this->IQLRecordArguments = fCtxt->RecordArguments;
  this->IQLOutputRecord = fCtxt->OutputRecord;
}

void CodeGenerationContext::saveAggregateContext(CodeGenerationFunctionContext * fCtxt)
{
  fCtxt->Builder = this->LLVMBuilder;
  fCtxt->mSymbolTable = this->mSymbolTable;
  fCtxt->Function = this->LLVMFunction;
  fCtxt->RecordArguments = this->IQLRecordArguments;
  fCtxt->OutputRecord = this->IQLOutputRecord;
  fCtxt->AllocaCache = this->AllocaCache;
}

void CodeGenerationContext::addInputRecordType(const char * name, 
					       const char * argumentName, 
					       const RecordType * rec,
					       const char * prefix)
{
  boost::dynamic_bitset<> mask;
  mask.resize(rec->size(), true);
  addInputRecordType(name, argumentName, rec, mask, prefix);
}

void CodeGenerationContext::addInputRecordType(const char * name, 
					       const char * argumentName, 
					       const RecordType * rec,
					       const boost::dynamic_bitset<>& mask,
					       const char * prefix)
{
  llvm::Value * basePointer = llvm::unwrap(lookupValue(argumentName)->getValue());
  for(RecordType::const_member_iterator it = rec->begin_members();
      it != rec->end_members();
      ++it) {
    std::size_t idx = (std::size_t) std::distance(rec->begin_members(), it);
    if (!mask.test(idx)) continue;
    rec->LLVMMemberGetPointer(it->GetName(), 
			      this, 
			      basePointer,
			      true, // Put the member into the symbol table
			      prefix);
  }
  std::map<std::string, std::pair<std::string, const RecordType *> >& recordTypes(*unwrap(IQLRecordArguments));
  recordTypes[name] = std::make_pair(argumentName, rec);
}

LLVMSymbolTableRef LLVMSymbolTableCreate()
{
  return reinterpret_cast<LLVMSymbolTableRef>(new std::map<std::string, IQLToLLVMValueRef>());
}

void LLVMSymbolTableFree(LLVMSymbolTableRef symTable)
{
  delete reinterpret_cast<std::map<std::string, IQLToLLVMValueRef> *>(symTable);
}

IQLToLLVMValueRef LLVMSymbolTableLookup(LLVMSymbolTableRef symTable, const char * name)
{
  std::map<std::string, IQLToLLVMValueRef> * tab = reinterpret_cast<std::map<std::string, IQLToLLVMValueRef> *>(symTable);
  std::map<std::string, IQLToLLVMValueRef>::const_iterator it = tab->find(name);
  if (tab->end() == it) {
    throw std::runtime_error((boost::format("Undefined variable: %1%") % name).str());
    //return NULL;
  }
  return it->second;
}

void LLVMSymbolTableAdd(LLVMSymbolTableRef symTable, const char * name, IQLToLLVMValueRef value)
{
  std::map<std::string, IQLToLLVMValueRef> * tab = reinterpret_cast<std::map<std::string, IQLToLLVMValueRef> *>(symTable);
  std::map<std::string, IQLToLLVMValueRef>::const_iterator it = tab->find(name);
  (*tab)[name] = value;
}

void LLVMSymbolTableClear(LLVMSymbolTableRef symTable)
{
  reinterpret_cast<std::map<std::string, IQLToLLVMValueRef> *>(symTable)->clear();
}

void LLVMSymbolTableDump(LLVMSymbolTableRef symTable)
{
  const std::map<std::string, IQLToLLVMValueRef>& tab(*reinterpret_cast<std::map<std::string, IQLToLLVMValueRef> *>(symTable));
  for(std::map<std::string, IQLToLLVMValueRef>::const_iterator it = tab.begin();
      it != tab.end();
      ++it) {
    std::cerr << it->first.c_str() << ":";
    llvm::unwrap(unwrap(it->second)->getValue())->dump();
  }
}

