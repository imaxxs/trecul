#ifndef __IQLBUILDTREE_H
#define __IQLBUILDTREE_H

// C interface to build tree to avoid renaming
// generated Antlr files to enable C++ compilation.
// Who knows, maybe there will turn out to be another
// use for a C binding.
#ifdef __cplusplus
extern "C" {
#endif

  typedef struct IQLTreeFactoryStruct * IQLTreeFactoryRef;
  typedef struct IQLExpressionStruct * IQLExpressionRef;
  typedef struct IQLExpressionListStruct * IQLExpressionListRef;
  typedef const struct IQLFieldTypeStruct * IQLFieldTypeRef;
  typedef struct IQLRecordConstructorStruct * IQLRecordConstructorRef;
  typedef struct IQLFieldConstructorStruct * IQLFieldConstructorRef;
  typedef struct IQLFieldConstructorListStruct * IQLFieldConstructorListRef;

  IQLFieldConstructorListRef IQLFieldConstructorListCreate(IQLTreeFactoryRef ctxtRef);
  void IQLFieldConstructorListFree(IQLTreeFactoryRef ctxtRef,
				   IQLFieldConstructorListRef l);
  void IQLFieldConstructorListAppend(IQLTreeFactoryRef ctxtRef,
				     IQLFieldConstructorListRef l,
				     IQLFieldConstructorRef e);

  IQLFieldConstructorRef IQLBuildAddFields(IQLTreeFactoryRef ctxtRef, 
					   const char * recordName);
  IQLFieldConstructorRef IQLBuildAddField(IQLTreeFactoryRef ctxtRef, 
					  const char * fieldName,
					  IQLExpressionRef expr);
  IQLFieldConstructorRef IQLBuildQuotedId(IQLTreeFactoryRef ctxtRef, 
					  const char * pattern,
					  const char * names);
  IQLRecordConstructorRef IQLBuildRecord(IQLTreeFactoryRef ctxtRef,
					 IQLFieldConstructorListRef fields);

  IQLFieldTypeRef IQLBuildInt32Type(IQLTreeFactoryRef ctxtRef, int nullable);
  IQLFieldTypeRef IQLBuildInt64Type(IQLTreeFactoryRef ctxtRef, int nullable);
  IQLFieldTypeRef IQLBuildDoubleType(IQLTreeFactoryRef ctxtRef, int nullable);
  IQLFieldTypeRef IQLBuildDecimalType(IQLTreeFactoryRef ctxtRef, int nullable);
  IQLFieldTypeRef IQLBuildDateType(IQLTreeFactoryRef ctxtRef, int nullable);
  IQLFieldTypeRef IQLBuildDatetimeType(IQLTreeFactoryRef ctxtRef, int nullable);
  IQLFieldTypeRef IQLBuildNVarcharType(IQLTreeFactoryRef ctxtRef, int nullable);
  IQLFieldTypeRef IQLBuildVarcharType(IQLTreeFactoryRef ctxtRef, int nullable);
  IQLFieldTypeRef IQLBuildCharType(IQLTreeFactoryRef ctxtRef, const char * sz, int nullable);
  IQLFieldTypeRef IQLBuildBooleanType(IQLTreeFactoryRef ctxtRef, int nullable);
  IQLFieldTypeRef IQLBuildNilType(IQLTreeFactoryRef ctxtRef);
  IQLFieldTypeRef IQLBuildType(IQLTreeFactoryRef ctxtRef, const char * typeName, int nullable);

  IQLExpressionListRef IQLExpressionListCreate(IQLTreeFactoryRef ctxtRef);
  void IQLExpressionListFree(IQLTreeFactoryRef ctxtRef,
			     IQLExpressionListRef l);
  void IQLExpressionListAppend(IQLTreeFactoryRef ctxtRef,
			       IQLExpressionListRef l,
			       IQLExpressionRef e);

  IQLExpressionRef IQLBuildLogicalOr(IQLTreeFactoryRef ctxtRef,
  				     IQLExpressionRef left,
  				     IQLExpressionRef right,
  				     int line, int column);
  IQLExpressionRef IQLBuildLogicalAnd(IQLTreeFactoryRef ctxtRef,
				      IQLExpressionRef left,
				      IQLExpressionRef right,
				      int line, int column);
  IQLExpressionRef IQLBuildLogicalNot(IQLTreeFactoryRef ctxtRef,
				      IQLExpressionRef left,
				      int line, int column);
  IQLExpressionRef IQLBuildEquals(IQLTreeFactoryRef ctxtRef,
				  IQLExpressionRef left,
				  IQLExpressionRef right,
				  int line, int column);
  IQLExpressionRef IQLBuildNotEquals(IQLTreeFactoryRef ctxtRef,
				     IQLExpressionRef left,
				     IQLExpressionRef right,
				     int line, int column);
  IQLExpressionRef IQLBuildGreaterThan(IQLTreeFactoryRef ctxtRef,
				       IQLExpressionRef left,
				       IQLExpressionRef right,
				       int line, int column);
  IQLExpressionRef IQLBuildLessThan(IQLTreeFactoryRef ctxtRef,
				    IQLExpressionRef left,
				    IQLExpressionRef right,
				    int line, int column);
  IQLExpressionRef IQLBuildGreaterThanEquals(IQLTreeFactoryRef ctxtRef,
					     IQLExpressionRef left,
					     IQLExpressionRef right,
					     int line, int column);
  IQLExpressionRef IQLBuildLessThanEquals(IQLTreeFactoryRef ctxtRef,
					  IQLExpressionRef left,
					  IQLExpressionRef right,
					  int line, int column);
  IQLExpressionRef IQLBuildLike(IQLTreeFactoryRef ctxtRef,
				IQLExpressionRef left,
				IQLExpressionRef right,
				int line, int column);
  IQLExpressionRef IQLBuildRLike(IQLTreeFactoryRef ctxtRef,
				 IQLExpressionRef left,
				 IQLExpressionRef right,
				 int line, int column);
  IQLExpressionRef IQLBuildPlus(IQLTreeFactoryRef ctxtRef,
				 IQLExpressionRef left,
				 IQLExpressionRef right,
				 int line, int column);
  IQLExpressionRef IQLBuildMinus(IQLTreeFactoryRef ctxtRef,
				 IQLExpressionRef left,
				 IQLExpressionRef right,
				 int line, int column);
  IQLExpressionRef IQLBuildTimes(IQLTreeFactoryRef ctxtRef,
				 IQLExpressionRef left,
				 IQLExpressionRef right,
				 int line, int column);
  IQLExpressionRef IQLBuildDivide(IQLTreeFactoryRef ctxtRef,
				 IQLExpressionRef left,
				 IQLExpressionRef right,
				 int line, int column);
  IQLExpressionRef IQLBuildModulus(IQLTreeFactoryRef ctxtRef,
				 IQLExpressionRef left,
				 IQLExpressionRef right,
				 int line, int column);
  IQLExpressionRef IQLBuildUnaryPlus(IQLTreeFactoryRef ctxtRef,
				     IQLExpressionRef left,
				     int line, int column);
  IQLExpressionRef IQLBuildUnaryMinus(IQLTreeFactoryRef ctxtRef,
				      IQLExpressionRef left,
				      int line, int column);
  IQLExpressionRef IQLBuildBitwiseAnd(IQLTreeFactoryRef ctxtRef,
				      IQLExpressionRef left,
				      IQLExpressionRef right,
				      int line, int column);
  IQLExpressionRef IQLBuildBitwiseOr(IQLTreeFactoryRef ctxtRef,
				     IQLExpressionRef left,
				     IQLExpressionRef right,
				     int line, int column);
  IQLExpressionRef IQLBuildBitwiseXor(IQLTreeFactoryRef ctxtRef,
				      IQLExpressionRef left,
				      IQLExpressionRef right,
				      int line, int column);
  IQLExpressionRef IQLBuildBitwiseNot(IQLTreeFactoryRef ctxtRef,
				      IQLExpressionRef left,
				      int line, int column);
  IQLExpressionRef IQLBuildCase(IQLTreeFactoryRef ctxtRef,
				IQLExpressionListRef args,
				int line, int column);
  IQLExpressionRef IQLBuildCast(IQLTreeFactoryRef ctxtRef,
				IQLFieldTypeRef ty,
				IQLExpressionRef arg,
				int line, int column);
  IQLExpressionRef IQLBuildLiteralCast(IQLTreeFactoryRef ctxtRef,
				       const char * ty,
				       const char * arg,
				       int line, int column);
  IQLExpressionRef IQLBuildCall(IQLTreeFactoryRef ctxtRef,
				const char * fun,
				IQLExpressionListRef args,
				int line, int column);
  IQLExpressionRef IQLBuildUnaryFun(IQLTreeFactoryRef ctxtRef,
				    const char * fun,
				    IQLExpressionRef leftRef,
				    int line, int column);
  IQLExpressionRef IQLBuildInt32(IQLTreeFactoryRef ctxtRef,
				 const char * text,
				 int line, int column);
  IQLExpressionRef IQLBuildInt64(IQLTreeFactoryRef ctxtRef,
				 const char * text,
				 int line, int column);
  IQLExpressionRef IQLBuildInterval(IQLTreeFactoryRef ctxtRef,
				    const char * text,
				    IQLExpressionRef arg,
				    int line, int column);
  IQLExpressionRef IQLBuildBoolean(IQLTreeFactoryRef ctxtRef,
				   int isTrue,
				   int line, int column);
  IQLExpressionRef IQLBuildNil(IQLTreeFactoryRef ctxtRef,
			       int line, int column);
  IQLExpressionRef IQLBuildIsNull(IQLTreeFactoryRef ctxtRef,
				  IQLExpressionRef left,
				  int isNot,
				  int line, int column);
  IQLExpressionRef IQLBuildDouble(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  int line, int column);
  IQLExpressionRef IQLBuildDecimal(IQLTreeFactoryRef ctxtRef,
				   const char * text,
				   int line, int column);
  IQLExpressionRef IQLBuildString(IQLTreeFactoryRef ctxtRef,
				  const char * text,
				  int line, int column);
  IQLExpressionRef IQLBuildVariable(IQLTreeFactoryRef ctxtRef,
				    const char * text,
				    const char * text2,
				    int line, int column);
  IQLExpressionRef IQLBuildArrayRef(IQLTreeFactoryRef ctxtRef,
				    const char * text,
				    IQLExpressionRef idx,
				    int line, int column);
  IQLExpressionRef IQLBuildArray(IQLTreeFactoryRef ctxtRef,
				 IQLExpressionListRef args,
				 int line, int column);


#ifdef __cplusplus
}
#endif

#endif
