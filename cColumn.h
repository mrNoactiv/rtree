#pragma once
#include "dstruct/paged/b+tree/cB+Tree.h"
#include "common/random/cGaussRandomGenerator.h"
#include "common/data/cDataCollection.h"
#include "common/data/cTuplesGenerator.h"
#include "dstruct/paged/core/cBulkLoad.h"
#include "common/datatype/tuple/cCommonNTuple.h"
#include <algorithm>
#include <array>
#include <vector>

class cColumn
{
public:
	std::string name;
	int size;
	cDataType *cType;
	//cBasicType<cDataType*> cType;
	bool notNull;
	bool primaryKey;
	cSpaceDescriptor *columnSD;
	int positionInTable;

	cBasicType<cDataType*> GetColumnType();
};



inline cBasicType<cDataType*> cColumn::GetColumnType()
{
	return cType;
}




