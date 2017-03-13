#pragma once
#include "cTranslatorCreate.h"


enum Type { CREATE , INDEX  };
class cTypeOfTranslator
{

public:

	Type type;

	void SetType(string input);
	cTypeOfTranslator();



};

cTypeOfTranslator::cTypeOfTranslator() :type()
{

}

void cTypeOfTranslator::SetType(string input)
{
	if (std::size_t foundDDL = input.find("create table", 0) == 0)
	{
		type = CREATE;
	}
	else if (std::size_t foundDDL = input.find("create index", 0) == 0)
	{
		type = INDEX;
	}
}