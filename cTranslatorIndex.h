#pragma once
#include <algorithm>
#include <array>
#include <vector>
#include "cColumn.h"

class cTranslatorIndex
{
public:
	string indexName;
	int position;
	string tableName;
	string columnName;

	cTranslatorIndex();
	void TranslateCreateIndex(string input);
};

inline cTranslatorIndex::cTranslatorIndex():position(13)
{
}

inline void cTranslatorIndex::TranslateCreateIndex(string input)
{
	string tempName;

	while (input.find(" ", position) != position)//vyparsování názvu indexu, dokud nenajde meyeru
	{
		tempName.insert(0, 1, input.at(position));
		position++;
	}
	std::reverse(tempName.begin(), tempName.end());//preklopení názvu indexu
	indexName = tempName;
	//indexName = tempName.c_str();

	position++;

	if (input.find("ON", position) != position)
	{
		cout << "syntax error" << endl;
	}
	else
	{
		position += 2;
	}
	position++;
	string tempTableName;
	while (input.find("(", position) != position)//vyparsování názvu tabulky pro index, dokud nenajde (
	{
		tempTableName.insert(0, 1, input.at(position));
		position++;
	}
	std::reverse(tempTableName.begin(), tempTableName.end());
	tableName = tempTableName;
	//tableName = tempTableName.c_str();
	
	position++;
	
	while (input.find(")", position) != position)//vyparsování názvu sloupce pro index, dokud nenajde )
	{											//není řešený složený index!!!!
		columnName.insert(0, 1, input.at(position));
		position++;
	}
	std::reverse(columnName.begin(), columnName.end());
	position++;
}
