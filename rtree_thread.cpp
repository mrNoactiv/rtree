// #include <iostream>

#include <thread>

#include "common/cCommon.h"
#include "dstruct/paged/core/cQuickDB.h"
#include "common/datatype/tuple/cSpaceDescriptor.h"
#include "common/datatype/tuple/cTuple.h"
#include "common/datatype/tuple/cHNTuple.h"
#include "common/datatype/tuple/cMBRectangle.h"
#include "common/datatype/cBasicType.h"
#include "dstruct/paged/rtree/cRTree.h"
#include "dstruct/paged/rtree/cRTreeHeader.h"
#include "dstruct/paged/rtree/cSignatureController.h"
#include "common/utils/cTimer.h"
#include "common/random/cGaussRandomGenerator.h"
#include "common/cBitStringNew.h"
#include "dstruct/paged/rtree/cRangeQueryConfig.h"
#include "dstruct/paged/queryprocessing/cQueryStat.h"

using namespace common::utils;
using namespace dstruct::paged::core;
using namespace dstruct::paged::rtree;
using namespace dstruct::paged;
using namespace common::datatype::tuple;
using namespace common::random;

typedef cTuple tKey;
typedef cUInt tDomain;
typedef cUInt tLeafData;

void CreateDataCollection();
void Build(cRTreeHeader<tKey>* header);
void RunPointQuery(cRTreeHeader<tKey>* header);
void PointQueryData(cRTreeHeader<tKey>* header, unsigned int loQueryOrder, unsigned int hiQueryOrder);
void SetSignatureController(cSignatureController *controller, bool *sigEnabledLevel, uint *sigCount, uint *bitCount, uint signatureBuildType);

cSpaceDescriptor* sd;
tKey *dataCollection;

unsigned int DIM = 4; // 5; // 2;
unsigned int COUNT = 1000000;
unsigned int MAX_VALUE = 2000000;
bool CODING_USED = false;

unsigned int QUERYPROCESS = QueryType::SINGLEQUERY; /*SINGLEQUERY*/ /*BATCHQUERY*/ /**/ //CARTESIANQUERY; // defines the type of query processing
const unsigned int NOF_THREADS = 1;                 // for QueryType::SINGLEQUERY
const unsigned int NOF_THREADS_M1 = NOF_THREADS-1;  // for QueryType::SINGLEQUERY
unsigned int QUERIES_IN_MULTIQUERY = 4; // number of queries in multiquery

bool BULK_READ = false;  // if true, buffered read is enabled
unsigned int NODEINDEX_CAPACITY_BULKREAD = 256; // defines how many node indices can be stored in the leafIndices array
unsigned int MAX_READ_NODES_BULKREAD = 32; // defines how many leaf nodes can be read into the buffer
unsigned int MAX_INDEXDIFF_BULKREAD = 32;

bool CLOSEOPEN_DATAFILE = true; // define, if tree has to be stored after all items are inserted and open before querying
bool LOAD_DATAFILE_INTO_MEMORY = false; // define, if tree will be in memory or also 

bool SignatureEnabled = true;  // defines, if tree will use signatures for more efficient range query processing
unsigned int SignatureBuildType = cSignatureController::SignatureBuild_Insert; // defines the type of building signatures	
const unsigned int LEVELS_WITH_SIGNATURES = 3; // defines the number of lowest tree levels to be indexed by signatures
unsigned int sigCount[LEVELS_WITH_SIGNATURES] = {1, 1, 1};
unsigned int bitCount[LEVELS_WITH_SIGNATURES] = {1, 1, 1};
bool enabledLevel[LEVELS_WITH_SIGNATURES] = {true, true, true};

const unsigned int LeafDataSize = tLeafData::SER_SIZE; // int or float
const unsigned int BlockSize = 2048 /* 8192 */ /*32768*/;
const unsigned int CacheSizeInNodes = 20000;
const unsigned int MaxNodeInMemSize = /* 2600; */ (unsigned int)((double)BlockSize * 1.25);

cRTree<tKey> *Tree;
cQuickDB *quickDB;
// char dbPath[1024] = "D:\\Tests\\quickDB";
char dbPath[1024] = "quickdb";

int main(void)
{
	/*_CrtSetReportMode( _CRT_WARN, _CRTDBG_MODE_FILE );
	_CrtSetReportFile( _CRT_WARN, _CRTDBG_FILE_STDOUT );
	_CrtSetReportMode( _CRT_ERROR, _CRTDBG_MODE_FILE );
	_CrtSetReportFile( _CRT_ERROR, _CRTDBG_FILE_STDOUT );
	_CrtSetReportMode( _CRT_ASSERT, _CRTDBG_MODE_FILE );
	_CrtSetReportFile( _CRT_ASSERT, _CRTDBG_FILE_STDOUT );*/

	uint dsmode;
	if (CODING_USED) dsmode = cDStructConst::DSMODE_CODING;
	else  dsmode = cDStructConst::DSMODE_DEFAULT;

	sd = new cSpaceDescriptor(DIM, new tDomain());

	// create header
	cSignatureController sigController(LEVELS_WITH_SIGNATURES);
	SetSignatureController(&sigController, enabledLevel, sigCount, bitCount, SignatureBuildType);

	cRTreeHeader<tKey> header(sd, LeafDataSize, false, dsmode);
	header.SetCodeType(FIBONACCI2_FAST);
	header.SetSignatureEnabled(SignatureEnabled);
	header.SetOnlyMemoryProcessing(LOAD_DATAFILE_INTO_MEMORY);
	header.SetSignatureController(&sigController);
	header.HeaderSetup(BlockSize);

	if (SignatureEnabled)
	{
		sigController.Print();
	}

	CreateDataCollection();
	Build(&header);
	sigController.Print();

	// samostatna metoda
	if (CLOSEOPEN_DATAFILE)
	{
		quickDB = new cQuickDB();
		if (!quickDB->Open(dbPath, CacheSizeInNodes, MaxNodeInMemSize, BlockSize))
		{
			printf("Critical Error: Cache Data File was not open!\n");
			exit(1);
		}

		Tree = new cRTree<tKey>();
		if (!Tree->Open(&header, quickDB, true))
		{
			printf("Critical Error: Creation of index file failed!\n");
			exit(1);
		}
		
		if (LOAD_DATAFILE_INTO_MEMORY)
		{
			Tree->Preload();
			quickDB->GetNodeCache()->GetCacheStatistics()->Reset();
		}
	}

	RunPointQuery(&header);

	// samostatna metoda
	// close tree and print complete time
	quickDB->GetNodeCache()->GetCacheStatistics()->Print();
 	Tree->Close();
	//Tree->PrintInfo();

	// delete all
	delete[] dataCollection;
	quickDB->Close();
	delete Tree;
	delete quickDB;
	// samostatna metoda

	delete sd;

	return 0;
}

void Build(cRTreeHeader<tKey>* header)
{
	cRangeQueryConfig config;
	config.SetBulkReadEnabled(BULK_READ);
	config.SetLeafIndicesCapacity(NODEINDEX_CAPACITY_BULKREAD);
	config.SetMaxReadNodes(MAX_READ_NODES_BULKREAD);

	config.SetNodeIndexCapacity_BulkRead(NODEINDEX_CAPACITY_BULKREAD);
	config.SetMaxIndexDiff_BulkRead(MAX_INDEXDIFF_BULKREAD);

	// create shared cache and pool
	quickDB = new cQuickDB();
	if (!quickDB->Create(dbPath, CacheSizeInNodes, MaxNodeInMemSize, BlockSize))
	{
		printf("Critical Error: Cache Data File was not open!\n");
		exit(1);
	}

	// create tree
	Tree = new cRTree<tKey>();

	if (!Tree->Create(header, quickDB))
	{
		printf("Critical Error: Creation of index file failed!\n");
		exit(1);
	}
	printf("\n");

	cTimer tmrInsert;
	tmrInsert.Start();

	int cnt = 0, itemCount = 0;
	char data[LeafDataSize];

	// insert tuples
	for (unsigned int i = 0 ; i < COUNT ; i++)
	{
		if (i % 10000 == 0)
		{
			printf("Number of inserted tuples: %d   \r", cnt);
		}

		int insertFlag = Tree->Insert(dataCollection[i], data); /*Tree->Insert_MP(dataCollection[i], data);*/

		if (insertFlag == cRTreeConst::INSERT_YES)
		{
			itemCount++;
			if (Tree->GetHeader()->GetLeafItemCount() != itemCount)
			{
				printf("Cirtical Error: GetHeader()->GetLeafItemCount() != itemCount: %d, %d\n", Tree->GetHeader()->GetLeafItemCount(), itemCount);
			}

			bool findFlag = false;
			if (findFlag)
			{
				// for (int j = 0 ; j <= i ; j++)
				{
					bool findf = Tree->Find(dataCollection[i], data, &config);
					if (!findf)
					{
						dataCollection[i].Print("\n", sd);
						// tree.Print(cObject::MODE_DEC);
						printf("\nCritical Error: Tuple was inserted but wasn't finded: %i,%j!\n", i, i);
						exit(0);
					}
				}
			}
			cnt++;
		}
		else if (insertFlag == cRTreeConst::INSERT_DUPLICATE)
		{
			printf("\nWarning: Duplicate tuple wasn't inserted!\n");
			dataCollection[i].Print("\n", sd);
		}
		else
		{
			printf("Error: Tuple wasn't inserted!\n");
		}
	}

	printf("\n");

	// print timers and counters
	tmrInsert.Stop();
	tmrInsert.Print(" - Insert time\n");
	printf("Performance: %.1f Inserts/s\n\n", Tree->GetHeader()->GetLeafItemCount() / tmrInsert.GetTime());

	if (SignatureEnabled)
	{
		if (SignatureBuildType == cSignatureController::SignatureBuild_Bulkload)
		{
			Tree->CreateSignatureIndex(quickDB);
		}
	
		Tree->ComputeSignatureWeights();
	}

	Tree->PrintInfo();

	if (CLOSEOPEN_DATAFILE)
	{
		Tree->Close();
		quickDB->Close();
		delete Tree;
		delete quickDB;
	}
}

cHNTuple * CreateComplexTuple(cSpaceDescriptor* hnSD, cSpaceDescriptor* lnSD)
{
	cHNTuple *tuple = new cHNTuple();

	for (unsigned int j = 0; j < DIM; j++)
	{
		hnSD->SetInnerSpaceDescriptor(j, lnSD);
		hnSD->SetType(j, new cLNTuple());
	}

	hnSD->ComputeIndexes();
	tuple->Resize(hnSD);

	return tuple;
}

void RunPointQuery(cRTreeHeader<tKey>* header)
{
	std::thread **threads = new std::thread*[NOF_THREADS_M1];
	unsigned count = COUNT / NOF_THREADS;
	unsigned int loQueryOrder = 0;
	unsigned int hiQueryOrder = count;
	cTimer tmrQuery;
	tmrQuery.Start();

	cout << "Point Querying ";

	for (unsigned int i = 0 ; i < NOF_THREADS_M1 ; i++)
	{
		threads[i] = new std::thread(PointQueryData, header, loQueryOrder, hiQueryOrder);
		loQueryOrder += count;
		hiQueryOrder += count;

	}
	PointQueryData(header, loQueryOrder, COUNT);

	for (unsigned int i = 0 ; i < NOF_THREADS-1 ; i++)
	{
		threads[i]->join();  // wait for all threads created
	}

	for (unsigned int i = 0 ; i < NOF_THREADS-1 ; i++)
	{
		delete threads[i];
	}
	delete []threads;

	// print timers and counters
	tmrQuery.Stop();
	tmrQuery.Print(" - Query time\n");
	printf("Performance: %.1f Queries/s\n\n", Tree->GetHeader()->GetLeafItemCount() / tmrQuery.GetTime());
}

void PointQueryData(cRTreeHeader<tKey>* header, unsigned int loQueryOrder, unsigned int hiQueryOrder)
{
	cRangeQueryConfig config;
	config.SetFinalResultSize(1);
	config.SetQueryProcessingType(QUERYPROCESS);
	config.SetBulkReadEnabled(BULK_READ);
	config.SetLeafIndicesCapacity(NODEINDEX_CAPACITY_BULKREAD);
	config.SetMaxReadNodes(MAX_READ_NODES_BULKREAD);

	config.SetNodeIndexCapacity_BulkRead(NODEINDEX_CAPACITY_BULKREAD);
	config.SetMaxIndexDiff_BulkRead(MAX_INDEXDIFF_BULKREAD);

	unsigned int count = COUNT / QUERIES_IN_MULTIQUERY;
	char data[LeafDataSize];
	cQueryStat queryStat;

	if (QUERYPROCESS == QueryType::SINGLEQUERY)
	{
		for (unsigned int i = loQueryOrder ; i < hiQueryOrder ; i++)
		{
			// printf("i: %d    \r", i);
			if (i % 25000 == 0)
			{
				// printf("#Thread: %d, #Query: %d, #IN: %.2f, #LN: %.2f      \r", std::thread::id(), i, queryStat.GetLarInAvg(), queryStat.GetLarLnAvg());
				 // cout << "#Thread: " << std::this_thread::get_id() << ", #Query: " << i << ", #IN: " << queryStat.GetLarInAvg() << 
//					", #LN: " << queryStat.GetLarLnAvg() << "\r";
				cout << ".";
			}

			if (!Tree->Find(dataCollection[i], data, &config, &queryStat))
			{
				dataCollection[i].Print("\n", sd);
				printf("\nCritical Error: Tuple was not found: %i!\n", i);
			}
		}
	}
	
	if (QUERYPROCESS == QueryType::BATCHQUERY)
	{
		for (unsigned int i = 0; i < COUNT; i += QUERIES_IN_MULTIQUERY)
		{
			if (i % (1000 * QUERIES_IN_MULTIQUERY) == 0)
			{
				printf("#Query: %d, #IN: %.2f, #LN: %.2f      \r", i, queryStat.GetLarInAvg(), queryStat.GetLarLnAvg());
			}

			unsigned int queriesCount = ((COUNT - i >= QUERIES_IN_MULTIQUERY) ? QUERIES_IN_MULTIQUERY : COUNT - i);
			if (!Tree->FindBatchQuery(&dataCollection[i], data, &config, queriesCount, &queryStat))
			{
				dataCollection[i].Print("\n", sd);
				printf("\nCritical Error: Tuple was not found: %i!\n", i);
			}
		}
	}

	if (QUERYPROCESS == QueryType::CARTESIANQUERY) 
	{
		cSpaceDescriptor* hnSD = new cSpaceDescriptor(DIM);
		cSpaceDescriptor* lnSD = new cSpaceDescriptor(QUERIES_IN_MULTIQUERY, new tDomain());

		cLNTuple *lnTuple = new cLNTuple(lnSD, 0);
		cHNTuple *queryTuple = CreateComplexTuple(hnSD, lnSD);

		for (unsigned int i = 0; i < COUNT; i += QUERIES_IN_MULTIQUERY)
		{
			if (i % (1000 * QUERIES_IN_MULTIQUERY) == 0)
			{
				printf("#Query: %d, #IN: %.2f, #LN: %.2f      \r", i, queryStat.GetLarInAvg(), queryStat.GetLarLnAvg());
			}

			unsigned int queriesCount = ((COUNT - i >= QUERIES_IN_MULTIQUERY) ? QUERIES_IN_MULTIQUERY : COUNT - i);
			for (unsigned int j = 0; j < DIM; j++)
			{
				lnTuple->SetLength(0);
				for (unsigned int k = i; k < i + queriesCount; k++)
				{
					if ((k == 0) || (dataCollection[k].GetUInt(j, sd) != lnTuple->GetLastUInt(lnSD)))
					{
						 lnTuple->AddValue(dataCollection[k].GetUInt(j, sd), lnSD);
					}
				}
				queryTuple->SetValue(j, *lnTuple, hnSD);
			}

			// unsigned int aln = Tree->RQS_AccessedLeafNodes; // obsolote: rewrite it
			unsigned int c1 = cMBRectangle<tKey>::II_Compares;

			if (!Tree->FindCartesianQuery(queryTuple, hnSD, data, &config, queriesCount, &queryStat))
			{
				dataCollection[i].Print("\n", sd);
				printf("\nCritical Error: Tuple was not found: %i!\n", i);
			}
		}
	}

	if (NOF_THREADS == 1)
	{
		printf("\n");
		cout << "#Thread: " << std::this_thread::get_id();
		queryStat.Print();
		printf("\n");
	}
}

void CreateDataCollection()
{
	// generate tuples
	cGaussRandomGenerator rg(false);
	// cUniformRandomGenerator rg(false);
	// cExponentialRandomGenerator rg(false);

	dataCollection = new tKey[COUNT];

	for (unsigned int i = 0 ; i < COUNT ; i++)
	{
		dataCollection[i].Resize(sd);
		unsigned int dim = sd->GetDimension();
		for (unsigned int j = 0 ; j < dim ; j++)
		{
			unsigned int value = rg.GetNextUInt(MAX_VALUE);
			if (CODING_USED) 
			{
				value++;
			}
			dataCollection[i].SetValue(j, value, sd);
		}
	}
}

void SetSignatureController(cSignatureController *controller, bool* sigEnabledLevel, uint *sigCount, uint *bitCount, uint pSignatureBuildType)
{
	controller->SetLevelEnabled(enabledLevel);
	controller->SetSignatureCounts(sigCount);
	controller->SetBitCounts(bitCount);
	controller->SetBuildType(pSignatureBuildType);
}
