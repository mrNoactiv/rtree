#include "dstruct/paged/rtree/cRTree.h"
#include "common/data/cDataCollection.h"
#include "common/data/cTuplesGenerator.h"
#include "common/utils/cSSEUtils.h"
#include "dstruct/paged/core/cBulkLoad.h"
#include "dstruct/paged/core/cBulkLoadArray.h"
#include "cTable.h"



#ifdef CUDA_ENABLED
#include "dstruct/paged/cuda/cCudaTimer.h"
#endif
using namespace dstruct::paged;
using namespace dstruct::paged::rtree;
using namespace common::data;

uint ITEM_COUNT = 1000000;
uint DOMAIN_MAX = 200000;    //  Maximum value for random generator in cTuplesGenerator.

uint CACHE_SIZE = 20000;
uint BLOCK_SIZE = 2048 /*8192*/;
uint MAX_NODE_INMEM_SIZE = 1.25 * BLOCK_SIZE;

// defines the dimension of the inserting tuples
const uint DIMENSIONS[] = { 4 };
const uint DIM = 4;

// defines the length of the inserting data
const uint DATA_LENGTHS[] = { 4 };

// defines the type of data structure mode
const uint DSMODES[] = {  cDStructConst::DSMODE_DEFAULT,
						  cDStructConst::DSMODE_CODING,
						  cDStructConst::DSMODE_RI, 
						  cDStructConst::DSMODE_RICODING,
						  cDStructConst::DSMODE_SIGNATURES,
						  cDStructConst::DSMODE_ORDERED
};

// defines the type of sort order in bulkloading
uint SORTTYPE = cSortType::zOrder;
// Types of SORTS: 0 Lexicographical, 1 hOrder, 2 zOrder, 3 TaxiOrder 

// defines the type of coding in the case of CODING and RICODING
uint CODETYPE = FIXED_LENGTH_CODING;
// ELIAS_DELTA 1		FIBONACCI2 2		FIBONACCI3  3		ELIAS_FIBONACCI 4
// ELIAS_DELTA_FAST 5	FIBONACCI2_FAST 6	FIBONACCI3_FAST 7	ELIAS_FIBONACCI_FAST 8
// FIXED_LENGTH_CODING 9	FIXED_LENGTH_CODING_ALIGNED 10

// defines the usage of signatures - settings for all signature types
bool SIGNATURES = false;
uint SignatureType = cSignatureController::DimensionIndependent; // defines the type of building signatures	 
uint SignatureBuildType = cSignatureController::SignatureBuild_Insert; // defines the type of building signatures	
uint SignatureQuality = cSignatureController::PerfectSignature;  // defines if the signature are rebuilt or replicated in the case of node split     
const uint LEVELS_WITH_SIGNATURES = 3; // defines the number of lowest tree levels to be indexed by signatures
bool enabledLevel[LEVELS_WITH_SIGNATURES] = { true, true, true };
uint bitCount = 1;
float bLengthConstant = 1.0f;

// defines the usage of signatures - settings for DimensionDependent signatures
const uint QUERY_TYPES = 3;  // number of different query types 
uint CURRENT_QUERY_TYPE = 1; // current query type -> neccessary to define before query processing
bool queryTypes[QUERY_TYPES][DIM] = { { 1, 1, 1, 1 }, { 1, 0, 1, 0 }, { 0, 0, 0, 1 } };
uint domains[DIM] = { DOMAIN_MAX, DOMAIN_MAX, DOMAIN_MAX, DOMAIN_MAX };

// Ordered Rtree
bool ORDERED_RTREE = false;

// Histograms for each dimension will be created during build (works only on conventional R-tree version)
bool HISTOGRAMS = false;

// defines maximal compression ratio in RI, CODING and RICODING mode
uint COMPRESSION_RATIO = 2;

// defines the query processing
uint QUERYPROCESS = QueryType::SINGLEQUERY; /*SINGLEQUERY*/ /*BATCHQUERY*/ /**/ //CARTESIANQUERY; // defines the type of query processing
uint QUERIES_IN_MULTIQUERY = 4; // number of queries in multiquery

bool BULK_READ = false;  // if true, buffered read is enabled
uint NODEINDEX_CAPACITY_BULKREAD = 256; // defines how many node indices can be stored in the leafIndices array
uint MAX_READ_NODES_BULKREAD = 32; // defines how many leaf nodes can be read into the buffer
uint MAX_INDEXDIFF_BULKREAD = 32;

// defines the manipulation with index
bool CLOSEOPEN_DATAFILE = false; // define, if tree has to be stored after all items are inserted and open before querying
bool LOAD_DATAFILE_INTO_MEMORY = false; // define, if tree will be in memory or also 
bool LOAD_DATAFILE_INSTEAD_BUILD = false;//if true, tree is loaded from data file instead of building it.
uint RUNTIME_MODE = cDStructConst::RTMODE_DEFAULT; // defines the mode (with or without validation) RTMODE_DEFAULT		RTMODE_VALIDATION

typedef cUInt tDomain; // defines domain used for loading tuples from collection file.
typedef cTuple tKey;   // defines the type of tree key
typedef cRTree<tKey> RTree; // defines tree structure
typedef cMBRectangle<tKey>  innerItem;// defines innerItem for Bulkloading
typedef cBulkLoad<RTree, cRTreeNode<innerItem>, cRTreeLeafNode<tKey>, tKey, cRTreeHeader<tKey>, tDomain> tBulkload;

/// specification of collections ///
int COLLECTION = cCollection::RANDOM;
int COMPUTER = Computer::DBEDU;
int QUERY_TYPE = cQueryType::INDEX_VALIDATION;
int SEARCH_MODE = cRangeQueryConfig::SEARCH_DFS;
int DEVICE = cRangeQueryConfig::DEVICE_CPU;
bool SAVE_RESULT = true; //if set the range query saves time and result size in a CSV file.


cTuplesGenerator<tDomain, tKey> *generator; //generates random tuples or load them from a file.
char *collectionFile, *queryFile;

/// quickdb name and path ///
char dbPath[1024] = "quickdb";
char resultFileName[1024] = "results.csv";

// char dbPath[1024] = "D:\\Tests\\quickDB";

/// global variables ///
char *mData, *resultData;
cQuickDB *mQuickDB;
cRTree<tKey> *mTree;

uint DSMODE, DIMENSION, DATA_LENGTH;

/// list of methods ///
void printHeader();
void SimpleTest();
void Bulkloading();
void InitializeTuplesGenerator(bool computeOrderValues = false);
void Build(cRTreeHeader<tKey>* header);
void Destroy(cRTreeHeader<tKey>* header);
void IndexReOpen(cRTreeHeader<tKey>* header);
void PointQuery(cRTreeHeader<tKey>* header);
void RangeQuery(cRTreeHeader<tKey>* header);
void SetTreeHeader(cRTreeHeader<tKey> *header, cSignatureController* sigController);
void SetRangeQueryConfig(cRangeQueryConfig* config, uint queryProcess);
void SetSignatureController(cSignatureController *controller);
void ParseCommandLineArguments(int argc, char** argv);
void PrintHelp();
void PrintConfiguration();
void SaveResultToCsvFile(uint queryOrder, uint resultSize, float realTime, cQueryProcStat &queryStat);
uint DSMODES_COUNT() { return sizeof(DSMODES) / sizeof(uint); };
uint DIMENSIONS_COUNT() { return sizeof(DIMENSIONS) / sizeof(uint); };
uint DATA_LENGTHS_COUNT() { return sizeof(DATA_LENGTHS) / sizeof(uint); };

int main(int argc, char *argv[])
{
	/*_CrtSetReportMode(_CRT_WARN, _CRTDBG_MODE_FILE);
	_CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDOUT);
	_CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_FILE);
	_CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDOUT);
	_CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);
	_CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDOUT);*/

	/**/

	/*my code*/
	/*
	mQuickDB = new cQuickDB();
	if (!mQuickDB->Create(dbPath, CACHE_SIZE, MAX_NODE_INMEM_SIZE, BLOCK_SIZE))
	{
		printf("Critical Error: Cache Data File was not open!\n");
		exit(1);
	}

	string query = "CREATE TABLE ahoj(ID INT PRIMARY KEY,AGE INT NOT NULL";
	cTable *table1 = new cTable();


	table1->CreateTable(query, mQuickDB, BLOCK_SIZE, DSMODE, COMPRESSION_RATIO, CODETYPE, RUNTIME_MODE, HISTOGRAMS, 0);



	*/
	/*zdroj*/
	ParseCommandLineArguments(argc, argv);
	if (COLLECTION == cCollection::RANDOM && collectionFile == NULL && queryFile == NULL)
	{
		for (uint i = 0; i < DIMENSIONS_COUNT(); i++)
		{
			DIMENSION = DIMENSIONS[i];
			for (uint j = 0; j < DATA_LENGTHS_COUNT(); j++)
			{
				DATA_LENGTH = DATA_LENGTHS[j];
				for (uint k = 0; k < DSMODES_COUNT(); k++)
				{
					DSMODE = DSMODES[k];
					SimpleTest();
				}
			}
		}
	}
	else //load data collection from file
	{
		DATA_LENGTH = DATA_LENGTHS[0];
		DSMODE = cDStructConst::DSMODE_DEFAULT;

		if (collectionFile == NULL)
			collectionFile = cDataCollection::COLLECTION_FILE(COLLECTION, COMPUTER);
		if (queryFile == NULL)
			queryFile = cDataCollection::QUERY_FILE(COLLECTION, COMPUTER);
		SimpleTest();
		delete[] collectionFile;
		delete[] queryFile;
	}

	//// Bulkloading
	DIMENSION = DIMENSIONS[0];
	DATA_LENGTH = DATA_LENGTHS[0];
	DSMODE = DSMODES[0];
	Bulkloading();

#ifdef CUDA_ENABLED
	cudaDeviceReset();
#endif

	return 0;
}

void ParseCommandLineArguments(int argc, char** argv)
{
	char *switcher = 0;
	bool switcherSet = false;
	char collectionName[1024]="";
	for (int i = 1; i < argc; i++)
	{
		if ((strcmp(argv[i], "-h") == 0))
		{
			PrintHelp();
		}
		if ((strcmp(argv[i], "-c") == 0) ||				// collection file path
			(strcmp(argv[i], "-q") == 0) ||				// queries file path
			(strcmp(argv[i], "-r") == 0) ||				// results file path (stores results into CSV)
			(strcmp(argv[i], "-d") == 0) ||				// device (0-CPU, 1-GPU, 2-PHI)
			(strcmp(argv[i], "-l") == 0) ||				// load index instead of build (default false)
			(strcmp(argv[i], "-m") == 0) ||				// tree traversal mode (0-DFS, 1-BFS, 2-DBFS)
			(strcmp(argv[i], "-bs") == 0) ||			// default block size
			(strcmp(argv[i], "-db") == 0) ||			// database file path
			(strcmp(argv[i], "-qt") == 0))				// query type (0-single query, 1-batch query)
		{
			switcher = argv[i];
			switcherSet = true;
			continue;
		}

		if (switcherSet)
		{
		
			if(strcmp(switcher, "-c") == 0)
			{
				collectionFile = new char[1024];
				strcpy(collectionFile, argv[i]);
			}
			else if (strcmp(switcher, "-q") == 0)
			{
				queryFile = new char[1024];
				strcpy(queryFile, argv[i]);
			}
			else if (strcmp(switcher, "-db") == 0)
			{
				strcpy(dbPath, argv[i]);
			}
			//create index or load existing
			else if (strcmp(switcher, "-l") == 0)
			{
				LOAD_DATAFILE_INSTEAD_BUILD = atoi(argv[i]);
			}
			else if (strcmp(switcher, "-bs") == 0)
			{
				BLOCK_SIZE = atoi(argv[i]);
				MAX_NODE_INMEM_SIZE = 1.25 * BLOCK_SIZE;
				printf("BlockSize = %d\n",BLOCK_SIZE);
			}
			//result file name
			else if (strcmp(switcher, "-r") == 0)
			{
				strcpy(resultFileName, argv[i]);
			}
			else if (strcmp(switcher, "-d") == 0)
			{
				DEVICE = atoi(argv[i]);
			}
			else if (strcmp(switcher, "-m") == 0)
			{
				SEARCH_MODE = atoi(argv[i]);
			}
			else if (strcmp(switcher, "-qt") == 0)
			{
				QUERYPROCESS = atoi(argv[i]);
			}
			switcherSet = false;
		}
	}
}
void PrintHelp()
{
	printf("\nR-Tree supported command line arguments:\n");
	printf("  -c <path> \t\t Path to collection file.\n");
	printf("  -q <path> \t\t Path to query file.\n");
	printf("  -r <path> \t\t Path to resutl file.\n");
	printf("  -l <0|1> \t\t  Load the index instead of building it. (default false).\n");
	printf("  -d <0|1|2> \t\t Sets the device for query processing (0-CPU, 1-GPU, 2-PHI).\n");
	printf("  -m <0|1|2> \t\t Sets tree traversal mode (0-DFS, 1-BFS, 2-DBFS).\n");
	printf("  -db <path> \t\t Path to DB file (Use without extension '.dat').\n");
	printf("  -bs <value> \t Sets the block size.\n");
	printf("  -qt <value> \t Sets the query processing type (0-single query, 1-batch query).\n");
	printf("\n");
	exit(EXIT_SUCCESS);
}

void PrintConfiguration()
{
	printf("\n--------TREE CONFIGURATION------------\n");
	printf("Block size: %d\n", BLOCK_SIZE);
	printf("Default device: ");
	switch (DEVICE)
	{
	default:
		DEVICE = cRangeQueryConfig::DEVICE_CPU;
	case cRangeQueryConfig::DEVICE_CPU:
		printf("CPU\n"); break;
	case cRangeQueryConfig::DEVICE_GPU:
		printf("GPU\n"); break;
	case cRangeQueryConfig::DEVICE_PHI:
		printf("PHI\n"); break;
	}
	printf("Default tree traversal: ", SEARCH_MODE);
	switch (SEARCH_MODE)
	{
	default:
		SEARCH_MODE = cRangeQueryConfig::SEARCH_DFS;
	case cRangeQueryConfig::SEARCH_DFS:
		printf("DFS\n"); break;
	case cRangeQueryConfig::SEARCH_BFS:
		printf("BFS\n"); break;
	case cRangeQueryConfig::SEARCH_DBFS:
		printf("DBFS\n"); break;
	}
	if (COLLECTION == cCollection::RANDOM)
	{
		printf("Collection: RANDOM\n");
	}
	else
	{
		printf("Collection file: %s\n", cDataCollection::COLLECTION_FILE(COLLECTION, COMPUTER));		
		printf("Query file: %s\n", cDataCollection::QUERY_FILE(COLLECTION, COMPUTER));
	}
}
void SaveResultToCsvFile(uint queryOrder, uint resultSize, float realTime, cQueryProcStat &queryStat)
{
	FILE *fp = fopen(resultFileName, "a");
	fseek(fp, -45, SEEK_END);
	fprintf(fp, "\n%02d.| RS: ;%d; T[s]: ;%g;", queryOrder, resultSize, realTime);
	fprintf(fp, "Accessed Inners: ;%d;Accessed Leafs: ;%d%;Search calls Inners: ;%d;Search calls Leafs: ;%d;", queryStat.GetLarInQuery(), queryStat.GetLarLnQuery(), queryStat.GetSiInQuery(), queryStat.GetSiLnQuery());
	fclose(fp);

}
void SimpleTest()
{
	if (DSMODE == cDStructConst::DSMODE_SIGNATURES)
	{
		DSMODE = cDStructConst::DSMODE_DEFAULT;
		SIGNATURES = true;
		ORDERED_RTREE = false;
	}
	else if (DSMODE == cDStructConst::DSMODE_ORDERED)
	{
		DSMODE = cDStructConst::DSMODE_DEFAULT;
		SIGNATURES = false;
		ORDERED_RTREE = true;
	}
	else
	{
		SIGNATURES = false;
		ORDERED_RTREE = false;
	}

	InitializeTuplesGenerator();
	printHeader();
	cSignatureController sigController(LEVELS_WITH_SIGNATURES, QUERY_TYPES, DIM);
	SetSignatureController(&sigController);

	cRTreeHeader<tKey> *treeHeader = new cRTreeHeader<tKey>(generator->GetSpaceDescriptor(), DATA_LENGTH, false, DSMODE, cDStructConst::RTREE, COMPRESSION_RATIO);
	treeHeader->SetRuntimeMode(RUNTIME_MODE);
	SetTreeHeader(treeHeader, &sigController);
	PrintConfiguration();
	if (LOAD_DATAFILE_INSTEAD_BUILD) //load data file instead of building it
	{
		CLOSEOPEN_DATAFILE = true;
	}
	else
	{
		Build(treeHeader);
	}
	IndexReOpen(treeHeader);
	
#ifdef CUDA_ENABLED
	mTree->InitGpu(BLOCK_SIZE, generator->GetSpaceDescriptor()->GetDimension(), NODEINDEX_CAPACITY_BULKREAD, treeHeader->GetLeafNodeItemCapacity());
#endif
	if (QUERY_TYPE == cQueryType::INDEX_VALIDATION)
	{
		PointQuery(treeHeader); //test if all tuples were inserted
	}
	else
	{
		if (queryFile != NULL) //file mode
		{
			RangeQuery(treeHeader); //test range queries for the first time
		}
		else
		{
			throw  "Critical Error! rtree::SimpleTest(). Range query test is selected but no range query file is specified!";
		}
	}

	//_CrtCheckMemory();

	Destroy(treeHeader);
	printf("--------------------------------------------------------------------------------\n");
#ifdef CUDA_ENABLED
	cudaDeviceReset();
#endif
}

void InitializeTuplesGenerator(bool computeOrderValues)
{
	mData = new char[DATA_LENGTH];
	for (uint j = 0; j < DATA_LENGTH; j++)
	{
		mData[j] = (char) j;
	}
	resultData = new char[DATA_LENGTH];


	if (COLLECTION == cCollection::RANDOM)
	{
		generator = new cTuplesGenerator<tDomain,tKey>(ITEM_COUNT, DIMENSION, DOMAIN_MAX, false, computeOrderValues);
	}
	else
	{
		generator = new cTuplesGenerator<tDomain, tKey>(collectionFile, false, computeOrderValues);
	}

}

void Build(cRTreeHeader<tKey>* header)
{
	cRangeQueryConfig config;
	SetRangeQueryConfig(&config, QUERYPROCESS);

	// Shared cache and pool
	mQuickDB = new cQuickDB();
	if (!mQuickDB->Create(dbPath, CACHE_SIZE, MAX_NODE_INMEM_SIZE, BLOCK_SIZE))
	{
		printf("Critical Error: Cache Data File was not open!\n");
		exit(1);
	}

	// R-tree
	mTree = new cRTree<tKey>();
	if (!mTree->Create(header, mQuickDB))
	{
		printf("Critical Error: Creation of index file failed!\n");
		exit(1);
	}

	cTimer tmrInsert;
	tmrInsert.Start();

	// insert tuples
	for (uint i = 0; i < generator->GetTuplesCount(); i++)
	{
		if (i % 25000 == 0)
		{
			printf("Inserts: %d\r", i);
			fflush(stdout);
		}
		tKey* mTuple = generator->GetNextTuple();
		int insertFlag = mTree->Insert(*mTuple, mData);

		if (insertFlag == cRTreeConst::INSERT_YES)
		{
			bool findFlag = false;
			if (findFlag)
			{

				if (!mTree->Find(*mTuple, resultData, &config))
				{
					mTuple->Print("\n", generator->GetSpaceDescriptor());
					printf("\nCritical Error: Tuple was inserted but wasn't found: %i,%j!\n", i, i);
				}
				else
				{
					for (unsigned int j = 0; j < DATA_LENGTH; j++)
					{
						if (mData[j] != resultData[j])
						{
							printf("Critical Error: Data are not correct: mTuple order: %u,%u!", i, j);
						}
					}
				}

			}
		}
		else if (insertFlag == cRTreeConst::INSERT_DUPLICATE)
		{
			printf("\nWarning: Duplicate tuple wasn't inserted!\n");
			mTuple->Print("\n", generator->GetSpaceDescriptor());
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
	printf("Performance: %.1f Inserts/s\n", mTree->GetHeader()->GetLeafItemCount() / tmrInsert.GetRealTime());

	printf("#Inner/Leaf Nodes: %d/%d ", mTree->GetHeader()->GetInnerNodeCount(), mTree->GetHeader()->GetLeafNodeCount());
	mTree->PrintIndexSize(BLOCK_SIZE);
	//mTree->PrintInfo();

	if (SIGNATURES)
	{
		if (SignatureBuildType == cSignatureController::SignatureBuild_Bulkload)
		{
			mTree->CreateSignatureIndex(mQuickDB);
		}
	
		mTree->PrintSignatureInfo(false);

	}
	printf("\n");

	if (HISTOGRAMS)
	{
		mTree->PrintDimDistribution();	
	}

	if (CLOSEOPEN_DATAFILE)
	{
		mTree->Close();
		mQuickDB->Close();
		delete mTree;
		delete mQuickDB;
	}
}

void IndexReOpen(cRTreeHeader<tKey>* header)
{
	if (CLOSEOPEN_DATAFILE)
	{
		mQuickDB = new cQuickDB();
		if (!mQuickDB->Open(dbPath, CACHE_SIZE, MAX_NODE_INMEM_SIZE, BLOCK_SIZE))
		{
			printf("Critical Error: Cache Data File was not open!\n");
			exit(1);
		}

		mTree = new cRTree<tKey>();
		if (!mTree->Open(header, mQuickDB, true))
		{
			printf("Critical Error: Creation of index file failed!\n");
			exit(1);
		}

		if (LOAD_DATAFILE_INTO_MEMORY)
		{
			mTree->Preload();
			mQuickDB->GetNodeCache()->GetCacheStatistics()->Reset();
		}
	}
}


cHNTuple * CreateComplexTuple(cSpaceDescriptor* hnSD, cSpaceDescriptor* lnSD)
{
	cHNTuple *tuple = new cHNTuple();

	for (unsigned int j = 0; j < DIMENSION; j++)//každá dimenze nastavena na cLNTuple,nemá se nastavit jen ta dimenze, kde bude cLNTuple-cNTuple?
	{
		hnSD->SetDimSpaceDescriptor(j, lnSD);
		hnSD->SetDimensionType(j, new cLNTuple());
	}

	hnSD->Setup();
	tuple->Resize(hnSD);

	return tuple;
}

void PointQuery(cRTreeHeader<tKey>* header)
{
	cRangeQueryConfig config;
	SetRangeQueryConfig(&config, QUERYPROCESS);
	cQueryProcStat queryStat;
	uint deletedItems = 0;

	cTimer tmrQuery;
	tmrQuery.Start();

	generator->ResetPosition();
	if (QUERYPROCESS == QueryType::SINGLEQUERY)
	{
		for (uint i = 0; i < generator->GetTuplesCount(); i++)
		{
			if (i % 25000 == 0)
			{
				printf("Queries: %d\r", i);
				fflush(stdout);
			}

			tKey* tuple = generator->GetNextTuple();

			//tuple.Print("\n", generator->GetSpaceDescriptor());

			if (!mTree->Find(*tuple, resultData, &config, &queryStat))
			{
				tuple->Print("\n", generator->GetSpaceDescriptor());
				printf("\nCritical Error: Tuple was not found: %i!\n", i);
			}
			else
			{
				for (uint j = 0; j < DATA_LENGTH; j++)
				{
					if (mData[j] != resultData[j])
					{
						printf("Critical Error: Data are not correct: item order: %u,%u!", i, j);
					}
				}
			}

			bool ret = false;
			if (i % 10000 == 0)
			{
				if ((DSMODE == cDStructConst::DSMODE_DEFAULT) && (!ORDERED_RTREE) && (!SIGNATURES))
				{
					mTree->Delete(*tuple);
					if (mTree->Find(*tuple, resultData, &config, &queryStat))
					{
						printf("Critical Error: Deleted tuple is found!\n");
					}
					deletedItems++;
				}
			}
		}
	}

	if (QUERYPROCESS == QueryType::BATCHQUERY)
	{
		for (uint i = 0; i < generator->GetTuplesCount(); i += QUERIES_IN_MULTIQUERY)
		{
			if (i % (1000 * QUERIES_IN_MULTIQUERY) == 0)
			{
				printf("#Query: %d, #IN: %.2f, #LN: %.2f      \r", i, queryStat.GetLarInAvg(), queryStat.GetLarLnAvg());
			}

			uint queriesCount = -1;
			tKey* tuples = new tKey[QUERIES_IN_MULTIQUERY];
			generator->GetNextTuples(tuples, QUERIES_IN_MULTIQUERY, queriesCount);
			if (!mTree->FindBatchQuery(tuples, mData, &config, queriesCount, &queryStat))
			{
				printf("\nCritical Error: Tuple was not found: %i!\n", i);
			}
		}
	}

	if (QUERYPROCESS == QueryType::CARTESIANQUERY) 
	{
		cSpaceDescriptor* hnSD = new cSpaceDescriptor(DIMENSION, new cHNTuple(), new cLNTuple());
		cSpaceDescriptor* lnSD = new cSpaceDescriptor(QUERIES_IN_MULTIQUERY, new cLNTuple(), new tDomain());

		cLNTuple *lnTuple = new cLNTuple(lnSD, 0);
		cHNTuple *queryTuple = CreateComplexTuple(hnSD, lnSD);

		for (uint i = 0; i < generator->GetTuplesCount(); i += QUERIES_IN_MULTIQUERY)
		{
			if (i % (1000 * QUERIES_IN_MULTIQUERY) == 0)
			{
				printf("#Query: %d, #IN: %.2f, #LN: %.2f      \r", i, queryStat.GetLarInAvg(), queryStat.GetLarLnAvg());
			}

			unsigned int queriesCount = ((generator->GetTuplesCount() - i >= QUERIES_IN_MULTIQUERY) ? QUERIES_IN_MULTIQUERY : generator->GetTuplesCount() - i);
			for (uint j = 0; j < DIMENSION; j++)
			{
				lnTuple->SetLength((uint)0);
				for (uint k = i; k < i + queriesCount; k++)
				{
					tKey* tuple = generator->GetTuple(k);
					if ((k == 0) || (tuple->GetUInt(j, generator->GetSpaceDescriptor()) != lnTuple->GetLastUInt(lnSD)))
					{
						lnTuple->AddValue(tuple->GetUInt(j, generator->GetSpaceDescriptor()), lnSD);
					}
				}
				queryTuple->SetValue(j, *lnTuple, hnSD);
			}

			if (!mTree->FindCartesianQuery(queryTuple, hnSD, mData, &config, queriesCount, &queryStat))
			{
				tKey *tuple = generator->GetTuple(i);
				tuple->Print("\n", generator->GetSpaceDescriptor());
				printf("\nCritical Error: Tuple was not found: %i!\n", i);
			}
		}
	}

	// print timers and counters
	tmrQuery.Stop();
	printf("\n");
	tmrQuery.Print(" - Query time\n");
	//mTree->PrintInfo();
	if (mTree->GetHeader()->GetLeafItemCount() != generator->GetTuplesCount() - deletedItems)
	{
		printf("\n%i!\n%i", mTree->GetHeader()->GetLeafItemCount(), generator->GetTuplesCount());
		printf("Statistics error: Number of leaf items does not fit !!!\n");
	}

	printf("Performance: %.1f Queries/s\n", generator->GetTuplesCount() / tmrQuery.GetRealTime());
	//	mQuickDB->GetNodeCache()->GetCacheStatistics()->Print();
	queryStat.PrintLAR();

	if (SIGNATURES)
	{
		queryStat.PrintSigLAR(LEVELS_WITH_SIGNATURES, enabledLevel);
	}
}

void RangeQuery(cRTreeHeader<tKey>* header)
{
	printf("\nLoading queries.\n");
	cTuplesGenerator<tDomain, tKey> *generatorQ = new cTuplesGenerator<tDomain, tKey>(queryFile, true);

	cTimer tmrOneQuery;
	cTimer tmrQuery;
	tmrQuery.Start();

	cRangeQueryConfig config;
	SetRangeQueryConfig(&config, QUERYPROCESS);
	config.SetFinalResultSize(0);
	cQueryProcStat queryStat;

	//INITIALIZATION
	tKey *qls;
	tKey *qhs;
	cTreeItemStream<tKey>* resultSet;
	if (QUERYPROCESS == QueryType::SINGLEQUERY)
	{
		QUERIES_IN_MULTIQUERY = 1;
	}
	else if (QUERYPROCESS == QueryType::BATCHQUERY)
	{
#ifdef CUDA_ENABLED
		//QUERIES_IN_MULTIQUERY = generatorQ->GetTuplesCount();
#endif
	}

	qls = new tKey[QUERIES_IN_MULTIQUERY];
	qhs = new tKey[QUERIES_IN_MULTIQUERY];

	//QUERY PROCESSING
	if (QUERYPROCESS == QueryType::SINGLEQUERY)
	{
		for (unsigned int i = 0; i < generatorQ->GetTuplesCount(); i++)
		{
			tKey* tuple = generatorQ->GetNextTuple();
			qls[0] = *tuple;
			tuple = generatorQ->GetNextTuple();
			qhs[0] = *tuple;

			tmrOneQuery.Start();
			resultSet = mTree->RangeQuery(qls[0], qhs[0], &config, NULL, &queryStat);
			tmrOneQuery.Stop();
			printf("\n%02d.| RS: ;%d; T[s]: ;%g;", i + 1, resultSet->GetItemCount(), tmrOneQuery.GetRealTime());
#ifdef CUDA_MEASURE
			printf("GPU query: ;%g; GPU malloc: ;%g; GPU H->D: ;%g; GPU D->H: ;%g;GPU copy results: ;%g;GPU copy searchArray: ;%g;", cCudaTimer::TimeQuery, cCudaTimer::TimeMalloc, cCudaTimer::TimeHtoD, cCudaTimer::TimeDtoH, cCudaTimer::TimeResultVector, cCudaTimer::TimeSearchArray);
#endif
			if (SAVE_RESULT)
			{
				SaveResultToCsvFile(i + 1, resultSet->GetItemCount(), tmrOneQuery.GetRealTime(), queryStat);
			}
			resultSet->CloseResultSet();
		}
	}
	if (QUERYPROCESS == QueryType::BATCHQUERY)
	{
		printf("\n");
		for (uint i = 0; i < generatorQ->GetTuplesCount(); i += QUERIES_IN_MULTIQUERY)
		{

			if (i % (1000 * QUERIES_IN_MULTIQUERY) == 0)
			{
				printf("#Query: %d, #IN: %.2f, #LN: %.2f      \r", i, queryStat.GetLarInAvg(), queryStat.GetLarLnAvg());
			}
			uint queriesCount = -1;

			//LOAD QUERIES FROM TUPLESGENERATOR
			uint returnedCount = 0;
			generatorQ->GetNextTuples(qls, qhs, QUERIES_IN_MULTIQUERY, returnedCount); //not working properly
			tmrOneQuery.Start();
			resultSet = mTree->BatchRangeQuery(qls, qhs, &config, QUERIES_IN_MULTIQUERY, NULL, &queryStat);
			tmrOneQuery.Stop();
			printf("\n%02d.| RS: ;%d; T[s]: ;%g;", (i / QUERIES_IN_MULTIQUERY) + 1, resultSet->GetItemCount(), tmrOneQuery.GetRealTime());
			//bed157: same as in single query. make a method instead
			if (SAVE_RESULT)
			{
				SaveResultToCsvFile(i + 1, resultSet->GetItemCount(), tmrOneQuery.GetRealTime(),queryStat);
			}
			resultSet->CloseResultSet();
		}
	}

	// print timers and counters
	tmrQuery.Stop();
	printf("\n");
	tmrQuery.Print(" - Query time\n");
	printf("Performance: %.1f Queries/s\n", generatorQ->GetTuplesCount() / tmrQuery.GetRealTime());
	//	mQuickDB->GetNodeCache()->GetCacheStatistics()->Print();
	queryStat.PrintLAR();

	if (SIGNATURES)
	{
		queryStat.PrintSigLAR(LEVELS_WITH_SIGNATURES, enabledLevel);
	}
	delete generatorQ;
	delete[] qls;
	delete[] qhs;
}
void Destroy(cRTreeHeader<tKey>* header)
{
	mTree->Close();
	mQuickDB->Close();
	if (mData != NULL)
	{
		delete mData;
		mData = NULL;
	}
	if (resultData != NULL)
	{
		delete resultData;
		resultData = NULL;
	}
	delete mTree;
	delete mQuickDB; //nelze volat pokud u bylo zavoláno close()
	delete header;
	delete generator; //should be last beacause it has pointer to mSD
}
// **********************************************************************
// ********************** Tree & Query Configs *************************
// **********************************************************************

void SetTreeHeader(cRTreeHeader<tKey> *header, cSignatureController* sigController)
{
	header->SetCodeType(CODETYPE);
	header->SetSignatureEnabled(SIGNATURES);
	header->SetOnlyMemoryProcessing(LOAD_DATAFILE_INTO_MEMORY);
	header->SetSignatureController(sigController);
	header->HeaderSetup(BLOCK_SIZE);
	header->SetOrderingEnabled(ORDERED_RTREE);
	header->SetHistogramEnabled(HISTOGRAMS);

	if (SIGNATURES)
	{
		//sigController->Print();
	}
}

void SetRangeQueryConfig(cRangeQueryConfig *config, uint queryProcess)
{
	config->SetFinalResultSize(1);
	config->SetQueryProcessingType(queryProcess);
	config->SetBulkReadEnabled(BULK_READ);
	config->SetLeafIndicesCapacity(NODEINDEX_CAPACITY_BULKREAD);
	config->SetMaxReadNodes(MAX_READ_NODES_BULKREAD);
	config->SetNodeIndexCapacity_BulkRead(NODEINDEX_CAPACITY_BULKREAD);
	config->SetMaxIndexDiff_BulkRead(MAX_INDEXDIFF_BULKREAD);
	config->SetSearchMethod(SEARCH_MODE);
	config->SetDevice(DEVICE);

	if (QUERY_TYPE == cQueryType::RANGE_QUERY && config->GetSearchStruct() == cRangeQueryConfig::SEARCH_STRUCT_ARRAY && config->GetQueryProcessingType() == QueryType::BATCHQUERY)
	{
		printf("\nWARNING: Cannot use array structure for batched range query. Using hashtable instead."); //V tomto modu se na vnitrnich uzlech budou nachazet duplicity. Pokud by v budoucnu byly duplicity v resultsetu zamerne, je treba upravit cRtreeLeafNode aby pri skenu pridavaly duplicitni tuply do resultSetu - aktualne duplicity neprida.
		config->SetSearchStruct(cRangeQueryConfig::SEARCH_STRUCT_HASHTABLE);
	}
	else
	{
		config->SetSearchStruct(cRangeQueryConfig::SEARCH_STRUCT_ARRAY);
	}

#ifdef CUDA_ENABLED
	config->SetSearchMethod(cRangeQueryConfig::SEARCH_BFS);
	if (!BULK_READ)
		printf("Info: bulk read was enabled for GPU search.\n");
	config->SetBulkReadEnabled(true);
#endif
}

void SetSignatureController(cSignatureController *controller)
{
	controller->SetLevelEnabled(enabledLevel);
	controller->SetBuildType(SignatureBuildType);
	controller->SetSignatureQuality(SignatureQuality);
	controller->SetSignatureType(SignatureType);
	controller->SetBitLengthConstant(bLengthConstant);

	for (uint i = 0; i < QUERY_TYPES; i++)
	{
		controller->SetQueryTypes(queryTypes[i], i);
	}

	for (uint i = 0; i < LEVELS_WITH_SIGNATURES; i++)
	{
		for (uint j = 0; j < DIM; j++)
		{
			controller->SetBitCount(bitCount, i, j);
		}
	}

	controller->SetCurrentQueryType(CURRENT_QUERY_TYPE);
	controller->SetDomains(domains);
}

// **********************************************************************
// ********************** Header Generator *************************
// **********************************************************************

void printHeader()
{
	if (!ORDERED_RTREE)
	{
		//printf("---------------------------------------------\n");
		printf("R-tree test (dimension/data length: %d/%d)\n", DIMENSION, DATA_LENGTH);
	}
	else
	{
		//printf("---------------------------------------------\n");
		printf("Ordered R-tree test (dimension/data length: %d/%d)\n", DIMENSION, DATA_LENGTH);
	}

	switch (DSMODE)
	{
		case cDStructConst::DSMODE_DEFAULT: printf("DSMODE: DEFAULT "); break;
		case cDStructConst::DSMODE_RI: printf("DSMODE: REFERENCE ITEMS "); break;
		case cDStructConst::DSMODE_CODING: printf("DSMODE: CODING "); break;
		case cDStructConst::DSMODE_RICODING: printf("DSMODE: REFERENCE ITEMS & CODING "); break;
	}


	if ((DSMODE == cDStructConst::DSMODE_CODING) || (DSMODE == cDStructConst::DSMODE_RICODING))
	{
		printf("// ");
		switch (CODETYPE)
		{
			case ELIAS_DELTA: printf("CODETYPE: ELIAS_DELTA "); break;
			case FIBONACCI2: printf("CODETYPE: FIBONACCI2 "); break;
			case FIBONACCI3: printf("CODETYPE: FIBONACCI3 "); break;
			case ELIAS_FIBONACCI: printf("CODETYPE: ELIAS_FIBONACCI "); break;
			case ELIAS_DELTA_FAST: printf("CODETYPE: ELIAS_DELTA_FAST "); break;
			case FIBONACCI2_FAST: printf("CODETYPE: FIBONACCI2_FAST "); break;
			case FIBONACCI3_FAST: printf("CODETYPE: FIBONACCI3_FAST "); break;
			case ELIAS_FIBONACCI_FAST: printf("CODETYPE: ELIAS_FIBONACCI_FAST "); break;
			case FIXED_LENGTH_CODING: printf("CODETYPE: FIXED_LENGTH_CODING "); break;
			case FIXED_LENGTH_CODING_ALIGNED: printf("CODETYPE: FIXED_LENGTH_CODING_ALIGNED "); break;
		}
	}

	if (SIGNATURES)
	{
		printf("// SIGNATURES: ON ");
		(SignatureType == cSignatureController::DimensionDependent) ? printf("// Dimension Depedent") : printf("// Dimension Indepedent");
		(SignatureQuality == cSignatureController::PerfectSignature) ? printf("// Perfect Sigs") : printf("// Imperfect Sigs");
	}
	else
	{
		printf("// SIGNATURES: OFF");
	}
	
	printf("\n");
}

// **********************************************************************
// ********************** Bulk Loading **********************************
// **********************************************************************

void Bulkloading()
{
	InitializeTuplesGenerator(true);

	printf("R-tree Bulkload test (dimension/data length: %d/%d)\n", DIMENSION, DATA_LENGTH);

	mQuickDB = new cQuickDB();
	if (!mQuickDB->Create(dbPath, CACHE_SIZE, MAX_NODE_INMEM_SIZE, BLOCK_SIZE))
	{
		printf("Critical Error: Cache Data File was not created!\n");
		exit(1);
	}

	cRTreeHeader<tKey> *treeHeader = new cRTreeHeader<tKey>(generator->GetSpaceDescriptor(), DATA_LENGTH, false, cDStructConst::DSMODE_DEFAULT, cDStructConst::RTREE, COMPRESSION_RATIO);
	treeHeader->SetRuntimeMode(RUNTIME_MODE);
	treeHeader->SetSignatureEnabled(SIGNATURES);
	treeHeader->HeaderSetup(BLOCK_SIZE);

	mTree = new RTree();
	if (!mTree->Create(treeHeader, mQuickDB))
	{
		printf("Critical Error: Creation of Index file failed!\n");
		exit(1);
	}

	tBulkload *bulkLoad = new tBulkload(treeHeader, mTree, 0.8, 1.0, ITEM_COUNT, SORTTYPE, DATA_LENGTH, generator->GetSpaceDescriptor());

	cTimer tmrCrt;
	tmrCrt.Start();
	printf("Creation started \n");

	for (uint i = 0; i < ITEM_COUNT; i++)
	{
		bulkLoad->Add(*generator->GetNextTuple(), mData);
	}

	bulkLoad->Sort();
	bulkLoad->CreateRTree();

	tmrCrt.Stop();
	printf("Creating done \n");
	printf("Inner/Leaf Nodes: %d/%d \n", mTree->GetHeader()->GetInnerNodeCount(), mTree->GetHeader()->GetLeafNodeCount());
	tmrCrt.Print(" - time of R-tree creation \n");
	printf("Index Size: %.2f MB\n", mTree->GetIndexSizeMB(BLOCK_SIZE));

	PointQuery(treeHeader);

	delete bulkLoad;
	Destroy(treeHeader);
}


