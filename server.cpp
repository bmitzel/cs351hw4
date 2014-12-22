/** This program illustrates the server end of the message queue **/
#include <fstream>
#include <list>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "msg.h"

using namespace std;

/* The number of cells in the hash table */
#define NUMBER_OF_HASH_CELLS 100

/* The number of inserter threads */
#define NUM_INSERTERS 5

/**
 * The record in the hash table
 */
struct record
{
	/* The record id */
	int id;
	
	/* The first name */	
	string firstName;
		
	/* The first name */
	string lastName;	
}; /* struct record */

/**
 * The structure of a hash table cell
 */
class hashTableCell
{
public:
	/**
 	 * Default constructor initializes the cell mutex
 	 */
	hashTableCell()
	{
		/* Initialize the mutex using pthread_mutex_init() */
	    if (0 != pthread_mutex_init(&cellMutex, NULL))
	    {
	        perror("pthread_mutex_init");
	        exit(-1);
	    }
	} /* Default constructor */
	
	/**
 	 * Destructor deallocates the cell mutex
 	 */
	~hashTableCell()
	{
		/* Deallocate the mutex using pthread_mutex_destroy() */
	    if (0 != pthread_mutex_destroy(&cellMutex))
	    {
	        perror("pthread_mutex_destroy");
	        exit(-1);
	    }
	} /* Destructor */
	
	/**
 	 * Locks the cell mutex
 	 */
	void lockCell()
	{
        if(0 != pthread_mutex_lock(&cellMutex))
        {
            perror("pthread_mutex_lock");
            exit(-1);
        }
	} /* hashTableCell::lockCell() */
	
	/**
 	 * Unlocks the cell mutex
 	 */
	void unlockCell()
	{
        if(0 != pthread_mutex_unlock(&cellMutex))
        {
            perror("pthread_mutex_unlock");
            exit(-1);
        }
	} /* hashTableCell::unlockCell() */

	/* The linked list of records */
	list<record> recordList;
	
	/* Declare a cell mutex */
	pthread_mutex_t cellMutex;
}; /* class hashTableCell /*

/* The hash table */
vector<hashTableCell> hashTable(NUMBER_OF_HASH_CELLS);

/* The number of threads */
int numThreads = 0;

/* The message queue id */
int msqid;

/* The list of ids to look up */
list<int> idsToLookUpList;

/* Declare and initialize a mutex for protecting the idsToLookUpList */
pthread_mutex_t idsToLookUpListMutex = PTHREAD_MUTEX_INITIALIZER;

/* Declare and initialize the condition variable, threadPoolCondVar,
 * for implementing a thread pool.
 */
pthread_cond_t threadPoolCondVar = PTHREAD_COND_INITIALIZER;

/* Declare the mutex, threadPoolMutex, for protecting the thread pool
 * condition variable. 
 */
pthread_mutex_t threadPoolMutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * Prototype for createInserterThreads
 */
void createInserterThreads();

/**
 * A prototype for adding new records.
 */
void* addNewRecords(void* arg);

/**
 * Deallocates shared resources
 */
void cleanUp()
{
    /* Deallocate the message queue */
    if (msgctl(msqid, IPC_RMID, 0) < 0)
    {
        perror("msgctl");
        exit(-1);
    }

    /* Deallocate the mutex, threadPoolMutex, for protecting the thread pool
     * condition variable
     */
    if (0 != pthread_mutex_destroy(&threadPoolMutex))
    {
        perror("pthread_mutex_destroy");
        exit(-1);
    }

    /* Deallocate the condition variable, threadPoolCondVar,
     * for implementing a thread pool
     */
    if (0 != pthread_cond_destroy(&threadPoolCondVar))
    {
        perror("pthread_cond_destroy");
        exit(-1);
    }

    /* Deallocate the mutex for protecting the idsToLookUpList */
    if (0 != pthread_mutex_destroy(&idsToLookUpListMutex))
    {
        perror("pthread_mutex_destroy");
        exit(-1);
    }
} /* cleanUp() */

/**
 * Sends the message over the message queue
 * @param msqid - The message queue id
 * @param rec - The record to send
 */
void sendRecord(const int& msqid, const record& rec)
{
	/* The message to send */
	message msg; 
	
	/* Copy fields from the record into the message queue */	
	msg.messageType = SERVER_TO_CLIENT_MSG;
	msg.id = rec.id;
	strncpy(msg.firstName, rec.firstName.c_str(), MAX_NAME_LEN);	
	strncpy(msg.lastName, rec.lastName.c_str(), MAX_NAME_LEN);
	
	/* Send the message */
	sendMessage(msqid, msg);		
} /* sendRecord() */

/**
 * Prints the hash table
 */
void printHashTable()
{
	/* Go through the hash table */
	for(vector<hashTableCell>::const_iterator hashIt = hashTable.begin();
		hashIt != hashTable.end(); ++hashIt)
	{
		/* Go through the list at each hash location */
		for(list<record>::const_iterator lIt = hashIt->recordList.begin();
			lIt != hashIt->recordList.end(); ++lIt)
		{
			fprintf(stderr, "%d-%s-%s-%d\n", lIt->id, 
						lIt->firstName.c_str(), 
						lIt->lastName.c_str(),
						lIt->id % NUMBER_OF_HASH_CELLS
						);
		}
	}
} /* printHashTable() */

/**
 * Adds a record to the hash table
 * @param rec - The record to add
 */
void addToHashTable(const record& rec)
{
    /* The hash key for looking up the correct cell */
	int key = rec.id % NUMBER_OF_HASH_CELLS;

    /* Grab the mutex of the hash table cell */
	hashTable.at(key).lockCell();
	
	/* Save the record */
	hashTable.at(key).recordList.push_back(rec);
	
	/* Release the mutex of the hash table cell */
    hashTable.at(key).unlockCell();
} /* addToHashTable() */

/**
 * Retrieves a record from the hash table
 * @param id - The id of the record to retrieve
 * @return - The record from the hash table if it exists;
 * otherwise returns a record with the id field set to -1
 */
record getHashTableRecord(const int& id)
{
	/* Get pointer to the hash table cell */
	hashTableCell* hashTableCellPtr = &hashTable.at(id % NUMBER_OF_HASH_CELLS); 
	
	/* The record to return */
    record rec = {-1, "", ""};
	
    /* Grab mutex of the cell */
    hashTableCellPtr->lockCell();
	
	/* Get the iterator to the list of records hashing to this location */
	list<record>::iterator recIt = hashTableCellPtr->recordList.begin();
	
    /* Go through all the records */
	do
	{
		/* Save the record */
		if (recIt->id == id)
		{
			rec = *recIt;
		}
		
		/* Advance the record it */
		++recIt;
	} while ((rec.id != id) && (recIt != hashTableCellPtr->recordList.end()));
	
	/* Release the mutex of the cell */
	hashTableCellPtr->unlockCell();
	
	return rec;
} /* getHashTableRecord() */

/**
 * Loads the database into the hash table
 * @param fileName - The file name
 */
void populateHashTable(const string& fileName)
{	
	/* The record */
	record rec;
	
	/* Open the file */
	ifstream dbFile(fileName.c_str());
	
	/* Is the file open */
	if(!dbFile.is_open())
	{
		fprintf(stderr, "Could not open file %s\n", fileName.c_str());
		exit(-1);
	}
	
	/* Read the entire file */
	while(!dbFile.eof())
	{
		/* Read the id */
		dbFile >> rec.id;
		
		/* Make sure we did not hit the EOF */
		if(!dbFile.eof())
		{
			/* Read the first name and last name */
			dbFile >> rec.firstName >> rec.lastName;
						
			/* Add to hash table */
			addToHashTable(rec);	
		}
	}
	
	/* Close the file */
	dbFile.close();
} /* populateHashTable() */

/**
 * Gets ids to process from the work list
 * @return - The id of the record to look up, or
 * -1 if there is no work
 */
int getIdsToLookUp()
{
	/* The id */
	int id = -1;
	
	/* Acquire the idsToLookUpListMutex mutex */
    if (0 != pthread_mutex_lock(&idsToLookUpListMutex))
    {
        perror("pthread_mutex_lock");
        exit(-1);
    }
	
	/* Remove id from the list if it exists */
	if (!idsToLookUpList.empty())
	{
	    id = idsToLookUpList.front();
	    idsToLookUpList.pop_front();
	}
	
	/* Release idsToLookUpListMutex  */
    if (0 != pthread_mutex_unlock(&idsToLookUpListMutex))
    {
        perror("pthread_mutex_unlock");
        exit(-1);
    }
	
	return id;
} /* getIdsToLookUp() */

/**
 * Adds the id of a record to look up
 * @param id - The id to process
 */
void addIdsToLookUp(const int& id)
{
	/* Acquire idsToLookUpListMutex mutex */
    if (0 != pthread_mutex_lock(&idsToLookUpListMutex))
    {
        perror("pthread_mutex_lock");
        exit(-1);
    }
		
	/* Add the element to look up */
	idsToLookUpList.push_back(id);
		
	/* Release the idsToLookUpList mutex */
    if (0 != pthread_mutex_unlock(&idsToLookUpListMutex))
    {
        perror("pthread_mutex_unlock");
        exit(-1);
    }
} /* addIdsToLookUp() */

/**
 * The thread pool function
 * @param arg - Unused thread argument
 */
void* threadPoolFunc(void* arg)
{
	/* The id to process */
	int id = -1;
	
	/* Sleep until work arrives */
	while (true)
	{
		/* Lock the mutex protecting threadPoolCondVar from race conditions */
        if (0 != pthread_mutex_lock(&threadPoolMutex))
        {
            perror("pthread_mutex_lock");
            exit(-1);
        }
		
		/* Get the id to look up */
		id = getIdsToLookUp();
			
		/* No work to do */
		while (id == -1)
		{
			/* Sleep on the condition variable threadPoolCondVar */
            if (0 != pthread_cond_wait(&threadPoolCondVar, &threadPoolMutex))
            {
                perror("pthread_cond_wait");
                exit(-1);
            }
			
			/* Get the id to look up */
			id = getIdsToLookUp();	
		}
		
		/* Release the mutex protecting threadPoolCondVar from race conditions */
        if (0 != pthread_mutex_unlock(&threadPoolMutex))
        {
            perror("pthread_mutex_unlock");
            exit(-1);
        }
			
		/* Look up id */
		record rec = getHashTableRecord(id);
		
		/* Send the record to the client */
		sendRecord(msqid, rec);
	}
} /* threadPoolFunc() */

/**
 * Wakes up a thread from the thread pool
 */
void wakeUpThread()
{
	/* Lock the mutex protecting threadPoolCondVar from race conditions */
    if (0 != pthread_mutex_lock(&threadPoolMutex))
    {
        perror("pthread_mutex_lock");
        exit(-1);
    }

	/* Wake up a thread sleeping on threadPoolCondVar */
    if (0 != pthread_cond_signal(&threadPoolCondVar))
    {
        perror("pthread_cond_signal");
        exit(-1);
    }
	
	/* Release the mutex protecting threadPoolCondVar from race conditions */
    if (0 != pthread_mutex_unlock(&threadPoolMutex))
    {
        perror("pthread_mutex_unlock");
        exit(-1);
    }
} /* wakeUpThread() */

/**
 * Creates the threads for looking up ids
 * @param numThreads - The number of threads to create
 */
void createThreads(const int& numThreads)
{
    pthread_t threads[numThreads];

    for (int threadNum = 0; threadNum < numThreads; ++threadNum)
    {
        /* Create the thread */
        if (0 != pthread_create(&threads[threadNum], NULL, threadPoolFunc, NULL))
        {
            perror("pthread_create");
            exit(-1);
        }
    }
} /* createThreads() */

/**
 * Creates threads that update the database
 * with randomly generated records
 */
void createInserterThreads()
{
	/* Create NUM_INSERTERS threads that add new elements to the hash table
 	 * by calling addNewRecords(). 
 	 */
    pthread_t inserterThreads[NUM_INSERTERS];

    for (int threadNum = 0; threadNum < NUM_INSERTERS; ++threadNum)
    {
        /* Create the thread */
        if (0 != pthread_create(&inserterThreads[threadNum], NULL, addNewRecords, NULL))
        {
            perror("pthread_create");
            exit(-1);
        }
    }
} /* createInserterThreads() */

/**
 * Called by parent thread to process incoming messages
 */
void processIncomingMessages()
{
	/* The arriving message */
	message msg;
	
	/* The id of the record */
	int id = -1;
	
	/* Wait for messages forever */
	while (true)
	{
		/* Receive the message */
		recvMessage(msqid, msg, CLIENT_TO_SERVER_MSG);
		
		/* Add id to the list of ids that should be processed */
		addIdsToLookUp(msg.id);
			
		/* Wake up a thread to process the newly received id */
		wakeUpThread();
	}
} /* processIncomingMessages() */

/**
 * Generates a random record
 * @return - A random record
 */
record generateRandomRecord()
{
	/* A record */
	record rec;
		
	/* Generate a random id */
	rec.id = rand() % NUMBER_OF_HASH_CELLS;	
	
	/* Add the fake first name and last name */
	rec.firstName = "Random";
	rec.lastName = "Record";
	
	return rec;
} /* generateRandomRecord() */

/**
 * Threads inserting new records to the database
 * @param arg - Some argument (unused)
 */
void* addNewRecords(void* arg)
{	
	/* A randomly generated record */
	record rec;
		
	/* Keep generating random records */	
	while (true)
	{
		/* Generate a record */
		rec = generateRandomRecord();
		
		/* Add the record to hash table */
		addToHashTable(rec);
	}
} /* addNewRecords() */

/**
 * Handles the exit signal
 * @param signal - The signal type
 */
void ctrlCSignal(int signal)
{
    /* Deallocate shared resources */
    cleanUp();

    /* Terminate the program */
    exit(-1);
} /* ctrlCSignal() */

/**
 * Begins program execution
 * @param argc - The number of command line arguments
 * @param argv - An array of strings containing each command line argument
 */
int main(int argc, char* argv[])
{
	/* Check the command line */
	if (3 > argc)
	{
		fprintf(stderr, "Usage: %s <database file name> <number of threads>\n", argv[0]);
		exit(-1);
	}
	
	/* Install a signal handler for deallocating shared resources */
    if (SIG_ERR == signal(SIGINT, ctrlCSignal))
    {
        perror("signal");
        exit(-1);
    }
	
	/* Populate and print the hash table */
	populateHashTable(argv[1]);
	printHashTable();
	
	/* Get the number of lookup id threads */
	numThreads = atoi(argv[2]);
	
	/* Use a random file and a random character to generate
	 * a unique key. The same parameters to this function will
	 * always generate the same value. This is how multiple
	 * processes can connect to the same queue.
	 */
	key_t key = ftok("/bin/ls", 'O');
	
	/* Error check */
	if (0 > key)
	{
		perror("ftok");
		exit(-1);
	}
		
	/* Connect to the message queue */
	msqid = createMessageQueue(key);	
	
	/* Instantiate a message buffer for sending/receiving msgs 
 	 * from the message queue
 	 */
	message msg;
	
	/* Create the lookup id threads */
	createThreads(numThreads);				
	
	/* Create the inserter threads */
	createInserterThreads();
		
	/* Process incoming requests */
	processIncomingMessages();
	
	return 0;
} /* main() */
