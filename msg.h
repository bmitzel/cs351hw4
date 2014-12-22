#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <string.h>
#include <errno.h>


/* The maximum length for a name */
#define MAX_NAME_LEN 100

/* The structure representing the message */
struct message
{
	/** IMPORTANT: every message structure must start with this **/
	long messageType;
	
	/* A number */
	int id;
	
	/* The first name */
	char firstName[MAX_NAME_LEN];	
	
	/* The last name */
	char lastName[MAX_NAME_LEN];
	
	/**
 	 * Prints a record.
 	 * @param fp - the file stream to print to
 	 */
	void print(FILE* fp)
	{
		fprintf(fp, "messageType=%ld id=%d  firstName=%s lastName=%s\n", 
			messageType, id, firstName, lastName);
	}
	
};

/* The client to server message type */
#define CLIENT_TO_SERVER_MSG 1

/* The message sent from server to client message type */
#define SERVER_TO_CLIENT_MSG 2

/**
 * The key to use for the message queue
 * @param key - the key to use
 * @return - the message queue id
 */
int createMessageQueue(key_t key);

/**
 * The key to use for the message queue
 * @param key - the key to use
 * @return - the message queue id
 */
int connectToMessageQueue(key_t key);


/**
 * Sends the message over the message queue
 * @param msg - the message queue id
 * @param messagePtr - the pointer to the message
 */
void sendMessage(const int& msqid, message& msg);

/**
 * Retrieves a message from the message queue
 * @param msqid - the message queue id
 * @param msg - the structure where
 * to store the message received.
 * @param messageType - the message type
 */
void recvMessage(const int& msqid, message& msg, const int& messageType);

