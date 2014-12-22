#include "msg.h"

/**
 * The key to use for the message queue
 * @param key - the key to use.
 * @return - the message queue id
 */
int createMessageQueue(key_t key)
{
	/* Creates a message queue */
	int msqid = msgget(key, 0666 | IPC_CREAT);
	
	/* Error occured */
	if(msqid < 0)
	{
		perror("msgget");
		exit(-1);
	}
	
	return msqid;
}

/**
 * The key to use for the message queue
 * @param key - the key to use
 * @return - the message queue id
 */
int connectToMessageQueue(key_t key)
{
	/* Creates a message queue */
	int msqid = msgget(key, 0666);
	
	/* Error occured */
	if(msqid < 0)
	{
		perror("msgget");
		exit(-1);
	}
	
	return msqid;

}

/**
 * Sends the message over the message queue
 * @param msqid - the message queue id
 * @param msg - the pointer to the message
 */
void sendMessage(const int& msqid, message& msg)
{
	/* Sends the message */
	if(msgsnd(msqid, &msg, sizeof(msg) - sizeof(long), 0) < 0)
	{
		perror("msgsnd");
		exit(-1);
	}
}

/**
 * Retrieves a message from the message queue
 * @param msqid - the message queue id
 * @param msg - the structure where
 * to store the message received.
 * @param messageType - the message type
 */
void recvMessage(const int& msqid, message& msg, const int& messageType)
{
	/* Receive the message */
	if(msgrcv(msqid, &msg, sizeof(msg) - sizeof(long), messageType, 0) < 0)
	{
		perror("msgrcv");
		exit(-1);
	}
}



