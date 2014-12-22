
/** This program illustrates the client end of the message queue **/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <string.h>
#include <vector>
#include <list>
#include <string>

/* Defines message queue related functions */
#include "msg.h"

using namespace std;



int main() {
	/* Use a random file and a random character to generate a unique key.
	 * Same parameters to this function will always generate the same value. This
	 * is how multiple processes can connect to the same queue.
	 */
	key_t key = ftok("/bin/ls", 'O');
	
	/* Was the key allocation successful ? */
	if(key < 0) { perror("ftok"); exit(-1); }	
	
	/* Connect to the message queue; fail if the	there is no message
	 * queue associated with this key. This function returns the id of the queue. 
	 */	
	int msqid = connectToMessageQueue(key);	
	
	/* The message */
	message msg; 
	
	while(true) 
	{
	
		/* Generate a random id */
		msg.id = rand() % 1000;
	
		/* Send a message to the server */
		msg.messageType = CLIENT_TO_SERVER_MSG;	
	
		/* Send the message */
		sendMessage(msqid, msg);
		
		/* Receive a reply from the server */	
		recvMessage(msqid, msg, SERVER_TO_CLIENT_MSG);
	
		/* Print only existing records */
		if(msg.id != -1) 
		{	
			/* Print the message */
			msg.print(stderr); 
		}
			
	}
	
	return 0; 
}
