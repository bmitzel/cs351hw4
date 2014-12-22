all: server client

server: server.cpp msg.o
	g++ server.cpp msg.o -o server -lpthread

client:	client.cpp msg.o
	g++ client.cpp msg.o -o client -lpthread

msg.o:	msg.h msg.cpp
	g++ -c msg.cpp 

clean:
	rm -rf client server msg.o
