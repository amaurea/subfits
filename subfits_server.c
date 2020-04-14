#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <libgen.h>
#include <errno.h>
#include <error.h>
#include <string.h>
#include <pthread.h>
#include <stdarg.h>
#include <signal.h>
#include <time.h>
#include "slice_fits.h"

#define false 0
#define true 1

char * basedir =  ".";
void * server_thread(void *);
void daemonize();
void help();

typedef struct { int code; char * name; } HTTP_code;
enum { HTTP_200, HTTP_400, HTTP_403, HTTP_404, HTTP_405, HTTP_500 };
HTTP_code http_codes[] = {
	{ 200, "OK" },
	{ 400, "Bad Request" },
	{ 403, "Forbidden" },
	{ 404, "Not Found" },
	{ 405, "Method Not Allowed" },
	{ 500, "Internal Server Error" }
};

int main(int argc, char ** argv) {
	int server_port = 8200, maxconn = 20, nthread = 10;
	int daemon = false;
	char * ofname = NULL;
	int log_fd = 0;
	// server configuration
	for(int i = 1, narg = 0; i < argc; i++) {
		if     (!strcmp(argv[i], "-h")) help();
		else if(!strcmp(argv[i], "-p")) {
			if(++i == argc) help();
			server_port = atoi(argv[i]);
		}
		else if(!strcmp(argv[i], "-l")) {
			if(++i == argc) help();
			ofname = argv[i];
		}
		else if(!strcmp(argv[i], "-d")) daemon = true;
		else if(argv[i][0] == '-') help();
		else if(narg > 0) help();
		else basedir = argv[i];
	}
	if(!ofname) ofname = daemon ? "subfits_server.log" : "/dev/stderr";
	if(daemon) daemonize();
	log_fd = open(ofname, O_WRONLY|O_CREAT|O_TRUNC, 0666);
	// redirect stderr and stdout to this file. This is a bit hacky, but I'm not sure
	// how best to make the program behave sensibly both running normally and as a server.
	// I don't think this server program needs to distinguish between stdout and stderr anyway,
	// since it doesn't really have any user-oriented output.
	dup2(log_fd, 1);
	dup2(log_fd, 2);

	pthread_t * threads = malloc(sizeof(pthread_t)*nthread);
	pid_t pid = getpid();
	int server_sd = -1,  on = -1;
	struct sockaddr_in6 server_addr;
	int addrlen = sizeof(server_addr);
	if((server_sd = socket(AF_INET6, SOCK_STREAM, 0)) < 0) {
		perror("socket() failed"); goto cleanup;
	}
	if(setsockopt(server_sd, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof(on)) < 0) {
		perror("setsockopt(SO_REUSEADDR) failed"); goto cleanup;
	}
	memset(&server_addr, 0, sizeof(server_addr));
	server_addr.sin6_family = AF_INET6;
	server_addr.sin6_port   = htons(server_port);
	server_addr.sin6_addr   = in6addr_any;
	if(bind(server_sd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
		perror("bind() failed"); goto cleanup;
	}
	if(listen(server_sd, maxconn) < 0) {
		perror("listen() failed"); goto cleanup;
	}
	// Set up threads
	for(int i = 0; i < nthread; i++) {
		if(pthread_create(&threads[i], NULL, server_thread, &server_sd)) {
			perror("pthread_create() failed");
			goto cleanup;
		}
	}

	printf("subfits_server with PID %d listening for connections on port %d\n", pid, ntohs(server_addr.sin6_port));
	fflush(stdout);

	server_thread(&server_sd);

cleanup:
	if(server_sd >= 0) close(server_sd);
	if(threads) free(threads);

	return 0;
}

int starts_with(const char * pre, const char * str) {
	return strncmp(pre, str, strlen(pre)) == 0;
}

void send_header(int client_sd, int log_fd, char * addr_str, char * url, int code, char * extra_fmt, ...) {
	int nwritten;
	time_t rawtime;
	struct tm * timeinfo;
	char buf[0x1000], extra_buf[0x1000], tbuf[80];
	va_list ap;
	// Sent header to client
	if(!extra_fmt) extra_fmt = "";
	va_start(ap, extra_fmt);
	vsnprintf(extra_buf, sizeof(extra_buf), extra_fmt, ap);
	va_end(ap);
	nwritten = snprintf(buf, sizeof(buf), "HTTP/1.1 %d %s%s\r\n\r\n", http_codes[code].code, http_codes[code].name, extra_buf);
	send(client_sd, buf, nwritten, 0);
	// Output the log line
	if(log_fd >= 0) {
		time(&rawtime);
		timeinfo = localtime(&rawtime);
		strftime(tbuf, sizeof(tbuf), "%Y-%m-%dT%H:%M:%S", timeinfo);
		nwritten = snprintf(buf, sizeof(buf), "%s - %20s - %d - %s\n", tbuf, addr_str, http_codes[code].code, url);
		write(log_fd, buf, nwritten);
	}
}

// This represents a server thread that will run forever until interrupted. It repeatedly
// accepts connections, handles them, and then returns to accepting again.
void * server_thread(void * arg) {
	pthread_t thread_id = pthread_self();
	int server_sd = *(int*)arg, client_sd = -1, fd, log_fd = STDOUT_FILENO;
	struct sockaddr_in6 client_addr;
	int addrlen = sizeof(client_addr);
	char addr_str[INET6_ADDRSTRLEN];
	char read_buf[0x1000], send_buf[0x1000], work[0x1000], orig_url[0x1000];
	int nread, nwritten, code;
	size_t payload_size;
	char * method, * url, * prot, * query, * saveptr, * fname, * path = 0;
	while(true) {
		if((client_sd = accept(server_sd, NULL, NULL)) < 0) {
			perror("accept() failed"); goto cleanup;
		}
		getpeername(client_sd, (struct sockaddr*)&client_addr, &addrlen);
		inet_ntop(AF_INET6, &client_addr.sin6_addr, addr_str, sizeof(addr_str));
		//printf("Thread %2d got connection from %s:%d\n", thread_id, addr_str, ntohs(client_addr.sin6_port));
		if((nread = recv(client_sd, read_buf, sizeof(read_buf)-1, 0)) < 0) {
			perror("recv() error"); goto cleanup;
		}
		read_buf[nread] = 0;
		//printf(read_buf);
		// Parse the request. This has the form method url prot, key: value pairs, payload.
		// But in our case we only care about GET, so the method url prot part should be all we
		// need to care about
		method = strtok_r(read_buf, " \t\r\n", &saveptr);
		url    = strtok_r(NULL,     " \t",     &saveptr);
		prot   = strtok_r(NULL,     " \t\r\n", &saveptr);
		strncpy(orig_url, url, sizeof(orig_url));
		// Split the url into the path and the query string
		if((query = strchr(url, '?'))) *query++ = 0;
		else query = url-1;
		//printf("method: %s, url: %s, query: %s, prot: %s\n", method, url, query, prot);
		if(strcmp(method, "GET")) {
			snprintf(work, sizeof(work), "Only GET is supported, but got '%s'\r\n", method);
			send_header(client_sd, log_fd, addr_str, orig_url, HTTP_405, "\r\nOnly GET is supported, but got '%s'", method);
			goto cleanup;
		}
		// Build the full path, and ensure that it is still inside our basedir
		snprintf(work, sizeof(work), "%s/%s", basedir, url);
		path = realpath(work, NULL);
		if(!path || !starts_with(basedir, path)) {
			send_header(client_sd, log_fd, addr_str, orig_url, HTTP_404, NULL);
			goto cleanup;
		}
		// Try opening the file
		if((fd = open(path, O_RDONLY)) < 0) {
			send_header(client_sd, log_fd, addr_str, orig_url,
					errno == ENOENT ? HTTP_404 : errno == EACCES ? HTTP_403 : HTTP_500, NULL);
			goto cleanup;
		}
		// Test if the slice etc. make sense
		if((code = slice_fits(fd, -1, query, &payload_size)) != FSLICE_OFD) {
			send_header(client_sd, log_fd, addr_str, orig_url, code == FSLICE_EVALS ? HTTP_400 : HTTP_500, NULL);
			goto cleanup;
		}
		strncpy(work, path, sizeof(work));
		fname = basename(work);

		// Ok, it looks like everything is good
		send_header(client_sd, log_fd, addr_str, orig_url, HTTP_200,
				"\r\nContent-Length: %ld\r\nContent-Type: image/fits",
				payload_size);
		slice_fits(fd, client_sd, query, NULL);
	
cleanup:
		if(client_sd >= 0) close(client_sd);
		if(path) { free(path); path = 0; }
	}
	return 0;
}

void help() {
	fprintf(stderr, "Usage subfits_server [-h] [-p PORT] [root_dir]\n");
	fprintf(stderr, " -h        Print this help message and exit\n");
	fprintf(stderr, " -p PORT   Listen on the given port. Default: 8200\n");
	fprintf(stderr, " root_dir  Server paths are relative to this directory. No access outside it allowed.\n");
	exit(1);
}

void daemonize() {
	pid_t child;
	// Become own child by forking and having the parent exit
	if((child = fork()) < 0) {
		perror("fork() failed");
		exit(1);
	}
	if(child > 0) exit(0);
	if(setsid() < 0) {
		perror("setsid() failed");
		exit(1);
	}
	// Ignore some signals
	signal(SIGCHLD, SIG_IGN);
	signal(SIGHUP,  SIG_IGN);
	// Become own child again, apparently
	if((child = fork()) < 0) {
		perror("fork() failed");
		exit(1);
	}
	if(child > 0) exit(0);
	if(setsid() < 0) {
		perror("setsid() failed");
		exit(1);
	}
	// Normally one would close all file descriptors and reopen them here, but
	// we handle that directly
}
