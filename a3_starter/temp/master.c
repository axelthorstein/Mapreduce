#include <stdio.h> 
#include "mapreduce.h"
#include "mapworker.c"
#include "reduceworker.c"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

// Check if fork call was successful. 
int fork_check(int fork_return) {
	if (fork_return == -1) {
		perror("Failed fork: ");
		exit(1);
	}
	return 0;
}

// Check if pipe call was successful. 
int pipe_check(int pipe_return) {
	if (pipe_return == -1) {
		perror("Failed pipe: ");
		exit(1);
	}
	return 0;
}

// Check if pipe call was successful. 
int exec_check(int exec_return) {
	if (exec_return == -1) {
		perror("Failed exec: ");
		exit(1);
	}
	return 0;
}

// Check if wait call was successful. 
int wait_check(int *status) {
	if (wait(status) == -1) {
		perror("Wait error: ");
		exit(1);
	}
	return 0;
}

// Check if wait call was successful. 
int write_check(int write_success) {
	if (write_success == -1) {
		perror("Write error: ");
		exit(1);
	}
	return 0;
}

void free_workers(int **workers, int num_workers) {

	// Free all dynamic memory for the workers, and close their pipes. 
	for (int i = 0; i < num_workers; i++) {
		close(workers[i][0]);
		close(workers[i][1]);
		free(workers[i]);
	}
	free(workers);
}

// Create the specified number of workers.
int **create_workers(int num_workers, char *path, char *filename) {

	// Allocate memory for an array of file descriptors that correspond to map workers.  
	int **workers_fd = malloc(sizeof(int *) * num_workers); 
	for (int j = 0; j < num_workers; j++) {
		workers_fd[j] = malloc(sizeof(int) * 2);
	}

	for (int i = 0; i < num_workers; i++) {

		pipe_check(pipe(workers_fd[i])); // Allocate the open read and write file descriptors. 
		int worker_process = fork(); //Create a child process for the map worker to occupy. 
		fork_check(worker_process);

		if (worker_process == 0) { // In child

			if (strcmp(filename, "mapworker.c") == 0) {
				printf("Map worker %d: %d %d\n", i, workers_fd[i][1], workers_fd[i][0]);
				map_worker(workers_fd[i][1], workers_fd[i][0]);
			} else if (strcmp(filename, "reduceworker.c") == 0) {
				printf("Reduce worker %d: %d %d\n", i, workers_fd[i][1], workers_fd[i][0]);
				reduce_worker(workers_fd[i][1], workers_fd[i][0]);
			}
		}
	}
	return workers_fd;
}

int *list_files(char *dirname) {

	int status = 0;
	int *fd = malloc(sizeof(int) * 2); // Allocate the open read and write file descriptors. 

	pipe_check(pipe(fd));
	fcntl(fd[0], F_SETFL, O_NONBLOCK); // Sets the file descriptor to not block if it's waiting on a read call. Ask if this is okay
	
	int fork_success = fork();
	fork_check(fork_success);

	if (fork_success == 0) { // In child

		dup2(fd[1], STDOUT_FILENO); // Set STDIN for master to read from ls output. 
		close(fd[1]); // Close the read file descriptor that isn't being used. 

		chdir(dirname); // Change the directory for ls to be executed to on the specified directory. 
		int ls_success = execl("/bin/ls", "ls", (char *)0); // Execute ls on the directory.
		exec_check(ls_success);
	}
	
	// Wait for child to finish before continuing. 
	wait_check(&status);
	return fd;
}

int send_to_map_workers(char *dirname, int num_map_workers, 
	int **map_workers, int *ls) {

	dup2(ls[0], STDIN_FILENO); // Route the output of ls to the master STDIN.
	close(ls[0]);

	int i = 0;
	char filename[MAX_FILENAME];
	char path[MAX_FILENAME];
	while (scanf("%s", filename) != EOF) {
		// Append the filename to the dirname.
		strncpy(path, dirname, MAX_FILENAME);
		path [strlen(dirname)] = '/';
		strcat(path, filename);
		printf("%s sent to %d\n", filename, map_workers[i][1]);
		// Send one filename to each running map worker process.
		write_check(write(map_workers[i][1], path, MAX_FILENAME));
		// Go back to the first worker if you reach the last worker.
		if (i >= (num_map_workers - 1)) {
			i = 0;
		} else {
			i++;
		}
	}


	close(ls[1]);
	free(ls);

	return 0;
}

int send_to_reduce_workers(int num_map_workers, int **map_workers, 
	int num_reduce_workers, int **reduce_workers) {

	Pair pair;
//	int j = 0;
	for (int i = 0; i < num_map_workers; i++) {
		printf("in reduce\n");
		sleep(1);
		int r = read(map_workers[i][0], &pair, sizeof(pair));
		printf("r = %d\n", r);
		while (r > 0) {
//			printf("r = %d, Chunk: %s\n", r, pair.key);
//			r = read(map_workers[i][0], &pair, sizeof(pair));
//
			write_check(write(reduce_workers[j][1], &pair, sizeof(Pair)));
//
//			if (j >= (num_reduce_workers - 1)) {
//				j = 0;
//			} else {
//				j++;
//			}
		}
//		printf("REACHING TERMINATION");
	}
	printf("REACHING TERMINATION");
	return 0;
}

// Master controller for mapreduce. 
int master(char *dirname, int num_map_workers, int num_reduce_workers) {

	// Create necessary worker processes for map and reduce. 
	int **map_workers = create_workers(num_map_workers, "./mapworker.c", "mapworker.c");
	int **reduce_workers = create_workers(num_reduce_workers, "./reduceworker.c", "reduceworker.c");

	// Retrieve access to the filenames. 
	int *ls = list_files(dirname);

	send_to_map_workers(dirname, num_map_workers, map_workers, ls);

	send_to_reduce_workers(num_map_workers, map_workers, 
		num_reduce_workers, reduce_workers);

	//free_workers(map_workers, num_map_workers);
	//free_workers(reduce_workers, num_reduce_workers);
	
	return 0;
}



