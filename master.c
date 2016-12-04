#include <stdio.h> 
#include "mapreduce.h"
#include "mapworker.c"
#include "reduceworker.c"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#define MAPWORKER "mapworker.c"  // Filename of map worker. 
#define REDUCEWORKER "reduceworker.c" // Filename of reduce worker. 

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

// Check if pipe call was successful
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

// Check if write call was successful. 
int write_check(int write_success) {
	if (write_success == -1) {
		perror("Write error: ");
		exit(1);
	}
	return 0;
}

// Check if malloc call was successful. 
int malloc_check(int *malloc_success) {
	if (malloc_success == NULL) {
		perror("Malloc error: ");
		exit(1);
	}
	return 0;
}

void free_workers(int **workers, int num_workers) {

	// Free all dynamic memory for the workers, and close their pipes. 
	for (int i = 0; i < num_workers; i++) {
		free(workers[i]);
	}
	free(workers);
}

// Create an array of pipe file descriptors. 
int **create_pipes(int num_workers) {

	// Allocate memory for an array of file descriptors. 
	int **workers_fd = malloc(sizeof(int *) * num_workers); 
	if (workers_fd != NULL) {
		for (int i = 0; i < num_workers; i++) {

			workers_fd[i] = malloc(sizeof(int) * 2);

			malloc_check(workers_fd[i]);

			// Allocate the open read and write file descriptors. 
			pipe_check(pipe(workers_fd[i])); 
		}
	} else {
		perror("Malloc error: ");
		exit(1);
	}
	return workers_fd;
}

// Create the specified number of workers.
void create_workers(int **in_pipes, int **out_pipes, int num_workers, char *filename) {

	for (int i = 0; i < num_workers; i++) {

		//Create a child process for each map worker. 
		int worker_process = fork(); 
		fork_check(worker_process);

		if (worker_process == 0) {
			close(in_pipes[i][1]);
			close(out_pipes[i][0]);

			if (strcmp(filename, MAPWORKER) == 0) {
				map_worker(out_pipes[i][1], in_pipes[i][0]);
			} else if (strcmp(filename, REDUCEWORKER) == 0) {
				reduce_worker(out_pipes[i][1], in_pipes[i][0]);
			}
		}
		// Close unused ends. 
		close(in_pipes[i][0]);
		close(out_pipes[i][1]);
	}
}

int *list_files(char *dirname) {

	int status = 0;
	int *fd = malloc(sizeof(int) * 2);
	malloc_check(fd);

	pipe_check(pipe(fd));
	// Set read descriptor to non blocking. 
	fcntl(fd[0], F_SETFL, O_NONBLOCK); 
	
	int fork_success = fork();
	fork_check(fork_success);

	if (fork_success == 0) { 

		// Set STDIN for master to read from ls output. 
		dup2(fd[1], STDOUT_FILENO); 
		close(fd[1]);

		// Change the directory for ls to be executed and execute ls. 
		chdir(dirname); 
		int ls_success = execl("/bin/ls", "ls", (char *)0); 
		exec_check(ls_success);
	}
	
	// Wait for ls to finish before continuing. 
	wait_check(&status);

	return fd;
}

int send_to_map_workers(char *dirname, int num_map_workers, 
	int **filenames_pipe, int *ls) {

	// Route the output of ls to the master STDIN. 
	dup2(ls[0], STDIN_FILENO); 
	close(ls[0]);

	int i = 0;
	char filename[MAX_FILENAME];
	char path[MAX_FILENAME];
	
	while (scanf("%s", filename) == 1) {

		// Append the filename to the dirname. 
		strncpy(path, dirname, MAX_FILENAME);
		path [strlen(dirname)] = '/';
		strcat(path, filename);

		// Send one filename to each running map worker process.
		write_check(write(filenames_pipe[i][1], path, MAX_FILENAME)); 

		// Go back to the first worker if you reach the last worker.
		if (i >= (num_map_workers - 1)) {
			i = 0;
		} else {
			i++;
		}
	}

	// Send kill signal to map worker, and close pipe. 
	for (int j = 0; j < num_map_workers; j++) {
		write_check(write(filenames_pipe[j][1], "quit", MAX_FILENAME));
		close(filenames_pipe[j][1]);
	}

	close(ls[1]);
	free(ls);

	return 0;
}

int send_to_reduce_workers(int num_map_workers, int **chunks_pipe, 
	int num_reduce_workers, int **pairs_pipe) {

	Pair pair;
	int j = 0;
	
	for (int i = 0; i < num_map_workers; i++) {

		while (read(chunks_pipe[i][0], &pair, sizeof(pair)) > 0) {

			// Check if map worker is finished. 
			if (strcmp(pair.key, "quit read call") != 0) {

				write_check(write(pairs_pipe[j][1], &pair, sizeof(Pair)));

				// Go back to the first worker if you reach the last worker.
				if (j >= (num_reduce_workers - 1)) {
					j = 0;
				} else {
					j++;
				}
			// Check if map worker finished and if so move to next worker. 
			} else if (strcmp(pair.key, "quit read call") == 0){
				close(chunks_pipe[i][0]);
			}
		}
	}

	// Close used pipes.
	for (int m = 0; m < num_reduce_workers; m++) {
		Pair exit_pair = {"quit read call", "1"};
		write_check(write(pairs_pipe[m][1], &exit_pair, sizeof(Pair)));
		close(pairs_pipe[m][1]);
	}

	return 0;
}

void wait_for_children() {

	// Wait until there are no child proccesses still running. 
	while (waitpid(WAIT_ANY, NULL, 0) > 0) {
	   if (errno == ECHILD) {
	      perror("No child processes to wait on: ");
	      exit(1);
	   }
	}
	printf("Exiting\n");
}

// Master controller for mapreduce. 
int master(char *dirname, int num_map_workers, int num_reduce_workers) {

	// Create pipes for filenames and chunks to be sent through.
	int **filenames_pipe = create_pipes(num_map_workers);
	int **chunks_pipe = create_pipes(num_map_workers);

	// Create the map workers. 
	create_workers(filenames_pipe, chunks_pipe, num_map_workers, MAPWORKER);

	// Create pipes for pairs and reduced pairs to be sent through.
	int **pairs_pipe = create_pipes(num_reduce_workers);
	int **reduced_pipe = create_pipes(num_reduce_workers);

	// Create the reduce workers. 
	create_workers(pairs_pipe, reduced_pipe, num_reduce_workers, REDUCEWORKER);

	// Retrieve access to the filenames. 
	int *ls = list_files(dirname);

	// Send the filenames to the map workers.
	send_to_map_workers(dirname, num_map_workers, filenames_pipe, ls);

	// Send the word chunks to the reduce workers.
	send_to_reduce_workers(num_map_workers, chunks_pipe, 
		num_reduce_workers, pairs_pipe);

	// Wait for all children to exit. 
	wait_for_children();

	// Free all dynamically allocated variables. 
	free_workers(filenames_pipe, num_map_workers);
	free_workers(chunks_pipe, num_map_workers);
	free_workers(pairs_pipe, num_reduce_workers);
	free_workers(reduced_pipe, num_reduce_workers);
	
	return 0;
}



