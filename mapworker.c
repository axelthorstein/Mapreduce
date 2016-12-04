#include <stdio.h> 
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mapreduce.h"
#include "word_freq.c"


void map_worker(int outfd, int infd) {

	char filename[MAX_FILENAME];
	FILE *file;
	char chunk[READSIZE];

	while (read(infd, filename, MAX_FILENAME) > 0) {

		if (strcmp(filename, "quit") != 0) {

			file = fopen(filename, "r");

			// Read in each chunk from the file and send to be mapped. 
			if (file != NULL) {
				while (fread(chunk, 1, READSIZE, file) == READSIZE) {
					chunk[READSIZE - 1] = '\0';
					map(chunk, outfd); 
				}
			} else {
				perror("Unable to open file: ");
				exit(1);
			}
			if (fclose(file) != 0) {
				perror("File not closed properly:");
				exit(1);
			}
		} else if (strcmp(filename, "quit") == 0){

			Pair pair = {"quit read call", "1"};
			int exit_message = write(outfd, &pair, sizeof(Pair));

			if (exit_message != -1) {
				close(outfd);
				close(infd);
			} else {
				perror("Write error: ");
				exit(1);
			}
		}
	}
	exit(0);
}