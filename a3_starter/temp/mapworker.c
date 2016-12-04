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

		printf("Filename: %s from: %d\n", filename, outfd);
		file = fopen(filename, "r");

		// Read in each chunk from the file and send to be mapped. 
		if (file != NULL) {
			while (fgets(chunk, READSIZE, file)) {
				chunk[READSIZE - 1] = '\0';
//				printf("Chunk: %s\n", chunk);
				map(chunk, outfd);
			}

		} else {
			printf("%s\n\n", filename);
			perror("Unable to open file: ");
			exit(1);
		}
		if (fclose(file) != 0) {
			perror("File not closed properly:");
			exit(1);
		}
	}
	close(outfd);
	close(infd);
	printf("Exiting\n");
	exit(0);
}