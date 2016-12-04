#include <stdio.h> 
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mapreduce.h"
#include "linkedlist.c"

LLKeyValues *in_key_list(char *key, LLKeyValues *head) {

	LLKeyValues *curr = head;
	// Search in the keys list if a key exists. 
	while (curr != NULL) {
	    if (strcmp(curr->key, key) == 0) {
	       return curr;
	   }
	    curr = curr->next;
	}
	return curr;
}

void write_to_binary(LLKeyValues *keys) {
	LLKeyValues *curr = keys;
	Pair pair;
	FILE *file;
	char pid_str[15];
	
	// Get the pid filename to write to. 
	int pid = getpid();
	sprintf(pid_str, "%d", pid);
	strcat(pid_str, ".out");
	file = fopen(pid_str, "wb");

	if (file != NULL) {

		while (curr != NULL) {
			// Reduce the pair and write to binary file. 
		    pair = reduce(curr->key, curr->head_value);
		    int write_success = fwrite(&pair, sizeof(Pair), 1, file);
		    if (write_success == -1) {
		    	perror("Fwrite Failed:");
				exit(1);
		    }
		    curr = curr->next;
		}

	} else {
		perror("File failed to open:");
		exit(1);
	}
	if (fclose(file) != 0) {
		perror("File failed to close:");
		exit(1);
	}
}

void reduce_worker(int outfd, int infd) {

	LLKeyValues *keys = NULL;
	Pair pair;

	while (read(infd, &pair, sizeof(pair)) > 0) {

		if (strcmp(pair.key, "quit read call") != 0) {

			// Check if the pair has been seen already. 
			LLKeyValues *existing_key = in_key_list(pair.key, keys);

			// If not add it, otherwise append to values list of key. 
			if (existing_key == NULL) {
				insert_into_keys(&keys, pair);
			} else {
				insert_value(existing_key, pair.value);
			}

		} else if (strcmp(pair.key, "quit read call") == 0) {
			if (close(infd) != 0) {
				perror("File failed to close:");
				exit(1);
			}
		}
	}

	write_to_binary(keys);

	free_key_values_list(keys);

	close(outfd);

	exit(0);
}