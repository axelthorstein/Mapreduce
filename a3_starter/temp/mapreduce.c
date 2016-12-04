#include <stdio.h> 
#include "mapreduce.h"
#include "master.c"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

void print_usage() {
    printf("Usage: mapreduce -d dirname -m num -r num\n");
}

int main(int argc, char **argv) {

	// Set the default values to be changed only if they are provided. 
	int num_map_workers = 2;
	int num_reduce_workers = 2;
	char *dirname = NULL;
	int dirname_empty = 0;
  	int option;

  	while ((option = getopt (argc, argv, "d:m:r:")) != -1)
  		switch (option) {
  		case 'd':
  			dirname_empty = 1;
  			dirname = optarg;
  			break;
  		case 'm':
  			num_map_workers = atoi(optarg);
  			break;
  		case 'r':
  			num_reduce_workers = atoi(optarg);
  			break;
  		default: print_usage(); 
                 exit(EXIT_FAILURE);
  	}

  	if (dirname_empty == 0 || num_map_workers < 2 || num_reduce_workers < 2) {
        print_usage();
        exit(EXIT_FAILURE);
    }

	// DELETE BEFORE PRODUCTION
	printf("Dirname: %s, map_num: %d, reduce_num: %d\n", dirname, num_map_workers, num_reduce_workers);

	master(dirname, num_map_workers, num_reduce_workers);

	return 0;
}

// Ask about makefile
// Ask about closing file descriptors
// Ask about read blocking and fd closing




