#include <stdio.h> 
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mapreduce.h"


void reduce_worker(int outfd, int infd) {

	Pair pair;
	while (read(infd, &pair, sizeof(pair)) > 0) {
		//printf("Pair: {%s, %s}\n", pair.key, pair.value);
	}

//	printf("In reduce worker. \n");
	//Pair reduce(const char *key, const LLValues *values);
	//exit(0);
}