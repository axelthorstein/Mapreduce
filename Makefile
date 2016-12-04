	CFLAGS = -Wall -g -std=c99 -Werror

master: mapreduce.c reduceworker.c mapworker.c mapreduce.h 
	gcc $(CFLAGS) -o mapreduce mapreduce.c

word_freq:
	gcc $(CFLAGS) -o word_freq word_freq.c
