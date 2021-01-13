#ifndef __PFREADER_H__
#define __PFREADER_H__

#include <pthread.h>
#include <stdbool.h>

struct read_tracker
{ 
    pthread_mutex_t      mutex; // pthread mutex for data protection
    pthread_cond_t       cond; // condition for pthread synchronization
    int 		 n_threads;    //number of thread file-readers
    long                 per_thread_bufsize; // Buffer size per thread
    int                  *thread_fd;  //array of open file descriptors (1 per nthread)
    long                 *read_start_assignment;  //array of file offsets to start reading block of data
    char                 **data_buffers;          //array of pre-allocated space to store chunks of read data
    int                  *pfreader_mark_data_ready;             //array of ints (<0 = stop thread, 0=reading, 1=ready for buffer transfer
    long		 curr_read_start_offset; //current in-use max lseek() start address
    long                 curr_block_start_offset; //current block offset into file data stored in memory buffers.
    long                 *bytes_read;
    bool                 endofpfreader; // set to true when no more data to put in pfreader (like eof): reader must stop
};

typedef struct read_tracker pfreader;
extern pfreader g_pfreader;

// init and destroy
long  pfreader_init(pfreader *l, int numthreads, int *threadfds, long *assignments, char **databuffers, long databuffersize, long *bytes_read_storage, int *dr);
long  pfreader_destroy(pfreader *l);

// end of pfreader functions
struct timespec get_pfreader_timeout();
long  pfreader_set_end_of_pfreader(pfreader *l, bool state);
bool pfreader_get_end_of_pfreader(pfreader *l);
void *pfreader_thread_read_fct(void *args);
long pfreader_get_next_block(pfreader *l, unsigned char *datablock, long blocksize);

#endif // __PFREADER_H__
