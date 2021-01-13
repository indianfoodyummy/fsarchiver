#define _LARGEFILE64_SOURCE
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <assert.h>

#include "pfreader.h"
#include "fsarchiver.h"
#include "error.h"

struct timespec get_pfreader_timeout()
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
//    t.tv_sec++;
    t.tv_nsec+=1000000; //this should be 1 millisecond in the future
    return t;
}

long  pfreader_init(pfreader *l, int numthreads, int *threadfds, long *assignments, char **databuffers, long databuffersize, long *bytes_read_storage, int *dr)
{
    pthread_mutexattr_t attr;

    if (!l)
    {   msgprintf(MSG_FORCE,"pfreader object is NULL\n");
        return -1;
    }
    
    // ---- init default attributes
    l->thread_fd = threadfds;
    l->n_threads = numthreads;
    l->read_start_assignment = assignments;
    l->data_buffers=databuffers;
    l->per_thread_bufsize = databuffersize;
    l->curr_read_start_offset=0;
    l->curr_block_start_offset=0;
    l->bytes_read=bytes_read_storage;
    l->pfreader_mark_data_ready=dr;
    l->endofpfreader=false;

    for (int i=0;i<l->n_threads;i++) {
        l->read_start_assignment[i]=-1;
        l->pfreader_mark_data_ready[i]=0;
        l->bytes_read[i]=0;
    }
    
    // ---- init pthread structures
    assert(pthread_mutexattr_init(&attr)==0);
    assert(pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK)==0);
    if (pthread_mutex_init(&l->mutex, &attr)!=0)
    {   msgprintf(MSG_FORCE,"pthread_mutex_init failed\n");
        return -2;
    }
    
    if (pthread_cond_init(&l->cond,NULL)!=0)
    {   msgprintf(MSG_FORCE,"pthread_cond_init failed\n");
        return -2;
    }
    
    return 0;
}

long pfreader_destroy(pfreader *l)
{
    
    if (!l)
    {   msgprintf(MSG_FORCE, "pfreader pointer is NULL\n");
        return -1;
    }
    
    assert(pthread_mutex_destroy(&l->mutex)==0);
    assert(pthread_cond_destroy(&l->cond)==0);
    
    return 0;
}

long pfreader_set_end_of_pfreader(pfreader *l, bool state)
{
    if (!l)
    {   msgprintf(MSG_FORCE,"pfreader pointer is NULL\n");
        return -1;
    }

    assert(pthread_mutex_lock(&l->mutex)==0);
    l->endofpfreader=state;
    assert(pthread_mutex_unlock(&l->mutex)==0);
    pthread_cond_broadcast(&l->cond);
    return 0;
}

bool pfreader_get_end_of_pfreader(pfreader *l)
{
    bool res;

    if (!l)
    {   msgprintf(MSG_FORCE,"pfreader pointer is NULL\n");
        return -1;
    }

    assert(pthread_mutex_lock(&l->mutex)==0);
    res=l->endofpfreader;
    assert(pthread_mutex_unlock(&l->mutex)==0);
    return res;
}

bool pfreader_assignment_ready(pfreader *l, int mythread)
{
    bool ret=false;
    assert(pthread_mutex_lock(&l->mutex)==0);
    while ((l->read_start_assignment[mythread]>=0) && (l->pfreader_mark_data_ready[mythread] > 0)) {
        struct timespec t=get_pfreader_timeout();
        pthread_cond_timedwait(&l->cond, &l->mutex, &t);
    }
    if ((l->pfreader_mark_data_ready[mythread] == 0) &&
       (l->read_start_assignment[mythread] >= 0))  
        ret=true;
    assert(pthread_mutex_unlock(&l->mutex)==0);
    return ret;
}

void *pfreader_thread_read_fct(void *args)
{
    pfreader *l = (pfreader *)args;
    int i;
    int mythread=-1;
    int failed=0;
    int read_so_far=0;
    int read_this_time=0;
    long seek_retval=0;
    bool found_alive=true;

        assert(pthread_mutex_lock(&l->mutex)==0);
        for (i=0; (mythread < 0) && (i < l->n_threads); i++) {
            if (l->read_start_assignment[i]==-1) {
                mythread=i;
                if (i==0) {
                    l->curr_read_start_offset = 0;
                } else {
                    l->curr_read_start_offset += l->per_thread_bufsize-1;
                }
                l->read_start_assignment[mythread]= l->curr_read_start_offset;
            }
        }
        assert(pthread_mutex_unlock(&l->mutex)==0);

        memset(l->data_buffers[mythread],0,l->per_thread_bufsize);
        while((pfreader_get_end_of_pfreader(l)==false) && 
              (pfreader_assignment_ready(l,mythread)==true) && 
              (failed < 3)) {
            if (read_so_far <= l->per_thread_bufsize-1) {
                seek_retval = lseek64(l->thread_fd[mythread],l->read_start_assignment[mythread] + read_so_far, SEEK_SET);
                if (seek_retval >= 0) {
                    read_this_time = read(l->thread_fd[mythread],l->data_buffers[mythread] + read_so_far, l->per_thread_bufsize -1 - read_so_far);
                    if (read_this_time < 1) {
                        failed++;
                    } else {
                        failed=0;
                        read_so_far += read_this_time;
                        if (read_so_far >= l->per_thread_bufsize-1) {
                            assert(pthread_mutex_lock(&l->mutex)==0);
                                l->bytes_read[mythread] += read_so_far;
                                l->pfreader_mark_data_ready[mythread]=1;
                            assert(pthread_mutex_unlock(&l->mutex)==0);
                            read_this_time = 0;
                            read_so_far = 0;
                        }
                    }
                }
            }

            assert(pthread_mutex_lock(&l->mutex)==0);
                if (l->pfreader_mark_data_ready[mythread]==0) {
                    if (read_so_far > 0) {
                        l->bytes_read[mythread] += read_so_far;
                        l->pfreader_mark_data_ready[mythread]=1;
                    } else {
                        l->read_start_assignment[mythread]= -2;
                    }
                } 
            assert(pthread_mutex_unlock(&l->mutex)==0);
            read_this_time = 0;
            read_so_far=0;
        }

        for (int i=0; i< l->n_threads; i++) {
            if ( l->read_start_assignment[i] != -2) found_alive=true;
        }
        if (found_alive==false) {
            pfreader_set_end_of_pfreader(l, true);
        }

    return NULL;
}

long pfreader_get_next_block(pfreader *l, unsigned char *datablock, long blocksize) {
    bool found=false;
    long bytes_read=0;
    long bytes_in_first_block=0;
    int  reader_blocks_free[l->n_threads];
    int  i;
    int threads_done;

    for (i=0; i<l->n_threads; i++) reader_blocks_free[i]=0;

    while((pfreader_get_end_of_pfreader(l)==false) && (found==false)) {
        assert(pthread_mutex_lock(&l->mutex)==0);
        for (i=0, threads_done=0; (i < l->n_threads) && (bytes_read < blocksize); i++) {
            if (l->read_start_assignment[i]==-2) {
                threads_done++;
                continue;
            } else if ((l->read_start_assignment[i]<0) || (reader_blocks_free[i]==1)) {
                continue;
            } else if (l->read_start_assignment[i] >= l->curr_block_start_offset + blocksize - bytes_read) {
                continue;
            } else if (l->pfreader_mark_data_ready[i]!=1) {
                continue;
            } else if (l->read_start_assignment[i] + l->bytes_read[i] < l->curr_block_start_offset) {
                reader_blocks_free[i]=1;
                continue;
            } else if (l->read_start_assignment[i] == l->curr_block_start_offset) {
                long bytes_to_read = blocksize - bytes_read;
                if (bytes_to_read < l->bytes_read[i]) {
                    memcpy(datablock + bytes_read, l->data_buffers[i], bytes_to_read);
                    l->curr_block_start_offset += bytes_to_read;
                    bytes_read += bytes_to_read;
                } else {
                    memcpy(datablock + bytes_read, l->data_buffers[i], l->bytes_read[i]);
                    l->curr_block_start_offset += l->bytes_read[i];
                    bytes_read += l->bytes_read[i];
                    reader_blocks_free[i]=1;
                }
                continue;
            } else if ((l->read_start_assignment[i] < l->curr_block_start_offset) && 
                       (l->read_start_assignment[i] + l->bytes_read[i] > l->curr_block_start_offset) ) {
                  long start_offset = l->curr_block_start_offset - l->read_start_assignment[i];
                  bytes_in_first_block = l->bytes_read[i] - start_offset;
                  if (blocksize >= bytes_in_first_block) {
                      memcpy(datablock, l->data_buffers[i]+start_offset, bytes_in_first_block);
                      bytes_read = bytes_in_first_block;
                      l->curr_block_start_offset += bytes_in_first_block;
                      reader_blocks_free[i]=1;
                 } else {
                      memcpy(datablock, l->data_buffers[i]+start_offset, blocksize);
                      bytes_read = blocksize;
                      l->curr_block_start_offset += blocksize;
                 }
                 continue;
            } 
        }
        for (i=0; i<l->n_threads; i++) {
            if (reader_blocks_free[i]==1) {
                l->curr_read_start_offset += l->per_thread_bufsize-1;
                l->read_start_assignment[i]= l->curr_read_start_offset;
                l->bytes_read[i]=0;
                l->pfreader_mark_data_ready[i]=0;
                memset(l->data_buffers[i],0,l->per_thread_bufsize);
                reader_blocks_free[i]=0;
            }
        }
        struct timespec t=get_pfreader_timeout();
        pthread_cond_timedwait(&l->cond, &l->mutex, &t);
        int counter=0;
        for (i=0;i<l->n_threads;i++) { 
           if (l->read_start_assignment[i]>0) {
               msgprintf(MSG_DEBUG1,"%d threads done,thread %d assigned %ld, bytes_read=%ld, block_offset=%ld, bsz=%ld, remaining=%ld\n",
                      threads_done,i,l->read_start_assignment[i], l->bytes_read[i],
                      l->curr_block_start_offset,blocksize,blocksize-bytes_read);
           } else {
               counter++;
           }
        }
        msgprintf(MSG_DEBUG1, "%d threads actually done\n",counter);
        assert(pthread_mutex_unlock(&l->mutex)==0);
        if ((bytes_read == blocksize)||(threads_done==l->n_threads)) found=true;
    }
    
    return bytes_read;
}
