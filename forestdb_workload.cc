#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>

#include "libforestdb/forestdb.h"
#include "iniparser.h"
#include "strgen.h"
#include "xxhash.h"

// from config
static int numinitdocs;
static int numincrdocs;
static int read_batch;
static int lock_prob;
static int buffercachesizeMB;
static fdb_commit_opt_t FDB_COMMIT = FDB_COMMIT_MANUAL_WAL_FLUSH;

// general
FILE * resultfile;
static bool incr_index_build_done = false;
// This has to be 20. Some hardcoding logic. See fill_key.
const size_t docid_len = 20;
const size_t field_value_len = 50;

// control points
static bool indexbuilddone = false;
static bool incrindexbuilddone = false;

void fill_key(char* buffer, unsigned long long marker){
    //hash the marker and fill the result into buffer.
    const char *prefix = "secidx";
    sprintf(buffer, "%s", prefix);
    sprintf(buffer+6, "%014d", XXH64(&marker, sizeof(marker), 1));
}

int init_fdb_with_kvs()
{
    fdb_file_handle *fhandle;
    fdb_kvs_handle *main_handle;
    fdb_kvs_handle *back_handle;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_status status;

    config = fdb_get_default_config();
    config.max_writer_lock_prob = lock_prob;
    config.durability_opt = FDB_DRB_ASYNC;
    config.compaction_mode = FDB_COMPACTION_MANUAL;
    config.buffercache_size = (uint64_t)buffercachesizeMB*1024*1024;  //1GB
    kvs_config = fdb_get_default_kvs_config();

    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &main_handle,"main" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &back_handle,"back" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);

    fdb_kvs_close(main_handle);
    fdb_kvs_close(back_handle);
    fdb_close(fhandle);
    return true;
}

void * do_initial_write(void*)
{
    fdb_file_handle *fhandle;
    fdb_kvs_handle *main_handle;
    fdb_kvs_handle *back_handle;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_doc *main_doc, *back_doc;
    fdb_status status;
    char * main_key_buf, *back_key_buf, *back_value_buf;
    struct timeval index_starttime, index_stoptime;
    clock_t commit_checkpoint;

    config = fdb_get_default_config();
    config.max_writer_lock_prob = lock_prob;
    config.durability_opt = FDB_DRB_ASYNC;

    kvs_config = fdb_get_default_kvs_config();
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &main_handle,"main" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &back_handle,"back" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);

    /*
     * Main index is a covering index that concatenates field value and docid
     * back index is a docid -> value mapping
     * Initialize the buffers and docs for main and back indexes.
     */
    main_key_buf = (char *)malloc(sizeof(char) * (docid_len + field_value_len));
    back_key_buf = (char *)malloc(sizeof(char) * docid_len);
    back_value_buf = (char *)malloc(sizeof(char) * field_value_len);

    memset((void *)main_key_buf, 0, sizeof(char) * (docid_len + field_value_len));
    memset((void *)back_key_buf, 0, sizeof(char) * docid_len);
    memset((void *)back_value_buf, 0, sizeof(char) * field_value_len);

    status = FDB_RESULT_SUCCESS;
    status = fdb_doc_create(&main_doc, (const void *)main_key_buf, (docid_len + field_value_len),
                               NULL, 0, NULL, 0);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_doc_create(&back_doc, (const void *)back_key_buf, docid_len,
                               NULL, 0, (const void *)back_value_buf, field_value_len);
    assert(status == FDB_RESULT_SUCCESS);

    //start commit clock
    commit_checkpoint = clock();
    gettimeofday(&index_starttime, NULL);
    int numinitdocswritten = 0;

    while(numinitdocswritten<numinitdocs){
        // Populate doc for the back index. 
        strgen((char *)back_doc->body, field_value_len);
        back_doc->bodylen = field_value_len;
        fill_key((char *)back_doc->key, numinitdocswritten);
 
        // Populaate doc->key for main index using information from back_doc 
        memcpy(main_doc->key, back_doc->body, field_value_len);
        memcpy((void *)((char *)main_doc->key + field_value_len), back_doc->key, docid_len);
 
        do{
            status = fdb_set(main_handle, main_doc);
        } while(status!=FDB_RESULT_SUCCESS);
        numinitdocswritten++;
        do{
           status = fdb_set(back_handle, back_doc);
        } while(status!=FDB_RESULT_SUCCESS);
        numinitdocswritten++;
 
        // Every 300 seconds, call commit, during initial load phase
        if(((clock() - commit_checkpoint)/(double) CLOCKS_PER_SEC) > 300){
           status = fdb_commit(fhandle, FDB_COMMIT);
           commit_checkpoint = clock();
        }
    }

    //final commit
    status = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);

    gettimeofday(&index_stoptime, NULL);
    indexbuilddone = true;
    fprintf(resultfile, "\ninitial index build time: %d seconds\n",
                           (index_stoptime.tv_sec-index_starttime.tv_sec));
    printf("\nnuminitdocs written: %lu \ninitial index build time: %d seconds\n",
              numinitdocswritten,(index_stoptime.tv_sec-index_starttime.tv_sec));
    free(main_key_buf);
    free(back_key_buf);
    fdb_doc_free(main_doc);
    fdb_doc_free(back_doc);
    fdb_kvs_close(main_handle);
    fdb_kvs_close(back_handle);
    fdb_close(fhandle);
}

void do_initial_load()
{
    //Only one writer thread during initial load
    //I agree that loops are unnecessary

    int dummy = 0;
    pthread_t bench_thread[1];

    //spawn writer threads
    for (int i=0; i<1; i++){
        pthread_create(&bench_thread[i], NULL, do_initial_write, NULL);
    }

    for (int i = 0; i<1; i++){
        pthread_join(bench_thread[i],NULL);
    }
}


void* do_incremental_writes (void*)
{
}

int do_incremental_load()
{
    pthread_t bench_thread[2];
    pthread_create(&bench_thread[0], NULL, do_incremental_writes, NULL);
}

uint64_t get_filesize(const char *filename)
{
    struct stat filestat;
    assert(stat(filename, &filestat) == 0);
    return filestat.st_size;
}

int main(int argc, char* args[])
{
    // get config
    dictionary *cfg = iniparser_new((char*) "./bench_config.ini");
    numinitdocs = iniparser_getint(cfg, (char*)"workload:numinitdocs", 1000000);
    numincrdocs = iniparser_getint(cfg, (char*)"workload:numincrdocs", 1000000);
    read_batch  = iniparser_getint(cfg, (char*)"workload:iter_read_batch_size", 1000);
    buffercachesizeMB = iniparser_getint(cfg, (char*)"db_config:buffercachesizeMB", 1024);
    const char *resultfilename = (const char*) iniparser_getstring(cfg, (char*)"general:resultfilename",
                                                                   (char*)"initialsecondary");
    lock_prob = iniparser_getint(cfg, (char*)"db_config:lock_prob", 100);

    resultfile = fopen(resultfilename, "a");
    fprintf(resultfile, "\nNumber of initial documents: %d", numinitdocs);
    fprintf(resultfile, "\nNumber of incremental documents: %d", numincrdocs);
    fprintf(resultfile, "\nHow many iterator reads before moving the start key: %d", read_batch);
    fprintf(resultfile, "\nSize of buffer cache: %d MB", buffercachesizeMB);
    fprintf(resultfile, "\n");

    //initialize DB and KV instances
    assert( init_fdb_with_kvs() == true);

    //initialize random generator
    srand (time(NULL));
    
    do_initial_load();
    fprintf(resultfile, "\ndatabase file size after initial load: %.2f GB", (double)get_filesize(
                                                             "data/secondaryDB")/(1024*1024*1024));
    do_incremental_load(); //incremental load always uses 1 writer
    fprintf(resultfile, "\nfinal database file size: %.2f GB", (double)get_filesize(
                                                             "data/secondaryDB")/(1024*1024*1024));
    fprintf(resultfile, "\n");
    
}
