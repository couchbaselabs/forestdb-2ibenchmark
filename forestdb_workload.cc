/*
Notes

12/2/2015 dkao:
* What's the commit frequency during incremental phase?
* Remember to set buffer cache. Also introduce WAL SIZE when setting forestdb
*/

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
static int num_incr_ops;
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
static int create_draw = 0;
static int update_draw = 0;
static int create_performed = 0;
static int update_performed = 0;
static int create_marker = 0;
static int delete_marker = 0;

static int LOG_LEVEL = 1;

void wrap_fdb_get(fdb_kvs_handle * handle, fdb_doc * doc){
    fdb_status status;
    status = fdb_get(handle, doc);
    if (status != FDB_RESULT_SUCCESS){
        printf("fdb_get:");
        fwrite(doc->key, doc->keylen, 1, stdout);
        printf("\n");
    }
    status == 0 ? : printf("\nget status %i\n", status);
    assert(status == 0);
}

void wrap_fdb_set(fdb_kvs_handle * handle, fdb_doc * doc){
    fdb_status status;
    status = fdb_set(handle, doc);
    if (status != FDB_RESULT_SUCCESS or LOG_LEVEL >= 1){
        printf("fdb_set:");
        fwrite(doc->key, doc->keylen, 1, stdout);
        printf("\n");
    }
    status == 0 ? : printf("\nset status %i\n", status);
    assert(status == 0);
}

void wrap_fdb_del(fdb_kvs_handle * handle, fdb_doc * doc){
    fdb_status status;
    status = fdb_del(handle, doc);
    if (status != FDB_RESULT_SUCCESS or LOG_LEVEL >= 1){
        printf("fdb_del:");
        fwrite(doc->key, doc->keylen, 1, stdout);
        printf("\n");
    }
    status == 0 ? : printf("\ndel status %i\n", status);
    assert(status == 0);
}



void fill_key(char* buffer, unsigned long long marker){
    //hash the marker and fill the result into buffer.
    const char *prefix = "secidx";
    sprintf(buffer, "%s", prefix);
    sprintf(buffer+6, "%014d", XXH64(&marker, sizeof(marker), 1));
    //fwrite(buffer, 20, 1, stdout);
    //printf(" fill key marker %i\n", marker);
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
    config.buffercache_size = (uint64_t)buffercachesizeMB*1024*1024;  //1024MB
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
        // Populate back_doc for the back index. 
        fill_key((char *)back_doc->key, numinitdocswritten);
        strgen((char *)back_doc->body, field_value_len);
 
        // Populaate main_doc->key for main index using information from back_doc 
        memcpy(main_doc->key, back_doc->body, field_value_len);
        memcpy((void *)((char *)main_doc->key + field_value_len), back_doc->key, docid_len);
 
        assert(fdb_set(main_handle, main_doc) == FDB_RESULT_SUCCESS);

        wrap_fdb_set(back_handle, back_doc);

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

    fprintf(resultfile,
            "\nnuminitdocs written: %lu \ninitial index build time: %d seconds\n",
            numinitdocswritten, (index_stoptime.tv_sec-index_starttime.tv_sec));

    free(main_key_buf);
    free(back_key_buf);
    free(back_value_buf);
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

    printf("Initial build phase started\n");

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


void* do_incremental_mutations (void*)
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

    status = fdb_doc_create(&main_doc, (const void *)main_key_buf, (docid_len + field_value_len),
                               NULL, 0, NULL, 0);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_doc_create(&back_doc, (const void *)back_key_buf, docid_len,
                               NULL, 0, (const void *)back_value_buf, field_value_len);
    assert(status == FDB_RESULT_SUCCESS);

    //start commit clock
    commit_checkpoint = clock();
    gettimeofday(&index_starttime, NULL);
    int num_ops = 0;
    create_marker = numinitdocs;
    int update_marker = 0;

    int which_op = 0;
    setbuf(stdout, NULL);
    while(num_ops < num_incr_ops){
        which_op = rand() % 100;
        if (which_op < create_draw){
            printf( "create op\n");
            //generate a new document
            fill_key((char *)back_doc->key, create_marker);
            strgen((char *)back_doc->body, field_value_len);
 
            // Populaate doc->key for main index using information from back_doc 
            memcpy(main_doc->key, back_doc->body, field_value_len);
            memcpy((void *)((char *)main_doc->key + field_value_len), back_doc->key, docid_len);
            
            // Write both documents to both indexes
            assert(fdb_set(main_handle, main_doc) == FDB_RESULT_SUCCESS);
            wrap_fdb_set(back_handle, back_doc);

            create_marker++;
            create_performed++;
        } else if (which_op < update_draw) {
            int temp = rand() % (create_marker - delete_marker);
            update_marker = delete_marker + temp;
            printf("update marker %i\n", update_marker);

            //re-generate an old doc id from marker.
            fill_key((char *)back_doc->key, update_marker);
            // Get the body for the key
            memset((void *) back_doc->body, 0, sizeof(char) * field_value_len);

            wrap_fdb_get(back_handle, back_doc);

            // This is the main doc to delete
            memcpy(main_doc->key, back_doc->body, field_value_len);
            memcpy((void *)((char *)main_doc->key + field_value_len), back_doc->key, docid_len);
            assert(fdb_del(main_handle, main_doc) == FDB_RESULT_SUCCESS);

            // new main doc with new field value
            strgen((char *)back_doc->body, field_value_len);
            memcpy(main_doc->key, back_doc->body, field_value_len);

            // Write both documents to both indexes
            assert(fdb_set(main_handle, main_doc) == FDB_RESULT_SUCCESS);
            wrap_fdb_set(back_handle, back_doc);

            update_performed++;
        } else if (delete_marker < create_marker) {
            printf("delete op %i\n", delete_marker);
            //delete draw
            //re-generate an old doc id from marker.
            fill_key((char *)back_doc->key, delete_marker);
            // Get the body for the key
            memset((void *) back_doc->body, 0, sizeof(char) * field_value_len);

            wrap_fdb_get(back_handle, back_doc);

            // This is the main doc to delete
            memcpy(main_doc->key, back_doc->body, field_value_len);
            memcpy((void *)((char *)main_doc->key + field_value_len), back_doc->key, docid_len);
            assert(fdb_del(main_handle, main_doc) == FDB_RESULT_SUCCESS);

            // Also delete back_doc
            wrap_fdb_del(back_handle, back_doc);

            delete_marker++;
        }
        num_ops++;

        // only makes sense to consider commit after a fdb_set.
        if (which_op < update_draw){
            if(((clock() - commit_checkpoint) * 1000 /(double) CLOCKS_PER_SEC) > 100){
               status = fdb_commit(fhandle, FDB_COMMIT);
               commit_checkpoint = clock();
            }
        }

    } // while loop 

    //final commit
    status = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);

    gettimeofday(&index_stoptime, NULL);

    fprintf(resultfile,
            "\nincrmental index build time: %d seconds\n",
            (index_stoptime.tv_sec-index_starttime.tv_sec));

    free(main_key_buf);
    free(back_key_buf);
    free(back_value_buf);
    fdb_doc_free(main_doc);
    fdb_doc_free(back_doc);
    fdb_kvs_close(main_handle);
    fdb_kvs_close(back_handle);
    fdb_close(fhandle);
}

void do_incremental_load()
{

    printf("Incrmental build phase started\n");

    pthread_t incr_threads[3];
    pthread_create(&incr_threads[0], NULL, do_incremental_mutations, NULL);

    // also start compactor thread here

    // start reader thread

    for (int i = 0; i<1; i++){
        pthread_join(incr_threads[i], NULL);
    }

    fprintf(resultfile, "\nnumber of creates performed %i", create_performed);
    fprintf(resultfile, "\nnumber of updates performed %i", update_performed);
    fprintf(resultfile, "\nnumber of deletes performed %i", delete_marker);
    fprintf(resultfile, "\ncreate marker %i", create_marker);
    fprintf(resultfile, "\n");
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
    num_incr_ops = iniparser_getint(cfg, (char*)"workload:num_incr_ops", 1000000);

    // out of a hundred, the distribution of the CUD ops.
    int create_ratio = iniparser_getint(cfg, (char*)"workload:creates", 1);
    int update_ratio = iniparser_getint(cfg, (char*)"workload:updates", 98);
    // delete ratio becomes symbolic. Helps checks user input.
    int delete_ratio = iniparser_getint(cfg, (char*)"workload:deletes", 1);
    assert(create_ratio + update_ratio + delete_ratio == 100);
    create_draw = create_ratio;
    update_draw = create_draw + update_ratio;

    read_batch  = iniparser_getint(cfg, (char*)"workload:iter_read_batch_size", 1000);
    buffercachesizeMB = iniparser_getint(cfg, (char*)"db_config:buffercachesizeMB", 1024);
    const char *resultfilename = (const char*) iniparser_getstring(cfg, (char*)"general:resultfilename",
                                                                   (char*)"initialsecondary");
    lock_prob = iniparser_getint(cfg, (char*)"db_config:lock_prob", 100);

    resultfile = fopen(resultfilename, "a");
    fprintf(resultfile, "\nNumber of intended initial documents: %d", numinitdocs);
    fprintf(resultfile, "\nNumber of intended ops during incremental phase: %d", num_incr_ops);
    fprintf(resultfile, "\nHow many iterator reads before moving the start key: %d", read_batch);
    fprintf(resultfile, "\nSize of buffer cache: %d MB", buffercachesizeMB);
    fprintf(resultfile, "\n");

    //initialize DB and KV instances
    assert(init_fdb_with_kvs() == true);

    //initialize random generator
    //srand (time(NULL));
    srand(1);
    
    do_initial_load();
    fprintf(resultfile, "\ndatabase file size after initial load: %.4f GB", (double)get_filesize(
                                                             "data/secondaryDB")/(1024*1024*1024));
    do_incremental_load(); //incremental load always uses 1 writer
    fprintf(resultfile, "\nfinal database file size: %.4f GB", (double)get_filesize(
                                                             "data/secondaryDB")/(1024*1024*1024));
    fprintf(resultfile, "\n");
    
}
