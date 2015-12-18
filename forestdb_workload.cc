/*
[ Notes ]

12/16/2015 dkao:
* No locks are put around writing/reading integers. But typically only one
* writer for all shared static integers.

12/2/2015 dkao:
* What's the commit frequency during incremental phase?  Remember to set buffer
* cache. Also introduce WAL SIZE when setting forestdb

*/

#include <assert.h>
#include <execinfo.h>
#include <deque>
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
static int lock_prob;
static int buffercachesizeMB;
static int wal_size;
static int incr_commit_interval_ms = 0;
static int incr_compaction_interval = 0;
static int incr_inmem_snapshot_interval_ms = 0;
static int iter_reads_batch_size;

// general
FILE * resultfile;
static bool incr_index_build_done = false;
// This has to be 20. Some hardcoding logic. See fill_key.
const size_t docid_len = 20;
const size_t field_value_len = 50;
//persist_lock needed if we open persisted snapshot for reading
pthread_mutex_t persist_lock;
// Synchronize inmemory snapshot creation and reading
pthread_mutex_t inmem_lock;
// Synchronize close of file handle between incrmental writer thread and reader thread
pthread_mutex_t file_handle_lock;
// store open in memory snapshots
static std::deque<fdb_kvs_handle *> inmem_snapshots;

// control points
static bool incrindexbuilddone = false;
static int num_incr_mutations = 0;
static int create_draw = 0;
static int update_draw = 0;
static int create_performed = 0;
static int update_performed = 0;
static int read_count = 0;
static int read_performed = 0;
static int create_marker = 0;
static int delete_marker = 0;

static int LOG_LEVEL = 0;

// STATS
static int num_snapshots_opened = 0;

void print_call_stack(){
    const int MAX_FRAMES = 128;
    void* callstack[MAX_FRAMES];
    int num_frames = backtrace(callstack, MAX_FRAMES);
    char** strs = backtrace_symbols(callstack, num_frames);
    for (int i = 0; i < num_frames; i++){
        printf("%s\n", strs[i]);
    }
    free(strs);
}

/*
 * Prints stack trace before an assert that is about to fail.
 */
void passert(bool boolean)
{
    if (boolean) return;
    print_call_stack();
    assert(boolean);
}
/* To reuse fdb_doc and preserve the flags and other meta data after an initial
 * fdb_doc_create, make sure you always use these wrap_fdb_* calls which saves
 * the previous state. The fdb_doc's non-pointer values should be kept the same
 * as after the fdb_doc_create call.
 */
void wrap_fdb_get(fdb_kvs_handle * handle, fdb_doc * doc){
    fdb_doc doc_backup;
    doc_backup = *doc;
    
    fdb_status status;
    status = fdb_get(handle, doc);
    if (status != FDB_RESULT_SUCCESS or LOG_LEVEL >= 3){
        printf("fdb_get:");
        fwrite(doc->key, doc->keylen, 1, stdout);
        printf("\n");
    }
    if (status != 0) printf("\nget status %i\n", status);
    passert(status == FDB_RESULT_SUCCESS);

    *doc = doc_backup;
}

void wrap_fdb_set(fdb_kvs_handle * handle, fdb_doc * doc){
    fdb_doc doc_backup;
    doc_backup = *doc;

    fdb_status status;
    status = fdb_set(handle, doc);
    if (status != FDB_RESULT_SUCCESS or LOG_LEVEL >= 3){
        printf("fdb_set:");
        fwrite(doc->key, doc->keylen, 1, stdout);
        printf("\n");
    }
    if (status != 0) printf("\nset status %i\n", status);
    passert(status == FDB_RESULT_SUCCESS);

    *doc = doc_backup;
}

void wrap_fdb_del(fdb_kvs_handle * handle, fdb_doc * doc){
    fdb_doc doc_backup;
    doc_backup = *doc;

    fdb_status status;
    status = fdb_del(handle, doc);
    if (status != FDB_RESULT_SUCCESS or LOG_LEVEL >= 3){
        printf("fdb_del:");
        fwrite(doc->key, doc->keylen, 1, stdout);
        printf("\n");
    }
    if (status != 0) printf("\ndel status %i\n", status);
    passert(status == FDB_RESULT_SUCCESS);

    *doc = doc_backup;
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
    config.buffercache_size = (uint64_t)buffercachesizeMB*1024*1024;//default 30*1GB
    config.wal_threshold = (uint64_t) wal_size;
    kvs_config = fdb_get_default_kvs_config();

    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    passert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &main_handle,"main" , &kvs_config);
    passert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &back_handle,"back" , &kvs_config);
    passert(status == FDB_RESULT_SUCCESS);

    fdb_kvs_close(main_handle);
    fdb_kvs_close(back_handle);
    fdb_close(fhandle);
    return true;
}

void* do_initial_write(void*)
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
    config.wal_threshold = (uint64_t) wal_size;

    kvs_config = fdb_get_default_kvs_config();
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    passert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &main_handle,"main" , &kvs_config);
    passert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &back_handle,"back" , &kvs_config);
    passert(status == FDB_RESULT_SUCCESS);

    /*
     * Main index is a covering index that concatenates field value and docid
     * back index is a docid -> value KV mapping
     */
    main_key_buf = (char *)malloc(sizeof(char) * (docid_len + field_value_len));
    back_key_buf = (char *)malloc(sizeof(char) * docid_len);
    back_value_buf = (char *)malloc(sizeof(char) * field_value_len);

    memset((void *)main_key_buf, 0, sizeof(char) * (docid_len + field_value_len));
    memset((void *)back_key_buf, 0, sizeof(char) * docid_len);
    memset((void *)back_value_buf, 0, sizeof(char) * field_value_len);

    status = fdb_doc_create(&main_doc, (const void *)main_key_buf, (docid_len + field_value_len),
                               NULL, 0, NULL, 0);
    passert(status == FDB_RESULT_SUCCESS);
    status = fdb_doc_create(&back_doc, (const void *)back_key_buf, docid_len,
                               NULL, 0, (const void *)back_value_buf, field_value_len);
    passert(status == FDB_RESULT_SUCCESS);

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
 
        wrap_fdb_set(main_handle, main_doc);
        wrap_fdb_set(back_handle, back_doc);

        numinitdocswritten++;
 
        // Every 300 seconds, call commit, during initial load phase
        if(((clock() - commit_checkpoint)/(double) CLOCKS_PER_SEC) > 300){
            status = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);
            commit_checkpoint = clock();
        }
    }

    //final commit
    status = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);

    gettimeofday(&index_stoptime, NULL);

    fprintf(resultfile,
            "\nnuminitdocs written: %lu \ninitial index build time: %d seconds\n",
            numinitdocswritten, (index_stoptime.tv_sec-index_starttime.tv_sec));

    status = fdb_compact(fhandle, NULL);
    passert(status == FDB_RESULT_SUCCESS);

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

void* do_incremental_mutations(void*)
{
    fdb_file_handle *fhandle;
    fdb_kvs_handle *main_handle;
    fdb_kvs_handle *back_handle;
    fdb_kvs_handle *snapshot;
    fdb_kvs_handle *snapshot_clone;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_doc *main_doc, *back_doc;
    fdb_status status;
    char * main_key_buf, *back_key_buf, *back_value_buf;
    struct timeval index_starttime, index_stoptime;
    clock_t commit_checkpoint;
    clock_t inmem_snapshot_checkpoint;

    config = fdb_get_default_config();
    config.max_writer_lock_prob = lock_prob;
    config.durability_opt = FDB_DRB_ASYNC;
    config.wal_threshold = (uint64_t) wal_size;

    kvs_config = fdb_get_default_kvs_config();
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    passert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &main_handle,"main" , &kvs_config);
    passert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &back_handle,"back" , &kvs_config);
    passert(status == FDB_RESULT_SUCCESS);

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
    passert(status == FDB_RESULT_SUCCESS);
    status = fdb_doc_create(&back_doc, (const void *)back_key_buf, docid_len,
                               NULL, 0, (const void *)back_value_buf, field_value_len);
    passert(status == FDB_RESULT_SUCCESS);

    //start commit clock
    commit_checkpoint = clock();
    inmem_snapshot_checkpoint = clock();
    gettimeofday(&index_starttime, NULL);
    create_marker = numinitdocs;
    int update_marker = 0;

    int which_op = 0;
    setbuf(stdout, NULL);
    while(num_incr_mutations < num_incr_ops){
        which_op = rand() % 100;
        if (which_op < create_draw){
            if (LOG_LEVEL >= 2) printf( "create op\n");

            //generate a new document
            fill_key((char *)back_doc->key, create_marker);
            strgen((char *)back_doc->body, field_value_len);
 
            // Populaate doc->key for main index using information from back_doc 
            memcpy(main_doc->key, back_doc->body, field_value_len);
            memcpy((void *)((char *)main_doc->key + field_value_len), back_doc->key, docid_len);
            
            // Write both documents to both indexes
            wrap_fdb_set(main_handle, main_doc);
            wrap_fdb_set(back_handle, back_doc);

            create_marker++;
            create_performed++;
        } else if (which_op < update_draw) {
            int temp = rand() % (create_marker - delete_marker);
            update_marker = delete_marker + temp;

            if (LOG_LEVEL >= 2) printf("update marker %i\n", update_marker);

            //re-generate an old doc id from marker.
            fill_key((char *)back_doc->key, update_marker);
            // Get the body for the key
            memset((void *) back_doc->body, 0, sizeof(char) * field_value_len);

            wrap_fdb_get(back_handle, back_doc);

            // This is the main doc to delete
            memcpy(main_doc->key, back_doc->body, field_value_len);
            memcpy((void *)((char *)main_doc->key + field_value_len), back_doc->key, docid_len);
            wrap_fdb_del(main_handle, main_doc);

            // new main doc with new field value
            strgen((char *)back_doc->body, field_value_len);
            memcpy(main_doc->key, back_doc->body, field_value_len);

            // Write both documents to both indexes
            wrap_fdb_set(main_handle, main_doc);
            wrap_fdb_set(back_handle, back_doc);

            update_performed++;
        } else if (delete_marker < create_marker) {
            if (LOG_LEVEL >= 2) printf("delete op %i\n", delete_marker);
            //delete draw
            //re-generate an old doc id from marker.
            fill_key((char *)back_doc->key, delete_marker);
            // Get the body for the key
            memset((void *) back_doc->body, 0, sizeof(char) * field_value_len);

            wrap_fdb_get(back_handle, back_doc);

            // This is the main doc to delete
            memcpy(main_doc->key, back_doc->body, field_value_len);
            memcpy((void *)((char *)main_doc->key + field_value_len), back_doc->key, docid_len);
            wrap_fdb_del(main_handle, main_doc);

            // Also delete back_doc
            wrap_fdb_del(back_handle, back_doc);

            delete_marker++;
        }
        num_incr_mutations++;

        // open in-memory snapshot every X miliseconds
        if(((clock() - inmem_snapshot_checkpoint) * 1000 /(double) CLOCKS_PER_SEC) > incr_inmem_snapshot_interval_ms){
            status = fdb_snapshot_open(main_handle, &snapshot, FDB_SNAPSHOT_INMEM); 
            passert(status == FDB_RESULT_SUCCESS);
            ///status = fdb_snapshot_open(snapshot, &snapshot_clone, FDB_SNAPSHOT_INMEM); 
            ///passert(status == FDB_RESULT_SUCCESS);

            pthread_mutex_lock(&inmem_lock);
            ///inmem_snapshots.push_front(snapshot_clone);
            inmem_snapshots.push_front(snapshot);
            pthread_mutex_unlock(&inmem_lock);
            num_snapshots_opened++;
            
            inmem_snapshot_checkpoint = clock();
        }
        // commit every how many miliseconds
        if(((clock() - commit_checkpoint) * 1000 /(double) CLOCKS_PER_SEC) > incr_commit_interval_ms){
           status = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);
           commit_checkpoint = clock();
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

    incrindexbuilddone = true;

    pthread_mutex_lock(&file_handle_lock);
    fdb_close(fhandle);

}


void* do_compact(void*)
{
    fdb_file_handle *fhandle;
    fdb_status status;
    fdb_config config;
    fdb_snapshot_info_t *markers;
    uint64_t num_markers;
    uint64_t upto;
    
    config = fdb_get_default_config();
    config.compaction_mode = FDB_COMPACTION_MANUAL;
    config.wal_threshold = (uint64_t) wal_size;
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    passert(status == FDB_RESULT_SUCCESS);

    fprintf(resultfile, "compactor thread has started\n");

    int compact_num = 0;
    struct timeval compact_starttime, compact_stoptime;
    while(!incrindexbuilddone){
        sleep(incr_compaction_interval);

        //keep at least last five of markers when possible.
        status = fdb_get_all_snap_markers(fhandle, &markers, &num_markers);
        passert(status == FDB_RESULT_SUCCESS);
        if (num_markers > 5){
            upto = 4;
        } else {
            upto = num_markers - 1;
        }
        gettimeofday(&compact_starttime, NULL);
        status = fdb_compact_upto(fhandle, NULL, markers[upto].marker);
        gettimeofday(&compact_stoptime, NULL);
        passert(status == FDB_RESULT_SUCCESS);
        passert(fdb_free_snap_markers(markers, num_markers) == FDB_RESULT_SUCCESS);

        compact_num++;
        fprintf(resultfile,"Compaction run %d took %lu seconds upto %ju\n", compact_num,
                (compact_stoptime.tv_sec - compact_starttime.tv_sec), upto);
    }

    // final compact
    gettimeofday(&compact_starttime, NULL);
    status = fdb_compact(fhandle, NULL);
    gettimeofday(&compact_stoptime, NULL);
    passert(status == FDB_RESULT_SUCCESS);

    compact_num++;
    fprintf(resultfile,"Compaction run %d took %lu seconds to complete\n", compact_num,
            (compact_stoptime.tv_sec - compact_starttime.tv_sec));

    fprintf(resultfile, "compactor thread has ended\n");
    fdb_close(fhandle);
}

void* do_read(void*)
{
    fdb_kvs_handle *snapshot;
    fdb_doc *doc = NULL;
    fdb_status status;
    fdb_iterator *itr_key;

    while(!incrindexbuilddone){
        if (read_performed > num_incr_mutations) continue;

        pthread_mutex_lock(&inmem_lock);
        if (inmem_snapshots.size() == 0){
            pthread_mutex_unlock(&inmem_lock);
            continue;
        }
        while(inmem_snapshots.size()>5){
            snapshot = inmem_snapshots.back();
            inmem_snapshots.pop_back();
            fdb_kvs_close(snapshot);
        }
        snapshot = inmem_snapshots.back();
        pthread_mutex_unlock(&inmem_lock);
        
        status = fdb_iterator_init(
                    snapshot, &itr_key,
                    NULL, 0,
                    NULL, 0,
                    FDB_ITR_NO_DELETES);
        passert(status == FDB_RESULT_SUCCESS);
        read_count = 0;
        do {
            status = fdb_iterator_get(itr_key, &doc);
            if (status != FDB_RESULT_SUCCESS) {
                break;
            }
            fdb_doc_free(doc);
            doc = NULL;
            read_count++;
            read_performed++;
        } while(read_count < iter_reads_batch_size and fdb_iterator_next(itr_key) != FDB_RESULT_ITERATOR_FAIL);
        status = fdb_iterator_close(itr_key);
        itr_key = NULL;
        passert(status == FDB_RESULT_SUCCESS);
    }

    pthread_mutex_unlock(&file_handle_lock);
}

void* do_periodic_stats(void *)
{
    while(!incrindexbuilddone){
        sleep(2);
        printf("\nnumber of creates performed %i", create_performed);
        printf("\nnumber of updates performed %i", update_performed);
        printf("\nnumber of deletes performed %i", delete_marker);
        printf("\nnumber of reads performed %i", read_performed);
        printf("\ncreate marker %i", create_marker);
        printf("\nnum snapshots opened %i", num_snapshots_opened);
        printf("\n");
    }
}

void do_incremental_load()
{

    printf("Incrmental build phase started\n");

    pthread_t incr_threads[4];
    pthread_create(&incr_threads[0], NULL, do_incremental_mutations, NULL);

    // block incrmental thread, to be unlocked by reader thread
    pthread_mutex_lock(&file_handle_lock);
    // also start compactor thread here
    pthread_create(&incr_threads[1], NULL, do_compact, NULL);

    // start reader thread
    pthread_create(&incr_threads[2], NULL, do_read, NULL);

    // start periodic stats
    pthread_create(&incr_threads[3], NULL, do_periodic_stats, NULL);

    for (int i = 0; i<4; i++){
        pthread_join(incr_threads[i], NULL);
    }

    fprintf(resultfile, "\nnumber of creates performed %i", create_performed);
    fprintf(resultfile, "\nnumber of updates performed %i", update_performed);
    fprintf(resultfile, "\nnumber of deletes performed %i", delete_marker);
    fprintf(resultfile, "\nnumber of reads performed %i", read_performed);
    fprintf(resultfile, "\ncreate marker %i", create_marker);
    fprintf(resultfile, "\nnum snapshots opened %i", num_snapshots_opened);
    fprintf(resultfile, "\n");
}

uint64_t get_filesize(const char *filename)
{
    struct stat filestat;
    passert(stat(filename, &filestat) == 0);
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
    // delete ratio not really required because CUD adds up to 100, but this helps checks configuration file.
    int delete_ratio = iniparser_getint(cfg, (char*)"workload:deletes", 1);
    passert(create_ratio + update_ratio + delete_ratio == 100);
    // used to determine what KV op to run later.
    create_draw = create_ratio;
    update_draw = create_draw + update_ratio;

    incr_inmem_snapshot_interval_ms = iniparser_getint(cfg, (char*)"workload:incr_inmem_snapshot_interval_ms", 500);
    incr_commit_interval_ms = iniparser_getint(cfg, (char*)"workload:incr_commit_interval_ms", 500);
    incr_compaction_interval = iniparser_getint(cfg, (char*)"workload:incr_compaction_interval", 10);
    iter_reads_batch_size = iniparser_getint(cfg, (char*)"workload:iter_reads_batch_size", 1000);

    //db_config
    buffercachesizeMB = iniparser_getint(cfg, (char*)"db_config:buffercachesizeMB", 1024);
    wal_size = iniparser_getint(cfg, (char*)"db_config:wal_size", 40960);
    const char *resultfilename = (const char*) iniparser_getstring(cfg, (char*)"general:resultfilename",
                                                                   (char*)"initialsecondary");
    lock_prob = iniparser_getint(cfg, (char*)"db_config:lock_prob", 30);

    resultfile = fopen(resultfilename, "a");
    fprintf(resultfile, "\nNumber of intended initial documents: %d", numinitdocs);
    fprintf(resultfile, "\nNumber of intended ops during incremental phase: %d", num_incr_ops);
    fprintf(resultfile, "\nHow many iterator reads before moving the start key: %d", iter_reads_batch_size);
    fprintf(resultfile, "\nSize of buffer cache: %d MB", buffercachesizeMB);
    fprintf(resultfile, "\n");

    //initialize DB and KV instances
    passert(init_fdb_with_kvs() == true);

    //initialize locks
    passert(pthread_mutex_init(&inmem_lock, NULL) == 0);

    //initialize random generator
    //srand (time(NULL));
    srand(1);
    
    do_initial_load();
    fprintf(resultfile, "\ndatabase file size after initial load: %.4f GB\n", (double)get_filesize(
                                                             "data/secondaryDB")/(1024*1024*1024));
    do_incremental_load(); //incremental load always uses 1 writer
    fprintf(resultfile, "\nfinal database file size: %.4f GB\n", (double)get_filesize(
                                                             "data/secondaryDB")/(1024*1024*1024));
    fprintf(resultfile, "\n");
    
}
