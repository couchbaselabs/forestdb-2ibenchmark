#include "forestdb_workload.h"
#include "strgen.h"
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <pthread.h>
#include <queue>
#include <deque>
#include <dirent.h>
#include "iniparser.h"

#define SEQ   1
#define RANGE 2
#define COMMIT_INTERVAL 5

//static fdb_kvs_handle *snapshot[5];
static FILE *resultfile;
static FILE *statsfile;
static FILE *writestatsfile;
static int snaphist [7];
static int numinitdocs;
static int numcommits;
static int primarykey;
static int lock_prob;
static int numinitdocswritten;
static int numincrmutations;
static int numincrmutationsdone;
static int buffercachesizeMB;
static bool num_wthreads;
static int num_rthreads;
static int read_pattern;
static int max_itr_reads;
static int num_snaps_opened;
static int wbatch_size;
static int indexbuilddone;
static int incrindexbuilddone;
static pthread_mutex_t lock;
static std::queue<std::pair<void *,size_t> > deleteQueue_kvs1, deleteQueue_kvs2; 
static std::deque<fdb_kvs_handle *> inmem_snapshots; 
static fdb_commit_opt_t FDB_COMMIT;
//foreward declaration of compactor
void *compactor(void *arg);

int lockmemory(int buffercachesizeMB)
{
   struct sysinfo mysysinfo;
   unsigned long lockmem, mem_locked;
   unsigned long buf_cachesiz;
   void *mem;
   int status = sysinfo(&mysysinfo);
   assert(status == 0);
   buf_cachesiz = buffercachesizeMB*1024*1024;
   lockmem = mysysinfo.totalram - 0.2*mysysinfo.totalram - buf_cachesiz;
   mem_locked = 0;
   while(mem_locked<lockmem)
   {
        mem = malloc(4096);//4 KB at a time
        mem_locked = mem_locked + 4096;
   }
   return lockmem;    
}


void getnextprimarykey(char* buffer){
     
     const char *prefix = "secidx";
     sprintf(buffer, "%s", prefix);
     sprintf(buffer+6, "%014d", primarykey++);   

}

void getnextprimarykeylookup(char* buffer){
     
     int randkey;
     const char *prefix = "secidx";
     sprintf(buffer, "%s", prefix);
     randkey = rand() % numinitdocs;
     sprintf(buffer+6, "%014d", randkey);   

}

int gethistindex(long time){

     if(time>0 && time<10000)
        return 0;
     else if(time>10000 && time<50000)
        return 1;
     else if(time>50000 && time<100000)
        return 2;
     else if(time>100000 && time<200000)
        return 3;
     else if(time>200000 && time<500000)
        return 4;
     else if(time>500000 && time<1000000)
        return 5;
     else if(time>1000000)
        return 6;
}


uint64_t get_filesize(const char *filename)
{
    struct stat filestat;
    stat(filename, &filestat);
    return filestat.st_size;
}

fdb_status fdb_doc_new(fdb_doc **doc, const void *key, size_t keylen,
                       const void *meta, size_t metalen,
                       const void *body, size_t bodylen)
{
     memset((*doc)->key, 0, sizeof(char)*keylen);
     memcpy((*doc)->key, key, keylen);
     (*doc)->keylen = keylen;
     (*doc)->meta = NULL;
     (*doc)->metalen = 0;
     (*doc)->body = NULL;
     (*doc)->bodylen = 0;

}

int init_fdb_with_kvs(fdb_kvs_config* kvs_list [], size_t num_kvs)
{

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle1;
    fdb_kvs_handle *kvhandle2;
    fdb_status status;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_seqnum_t seqnum;

    config = fdb_get_default_config();
    config.max_writer_lock_prob = lock_prob;
    config.durability_opt = FDB_DRB_ASYNC;
    config.compaction_mode = FDB_COMPACTION_MANUAL;
    config.buffercache_size = (uint64_t)buffercachesizeMB*1024*1024;  //1GB
    //config.num_wal_partitions = 8;
    kvs_config = fdb_get_default_kvs_config();

    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &kvhandle1,"main" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &kvhandle2,"back" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
   
    fdb_kvs_close(kvhandle1); 
    fdb_kvs_close(kvhandle2); 
    fdb_close(fhandle); 
    return true;
}

void *do_deletes(void *arg){};

void *do_writes(void *arg)
{

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle1;
    fdb_kvs_handle *kvhandle2;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_doc *doc1, *doc2;
    fdb_snapshot_info_t *markers;
    uint64_t num_markers;
    //std::vector<fdb_doc *> kvs1_deletelist, kvs2_deletelist;
    fdb_status status;
    size_t keylen;
    size_t valuelen;
    size_t mainkeylen;
    char *keybuf1, *keybuf2, *valuebuf2, *delbuf1, *delbuf2;
    clock_t start;
    int deldoccounter; //use the counter to capture every nth key into delete list
    int commitflag;    //to reset the flag after every delete
    struct timeval index_starttime, index_stoptime;
    config = fdb_get_default_config();
    config.max_writer_lock_prob = lock_prob;
    config.durability_opt = FDB_DRB_ASYNC;
    kvs_config = fdb_get_default_kvs_config();
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle1,"main" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle2,"back" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    keylen = 20; 
    valuelen = 50; 
    mainkeylen = 70; 
    keybuf1 = (char *)malloc(sizeof(char)*mainkeylen);
    keybuf2 = (char *)malloc(sizeof(char)*keylen);
    valuebuf2 = (char *)malloc(sizeof(char)*valuelen);
    memset((void *)keybuf1, 0, sizeof(char)*mainkeylen);
    memset((void *)keybuf2, 0, sizeof(char)*keylen);
    memset((void *)valuebuf2, 0, sizeof(char)*valuelen);

    status = FDB_RESULT_SUCCESS;
    status = fdb_doc_create(&doc1, (const void *)keybuf1, mainkeylen, 
                               NULL, 0, NULL, 0);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_doc_create(&doc2, (const void *)keybuf2, keylen, 
                               NULL, 0, (const void *)valuebuf2, valuelen);
    assert(status == FDB_RESULT_SUCCESS);
    
    //start commit clock
    start = clock();
    gettimeofday(&index_starttime, NULL);
    deldoccounter = 0;
    numinitdocswritten = 0; 
    while(numinitdocswritten<numinitdocs){
       
       //reuse the fdb_doc structure created above to insert new KV pairs 
       //insert primary key as key and secondary key as value into back index
       strgen((char *)doc2->body, valuelen);
       doc2->bodylen = valuelen;
       getnextprimarykey((char *)doc2->key); 
       
       //copy secondary key and primary key from doc2 to doc1 
       memcpy(doc1->key, doc2->body, valuelen);  
       memcpy((void *)((char *)doc1->key+valuelen), doc2->key, keylen);
       
       do{
           status = fdb_set(kvhandle1, doc1);
       } while(status!=FDB_RESULT_SUCCESS); 
       
       do{
          status = fdb_set(kvhandle2, doc2);
       } while(status!=FDB_RESULT_SUCCESS); 
       numinitdocswritten++;
       deldoccounter++;
       

       //every 300 seconds,  call commit
       if(((clock()-start)/(double) CLOCKS_PER_SEC)>300){
          status = fdb_commit(fhandle, FDB_COMMIT);
          start = clock();
       }
    }
    
    //final commit
    status = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);

    gettimeofday(&index_stoptime, NULL);
    indexbuilddone = true;
    fprintf(resultfile, "\ninitial index build time: %lu seconds\n", 
                           (index_stoptime.tv_sec-index_starttime.tv_sec));
    printf("\nnuminitdocs written: %d \ninitial index build time: %lu seconds\n", 
              numinitdocswritten,(index_stoptime.tv_sec-index_starttime.tv_sec));
    printf("\ndelete queue size: %lu\n", deleteQueue_kvs1.size()+deleteQueue_kvs2.size()); 
    free(keybuf1);
    free(keybuf2);
    free(valuebuf2);
    fdb_doc_free(doc1);  
    fdb_doc_free(doc2);
    //status = fdb_compact(fhandle, NULL);
    status = fdb_get_all_snap_markers(fhandle, &markers, &num_markers);
    status = fdb_compact_upto(fhandle, NULL, markers[4].marker);//manual compaction
    status = fdb_free_snap_markers(markers, num_markers);
    fdb_kvs_close(kvhandle1); 
    fdb_kvs_close(kvhandle2); 
    fdb_close(fhandle);  
    return arg;
}

void __attribute__((noinline)) gdb_loop() {
    fprintf(stdout, "gdb --pid=%d\n", getpid());
    while(1);
}

void *do_incremental_writes(void *arg)
{

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle1;
    fdb_kvs_handle *kvhandle2;
    fdb_kvs_handle *snapshot;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_doc *doc1, *doc2;
    //std::vector<fdb_doc *> kvs1_deletelist, kvs2_deletelist;
    fdb_status status;
    size_t keylen;
    size_t valuelen;
    size_t mainkeylen;
    char *keybuf1, *keybuf2, *valuebuf2;
    clock_t start;
    int oldnumincrdone;
    int deldoccounter; //use the counter to capture every nth key into delete list
    int commitflag;    //to reset the flag after every delete
    struct timeval writesample_start, writesample_curr;
    struct timeval index_starttime, index_stoptime;
    struct timeval inmem_starttime, inmem_stoptime; 
    struct timeval inmem_intervalprev, inmem_intervalcurr; 
    struct timeval commit_intervalprev, commit_intervalcurr; 
    struct timeval set_starttime, set_stoptime;
    struct timeval get_starttime, get_stoptime;
    struct timeval del_starttime, del_stoptime;
    unsigned long get_avg = 0, set_avg = 0, del_avg = 0;
    unsigned long num_opened_snapshots = 0;
    int *argcompact;
    pthread_t compactor_thread;
    config = fdb_get_default_config();
    config.max_writer_lock_prob = lock_prob;
    config.durability_opt = FDB_DRB_ASYNC;
    kvs_config = fdb_get_default_kvs_config();
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle1,"main" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle2,"back" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    keylen = 20; 
    valuelen = 50; 
    mainkeylen = 70; 
    keybuf1 = (char *)malloc(sizeof(char)*mainkeylen);
    keybuf2 = (char *)malloc(sizeof(char)*keylen);
    valuebuf2 = (char *)malloc(sizeof(char)*valuelen);
    memset((void *)keybuf1, 0, sizeof(char)*mainkeylen);
    memset((void *)keybuf2, 0, sizeof(char)*keylen);
    memset((void *)valuebuf2, 0, sizeof(char)*valuelen);

    status = FDB_RESULT_SUCCESS;
    status = fdb_doc_create(&doc1, (const void *)keybuf1, mainkeylen, 
                               NULL, 0, NULL, 0);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_doc_create(&doc2, (const void *)keybuf2, keylen, 
                               NULL, 0, (const void *)valuebuf2, valuelen);
    assert(status == FDB_RESULT_SUCCESS);
    //spawn compactor thread
    argcompact = (int *)malloc(sizeof(int));
    *argcompact = 0;
    pthread_create(&compactor_thread, NULL, compactor, (void *)argcompact);   
     
    //start commit clock
    start = clock();
    gettimeofday(&index_starttime, NULL);
    gettimeofday(&writesample_start, NULL);
    gettimeofday(&commit_intervalprev, NULL);
    gettimeofday(&inmem_intervalprev, NULL);
    deldoccounter = 0;
    numincrmutationsdone = 0;
    oldnumincrdone = 0; 
    while(numincrmutationsdone<numincrmutations){
       
       //create fdb_doc struct with next key 
       getnextprimarykeylookup((char *)doc2->key);
       //fetch the doc body and meta for the random key
       gettimeofday(&get_starttime, NULL);
       do{
          status = fdb_get(kvhandle2, doc2);
          if(status != FDB_RESULT_SUCCESS){
              fprintf(resultfile, "\n unsuccesful key: %s status: %d\n",(char *)doc2->key, status);
              fflush(resultfile);
          }
       } while(status!=FDB_RESULT_SUCCESS); 
       gettimeofday(&get_stoptime, NULL);
       get_avg = get_avg + (get_stoptime.tv_sec*1e6 + get_stoptime.tv_usec)-(get_starttime.tv_sec*1e6 + get_starttime.tv_usec);
        
       //reuse the fdb_doc structure created above to insert new KV pairs 
       doc1->keylen = doc2->bodylen + doc2->keylen;
       
       //update doc2 body
       strgen((char *)doc2->body, doc2->bodylen);
       gettimeofday(&set_starttime, NULL);
       do{
          status = fdb_set(kvhandle2, doc2);
       } while(status!=FDB_RESULT_SUCCESS); 
       gettimeofday(&set_stoptime, NULL);
       set_avg = set_avg + (set_stoptime.tv_sec*1e6 + set_stoptime.tv_usec)-(set_starttime.tv_sec*1e6 + set_starttime.tv_usec);
        
       //delete doc1 from main index
       gettimeofday(&del_starttime, NULL);
       do{
           status = fdb_del(kvhandle1, doc1);
       } while(status!=FDB_RESULT_SUCCESS); 
       gettimeofday(&del_stoptime, NULL);
       del_avg = del_avg + (del_stoptime.tv_sec*1e6 + del_stoptime.tv_usec)-(del_starttime.tv_sec*1e6 + del_starttime.tv_usec);
       
       //add new entry in main index
       memcpy(doc1->key, doc2->body, doc2->bodylen);  
       do{
           status = fdb_set(kvhandle1, doc1);
       } while(status!=FDB_RESULT_SUCCESS); 
       numincrmutationsdone++;
       deldoccounter++;
       
       gettimeofday(&inmem_intervalcurr, NULL);
       //open in-memory snapshots every 1 second
       long inmemt = (inmem_intervalcurr.tv_sec*1e6 + inmem_intervalcurr.tv_usec)-(inmem_intervalprev.tv_sec*1e6 + inmem_intervalprev.tv_usec);
       if(inmemt > 1000000){
          gettimeofday(&inmem_starttime, NULL);
          do{
             status = fdb_snapshot_open(kvhandle1, &snapshot, FDB_SNAPSHOT_INMEM);
          }while(status != FDB_RESULT_SUCCESS);
          gettimeofday(&inmem_stoptime, NULL); 
          gettimeofday(&inmem_intervalprev, NULL);
          num_opened_snapshots++;
          
          //lock the inmem_snapshots deque for isolation with reader threads
          pthread_mutex_lock(&lock);
          if(inmem_snapshots.size()==5){
             status = fdb_kvs_close(inmem_snapshots.back());
             assert(status == FDB_RESULT_SUCCESS);
             inmem_snapshots.pop_back();
          }  
          inmem_snapshots.push_front(snapshot);
          pthread_mutex_unlock(&lock);
          
          //if(num_opened_snapshots%5 == 0){
          long time = (inmem_stoptime.tv_sec*1e6 + inmem_stoptime.tv_usec) - (inmem_starttime.tv_sec*1e6 + inmem_starttime.tv_usec);
          snaphist[gethistindex(time)]++;
            
          //fprintf(statsfile, "snapshot num:i %d took  %lu microseconds\n", num_opened_snapshots, time); 
              
       }
       
       gettimeofday(&commit_intervalcurr, NULL);
       //commit every 5 seconds
       long tcommit = (commit_intervalcurr.tv_sec*1e6 + commit_intervalcurr.tv_usec)-(commit_intervalprev.tv_sec*1e6 + commit_intervalprev.tv_usec);
       if(tcommit>(COMMIT_INTERVAL*1000000)){
          gettimeofday(&writesample_curr, NULL);
          long wtime = (writesample_curr.tv_sec*1e6 + writesample_curr.tv_usec) - (writesample_start.tv_sec*1e6 + writesample_start.tv_usec);
          long newincr = numincrmutationsdone-oldnumincrdone;
          /*if(newincr < 100){
              fprintf(writestatsfile, "current write throughput is less than 100");
              fflush(writestatsfile);
              gdb_loop();
          }*/
          gettimeofday(&writesample_start, NULL);
          status = fdb_commit(fhandle, FDB_COMMIT);
          gettimeofday(&commit_intervalprev, NULL);
          numcommits++;
          fprintf(writestatsfile, "current write throughput, %lu, commitnumber, %lu\n",
                                                             (newincr*1000000)/wtime, numcommits); 
          oldnumincrdone = numincrmutationsdone;
       }
    }
    get_avg /= numincrmutationsdone; 
    set_avg /= numincrmutationsdone; 
    del_avg /= numincrmutationsdone; 
    
    gettimeofday(&index_stoptime, NULL);
    incrindexbuilddone = true;
    fprintf(resultfile, "\nincremental index build time: %lu seconds\n", 
                           (index_stoptime.tv_sec-index_starttime.tv_sec));
    fprintf(resultfile, "\navg set time: %lu us, avg get time: %lu, avg del time: %lu \n", 
                           set_avg, get_avg, del_avg);
    printf("\nnumincrmutations written: %d \nincremental index build time: %lu seconds\n", 
              numincrmutationsdone,(index_stoptime.tv_sec-index_starttime.tv_sec));
    free(keybuf1);
    free(keybuf2);
    free(valuebuf2);
    fdb_doc_free(doc1);  
    fdb_doc_free(doc2);  
    //wait for compactor thread to terminate
    pthread_join(compactor_thread,NULL);
    sleep(20);
    fdb_kvs_close(kvhandle1); 
    fdb_kvs_close(kvhandle2); 
    fdb_close(fhandle);  
    free(argcompact);
    return arg;
}

void *do_reads(void *arg)
{
   fdb_kvs_handle *readsnap;
   fdb_iterator *itr_kv;
   fdb_doc *doc = NULL;
   fdb_status status;
   int itr_reads;
   int keylen;
   struct timeval read_starttime, read_stoptime;
   unsigned long numreads = 0;
   int tid = *(int *)arg;
   void *startkey;
   int startkeylen;
   startkey = malloc(5*sizeof(char));//generate random start keys of length 5 chars
   startkeylen = 5;
   printf("\nstarting reader thread id: %d", tid); 
   
   assert(status == FDB_RESULT_SUCCESS);
   readsnap = NULL;
   startkey = NULL;
   startkeylen = 0;
   gettimeofday(&read_starttime, NULL);
   while(!incrindexbuilddone){ 
         
       max_itr_reads = 1; 
       strgen((char *)startkey, startkeylen); 
       //always open in-memory snapshots if there are writes between COMMIT
       //and snapshot_open
       //lock the inmem_snapshots deque for isolation with reader threads
       if(inmem_snapshots.size() >= 1){
          pthread_mutex_lock(&lock);
          fdb_kvs_handle *oldest = inmem_snapshots.back(); 
          status = fdb_snapshot_open(oldest, &readsnap, FDB_SNAPSHOT_INMEM);
          pthread_mutex_unlock(&lock);
       }
       else
          continue;
       
       status = fdb_iterator_init(readsnap, &itr_kv,
                                     NULL, 0, NULL, 0,
                                     FDB_ITR_NONE);
       assert(status == FDB_RESULT_SUCCESS);
       status = fdb_iterator_get(itr_kv, &doc);
       assert (status == FDB_RESULT_SUCCESS);
       numreads++;
       status = fdb_iterator_close(itr_kv);
       status = fdb_kvs_close(readsnap);
   } 
   
   gettimeofday(&read_stoptime, NULL); 
   fdb_doc_free(doc);  
   free(startkey);
   printf("\nreader thread id: %d num of reads done: %lu", tid, numreads); 
   fprintf(resultfile,"\nreader thread id: %d  num of reads done: %lu time elapsed for reads:%lu"
                      ,tid, numreads, (read_stoptime.tv_sec-read_starttime.tv_sec)); 
   return arg;
}

void *compactor(void *arg)
{
    
    fdb_file_handle *fhandle;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_snapshot_info_t *markers;
    uint64_t num_markers;
    fdb_status status;
    clock_t start;
    struct timeval compact_starttime, compact_stoptime;
    int compactrunnum;
    config = fdb_get_default_config();
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    sleep(10);
    printf("compactor starting");
    fprintf(resultfile, "compactor thread has started");
    compactrunnum = 0; 
    while(!incrindexbuilddone){
       gettimeofday(&compact_starttime, NULL); 
       fprintf(resultfile,"\nCommit number prior to compaction: %d", numcommits);
       status = fdb_get_all_snap_markers(fhandle, &markers, &num_markers);
       status = fdb_compact_upto(fhandle, NULL, markers[4].marker);//manual compaction every 60 seconds
       gettimeofday(&compact_stoptime, NULL); 
       status = fdb_free_snap_markers(markers, num_markers);
       //status = fdb_compact(fhandle, NULL);
       fprintf(resultfile,"\nCommit number after compaction: %d", numcommits);
       compactrunnum++;
       fprintf(resultfile,"\nCompaction run %d took %lu seconds to complete", compactrunnum,
                                                        (compact_stoptime.tv_sec-compact_starttime.tv_sec));
       sleep(60);
    }
    fdb_close(fhandle);
    return arg;
}

int do_initial_load(int num_wthreads, int num_rthreads, int num_delthreads)
{
   pthread_t bench_thread[num_wthreads+num_rthreads+num_delthreads];
   int *arg[num_wthreads+num_rthreads+num_delthreads];
   
   //spawn writer threads
   for (int i=0; i<num_wthreads; i++){
       arg[i] = (int *)malloc(sizeof(int));
       *arg[i] = i;
       pthread_create(&bench_thread[i],NULL, do_writes, (void *)arg[i]);                
   }
   
   
   //spawn reader threads
   for (int i=0; i<num_rthreads; i++){
       arg[num_wthreads+i] = (int *)malloc(sizeof(int));
       *arg[num_wthreads+i] = i;
       printf("\nspawning reader thread id: %d", *arg[num_wthreads+i]); 
       pthread_create(&bench_thread[num_wthreads+i],NULL, do_reads, 
                      (void *)arg[num_wthreads+i]);                
   }
   
   //spawn delete threads
   for (int i=0; i<num_delthreads; i++){
       arg[num_wthreads+num_rthreads+i] = (int *)malloc(sizeof(int));
       *arg[num_wthreads+num_rthreads+i] = i;
       printf("\nspawning delete thread id: %d", *arg[num_wthreads+num_rthreads+i]); 
       pthread_create(&bench_thread[num_wthreads+num_rthreads+i],NULL, do_deletes, 
                      (void *)arg[num_wthreads+num_rthreads+i]);                
   }


   for (int i = 0; i<num_wthreads+num_rthreads+num_delthreads; i++){
       pthread_join(bench_thread[i],NULL);
       free(arg[i]);
   }
   return true;
}

int do_incremental_load(int num_wthreads, int num_rthreads)
{ 
   pthread_t bench_thread[num_wthreads+num_rthreads];
   int *arg[num_wthreads+num_rthreads];
   
   //spawn writer threads
   for (int i=0; i<num_wthreads; i++){
       arg[i] = (int *)malloc(sizeof(int));
       *arg[i] = i;
       printf("\nspawning incremental writer thread id: %d", *arg[i]); 
       pthread_create(&bench_thread[i],NULL, do_incremental_writes, (void *)arg[i]);                
   }
   
   //spawn reader threads
   for (int i=0; i<num_rthreads; i++){
       arg[num_wthreads+i] = (int *)malloc(sizeof(int));
       *arg[num_wthreads+i] = i;
       printf("\nspawning reader thread id: %d", *arg[num_wthreads+i]); 
       pthread_create(&bench_thread[num_wthreads+i],NULL, do_reads, 
                      (void *)arg[num_wthreads+i]);                
   }
   
   for (int i = 0; i<num_wthreads+num_rthreads; i++){
       pthread_join(bench_thread[i],NULL);
       free(arg[i]);
   }
   
   return true;
}

int main(int argc, char* args[])
{

    struct sysinfo mysysinfo;
    fdb_status status;
    static dictionary *cfg = iniparser_new((char*)"./bench_config.ini");
    char *testtype = iniparser_getstring(cfg, (char*)"test_type:testtype", (char*)"initial");
    char *indextype = iniparser_getstring(cfg, (char*)"index_type:indextype", (char*)"secondary");
    char *committype = iniparser_getstring(cfg, (char*)"commit_type:committype", (char*)"normal");
    numinitdocs = iniparser_getint(cfg, (char*)"document:numinitdocs", 10000);
    lock_prob = iniparser_getint(cfg, (char*)"compaction:lock_prob", 100);
    numincrmutations = iniparser_getint(cfg, (char*)"document:numincrmutations", 10000);
    buffercachesizeMB = iniparser_getint(cfg, (char*)"db_config:buffercachesizeMB", 1024);
    num_wthreads = iniparser_getint(cfg, (char*)"threads:writers", 1);
    num_rthreads = iniparser_getint(cfg, (char*)"threads:readers", 4);
    read_pattern = iniparser_getint(cfg, (char*)"workload_pattern:read_pattern", 2);
    wbatch_size = iniparser_getint(cfg, (char*)"batchparams:wbatch_size", 100);
    max_itr_reads = iniparser_getint(cfg, (char*)"batchparams:maxreads_per_iterator", 1000000);
    const char *resultfilename = (const char*)iniparser_getstring(cfg, (char*)"result_file:resultfilename", 
                                                                                (char*)"initialsecondary");
    const char *statsfilename = (const char*)iniparser_getstring(cfg, (char*)"result_file:statsfilename", 
                                                                                (char*)"statsfile");
    const char *writestatsfilename = (const char*)iniparser_getstring(cfg, (char*)"result_file:writestatsfilename", 
                                                                                (char*)"writestatsfile");
    indexbuilddone = false;
    incrindexbuilddone = false;
    resultfile = fopen(resultfilename,"a"); 
    statsfile = fopen(statsfilename,"a"); 
    writestatsfile = fopen(writestatsfilename,"a"); 
    fprintf(resultfile, "ForestDB Benchmarking\n", testtype);
    //set the forestdb commit type for writes
    if(committype[0]=='n'||committype[0]=='N') FDB_COMMIT = FDB_COMMIT_NORMAL; 
    else FDB_COMMIT = FDB_COMMIT_MANUAL_WAL_FLUSH; 
    //record test params
    fprintf(resultfile, "\ntest type: %s", testtype);
    fprintf(resultfile, "\nindex type: %s", indextype);
    fprintf(resultfile, "\nnumber of documents: %d", numinitdocs);
    fprintf(resultfile, "\ncompactor lock probability: %d", lock_prob);
    fprintf(resultfile, "\nbuffer cache size: %d MB", buffercachesizeMB);
    fprintf(resultfile, "\nwrite batch size: %d", wbatch_size);
    fprintf(resultfile, "\nnumber of incremental writer threads: %d", num_wthreads);
    fprintf(resultfile, "\nnumber of incremental reader threads: %d", num_rthreads);
    fprintf(resultfile, "\ncommit type: %s", committype);
    //initialize DB and KV instances
    init_fdb_with_kvs(NULL,2);
    //initialize random generator
    srand (time(NULL));
    //lock memory to minimize OS page cache
    //lockmemory(buffercachesizeMB); commenting out due to OOM killer trouble
    //start workload
    do_initial_load(1,0,0); //initial load always uses 1 writer, 0 readers, 0 deleters
    fprintf(resultfile, "\ndatabase file size after initial load: %.2f GB", (double)get_filesize(
                                                             "data/secondaryDB")/(1024*1024*1024));
    if(testtype[0]=='o'||testtype[0]=='O')
    {
       do_incremental_load(num_wthreads, num_rthreads); //incremental load always uses 1 writer
    }
    fprintf(resultfile, "\nfinal database file size: %.2f GB", (double)get_filesize(
                                                             "data/secondaryDB")/(1024*1024*1024));
    for(int i = 0; i < 7; i++ ){
    
       fprintf(resultfile, "hist bucket %d: value: %lu",i, snaphist[i]);

    }
    status = fdb_shutdown();
    assert(status == FDB_RESULT_SUCCESS);
} 
