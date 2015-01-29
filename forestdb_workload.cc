#include "forestdb_workload.h"
#include "strgen.h"
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <pthread.h>
#include "iniparser.h"

//static fdb_kvs_handle *snapshot[5];
static FILE *resultfile;
static int numdocs;
static int numdocswritten;
static int buffercachesizeMB;
static int num_rthreads;
static int max_reads;
static int max_itr_reads;
static int num_snaps_opened;
static int wbatch_size;
static int indexbuilddone; 
static fdb_commit_opt_t FDB_COMMIT;

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
    config.durability_opt = FDB_DRB_ASYNC;
    config.compaction_mode = FDB_COMPACTION_MANUAL;
    config.buffercache_size = buffercachesizeMB*1024*1024;  //1GB
    kvs_config = fdb_get_default_kvs_config();

    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &kvhandle1,"kvstore1" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &kvhandle2,"kvstore2" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    
    return true;
}

//create a for loop and at the beginning of the loop close snapshot and then open a new one
/*void *cycle_snapshots(void *arg)
{

    fdb_status status;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle;
    fdb_seqnum_t seqnum;
    config = fdb_get_default_config();
    kvs_config = fdb_get_default_kvs_config();
    
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle,"kvstore1" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    num_snaps_opened = 0;
    while(numdocswritten<=numdocs){
       for(int i=0;i<5;i++ ){
          usleep(2000);
          status = fdb_kvs_close(snapshot[i]);
          status = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
          status = fdb_get_kvs_seqnum(kvhandle, &seqnum);
          status = fdb_snapshot_open(kvhandle, &snapshot[i], seqnum);
          num_snaps_opened++;
       }
    }
    printf("\nnum_snaps_opened: %d", num_snaps_opened);
    return arg;
}*/

void *do_writes(void *arg)
{

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle1;
    fdb_kvs_handle *kvhandle2;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_doc *doc1, *doc2;
    //std::vector<fdb_doc *> kvs1_deletelist, kvs2_deletelist;
    fdb_status status;
    size_t keylen;
    char *keybuf1, *keybuf2;
    clock_t start;
    int deldoccounter; //use the counter to capture every nth key into delete list
    int commitflag;    //to reset the flag after every delete
    struct timeval index_starttime, index_stoptime;
    numdocswritten = 0; 
    config = fdb_get_default_config();
    config.durability_opt = FDB_DRB_ASYNC;
    kvs_config = fdb_get_default_kvs_config();
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle1,"kvstore1" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle2,"kvstore2" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    keylen = 4096; 
    keybuf1 = (char *)malloc(sizeof(char)*keylen);
    keybuf2 = (char *)malloc(sizeof(char)*keylen);
    memset((void *)keybuf1, 0, sizeof(char)*keylen);
    memset((void *)keybuf2, 0, sizeof(char)*keylen);

    status = FDB_RESULT_SUCCESS;
    status = fdb_doc_create(&doc1, (const void *)keybuf1, keylen, 
                               NULL, 0, NULL, 0);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_doc_create(&doc2, (const void *)keybuf2, keylen, 
                               NULL, 0, NULL, 0);
    assert(status == FDB_RESULT_SUCCESS);
    
    //start commit clock
    start = clock();
    gettimeofday(&index_starttime, NULL);
    while(numdocswritten<numdocs){
       
       //generate the random key length
       keylen = rand()%4000+64; 
       strgen(keybuf1, keylen); 
       strgen(keybuf2, keylen);
       
       //reuse the fdb_doc structure created above to insert new KV pairs 
       memset(doc1->key, 0, sizeof(char)*keylen);
       memcpy(doc1->key, keybuf1, keylen);
       doc1->keylen = keylen;
       memset(doc2->key, 0, sizeof(char)*keylen);
       memcpy(doc2->key, keybuf2, keylen);
       doc2->keylen = keylen;
      
       do{
           status = fdb_set(kvhandle1, doc1);
       } while(status!=FDB_RESULT_SUCCESS); 
       numdocswritten++;
       do{
          status = fdb_set(kvhandle2, doc2);
       } while(status!=FDB_RESULT_SUCCESS); 
       numdocswritten++;
       memset((void *)keybuf1, 0, sizeof(char)*keylen);
       memset((void *)keybuf2, 0, sizeof(char)*keylen);
        
       //every 30 seconds,  call commit
       /*if(((clock()-start)/(double) CLOCKS_PER_SEC)>30){
          status = fdb_commit(fhandle, FDB_COMMIT);
          start = clock();
       }*/
    }
    
    //final commit
    fdb_file_handle *fhandle_2;
    config = fdb_get_default_config();
    status = fdb_open(&fhandle_2, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_commit(fhandle_2, FDB_COMMIT_MANUAL_WAL_FLUSH);

    gettimeofday(&index_stoptime, NULL);
    indexbuilddone = true;
    fprintf(resultfile, "\nindex build time: %d seconds\n", (index_stoptime.tv_sec-index_starttime.tv_sec));
    printf("\nnumdocs written: %lu \nindex build time: %d seconds\n", numdocswritten,
                                    (index_stoptime.tv_sec-index_starttime.tv_sec)); 
    free(keybuf1);
    free(keybuf2);
    fdb_doc_free(doc1);  
    fdb_doc_free(doc2);  
    return arg;
}

void *do_reads(void *arg)
{
   fdb_file_handle *fhandle;
   fdb_kvs_handle *kvhandle;
   fdb_kvs_handle *snapshot;
   fdb_kvs_config kvs_config;
   fdb_config config;
   fdb_seqnum_t seqnum;
   fdb_iterator *itr_kv;
   fdb_doc *doc;
   fdb_status status;
   int itr_reads;

   unsigned long numreads = 0;
   unsigned long num_opened_snapshots = 0;
   int tid = *(int *)arg;
   printf("\nstarting reader thread id: %d", tid); 
   
   config = fdb_get_default_config();
   kvs_config = fdb_get_default_kvs_config();
   status = fdb_open(&fhandle, "data/secondaryDB", &config);
   assert(status == FDB_RESULT_SUCCESS);
   if(tid%2==0){
      status = fdb_kvs_open(fhandle, &kvhandle,"kvstore1" , &kvs_config);
      assert(status == FDB_RESULT_SUCCESS);
      printf("\nreader thread id: %d opened kvstore1", tid); 
   }
   else{
      status = fdb_kvs_open(fhandle, &kvhandle,"kvstore2" , &kvs_config);
      assert(status == FDB_RESULT_SUCCESS);
      printf("\nreader thread id: %d opened kvstore2", tid); 
   }   
   
   while(!indexbuilddone){ 
      //always open in-memory snapshots if there are writes between COMMIT
      //and snapshot_open
      do{
         status = fdb_snapshot_open(kvhandle, &snapshot, FDB_SNAPSHOT_INMEM);
      }while(status != FDB_RESULT_SUCCESS);
      num_opened_snapshots++; 
      do{
         status = fdb_iterator_init(snapshot, &itr_kv,
                                    NULL, 0, NULL, 0,
                                    FDB_ITR_NONE);
      } while(status != FDB_RESULT_SUCCESS); 
      
      itr_reads = 0; 
      
      while(fdb_iterator_next(itr_kv) != FDB_RESULT_ITERATOR_FAIL){
         status = fdb_iterator_get(itr_kv, &doc);
         if (status != FDB_RESULT_SUCCESS){
            continue;
         }
         numreads++;
         itr_reads++;
         fdb_doc_free(doc);
         if(itr_reads>max_itr_reads)
            break;
      }   
      status = fdb_kvs_close(snapshot);
      usleep(500000);
   }
   printf("\nreader thread id: %d num snapshost opened: %lu num of reads done: %lu", tid,num_opened_snapshots, numreads); 
   return arg;
}

int do_load(int num_wthreads, int num_rthreads)
{

   pthread_t bench_thread[num_wthreads+num_rthreads];
   int *arg[num_wthreads+num_rthreads];
   
   //spawn writer threads
   for (int i=0; i<num_wthreads; i++){
       arg[i] = (int *)malloc(sizeof(int));
       *arg[i] = i;
       pthread_create(&bench_thread[i],NULL, do_writes, (void *)arg[i]);                
   }
   
   //spawn threads to cycle snapshots
   //*arg = num_wthreads+num_rthreads;
   //pthread_create(&bench_thread[num_wthreads+num_rthreads],NULL, cycle_snapshots, (void *)arg);   
   
   //spawn reader threads
   for (int i=0; i<num_rthreads; i++){
       arg[num_wthreads+i] = (int *)malloc(sizeof(int));
       *arg[num_wthreads+i] = i;
       printf("\nspawning reader thread id: %d", *arg[num_wthreads+i]); 
       pthread_create(&bench_thread[num_wthreads+i],NULL, do_reads, (void *)arg[num_wthreads+i]);                
   }


   for (int i = 0; i<num_wthreads+num_rthreads; i++){
       pthread_join(bench_thread[i],NULL);
   }
   return true;
}

int main(int argc, char* args[])
{

    struct sysinfo mysysinfo;
    static dictionary *cfg = iniparser_new((char*)"./bench_config.ini");
    char *testtype = iniparser_getstring(cfg, (char*)"test_type:testtype", (char*)"initial");
    char *indextype = iniparser_getstring(cfg, (char*)"index_type:indextype", (char*)"secondary");
    char *committype = iniparser_getstring(cfg, (char*)"commit_type:committype", (char*)"normal");
    numdocs = iniparser_getint(cfg, (char*)"document:ndocs", 10000);
    buffercachesizeMB = iniparser_getint(cfg, (char*)"db_config:buffercachesizeMB", 1024);
    num_rthreads = iniparser_getint(cfg, (char*)"threads:readers", 4);
    wbatch_size = iniparser_getint(cfg, (char*)"batchparams:wbatch_size", 100);
    max_itr_reads = iniparser_getint(cfg, (char*)"batchparams:maxreads_per_iterator", 1000);
    const char *resultfilename = (const char*)iniparser_getstring(cfg, (char*)"result_file:resultfilename", 
                                                                                (char*)"initialsecondary");
    indexbuilddone = false;
    max_reads = 200000;
    resultfile = fopen(resultfilename,"a"); 
    fprintf(resultfile, "ForestDB Benchmarking\n", testtype);
    //to apply read workload or not    
    if(testtype[0]=='i'||testtype[0]=='I')num_rthreads = 0;
    else 
    //set the forestdb commit type for writes
    if(committype[0]=='n'||committype[0]=='N') FDB_COMMIT = FDB_COMMIT_NORMAL; 
    else FDB_COMMIT = FDB_COMMIT_MANUAL_WAL_FLUSH; 
    //record test params
    fprintf(resultfile, "\ntest type: %s", testtype);
    fprintf(resultfile, "\nindex type: %s", indextype);
    fprintf(resultfile, "\nnumber of documents: %d", numdocs);
    fprintf(resultfile, "\nbuffer cache size: %d MB", buffercachesizeMB);
    fprintf(resultfile, "\nwrite batch size: %d", wbatch_size);
    fprintf(resultfile, "\nnumber of reader threads: %d", num_rthreads);
    fprintf(resultfile, "\ncommit type: %s", committype);
    //initialize DB and KV instances
    init_fdb_with_kvs(NULL,2);
    //initialize random generator
    srand (time(NULL));
    //lock memory to minimize OS page cache
    //lockmemory(buffercachesizeMB);
    //start workload
    do_load(1,num_rthreads);
    fprintf(resultfile, "\nfinal database file size: %.2f GB", (double)get_filesize(
                                                             "data/secondaryDB")/(1024*1024*1024));
} 
