#include "forestdb_workload.h"
#include "strgen.h"
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <pthread.h>

static fdb_kvs_handle *snapshot[5];
static char *docbody1KB = (char *)malloc(sizeof(char)*1024);
static int numdocs;
static int numdocswritten;
static int num_snaps_opened;
static int wbatch_size;
 
int init_fdb_with_kvs_snaps(fdb_kvs_config* kvs_list [], size_t num_kvs)
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
    kvs_config = fdb_get_default_kvs_config();

    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &kvhandle1,"kvstore1" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &kvhandle2,"kvstore2" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    
    for(int i=0;i<5;i++ ){
       //usleep(200000);   
       status = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
       assert(status == FDB_RESULT_SUCCESS);
       status = fdb_get_kvs_seqnum(kvhandle1, &seqnum);
       assert(status == FDB_RESULT_SUCCESS);
       status = fdb_snapshot_open(kvhandle1, &snapshot[i], seqnum);
       assert(status == FDB_RESULT_SUCCESS);
    }
    
    return true;
}

//create a for loop and at the beginning of the loop close snapshot and then open a new one
void *cycle_snapshots(void *arg)
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
    num_snaps_opened = 5;
    while(numdocswritten<=numdocs){
       usleep(10000);
       for(int i=0;i<5;i++ ){
          status = fdb_kvs_close(snapshot[i]);
          status = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
          status = fdb_get_kvs_seqnum(kvhandle, &seqnum);
          status = fdb_snapshot_open(kvhandle, &snapshot[i], seqnum);
          num_snaps_opened++;
       }
    }
    printf("num_snaps_opened: %d", num_snaps_opened);
    return arg;
}

void *do_writes(void *arg)
{

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle1;
    fdb_kvs_handle *kvhandle2;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_doc *doc[wbatch_size];
    fdb_status status;
    size_t keylen;
    char * keybuf;
    numdocswritten = 0; 
    config = fdb_get_default_config();
    config.durability_opt = FDB_DRB_ASYNC;
    kvs_config = fdb_get_default_kvs_config();
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle1,"kvstore1" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle1,"kvstore2" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    
    keybuf = (char *)malloc(sizeof(char)*keylen);
    for(int i=0; i<(numdocs)/(wbatch_size); i++){
       
       //set a batch of documents
       for(int j =0; j<wbatch_size; j++){
          numdocswritten = numdocswritten + wbatch_size;
          //generate the random key length
          keylen =  rand()%4000+64; 
          strgen(keybuf, keylen); 
          status = fdb_doc_create(&doc[j], (const void *)keybuf, keylen, NULL, 0,
                                      NULL, 0);
    
          if(status == FDB_RESULT_SUCCESS){
  
             status = fdb_set(kvhandle1, doc[j]);
             status = fdb_set(kvhandle2, doc[j]);
             status = fdb_doc_free(doc[j]);  
          } 
       }
       //after setting batch of documents call commit
       status = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
    }
    free(keybuf);
    return arg;
}

void *do_reads(void *arg)
{
   int tid = *(int *)arg;
   fdb_file_handle *fhandle;
   fdb_config config;
   fdb_doc *doc;
   fdb_iterator *itr_key;
   fdb_status status;
   printf("\nreader thread id: %d", tid); 
   do{
   status = fdb_iterator_init(snapshot[tid], &itr_key,
                           NULL, 0, NULL, 0,
                           FDB_ITR_NONE);
   } while(status != FDB_RESULT_SUCCESS); 
   while(fdb_iterator_next(itr_key) != FDB_RESULT_ITERATOR_FAIL){
      status = fdb_iterator_get(itr_key, &doc);
      if (status != FDB_RESULT_SUCCESS)
         break;
      fdb_doc_free(doc);
   }
   return arg;
}

int do_load(int num_wthreads, int num_rthreads)
{

   pthread_t bench_thread[num_wthreads+num_rthreads+1];
   int *arg = (int *)malloc(sizeof(int));
   //fdb_open(&fhandle, "/data/secondaryDB", &config);
   //fdb_kvs_open(fhandle, &kvhandle1,"kvstore1" , &kvs_config);
   //fdb_kvs_open(fhandle, &kvhandle2,"kvstore2" , &kvs_config);
   
   //spawn writer threads
   for (int i=0; i<num_wthreads; i++){
       *arg = i;
       pthread_create(&bench_thread[i],NULL, do_writes, (void *)arg);                
   }
   
   //spawn threads to cycle snapshots
   *arg = num_wthreads+num_rthreads;
   pthread_create(&bench_thread[num_wthreads+num_rthreads],NULL, cycle_snapshots, (void *)arg);   
   
   //spawn reader threads
   for (int i=0; i<num_rthreads; i++){
       *arg = i;
       pthread_create(&bench_thread[num_wthreads+i],NULL, do_reads, (void *)arg);                
   }


   for (int i = 0; i<=num_wthreads+num_rthreads; i++){
       pthread_join(bench_thread[i],NULL);
   }
   return true;
}

int main(int argc, char* args[])
{

    numdocs = 1000000;
    wbatch_size = 100;
    //int status = strgen(docbody1KB,1024);
    init_fdb_with_kvs_snaps(NULL,2);
    do_load(1,5);
    free(docbody1KB);
} 
