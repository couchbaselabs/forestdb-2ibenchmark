#include "forestdb_workload.h"
#include "strgen.h"
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <pthread.h>

//static fdb_kvs_handle *snapshot[5];
static int numdocs;
static int numdocswritten;
static int max_reads;
static int num_snaps_opened;
static int wbatch_size;
 
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
    fdb_doc *doc1[wbatch_size],*doc2[wbatch_size];
    fdb_status status;
    size_t keylen;
    char *keybuf1, *keybuf2;
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
     
    for(int i=0; i<(numdocs/wbatch_size); i++){
       
       //set a batch of documents
       for(int j =0; j<wbatch_size; j++){
          free(keybuf1);
          free(keybuf2);
          //generate the random key length
          keylen =  rand()%4000+64; 
          keybuf1 = (char *)malloc(sizeof(char)*keylen);
          keybuf2 = (char *)malloc(sizeof(char)*keylen);
          strgen(keybuf1, keylen); 
          strgen(keybuf2, keylen); 
          status = fdb_doc_create(&doc1[j], (const void *)keybuf1, keylen, 
                                     NULL, 0, NULL, 0);
          if(status != FDB_RESULT_SUCCESS)
             continue;
          status = fdb_doc_create(&doc2[j], (const void *)keybuf2, keylen, 
                                     NULL, 0, NULL, 0);
          if(status != FDB_RESULT_SUCCESS)
             continue;
  
          status = fdb_set(kvhandle1, doc1[j]);
          numdocswritten++;
          status = fdb_set(kvhandle2, doc2[j]);
          numdocswritten++;
          status = fdb_doc_free(doc1[j]);  
          status = fdb_doc_free(doc2[j]);  
       }
       //after setting batch of documents call commit
       status = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
    }
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
   
   int tid = *(int *)arg;
   unsigned long numreads = 0;
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
   
   while(numreads<max_reads){ 
      do{ 
         status = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
      }while(status != FDB_RESULT_SUCCESS);
      do{
         status = fdb_get_kvs_seqnum(kvhandle, &seqnum);
      }while(status != FDB_RESULT_SUCCESS);
      do{
         status = fdb_snapshot_open(kvhandle, &snapshot, seqnum);
      }while(status != FDB_RESULT_SUCCESS);
   
      do{
         status = fdb_iterator_init(snapshot, &itr_kv,
                                    NULL, 0, NULL, 0,
                                    FDB_ITR_NONE);
      } while(status != FDB_RESULT_SUCCESS); 
   
      while(fdb_iterator_next(itr_kv) != FDB_RESULT_ITERATOR_FAIL){
         status = fdb_iterator_get(itr_kv, &doc);
         if (status != FDB_RESULT_SUCCESS){
            continue;
         }
         numreads++;
         fdb_doc_free(doc);
      }   
      status = fdb_kvs_close(snapshot);
   }
   printf("\nreader thread id: %d num of reads done: %lu", tid,numreads); 
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

    numdocs = 1000000;
    wbatch_size = 100;
    max_reads = 200000;
    init_fdb_with_kvs(NULL,2);
    do_load(1,5);
} 
