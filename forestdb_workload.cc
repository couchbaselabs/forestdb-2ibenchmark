#include "forestdb_workload.h"
#include "strgen.h"
#include <unistd.h>
#include <pthread.h>

static fdb_kvs_handle *snapshot[5];
static char *docbody1KB = (char *)malloc(sizeof(char)*1024);
 
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
    kvs_config = fdb_get_default_kvs_config();

    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &kvhandle1,"kvstore1" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(fhandle, &kvhandle2,"kvstore2" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    
    for(int i=0;i<5;i++ ){
       usleep(200000);   
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
    for(int i=0;i<5;i++ ){
       status = fdb_kvs_close(snapshot[i]);
       usleep(200000);   
       status = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
       status = fdb_get_kvs_seqnum(kvhandle, &seqnum);
       status = fdb_snapshot_open(kvhandle, &snapshot[i], seqnum);
    }
    return arg;
}

void *do_writes(void *arg)
{

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle1;
    fdb_kvs_handle *kvhandle2;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_doc *doc;
    fdb_status status;
    size_t keylen;
    char * keybuf;
     
    config = fdb_get_default_config();
    kvs_config = fdb_get_default_kvs_config();
    status = fdb_open(&fhandle, "data/secondaryDB", &config);
    assert(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open(fhandle, &kvhandle1,"kvstore1" , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
    
    keylen = 100; 
    keybuf = (char *)malloc(sizeof(char)*keylen);
    for(int i=0; i<10000; i++){
       strgen(keybuf, keylen); 
       status = fdb_doc_create(&doc, (const void *)keybuf, keylen, NULL, 0,
                                      (const void *)docbody1KB, 1024);
    
       if(status == FDB_RESULT_SUCCESS){
  
          status = fdb_set(kvhandle1, doc);
          status = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
          status = fdb_doc_free(doc);  
       } 
    }
    return arg;
}

void *do_reads(void *arg)
{

   fdb_file_handle *fhandle;
   fdb_kvs_handle *kvhandle1;
   fdb_kvs_handle *kvhandle2;
   fdb_config config;
   fdb_kvs_config kvs_config;
   fdb_doc *doc;
   fdb_iterator *itr_key;
   fdb_status status;
   
   config = fdb_get_default_config();
   kvs_config = fdb_get_default_kvs_config();
   status = fdb_open(&fhandle, "data/secondaryDB", &config);
   assert(status == FDB_RESULT_SUCCESS);
   status = fdb_kvs_open(fhandle, &kvhandle1,"kvstore1" , &kvs_config);
   assert(status == FDB_RESULT_SUCCESS);

   status = fdb_iterator_init(kvhandle1, &itr_key,
                           NULL, 0, NULL, 0,
                           FDB_ITR_NONE);
   assert(status == FDB_RESULT_SUCCESS); 
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

   pthread_t bench_thread[num_wthreads+num_rthreads];
   //fdb_open(&fhandle, "/data/secondaryDB", &config);
   //fdb_kvs_open(fhandle, &kvhandle1,"kvstore1" , &kvs_config);
   //fdb_kvs_open(fhandle, &kvhandle2,"kvstore2" , &kvs_config);

   for (int i=0; i<num_wthreads; i++){
       pthread_create(&bench_thread[i],NULL, do_writes, NULL);                
   }
   
   for (int i=0; i<num_rthreads; i++){
       pthread_create(&bench_thread[num_wthreads+i],NULL, do_reads, NULL);                
   }
   for (int i = 0; i<num_wthreads+num_rthreads; i++){
       pthread_join(bench_thread[i],NULL);
   }
   return true;
}

int main(int argc, char* args[])
{

    int status = strgen(docbody1KB,1024);
    init_fdb_with_kvs_snaps(NULL,2);
    do_load(1,1);
} 
