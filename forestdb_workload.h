#include <assert.h>
#include <stdlib.h>
#include "libforestdb/forestdb.h"

//create fdb file with desired number of kvstores and provided kvs configs 
int init_fdb_with_kvs_snaps(fdb_kvs_config* kvs_list [], size_t num_kvs);

//adjust buffer cache
int set_bufcache(size_t bufsize);




