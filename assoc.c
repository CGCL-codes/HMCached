/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <signal.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t maintenance_lock = PTHREAD_MUTEX_INITIALIZER;

typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

/* how many powers of 2's worth of buckets we use */
unsigned int hashpower = HASHPOWER_DEFAULT;   // 16

#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/* Main hash table. This is where we look except during expansion. */
static item** primary_hashtable = 0;

/*
 * Previous hash table. During expansion, we look here for keys that haven't
 * been moved over to the primary yet.
 */
static item** old_hashtable = 0;

/* Flag: Are we in the middle of expanding now? */
static bool expanding = false;
static bool started_expanding = false;

/*
 * During expansion we migrate values with bucket granularity; this is how
 * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
 */
static unsigned int expand_bucket = 0;

void assoc_init(const int hashtable_init) {
    printf("assoc_init, hash_power: %d for DRAM_ZONE\n", hashtable_init);
    
    if (hashtable_init) {
        hashpower = hashtable_init;
    }
    primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
    if (! primary_hashtable) {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }
    STATS_LOCK();
    stats_state.hash_power_level = hashpower;
    stats_state.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

item *assoc_find(const char *key, const size_t nkey, const uint32_t hv) {
    item *it;
    unsigned int oldbucket;

    if (expanding && (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket) {
        it = old_hashtable[oldbucket];
    } else {
        it = primary_hashtable[hv & hashmask(hashpower)];
    }

    item *ret = NULL;
    int depth = 0;
    while (it) {
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */
static item** _hashitem_before (const char *key, const size_t nkey, const uint32_t hv) {
    item **pos;
    unsigned int oldbucket;

    if (expanding && (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket) {
        pos = &old_hashtable[oldbucket];
    } else {
        pos = &primary_hashtable[hv & hashmask(hashpower)];
    }

    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* grows the hashtable to the next power of 2. */
static void assoc_expand(void) {
    old_hashtable = primary_hashtable;

    primary_hashtable = calloc(hashsize(hashpower + 1), sizeof(void *));
    if (primary_hashtable) {
        
        printf("start assoc_expand for DRAM_ZONE\n");

        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");
        hashpower++;
        expanding = true;
        expand_bucket = 0;
        STATS_LOCK();
        stats_state.hash_power_level = hashpower;
        stats_state.hash_bytes += hashsize(hashpower) * sizeof(void *);
        stats_state.hash_is_expanding = true;
        STATS_UNLOCK();
    } else {
        primary_hashtable = old_hashtable;
        /* Bad news, but we can keep running. */
    }
}

void assoc_start_expand(uint64_t curr_items) {
    if (started_expanding)
        return;

    if (curr_items > (hashsize(hashpower) * 3) / 2 &&
          hashpower < HASHPOWER_MAX) {
        started_expanding = true;
        pthread_cond_signal(&maintenance_cond);
    }
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(item *it, const uint32_t hv) {
    if (it->memory_is_dram != 1)
        printf("error in assoc_insert()\n");

    unsigned int oldbucket;

//    assert(assoc_find(ITEM_key(it), it->nkey) == 0);  /* shouldn't have duplicately named things defined */

    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it->h_next = old_hashtable[oldbucket];
        old_hashtable[oldbucket] = it;
    } else {
        it->h_next = primary_hashtable[hv & hashmask(hashpower)];
        primary_hashtable[hv & hashmask(hashpower)] = it;
    }

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey);
    return 1;
}

void assoc_delete(const char *key, const size_t nkey, const uint32_t hv) {
    item **before = _hashitem_before(key, nkey, hv);

    if (*before) {
        item *nxt;
        /* The DTrace probe cannot be triggered as the last instruction
         * due to possible tail-optimization by the compiler
         */
        MEMCACHED_ASSOC_DELETE(key, nkey);
        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = nxt;
        return;
    }
    /* Note:  we never actually get here.  the callers don't delete things
       they can't find. */
    assert(*before != 0);
}


static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

static void *assoc_maintenance_thread(void *arg) {

    mutex_lock(&maintenance_lock);
    while (do_run_maintenance_thread) {
        int ii = 0;

        /* There is only one expansion thread, so no need to global lock. */
        for (ii = 0; ii < hash_bulk_move && expanding; ++ii) {
            item *it, *next;
            unsigned int bucket;
            void *item_lock = NULL;

            /* bucket = hv & hashmask(hashpower) =>the bucket of hash table
             * is the lowest N bits of the hv, and the bucket of item_locks is
             *  also the lowest M bits of hv, and N is greater than M.
             *  So we can process expanding with only one item_lock. cool! */
            if ((item_lock = item_trylock(expand_bucket))) {
                for (it = old_hashtable[expand_bucket]; NULL != it; it = next) {
                    next = it->h_next;
                    bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashpower);
                    it->h_next = primary_hashtable[bucket];
                    primary_hashtable[bucket] = it;
                }

                old_hashtable[expand_bucket] = NULL;

                expand_bucket++;
                if (expand_bucket == hashsize(hashpower - 1)) {
                    expanding = false;
                    free(old_hashtable);
                    STATS_LOCK();
                    stats_state.hash_bytes -= hashsize(hashpower - 1) * sizeof(void *);
                    stats_state.hash_is_expanding = false;
                    STATS_UNLOCK();
                    if (settings.verbose > 1)
                        fprintf(stderr, "Hash table expansion done\n");
                }
            } else {
                usleep(10*1000);
            }

            if (item_lock) {
                item_trylock_unlock(item_lock);
                item_lock = NULL;
            }
        }

        if (!expanding) {
            /* We are done expanding.. just wait for next invocation */
            started_expanding = false;
            pthread_cond_wait(&maintenance_cond, &maintenance_lock);
            /* assoc_expand() swaps out the hash table entirely, so we need
             * all threads to not hold any references related to the hash
             * table while this happens.
             * This is instead of a more complex, possibly slower algorithm to
             * allow dynamic hash table expansion without causing significant
             * wait times.
             */
            pause_threads(PAUSE_ALL_THREADS);
            assoc_expand();
            pause_threads(RESUME_ALL_THREADS);
        }
    }
    return NULL;
}

static pthread_t maintenance_tid;

int start_assoc_maintenance_thread() {
    //printf("Close start_assoc_maintenance_thread()...\n");
    return 0;

    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env != NULL) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }
    pthread_mutex_init(&maintenance_lock, NULL);
    if ((ret = pthread_create(&maintenance_tid, NULL,
                              assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

void stop_assoc_maintenance_thread() {
    mutex_lock(&maintenance_lock);
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    mutex_unlock(&maintenance_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
}


//static pthread_cond_t maintenance_cond_nvm = PTHREAD_COND_INITIALIZER;
//static pthread_mutex_t maintenance_lock_nvm = PTHREAD_MUTEX_INITIALIZER;

/* how many powers of 2's worth of buckets we use */
unsigned int hashpower_nvm = HASHPOWER_DEFAULT;

static bucket_nvm **primary_hashtable_nvm = 0;
static bucket_nvm **old_hashtable_nvm = 0;
//static item_nvm** primary_hashtable_nvm = 0;
// static item_nvm** old_hashtable_nvm = 0;
      
static bool expanding_nvm = false;
// static bool started_expanding_nvm = false;
static unsigned int expand_bucket_nvm = 0;


void assoc_init_nvm(const int hashtable_init) {
    printf("assoc_init_nvm, hash_power: %d for NVM_ZONE\n", hashtable_init);
    
    if (hashtable_init) {
        hashpower_nvm = hashtable_init;
    }
    primary_hashtable_nvm = calloc(hashsize(hashpower_nvm), sizeof(void *));
    if (! primary_hashtable_nvm) {
        fprintf(stderr, "Failed to init hashtable_nvm.\n");
        exit(EXIT_FAILURE);
    }
}

item_nvm *assoc_find_nvm(const char *key, const size_t nkey, const uint32_t hv) {
    bucket_nvm *bucket;
    unsigned int oldbucket_nvm;
    
    if (expanding_nvm && (oldbucket_nvm = (hv & hashmask(hashpower_nvm - 1))) >= expand_bucket_nvm) {
        bucket = old_hashtable_nvm[oldbucket_nvm];
    } else {
        bucket = primary_hashtable_nvm[hv & hashmask(hashpower_nvm)];
    }
    
    uint32_t sign = keysign(key, nkey);
    item_nvm *ret = NULL;
    int depth = 0;
    int i;
    index_nvm *index;
    while (bucket) {
        for (i = 0; i < 3; i++) {
            index = &(bucket->indexes[i]);
            if ((index->bucket_in_use == 1) && (index->keysign == sign)) {
                uint64_t tmp_kvitem = index->kvitem;
                struct _stritem_nvm *kvitem = (struct _stritem_nvm *)tmp_kvitem;
                if (memcmp(key, ITEM_key(kvitem), nkey) == 0) {
                    ret = kvitem;
                    return ret;
                }
            }
        }
        uint64_t tmp_bucket = bucket->next;
        bucket_nvm *next_bucket = (bucket_nvm *)tmp_bucket;
        bucket = next_bucket;
        //bucket = bucket->next;
        
        ++depth;
    }
    return ret;
}

/*  
static item_nvm** _hashitem_before_nvm(const char *key, const size_t nkey, const uint32_t hv) {
    item_nvm **pos;
    unsigned int oldbucket_nvm;

    if (expanding_nvm && (oldbucket_nvm = (hv & hashmask(hashpower_nvm - 1))) >= expand_bucket_nvm) {
        pos = &old_hashtable_nvm[oldbucket_nvm];
    } else {
        pos = &primary_hashtable_nvm[hv & hashmask(hashpower_nvm)];
    }
    
    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    
    return pos;
}
*/

/*
static void assoc_expand_nvm(void) {
    old_hashtable_nvm = primary_hashtable_nvm;
    
    primary_hashtable_nvm = calloc(hashsize(hashpower_nvm + 1), sizeof(void *));
    if (primary_hashtable_nvm) {
        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");
        hashpower_nvm++;
        expanding_nvm = true;
        expand_bucket_nvm = 0;
    } else {
        primary_hashtable_nvm = old_hashtable_nvm;
    }
}

void assoc_start_expand_nvm(uint64_t curr_items) {
    if (started_expanding_nvm)
        return;
    
    if (curr_items > (hashsize(hashpower_nvm) * 3) / 2 
            && hashpower_nvm < HASHPOWER_MAX) {
        started_expanding_nvm = true;
        pthread_cond_signal(&maintenance_cond_nvm);
    }
}
*/

// Note: this isn't an assoc_update. The key must not already exist to call this
int assoc_insert_nvm(item_nvm *it, const uint32_t hv) {
    if (it->memory_is_dram != 0)
        printf("error in assoc_insert_nvm()\n");
    
    unsigned int oldbucket_nvm;
    if (expanding_nvm && (oldbucket_nvm = (hv & hashmask(hashpower_nvm - 1))) >= expand_bucket_nvm) {
        bucket_nvm *bucket = old_hashtable_nvm[oldbucket_nvm];
        bucket_nvm *last_bucket = NULL;
        index_nvm *res = NULL;
        int i;
        while (bucket) {
            for (i = 0; i < 3; i++) {
                if ((bucket->indexes[i]).bucket_in_use == 0) {
                    res = &(bucket->indexes[i]);
                    break;
                }
            }
            if (res)
                break;
            last_bucket = bucket;

            uint64_t tmp_bucket = bucket->next;
            bucket_nvm *next_bucket = (bucket_nvm *)tmp_bucket;
            bucket = next_bucket;
            //bucket = bucket->next;
        }

        if (res == NULL) {
            bucket_nvm *new_bucket = slabs_alloc_bucket();
            if (new_bucket == NULL) {
                printf("error in assoc_insert_nvm()...\n");
                exit(0);
            }
            int j;
            for (j = 0; j < 3; j++)
                (new_bucket->indexes[j]).bucket_in_use = 0;
            if (last_bucket)
                last_bucket->next = (uint64_t)new_bucket;
            else
                old_hashtable_nvm[oldbucket_nvm] = new_bucket;
            res = &(new_bucket->indexes[0]);
        }

        res->memory_is_dram = 0;
        res->bucket_in_use = 1;
        res->idle_periods = it->index->idle_periods;
        res->refcount = it->index->refcount;
        res->counter = it->index->counter;
        res->keysign = keysign(ITEM_key(it), it->nkey);
        res->time = it->index->time;
        res->kvitem = (uint64_t)it;
        slabs_free_index(it->index);
        it->index = res;
    } else {
        bucket_nvm *bucket = primary_hashtable_nvm[hv & hashmask(hashpower_nvm)];
        bucket_nvm *last_bucket = NULL;
        index_nvm *res = NULL;
        int i;
        while (bucket) {
            for (i = 0; i < 3; i++) {
                if ((bucket->indexes[i]).bucket_in_use == 0) {
                    res = &(bucket->indexes[i]);
                    break;
                }
            }
            if (res)
                break;
            last_bucket = bucket;

            uint64_t tmp_bucket = bucket->next;
            bucket_nvm *next_bucket = (bucket_nvm *)tmp_bucket;
            bucket = next_bucket;
            //bucket = bucket->next;
        }

        if (res == NULL) {
            bucket_nvm *new_bucket = slabs_alloc_bucket();
            if (new_bucket == NULL) {
                printf("error in assoc_insert_nvm()...\n");
                exit(0);
            }
            int j;
            for (j = 0; j < 3; j++)
                (new_bucket->indexes[j]).bucket_in_use = 0;  // TODO LEEZW
            if (last_bucket)
                last_bucket->next = (uint64_t)new_bucket;
            else
                primary_hashtable_nvm[hv & hashmask(hashpower_nvm)] = new_bucket;
            res = &(new_bucket->indexes[0]);
        }

        res->memory_is_dram = 0;
        res->bucket_in_use = 1;
        res->idle_periods = it->index->idle_periods;
        res->refcount = it->index->refcount;
        res->counter = it->index->counter;
        res->keysign = keysign(ITEM_key(it), it->nkey);
        res->time = it->index->time;
        res->kvitem = (uint64_t)it;
        slabs_free_index(it->index);
        it->index = res;
    }
    
    // it->in_hashtable = 1;
    return 1;
}

void assoc_delete_nvm(const char *key, const size_t nkey, const uint32_t hv) {
    uint32_t sign = keysign(key, nkey);
    unsigned int oldbucket_nvm;
   
    index_nvm *res = NULL;
    if (expanding_nvm && (oldbucket_nvm = (hv & hashmask(hashpower_nvm - 1))) >= expand_bucket_nvm) {
        bucket_nvm *bucket = old_hashtable_nvm[oldbucket_nvm];
        
        index_nvm *index = NULL;
        int i;
        while (bucket) {
            for (i = 0; i < 3; i++) {
                index = &(bucket->indexes[i]);
                if ((index->bucket_in_use == 1) && (index->keysign == sign)) {
                    uint64_t tmp_kvitem = index->kvitem;
                    struct _stritem_nvm *kvitem = (struct _stritem_nvm *)tmp_kvitem;
                    if (memcmp(key, ITEM_key(kvitem), nkey) == 0) {
                        res = index;
                        break;
                    }
                }
            }
            if (res)
                break;

            uint64_t tmp_bucket = bucket->next;
            bucket_nvm *next_bucket = (bucket_nvm *)tmp_bucket;
            bucket = next_bucket;
            //bucket = bucket->next;
        }

        if (res) {
            index_nvm *new = slabs_alloc_index();
            if (new == NULL) {
                printf("error in assoc_delete_nvm()...\n");
                exit(0);
            }
            new->memory_is_dram = 0;
            new->bucket_in_use = 1;
            new->idle_periods = res->idle_periods;
            new->refcount = res->refcount;
            new->counter = res->counter;
            new->keysign = res->keysign;
            new->time = res->time;
        
            new->kvitem = res->kvitem;
            
            //res->kvitem->index = new;
            uint64_t tmp_kvitem = res->kvitem;
            struct _stritem_nvm *kvitem = (struct _stritem_nvm *)tmp_kvitem;
            kvitem->index = new;
            
            // res->kvitem = NULL;
            res->kvitem = 0;
            
            res->bucket_in_use = 0;
        }
    } else {
        bucket_nvm *bucket = primary_hashtable_nvm[hv & hashmask(hashpower_nvm)];
        index_nvm *index = NULL;
        int i;
        while (bucket) {
            for (i = 0; i < 3; i++) {
                index = &(bucket->indexes[i]);
                if ((index->bucket_in_use == 1) && (index->keysign == sign)) {
                    uint64_t tmp_kvitem = index->kvitem;
                    struct _stritem_nvm *kvitem = (struct _stritem_nvm *)tmp_kvitem;
                    if (memcmp(key, ITEM_key(kvitem), nkey) == 0) {
                        res = index;
                        break;
                    }
                }
            }
            if (res)
                break;
            
            uint64_t tmp_bucket = bucket->next;
            bucket_nvm *next_bucket = (bucket_nvm *)tmp_bucket;
            bucket = next_bucket;
            //bucket = bucket->next;
        }
        if (res) {
            index_nvm *new = slabs_alloc_index();
            if (new == NULL) {
                printf("error in assoc_delete_nvm()...\n");
                exit(0);
            }
            new->memory_is_dram = 0;
            new->bucket_in_use = 1;
            new->idle_periods = res->idle_periods;
            new->refcount = res->refcount;
            new->counter = res->counter;
            new->keysign = res->keysign;
            new->time = res->time;

            new->kvitem = res->kvitem;
            
            //res->kvitem->index = new;
            uint64_t tmp_kvitem = res->kvitem;
            struct _stritem_nvm *kvitem = (struct _stritem_nvm *)tmp_kvitem;
            kvitem->index = new;
            
            //res->kvitem = NULL;
            res->kvitem = 0;
            
            res->bucket_in_use = 0;
        }
    }
}


/*
static volatile int do_run_maintenance_thread_nvm = 1;

int hash_bulk_move_nvm = DEFAULT_HASH_BULK_MOVE;

static void *assoc_maintenance_thread_nvm(void *arg) {
    mutex_lock(&maintenance_lock_nvm);
    while (do_run_maintenance_thread_nvm) {
        int ii = 0;
        // There is only one expansion thread, so no need to global lock.
        for (ii = 0; ii < hash_bulk_move_nvm && expanding_nvm; ++ii) {
            item *it, *next;
            unsigned int bucket;
            void *item_lock = NULL;

            if ((item_lock = item_trylock_nvm(expand_bucket_nvm))) {
                for (it = old_hashtable_nvm[expand_bucket_nvm]; NULL != it; it = next) {
                    next = it->h_next;
                    bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashpower_nvm);                        
                    it->h_next = primary_hashtable_nvm[bucket];
                    primary_hashtable_nvm[bucket] = it;
                }
                old_hashtable_nvm[expand_bucket_nvm] = NULL;
                expand_bucket_nvm++;
                if (expand_bucket_nvm == hashsize(hashpower_nvm - 1)) {
                    expanding_nvm = false;
                    free(old_hashtable_nvm);
                }
            } else {
                usleep(10*1000);
            }

            if (item_lock) {
                item_trylock_unlock_nvm(item_lock);
                item_lock = NULL;
            }
        }

        if (!expanding_nvm) {
            started_expanding_nvm = false;
            pthread_cond_wait(&maintenance_cond_nvm, &maintenance_lock_nvm);
            // TODO TODO TODO
            pause_threads(PAUSE_ALL_THREADS);
            assoc_expand_nvm();
            pause_threads(RESUME_ALL_THREADS);
        }
    }

    return NULL;
}

static pthread_t maintenance_tid_nvm;

int start_assoc_maintenance_thread_nvm() {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env != NULL) {
        hash_bulk_move_nvm = atoi(env);
        if (hash_bulk_move_nvm == 0) {
            hash_bulk_move_nvm = DEFAULT_HASH_BULK_MOVE;
        }
    }
    
    pthread_mutex_init(&maintenance_lock_nvm, NULL);
    if ((ret = pthread_create(&maintenance_tid_nvm, NULL,
                    assoc_maintenance_thread_nvm, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }

    return 0;
}

void stop_assoc_maintenance_thread_nvm() {
    mutex_lock(&maintenance_lock_nvm);
    do_run_maintenance_thread_nvm = 0;
    pthread_cond_signal(&maintenance_cond_nvm);
    mutex_unlock(&maintenance_lock_nvm);
    pthread_join(maintenance_tid_nvm, NULL);
}
*/
