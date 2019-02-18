/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in size
 * and are divided into chunks. The chunk sizes start off at the size of the
 * "item" structure plus space for a small key and value. They increase by
 * a multiplier factor from there, up to half the maximum slab size. The last
 * slab size is always 1MB, since that's the maximum item size allowed by the
 * memcached protocol.
 */
#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <assert.h>
#include <pthread.h>
#include <numa.h>


//#define DEBUG_SLAB_MOVER
/* powers-of-N allocation structures */

typedef struct {
    unsigned int size;      /* sizes of items */
    unsigned int perslab;   /* how many items per slab */

    void *slots;           /* list of item ptrs */
    unsigned int sl_curr;   /* total free items in list */

    unsigned int slabs;     /* how many slabs were allocated for this class */

    void **slab_list;       /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    size_t requested; /* The number of requested bytes */

// for decay counter in NVM_ZONE
    unsigned int slabs_ptr;
    unsigned int perslab_ptr;

    bool decay_counter_done;
    unsigned int decay_slabs_ptr;
    unsigned int decay_perslab_ptr;
} slabclass_t;


static slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];
static size_t mem_limit = 0;
static size_t mem_malloced = 0;
/* If the memory limit has been hit once. Used as a hint to decide when to
 * early-wake the LRU maintenance thread */
static bool mem_limit_reached = false;
static int power_largest;

static void *mem_base = NULL;
static void *mem_current = NULL;
static size_t mem_avail = 0;
#ifdef EXTSTORE
static void *storage  = NULL;
#endif
/**
 * Access to the slab allocator is protected by this lock
 */
static pthread_mutex_t slabs_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t slabs_rebalance_lock = PTHREAD_MUTEX_INITIALIZER;


/*
 * Forward Declarations
 */
static int grow_slab_list (const unsigned int id);
static int do_slabs_newslab(const unsigned int id);
static void *memory_allocate(size_t size);
static void do_slabs_free(void *ptr, const size_t size, unsigned int id);

/* Preallocate as many slab pages as possible (called from slabs_init)
   on start-up, so users don't get confused out-of-memory errors when
   they do have free (in-slab) space, but no space to make new slabs.
   if maxslabs is 18 (POWER_LARGEST - POWER_SMALLEST + 1), then all
   slab types can be made.  if max memory is less than 18 MB, only the
   smaller ones will be made.  */
static void slabs_preallocate (const unsigned int maxslabs);
#ifdef EXTSTORE
void slabs_set_storage(void *arg) {
    storage = arg;
}
#endif


// BUCKET POOL
static slabclass_t slabbucket;
pthread_mutex_t slabs_lock_bucket = PTHREAD_MUTEX_INITIALIZER;
uint64_t mem_alloc_bucket = 0;

static int grow_slab_list_bucket(void);
static void split_slab_page_into_freelist_bucket(char *ptr);
static int do_slabs_newslab_bucket(void);
static void *do_slabs_alloc_bucket(void);
static void do_slabs_free_bucket(void *ptr);

// INDEX POOL
static slabclass_t slabindex;
pthread_mutex_t slabs_lock_index = PTHREAD_MUTEX_INITIALIZER;
uint64_t mem_alloc_index = 0;

static int grow_slab_list_index(void);
static void split_slab_page_into_freelist_index(char *ptr);
static int do_slabs_newslab_index(void);
static void *do_slabs_alloc_index(void);
static void do_slabs_free_index(void *ptr);


void *malloc_dram(unsigned int bytes);
void *malloc_nvm(unsigned int bytes);

static uint64_t dram_malloc_size = 0;
static uint64_t nvm_malloc_size = 0;

void *malloc_dram(unsigned int bytes) {
    void *ret = numa_alloc_onnode(bytes, 0);
    if (ret == NULL)
        printf("error in malloc_dram()\n");
    dram_malloc_size += bytes;
    return ret;
}

void *malloc_nvm(unsigned int bytes) {
    void *ret = numa_alloc_onnode(bytes, 1);
    if (ret == NULL)
        printf("error in malloc_nvm()\n");
    nvm_malloc_size += bytes;
    return ret;
}

uint64_t get_dram_allocated(void) {
    return dram_malloc_size;
}

uint64_t get_nvm_allocated(void) {
    return nvm_malloc_size;
}

/*
 * Figures out which slab class (chunk size) is required to store an item of
 * a given size.
 *
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(const size_t size) {
    int res = POWER_SMALLEST;

    if (size == 0 || size > settings.item_size_max)
        return 0;
    while (size > slabclass[res].size)
        if (res++ == power_largest)     /* won't fit in the biggest slab */
            return power_largest;
    return res;
}

/**
 * Determines the chunk sizes and initializes the slab class descriptors
 * accordingly.
 */
void slabs_init(const size_t limit, const double factor, const bool prealloc, const uint32_t *slab_sizes) {
    int i = POWER_SMALLEST - 1;
    unsigned int size = sizeof(item) + settings.chunk_size;

    size = 96;

    mem_limit = limit;

    if (prealloc) {
        /* Allocate everything in a big chunk with malloc */
        mem_base = malloc_dram(mem_limit);
        if (mem_base != NULL) {
            mem_current = mem_base;
            mem_avail = mem_limit;
        } else {
            fprintf(stderr, "Warning: Failed to allocate requested memory in"
                    " one large chunk.\nWill allocate in smaller chunks\n");
        }
    }

    memset(slabclass, 0, sizeof(slabclass));

    while (++i < MAX_NUMBER_OF_SLAB_CLASSES-1) {
        if (slab_sizes != NULL) {
            if (slab_sizes[i-1] == 0)
                break;
            size = slab_sizes[i-1];
        } else if (size >= settings.slab_chunk_size_max / factor) {
            break;
        }
        /* Make sure items are always n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES)
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);

        slabclass[i].size = size;
        slabclass[i].perslab = settings.slab_page_size / slabclass[i].size;
       
        if (i <= 20)
            printf("SlabClass %2d: %4d\n", i, slabclass[i].size);

        if (slab_sizes == NULL)
            size *= factor;
        if (settings.verbose > 1) {
            fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n",
                    i, slabclass[i].size, slabclass[i].perslab);
        }
    }

    power_largest = i;
    slabclass[power_largest].size = settings.slab_chunk_size_max;
    slabclass[power_largest].perslab = settings.slab_page_size / settings.slab_chunk_size_max;
    // printf("SlabClass %2d: %4d\n", power_largest, slabclass[power_largest].size);
    if (settings.verbose > 1) {
        fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n",
                i, slabclass[i].size, slabclass[i].perslab);
    }

    /* for the test suite:  faking of how much we've already malloc'd */
    {
        char *t_initial_malloc = getenv("T_MEMD_INITIAL_MALLOC");
        if (t_initial_malloc) {
            mem_malloced = (size_t)atol(t_initial_malloc);
        }

    }

    if (prealloc) {
        slabs_preallocate(power_largest);
    }
}

void slabs_prefill_global(void) {
    void *ptr;
    slabclass_t *p = &slabclass[0];
    int len = settings.slab_page_size;

    while (mem_malloced < mem_limit
            && (ptr = memory_allocate(len)) != NULL) {
        grow_slab_list(0);
        p->slab_list[p->slabs++] = ptr;
    }
    mem_limit_reached = true;

    printf("mem_limit_reached == true in slabs_prefill_global\n");
}

static void slabs_preallocate (const unsigned int maxslabs) {
    int i;
    unsigned int prealloc = 0;

    /* pre-allocate a 1MB slab in every size class so people don't get
       confused by non-intuitive "SERVER_ERROR out of memory"
       messages.  this is the most common question on the mailing
       list.  if you really don't want this, you can rebuild without
       these three lines.  */

    for (i = POWER_SMALLEST; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
        if (++prealloc > maxslabs)
            return;
        if (do_slabs_newslab(i) == 0) {
            fprintf(stderr, "Error while preallocating slab memory!\n"
                "If using -L or other prealloc options, max memory must be "
                "at least %d megabytes.\n", power_largest);
            exit(1);
        }
    }
}

static int grow_slab_list (const unsigned int id) {
    slabclass_t *p = &slabclass[id];
    if (p->slabs == p->list_size) {
        size_t new_size =  (p->list_size != 0) ? p->list_size * 2 : 16;
        void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
        if (new_list == 0) return 0;
        p->list_size = new_size;
        p->slab_list = new_list;
    }
    return 1;
}

static void split_slab_page_into_freelist(char *ptr, const unsigned int id) {
    slabclass_t *p = &slabclass[id];
    int x;
    for (x = 0; x < p->perslab; x++) {
        do_slabs_free(ptr, 0, id);
        ptr += p->size;
    }
}

/* Fast FIFO queue */
static void *get_page_from_global_pool(void) {
    slabclass_t *p = &slabclass[SLAB_GLOBAL_PAGE_POOL];
    if (p->slabs < 1) {
        return NULL;
    }
    char *ret = p->slab_list[p->slabs - 1];
    p->slabs--;
    return ret;
}

static int do_slabs_newslab(const unsigned int id) {
    slabclass_t *p = &slabclass[id];
    slabclass_t *g = &slabclass[SLAB_GLOBAL_PAGE_POOL];
    int len = (settings.slab_reassign || settings.slab_chunk_size_max != settings.slab_page_size)
              ? settings.slab_page_size
              : p->size * p->perslab;
    char *ptr;

    if ((mem_limit && mem_malloced + len > mem_limit /* && p->slabs > 0 */
         && g->slabs == 0)) {
        mem_limit_reached = true;
        MEMCACHED_SLABS_SLABCLASS_ALLOCATE_FAILED(id);
        return 0;
    }

    if ((grow_slab_list(id) == 0) ||
        (((ptr = get_page_from_global_pool()) == NULL) &&
        ((ptr = memory_allocate((size_t)len)) == 0))) {

        MEMCACHED_SLABS_SLABCLASS_ALLOCATE_FAILED(id);
        return 0;
    }

    memset(ptr, 0, (size_t)len);
    split_slab_page_into_freelist(ptr, id);

    p->slab_list[p->slabs++] = ptr;
    MEMCACHED_SLABS_SLABCLASS_ALLOCATE(id);

    return 1;
}

/*@null@*/
static void *do_slabs_alloc(const size_t size, unsigned int id,
        uint64_t *total_bytes, unsigned int flags) {
    slabclass_t *p;
    void *ret = NULL;
    item *it = NULL;

    if (id < POWER_SMALLEST || id > power_largest) {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, 0);
        return NULL;
    }
    p = &slabclass[id];
    assert(p->sl_curr == 0 || ((item *)p->slots)->slabs_clsid == 0);
    if (total_bytes != NULL) {
        *total_bytes = p->requested;
    }

    assert(size <= p->size);
    /* fail unless we have space at the end of a recently allocated page,
       we have something on our freelist, or we could allocate a new page */
    if (p->sl_curr == 0 && flags != SLABS_ALLOC_NO_NEWPAGE) {
        do_slabs_newslab(id);
    }

    if (p->sl_curr != 0) {
        /* return off our freelist */
        it = (item *)p->slots;
        p->slots = it->next;
        if (it->next) it->next->prev = 0;
        /* Kill flag and initialize refcount here for lock safety in slab
         * mover's freeness detection. */
        it->it_flags &= ~ITEM_SLABBED;
        it->refcount = 1;
        p->sl_curr--;
        ret = (void *)it;
    } else {
        ret = NULL;
    }

    if (ret) {
        ((item *)ret)->memory_is_dram = 1;
        p->requested += size;
        MEMCACHED_SLABS_ALLOCATE(size, id, p->size, ret);
    } else {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, id);
    }

    return ret;
}

static void do_slabs_free_chunked(item *it, const size_t size) {
    item_chunk *chunk = (item_chunk *) ITEM_data(it);
    slabclass_t *p;

    it->it_flags = ITEM_SLABBED;
    it->slabs_clsid = 0;
    it->prev = 0;
    // header object's original classid is stored in chunk.
    p = &slabclass[chunk->orig_clsid];
    if (chunk->next) {
        chunk = chunk->next;
        chunk->prev = 0;
    } else {
        // header with no attached chunk
        chunk = NULL;
    }

    // return the header object.
    // TODO: This is in three places, here and in do_slabs_free().
    it->prev = 0;
    it->next = p->slots;
    if (it->next) it->next->prev = it;
    p->slots = it;
    p->sl_curr++;
    // TODO: macro
    p->requested -= it->nkey + 1 + it->nsuffix + sizeof(item) + sizeof(item_chunk);
    if (settings.use_cas) {
        p->requested -= sizeof(uint64_t);
    }

    item_chunk *next_chunk;
    while (chunk) {
        assert(chunk->it_flags == ITEM_CHUNK);
        chunk->it_flags = ITEM_SLABBED;
        p = &slabclass[chunk->slabs_clsid];
        chunk->slabs_clsid = 0;
        next_chunk = chunk->next;

        chunk->prev = 0;
        chunk->next = p->slots;
        if (chunk->next) chunk->next->prev = chunk;
        p->slots = chunk;
        p->sl_curr++;
        p->requested -= chunk->size + sizeof(item_chunk);

        chunk = next_chunk;
    }

    return;
}


static void do_slabs_free(void *ptr, const size_t size, unsigned int id) {
    slabclass_t *p;
    item *it;

    assert(id >= POWER_SMALLEST && id <= power_largest);
    if (id < POWER_SMALLEST || id > power_largest)
        return;

    MEMCACHED_SLABS_FREE(size, id, ptr);
    p = &slabclass[id];

    it = (item *)ptr;
    if ((it->it_flags & ITEM_CHUNKED) == 0) {
#ifdef EXTSTORE
        bool is_hdr = it->it_flags & ITEM_HDR;
#endif
        it->it_flags = ITEM_SLABBED;
        it->slabs_clsid = 0;
        it->prev = 0;
        it->next = p->slots;
        if (it->next) it->next->prev = it;
        p->slots = it;

        p->sl_curr++;
#ifdef EXTSTORE
        if (!is_hdr) {
            p->requested -= size;
        } else {
            p->requested -= (size - it->nbytes) + sizeof(item_hdr);
        }
#else
        p->requested -= size;
#endif
    } else {
        do_slabs_free_chunked(it, size);
    }
    return;
}

/* With refactoring of the various stats code the automover won't need a
 * custom function here.
 */
void fill_slab_stats_automove(slab_stats_automove *am) {
    int n;
    pthread_mutex_lock(&slabs_lock);
    for (n = 0; n < MAX_NUMBER_OF_SLAB_CLASSES; n++) {
        slabclass_t *p = &slabclass[n];
        slab_stats_automove *cur = &am[n];
        cur->chunks_per_page = p->perslab;
        cur->free_chunks = p->sl_curr;
        cur->total_pages = p->slabs;
        cur->chunk_size = p->size;
    }
    pthread_mutex_unlock(&slabs_lock);
}

/* TODO: slabs_available_chunks should grow up to encompass this.
 * mem_flag is redundant with the other function.
 */
unsigned int global_page_pool_size(bool *mem_flag) {
    unsigned int ret = 0;
    pthread_mutex_lock(&slabs_lock);
    if (mem_flag != NULL)
        *mem_flag = mem_malloced >= mem_limit ? true : false;
    ret = slabclass[SLAB_GLOBAL_PAGE_POOL].slabs;
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

static int nz_strcmp(int nzlength, const char *nz, const char *z) {
    int zlength=strlen(z);
    return (zlength == nzlength) && (strncmp(nz, z, zlength) == 0) ? 0 : -1;
}

bool get_stats(const char *stat_type, int nkey, ADD_STAT add_stats, void *c) {
    bool ret = true;

    if (add_stats != NULL) {
        if (!stat_type) {
            /* prepare general statistics for the engine */
            STATS_LOCK();
            APPEND_STAT("bytes", "%llu", (unsigned long long)stats_state.curr_bytes);
            APPEND_STAT("curr_items", "%llu", (unsigned long long)stats_state.curr_items);
            APPEND_STAT("total_items", "%llu", (unsigned long long)stats.total_items);
            STATS_UNLOCK();
            pthread_mutex_lock(&slabs_lock);
            APPEND_STAT("slab_global_page_pool", "%u", slabclass[SLAB_GLOBAL_PAGE_POOL].slabs);
            pthread_mutex_unlock(&slabs_lock);
            item_stats_totals(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "items") == 0) {
            item_stats(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "slabs") == 0) {
            slabs_stats(add_stats, c);
            slabs_stats_nvm(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "sizes") == 0) {
            item_stats_sizes(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "sizes_enable") == 0) {
            item_stats_sizes_enable(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "sizes_disable") == 0) {
            item_stats_sizes_disable(add_stats, c);
        } else {
            ret = false;
        }
    } else {
        ret = false;
    }

    return ret;
}

/*@null@*/
static void do_slabs_stats(ADD_STAT add_stats, void *c) {
    int i, total;
    /* Get the per-thread stats which contain some interesting aggregates */
    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(&thread_stats);

    total = 0;
    for(i = POWER_SMALLEST; i <= power_largest; i++) {
        slabclass_t *p = &slabclass[i];
        if (p->slabs != 0) {
            uint32_t perslab, slabs;
            slabs = p->slabs;
            perslab = p->perslab;

            char key_str[STAT_KEY_LEN];
            char val_str[STAT_VAL_LEN];
            int klen = 0, vlen = 0;

            APPEND_NUM_STAT(i, "DRAM_ZONE", "%u", 0);
            APPEND_NUM_STAT(i, "chunk_size", "%u", p->size);
            APPEND_NUM_STAT(i, "chunks_per_page", "%u", perslab);
            APPEND_NUM_STAT(i, "total_pages", "%u", slabs);
            APPEND_NUM_STAT(i, "total_chunks", "%u", slabs * perslab);
            APPEND_NUM_STAT(i, "used_chunks", "%u",
                            slabs*perslab - p->sl_curr);
            APPEND_NUM_STAT(i, "free_chunks", "%u", p->sl_curr);
            /* Stat is dead, but displaying zero instead of removing it. */
            APPEND_NUM_STAT(i, "free_chunks_end", "%u", 0);
            APPEND_NUM_STAT(i, "mem_requested", "%llu",
                            (unsigned long long)p->requested);
            APPEND_NUM_STAT(i, "get_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].get_hits);
            APPEND_NUM_STAT(i, "cmd_set", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].set_cmds);
            APPEND_NUM_STAT(i, "delete_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].delete_hits);
            APPEND_NUM_STAT(i, "incr_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].incr_hits);
            APPEND_NUM_STAT(i, "decr_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].decr_hits);
            APPEND_NUM_STAT(i, "cas_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].cas_hits);
            APPEND_NUM_STAT(i, "cas_badval", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].cas_badval);
            APPEND_NUM_STAT(i, "touch_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].touch_hits);
            total++;
        }
    }

    /* add overall slab stats and append terminator */

    APPEND_STAT("active_slabs", "%d", total);
    APPEND_STAT("total_malloced", "%llu", (unsigned long long)mem_malloced / 1024 / 1024);
    add_stats(NULL, 0, NULL, 0, c);
}

static void *memory_allocate(size_t size) {
    void *ret;

    if (mem_base == NULL) {
        /* We are not using a preallocated large memory chunk */
        ret = malloc_dram(size);
    } else {
        ret = mem_current;

        if (size > mem_avail) {
            return NULL;
        }

        /* mem_current pointer _must_ be aligned!!! */
        if (size % CHUNK_ALIGN_BYTES) {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }

        mem_current = ((char*)mem_current) + size;
        if (size < mem_avail) {
            mem_avail -= size;
        } else {
            mem_avail = 0;
        }
    }
    mem_malloced += size;

    return ret;
}

/* Must only be used if all pages are item_size_max */
static void memory_release() {
    void *p = NULL;
    if (mem_base != NULL)
        return;

    if (!settings.slab_reassign)
        return;

    while (mem_malloced > mem_limit &&
            (p = get_page_from_global_pool()) != NULL) {
        free(p);
        mem_malloced -= settings.slab_page_size;
    }
}

void *slabs_alloc(size_t size, unsigned int id, uint64_t *total_bytes,
        unsigned int flags) {
    void *ret;

    pthread_mutex_lock(&slabs_lock);
    ret = do_slabs_alloc(size, id, total_bytes, flags);
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

void slabs_free(void *ptr, size_t size, unsigned int id) {
    pthread_mutex_lock(&slabs_lock);
    do_slabs_free(ptr, size, id);
    pthread_mutex_unlock(&slabs_lock);
}

void slabs_stats(ADD_STAT add_stats, void *c) {
    pthread_mutex_lock(&slabs_lock);
    do_slabs_stats(add_stats, c);
    pthread_mutex_unlock(&slabs_lock);
}

static bool do_slabs_adjust_mem_limit(size_t new_mem_limit) {
    /* Cannot adjust memory limit at runtime if prealloc'ed */
    if (mem_base != NULL)
        return false;
    settings.maxbytes = new_mem_limit;
    mem_limit = new_mem_limit;
    mem_limit_reached = false; /* Will reset on next alloc */
    memory_release(); /* free what might already be in the global pool */
    return true;
}

bool slabs_adjust_mem_limit(size_t new_mem_limit) {
    bool ret;
    pthread_mutex_lock(&slabs_lock);
    ret = do_slabs_adjust_mem_limit(new_mem_limit);
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

void slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal)
{
    pthread_mutex_lock(&slabs_lock);
    slabclass_t *p;
    if (id < POWER_SMALLEST || id > power_largest) {
        fprintf(stderr, "Internal error! Invalid slab class\n");
        abort();
    }

    p = &slabclass[id];
    p->requested = p->requested - old + ntotal;
    pthread_mutex_unlock(&slabs_lock);
}

unsigned int slabs_available_chunks(const unsigned int id, bool *mem_flag,
        uint64_t *total_bytes, unsigned int *chunks_perslab) {
    unsigned int ret;
    slabclass_t *p;

    pthread_mutex_lock(&slabs_lock);
    p = &slabclass[id];
    ret = p->sl_curr;
    if (mem_flag != NULL)
        *mem_flag = mem_malloced >= mem_limit ? true : false;
    if (total_bytes != NULL)
        *total_bytes = p->requested;
    if (chunks_perslab != NULL)
        *chunks_perslab = p->perslab;
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

/* The slabber system could avoid needing to understand much, if anything,
 * about items if callbacks were strategically used. Due to how the slab mover
 * works, certain flag bits can only be adjusted while holding the slabs lock.
 * Using these functions, isolate sections of code needing this and turn them
 * into callbacks when an interface becomes more obvious.
 */
void slabs_mlock(void) {
    pthread_mutex_lock(&slabs_lock);
}

void slabs_munlock(void) {
    pthread_mutex_unlock(&slabs_lock);
}

static pthread_cond_t slab_rebalance_cond = PTHREAD_COND_INITIALIZER;
static volatile int do_run_slab_thread = 1;
static volatile int do_run_slab_rebalance_thread = 1;

int DRAM_Repartition_number = 0;

#define DEFAULT_SLAB_BULK_CHECK 1
int slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;

static int slab_rebalance_start(void) {
    slabclass_t *s_cls;
    int no_go = 0;

    pthread_mutex_lock(&slabs_lock);

    if (slab_rebal.s_clsid < SLAB_GLOBAL_PAGE_POOL ||
        slab_rebal.s_clsid > power_largest  ||
        slab_rebal.d_clsid < SLAB_GLOBAL_PAGE_POOL ||
        slab_rebal.d_clsid > power_largest  ||
        slab_rebal.s_clsid == slab_rebal.d_clsid)
        no_go = -2;

    s_cls = &slabclass[slab_rebal.s_clsid];

    if (!grow_slab_list(slab_rebal.d_clsid)) {
        no_go = -1;
    }

    if (s_cls->slabs <= 0)
        no_go = -3;

    if (no_go != 0) {
        pthread_mutex_unlock(&slabs_lock);
        return no_go; /* Should use a wrapper function... */
    }

    /* TODO Always kill the first available slab page as it is most likely to
     * contain the oldest items
     */
    slab_rebal.slab_start = s_cls->slab_list[0];
    slab_rebal.slab_end   = (char *)slab_rebal.slab_start +
        (s_cls->size * s_cls->perslab);
    slab_rebal.slab_pos   = slab_rebal.slab_start;
    slab_rebal.done       = 0;
    // Don't need to do chunk move work if page is in global pool.
    if (slab_rebal.s_clsid == SLAB_GLOBAL_PAGE_POOL) {
        slab_rebal.done = 1;
    }

    slab_rebalance_signal = 2;

    if (settings.verbose > 1) {
        fprintf(stderr, "Started a slab rebalance\n");
    }

    pthread_mutex_unlock(&slabs_lock);

    STATS_LOCK();
    stats_state.slab_reassign_running = true;
    STATS_UNLOCK();

    return 0;
}

/* CALLED WITH slabs_lock HELD */
static void *slab_rebalance_alloc(const size_t size, unsigned int id) {
    slabclass_t *s_cls;
    s_cls = &slabclass[slab_rebal.s_clsid];
    int x;
    item *new_it = NULL;

    for (x = 0; x < s_cls->perslab; x++) {
        new_it = (item *)do_slabs_alloc(size, id, NULL, SLABS_ALLOC_NO_NEWPAGE);
        /* check that memory isn't within the range to clear */
        if (new_it == NULL) {
            break;
        }
        new_it->memory_is_dram = 1;
        if ((void *)new_it >= slab_rebal.slab_start
            && (void *)new_it < slab_rebal.slab_end) {
            /* Pulled something we intend to free. Mark it as freed since
             * we've already done the work of unlinking it from the freelist.
             */
            s_cls->requested -= size;
            new_it->refcount = 0;
            new_it->it_flags = ITEM_SLABBED|ITEM_FETCHED;
#ifdef DEBUG_SLAB_MOVER
            memcpy(ITEM_key(new_it), "deadbeef", 8);
#endif
            new_it = NULL;
            slab_rebal.inline_reclaim++;
        } else {
            break;
        }
    }

    return new_it;
}

/* CALLED WITH slabs_lock HELD */
/* detaches item/chunk from freelist. */
static void slab_rebalance_cut_free(slabclass_t *s_cls, item *it) {
    /* Ensure this was on the freelist and nothing else. */
    assert(it->it_flags == ITEM_SLABBED);
    if (s_cls->slots == it) {
        s_cls->slots = it->next;
    }
    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
    s_cls->sl_curr--;
}

enum move_status {
    MOVE_PASS=0, MOVE_FROM_SLAB, MOVE_FROM_LRU, MOVE_BUSY, MOVE_LOCKED
};

#define SLAB_MOVE_MAX_LOOPS 1000

/* refcount == 0 is safe since nobody can incr while item_lock is held.
 * refcount != 0 is impossible since flags/etc can be modified in other
 * threads. instead, note we found a busy one and bail. logic in do_item_get
 * will prevent busy items from continuing to be busy
 * NOTE: This is checking it_flags outside of an item lock. I believe this
 * works since it_flags is 8 bits, and we're only ever comparing a single bit
 * regardless. ITEM_SLABBED bit will always be correct since we're holding the
 * lock which modifies that bit. ITEM_LINKED won't exist if we're between an
 * item having ITEM_SLABBED removed, and the key hasn't been added to the item
 * yet. The memory barrier from the slabs lock should order the key write and the
 * flags to the item?
 * If ITEM_LINKED did exist and was just removed, but we still see it, that's
 * still safe since it will have a valid key, which we then lock, and then
 * recheck everything.
 * This may not be safe on all platforms; If not, slabs_alloc() will need to
 * seed the item key while holding slabs_lock.
 */

static int slab_rebalance_move(void) {
    slabclass_t *s_cls;
    int x;
    int was_busy = 0;
    int refcount = 0;
    uint32_t hv;
    void *hold_lock = NULL;
    enum move_status status = MOVE_PASS;

    pthread_mutex_lock(&slabs_lock);
    
    s_cls = &slabclass[slab_rebal.s_clsid];

    for (x = 0; x < slab_bulk_check; x++) {
        hv = 0;
        hold_lock = NULL;
        item *it = slab_rebal.slab_pos;
        status = MOVE_PASS;

        if (it->it_flags != (ITEM_SLABBED|ITEM_FETCHED)) {
            // ITEM_SLABBED can only be added/removed under the slabs_lock
            if (it->it_flags & ITEM_SLABBED) {
                slab_rebalance_cut_free(s_cls, it);
                status = MOVE_FROM_SLAB;
            } else if ((it->it_flags & ITEM_LINKED) != 0) {
                // If it doesn't have ITEM_SLABBED, the item could be in any 
                // state on its way to begin freed or written to. If no
                // ITEM_SLABBED, but it's had ITEM_LINKED, it must be active
                // and have the key written to it already.
                hv = hash(ITEM_key(it), it->nkey);
                if ((hold_lock = item_trylock(hv)) == NULL) {
                    status = MOVE_LOCKED;
                } else {    
                    bool is_linked = (it->it_flags & ITEM_LINKED);
                    refcount = refcount_incr(it);
                    if (refcount == 2) {  // item is linked but not busy
                        // Double check ITEM_LINKED flag here, since we're
                        // past a memory barrier from the mutex.
                        if (is_linked) {
                            status = MOVE_FROM_LRU;
                        } else {
                            // refcount == 1 + !ITEM_LINKED means the item is being
                            // uploaded to, or was just unlinked but hasn't been freed
                            // yet. Let it bleed off on its own and try again later.
                            status = MOVE_BUSY;
                        }
                    } else if (refcount > 2 && is_linked) {
                        // TODO Mark items for delete/rescue and process
                        // outside of the main loop
                        if (slab_rebal.busy_loops > SLAB_MOVE_MAX_LOOPS) {
                            slab_rebal.busy_deletes++;
                            // Only safe to hold slabs lock because refcount
                            // can't drop to 0 until we release item lock.
                            pthread_mutex_unlock(&slabs_lock);
                            do_item_unlink(it, hv);
                            pthread_mutex_lock(&slabs_lock);
                        }
                        status = MOVE_BUSY;
                    } else {
                        if (settings.verbose > 2) {
                            fprintf(stderr, "Slab reassign hit a busy item: refcount: %d (%d -> %d)\n",
                                    it->refcount, slab_rebal.s_clsid, slab_rebal.d_clsid);
                        }
                        status = MOVE_BUSY;
                    }

                    // Item lock must be held while modifying refcount
                    if (status == MOVE_BUSY) {
                        refcount_decr(it);
                        item_trylock_unlock(hold_lock);
                        hold_lock = NULL;
                    }
                }
            } else {
                // See above comment. No ITEM_SLABBED or ITEM_LINKED. Mark 
                // busy and wait for item to complete its upload.
                status = MOVE_BUSY;
            }
        }

        int save_item = 0;
        item *new_it = NULL;
        size_t ntotal = 0;

        switch (status) {
            case MOVE_FROM_LRU:
                // Lock order is LRU locks -> slabs_lock. unlink uses LRU lock.
                // We only need to hold the slabs_lock while initially looking
                // at an item, and at this point we have an exclusive refcount
                // (2) + the item is locked. Drop slabs lock, drop item to
                // refcount 1 (just our own, then fail through and wipe it)
                ntotal = ITEM_ntotal(it);
                
                // Check if expired or flushed
                if ((it->exptime != 0 && it->exptime < current_time)
                    || item_is_flushed(it)) {  // Expired, don't save
                    save_item = 0;
                } else if ((new_it = (item *)slab_rebalance_alloc(ntotal, slab_rebal.s_clsid)) != NULL) {
                    save_item = 1;
                } else {
                    save_item = 2;
                }

                pthread_mutex_unlock(&slabs_lock);
                unsigned int requested_adjust = 0;

//*****************************************************************************
                if (save_item == 2) {
                    assert(new_it == NULL);
                    int tries = 5;
                    item *search;
                    item *next_it;
                    
                    int i;
                    for (i = 0; i < it->MQ_num; i++) {  // MQ_COUNT == 16
                        int id = ITEM_clsid(it) * MQ_COUNT + i;
                        pthread_mutex_lock(&lru_locks[id]);
                        
                        search = get_mq_tails(id);
                        for (; tries > 0 && search != NULL; tries--, search = next_it) {
                            next_it = search->prev;
                            if (search->nbytes == 0 && search->nkey == 0 && search->it_flags == 1) {
                                tries++;
                                continue;
                            } else if (search->nbytes == 1 && search->nkey == 0 && search->it_flags == 1) {
                                tries++;
                                continue;
                            } else if ((void *)search >= slab_rebal.slab_start
                                    && (void *)search < slab_rebal.slab_end) {
                                tries++;
                                continue;
                            }

                            uint32_t hv_2 = hash(ITEM_key(search), search->nkey);            
                            void *hold_lock_2 = NULL;
                            
                            if (hv != hv_2 || hold_lock == NULL) {
                                if ((hold_lock_2 = item_trylock(hv_2)) == NULL)
                                    continue;
                            }

                            assert((search->it_flags & ITEM_LINKED) != 0);

                            if (refcount_incr(search) == 2) {
                                if ((search->exptime != 0 && search->exptime < current_time)
                                        || item_is_flushed(search)) {
                                    do_item_unlink_nolock(search, hv_2);
                                    // do_item_remove(search);   // TODO TODO
                                    // item_trylock_unlock(hold_lock_2);
                                    // continue;
                                } else {
                                    move_to_nvm_zone(search, hv_2, false);
                                    do_item_unlink_nolock(search, hv_2);
                                    // do_item_remove(search);  // TODO TODO
                                }
                                new_it = search;
                                
                                if (hv != hv_2 || hold_lock == NULL) {
                                    item_trylock_unlock(hold_lock_2);
                                    hold_lock_2 = NULL;
                                }
                            } else {
                                refcount_decr(search);
                                new_it = NULL;
                                if (hv != hv_2 || hold_lock == NULL) {
                                    item_trylock_unlock(hold_lock_2);
                                    hold_lock_2 = NULL;
                                }
                            }
                            if (new_it != NULL)
                                break;
                        }  // end of (for tries == 5)
                        
                        pthread_mutex_unlock(&lru_locks[id]);
                        if (new_it != NULL)
                            break;
                    }  // end of (for i = 0 -> i < it->MQ_num)
                    if (new_it != NULL)
                        save_item = 3;
                }  // end of (save_item == 2)

                if (save_item == 1 || save_item == 3) {
                    assert((new_it->it_flags & ITEM_CHUNKED) == 0);
                    // if free memory, memcpy, clear prev/next/h_bucket
                    memcpy(new_it, it, ntotal);
                    new_it->slabs_clsid = it->slabs_clsid;
                    new_it->prev = 0;
                    new_it->next = 0;
                    new_it->h_next = 0;
                    // These are definitely requested, else fails assert
                    new_it->it_flags &= ~ITEM_LINKED;
                    new_it->refcount = 0;
                    new_it->idle_cycle = it->idle_cycle;
                    new_it->counter = it->counter;
                    new_it->memory_is_dram = 1;
                    
                    do_item_replace(it, new_it, hv, false);
                    
                    it->refcount = 0;
                    it->it_flags = ITEM_SLABBED|ITEM_FETCHED;

                    slab_rebal.rescues++;
                    requested_adjust = ntotal;
                
                } else if (save_item == 2) {
                    // TODO TODO TODO
                    // already unlock slabs_lock
                    pthread_mutex_lock(&lru_locks[it->MQ_id]);
                    move_to_nvm_zone(it, hv, false);
                    do_item_unlink_nolock(it, hv);
                    pthread_mutex_unlock(&lru_locks[it->MQ_id]);
                    
                    it->refcount = 0;
                    it->it_flags = ITEM_SLABBED|ITEM_FETCHED;
                    requested_adjust = ntotal;
                
                } else {  // save_item == 0
                
                    // restore ntotal in case we tried saving a head chunk
                    ntotal = ITEM_ntotal(it);
                    do_item_unlink(it, hv);
                    slabs_free(it, ntotal, slab_rebal.s_clsid);
                    // Swing around again later to remove it from the freelist.
                    slab_rebal.busy_items++;
                    was_busy++;
                
                }

                item_trylock_unlock(hold_lock);
                hold_lock = NULL;
                pthread_mutex_lock(&slabs_lock);
                // Always remove the ntotal, as we added it in during
                // do_slabs_alloc() when copying the item.
                s_cls->requested -= requested_adjust;
                break;
            case MOVE_FROM_SLAB:        
                it->refcount = 0;
                it->it_flags = ITEM_SLABBED|ITEM_FETCHED;
                break;
            case MOVE_BUSY:
            case MOVE_LOCKED:
                slab_rebal.busy_items++;
                break;
            case MOVE_PASS:

                break;
        }

        slab_rebal.slab_pos = (char *)slab_rebal.slab_pos + s_cls->size;
        if (slab_rebal.slab_pos >= slab_rebal.slab_end)
            break;
    }

    if (slab_rebal.slab_pos >= slab_rebal.slab_end) {
        if (slab_rebal.busy_items) {
            slab_rebal.slab_pos = slab_rebal.slab_start;
            STATS_LOCK();
            stats.slab_reassign_busy_items += slab_rebal.busy_items;
            STATS_UNLOCK();
            slab_rebal.busy_items = 0;
            slab_rebal.busy_loops++;
        } else {
            slab_rebal.done++;
        }
    }

    pthread_mutex_unlock(&slabs_lock);

    return was_busy;
}

static void slab_rebalance_finish(void) {
    // printf("slab_rebalance_finish...\n");

    slabclass_t *s_cls;
    slabclass_t *d_cls;
    int x;
    uint32_t rescues;
    uint32_t evictions_nomem;
    uint32_t inline_reclaim;
    uint32_t chunk_rescues;
    uint32_t busy_deletes;

    pthread_mutex_lock(&slabs_lock);

    s_cls = &slabclass[slab_rebal.s_clsid];
    d_cls = &slabclass[slab_rebal.d_clsid];

#ifdef DEBUG_SLAB_MOVER
    /* If the algorithm is broken, live items can sneak in. */
    slab_rebal.slab_pos = slab_rebal.slab_start;
    while (1) {
        item *it = slab_rebal.slab_pos;
        assert(it->it_flags == (ITEM_SLABBED|ITEM_FETCHED));
        assert(memcmp(ITEM_key(it), "deadbeef", 8) == 0);
        it->it_flags = ITEM_SLABBED|ITEM_FETCHED;
        slab_rebal.slab_pos = (char *)slab_rebal.slab_pos + s_cls->size;
        if (slab_rebal.slab_pos >= slab_rebal.slab_end)
            break;
    }
#endif

    /* At this point the stolen slab is completely clear.
     * We always kill the "first"/"oldest" slab page in the slab_list, so
     * shuffle the page list backwards and decrement.
     */
    s_cls->slabs--;
    for (x = 0; x < s_cls->slabs; x++) {
        s_cls->slab_list[x] = s_cls->slab_list[x+1];
    }

    d_cls->slab_list[d_cls->slabs++] = slab_rebal.slab_start;
    /* Don't need to split the page into chunks if we're just storing it */
    if (slab_rebal.d_clsid > SLAB_GLOBAL_PAGE_POOL) {
        memset(slab_rebal.slab_start, 0, (size_t)settings.slab_page_size);
        split_slab_page_into_freelist(slab_rebal.slab_start,
            slab_rebal.d_clsid);
    } else if (slab_rebal.d_clsid == SLAB_GLOBAL_PAGE_POOL) {
        /* mem_malloc'ed might be higher than mem_limit. */
        mem_limit_reached = false;
        memory_release();
    }

    slab_rebal.busy_loops = 0;
    slab_rebal.done       = 0;
    slab_rebal.s_clsid    = 0;
    slab_rebal.d_clsid    = 0;
    slab_rebal.slab_start = NULL;
    slab_rebal.slab_end   = NULL;
    slab_rebal.slab_pos   = NULL;
    evictions_nomem    = slab_rebal.evictions_nomem;
    inline_reclaim = slab_rebal.inline_reclaim;
    rescues   = slab_rebal.rescues;
    chunk_rescues = slab_rebal.chunk_rescues;
    busy_deletes = slab_rebal.busy_deletes;
    slab_rebal.evictions_nomem    = 0;
    slab_rebal.inline_reclaim = 0;
    slab_rebal.rescues  = 0;
    slab_rebal.chunk_rescues = 0;
    slab_rebal.busy_deletes = 0;

    slab_rebalance_signal = 0;

    pthread_mutex_unlock(&slabs_lock);

    STATS_LOCK();
    stats.slabs_moved++;
    stats.slab_reassign_rescues += rescues;
    stats.slab_reassign_evictions_nomem += evictions_nomem;
    stats.slab_reassign_inline_reclaim += inline_reclaim;
    stats.slab_reassign_chunk_rescues += chunk_rescues;
    stats.slab_reassign_busy_deletes += busy_deletes;
    stats_state.slab_reassign_running = false;
    STATS_UNLOCK();

    if (settings.verbose > 1) {
        fprintf(stderr, "finished a slab move\n");
    }
}

/* Slab mover thread.
 * Sits waiting for a condition to jump off and shovel some memory about
 */
static void *slab_rebalance_thread(void *arg) {
    int was_busy = 0;
    /* So we first pass into cond_wait with the mutex held */
    mutex_lock(&slabs_rebalance_lock);

    while (do_run_slab_rebalance_thread) {
        if (slab_rebalance_signal == 1) {
            if (slab_rebalance_start() < 0) {
                /* Handle errors with more specificity as required. */
                slab_rebalance_signal = 0;
            }

            was_busy = 0;
        } else if (slab_rebalance_signal && slab_rebal.slab_start != NULL) {
            was_busy = slab_rebalance_move();
        }

        if (slab_rebal.done) {
            slab_rebalance_finish();
        } else if (was_busy) {
            /* Stuck waiting for some items to unlock, so slow down a bit
             * to give them a chance to free up */
            usleep(1000);
        }

        if (slab_rebalance_signal == 0) {
            /* always hold this lock while we're running */
            pthread_cond_wait(&slab_rebalance_cond, &slabs_rebalance_lock);
        }
    }
    return NULL;
}

/* Iterate at most once through the slab classes and pick a "random" source.
 * I like this better than calling rand() since rand() is slow enough that we
 * can just check all of the classes once instead.
 */
static int slabs_reassign_pick_any(int dst) {
    static int cur = POWER_SMALLEST - 1;
    int tries = power_largest - POWER_SMALLEST + 1;
    for (; tries > 0; tries--) {
        cur++;
        if (cur > power_largest)
            cur = POWER_SMALLEST;
        if (cur == dst)
            continue;
        if (slabclass[cur].slabs > 1) {
            return cur;
        }
    }
    return -1;
}

static enum reassign_result_type do_slabs_reassign(int src, int dst) {
    bool nospare = false;
    if (slab_rebalance_signal != 0)
        return REASSIGN_RUNNING;

    if (src == dst)
        return REASSIGN_SRC_DST_SAME;

    /* Special indicator to choose ourselves. */
    if (src == -1) {
        src = slabs_reassign_pick_any(dst);
        /* TODO: If we end up back at -1, return a new error type */
    }

    if (src < SLAB_GLOBAL_PAGE_POOL || src > power_largest ||
        dst < SLAB_GLOBAL_PAGE_POOL || dst > power_largest)
        return REASSIGN_BADCLASS;

    pthread_mutex_lock(&slabs_lock);
    if (slabclass[src].slabs <= 0)
        nospare = true;
    pthread_mutex_unlock(&slabs_lock);
    if (nospare)
        return REASSIGN_NOSPARE;

    slab_rebal.s_clsid = src;
    slab_rebal.d_clsid = dst;

    slab_rebalance_signal = 1;
    pthread_cond_signal(&slab_rebalance_cond);

    // printf("slab_rebalance_cut_signal == 1\n");

    return REASSIGN_OK;
}

enum reassign_result_type slabs_reassign(int src, int dst) {
    enum reassign_result_type ret;
    if (pthread_mutex_trylock(&slabs_rebalance_lock) != 0) {
        return REASSIGN_RUNNING;
    }
    ret = do_slabs_reassign(src, dst);
    pthread_mutex_unlock(&slabs_rebalance_lock);
    return ret;
}

/* If we hold this lock, rebalancer can't wake up or move */
void slabs_rebalancer_pause(void) {
    pthread_mutex_lock(&slabs_rebalance_lock);
}

void slabs_rebalancer_resume(void) {
    pthread_mutex_unlock(&slabs_rebalance_lock);
}

static pthread_t rebalance_tid;

int start_slab_maintenance_thread(void) {
    int ret;
    slab_rebalance_signal = 0;
    slab_rebal.slab_start = NULL;
    char *env = getenv("MEMCACHED_SLAB_BULK_CHECK");
    if (env != NULL) {
        slab_bulk_check = atoi(env);
        if (slab_bulk_check == 0) {
            slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;
        }
    }

    if (pthread_cond_init(&slab_rebalance_cond, NULL) != 0) {
        fprintf(stderr, "Can't initialize rebalance condition\n");
        return -1;
    }
    pthread_mutex_init(&slabs_rebalance_lock, NULL);

    if ((ret = pthread_create(&rebalance_tid, NULL,
                              slab_rebalance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create rebal thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

/* The maintenance thread is on a sleep/loop cycle, so it should join after a
 * short wait */
void stop_slab_maintenance_thread(void) {
    mutex_lock(&slabs_rebalance_lock);
    do_run_slab_thread = 0;
    do_run_slab_rebalance_thread = 0;
    pthread_cond_signal(&slab_rebalance_cond);
    pthread_mutex_unlock(&slabs_rebalance_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(rebalance_tid, NULL);
}


// NVM_ZONE
static slabclass_t slabclass_nvm[MAX_NUMBER_OF_SLAB_CLASSES];
static pthread_mutex_t decay_lock_nvm[MAX_NUMBER_OF_SLAB_CLASSES];

static size_t mem_limit_nvm = 0;
static size_t mem_malloced_nvm = 0;

static bool mem_limit_reached_nvm = false;
static int power_largest_nvm;

static void *mem_base_nvm = NULL;
static void *mem_current_nvm = NULL;
static size_t mem_avail_nvm = 0;

static pthread_mutex_t slabs_lock_nvm = PTHREAD_MUTEX_INITIALIZER;

static int grow_slab_list_nvm(const unsigned int id);               
static int do_slabs_newslab_nvm(const unsigned int id);
static void *memory_allocate_nvm(size_t size);
static void do_slabs_free_nvm(void *ptr, const size_t size, unsigned int id);
static void slabs_preallocate_nvm(const unsigned int maxslabs);


unsigned int slabs_clsid_nvm(const size_t size) {
    int res = POWER_SMALLEST;
    if (size == 0 || size > settings.item_size_max)
        return 0;
    while (size > slabclass_nvm[res].size)
        if (res++ == power_largest_nvm)
            return power_largest;
    return res;
}


void slabs_init_nvm(const size_t limit, const double factor, const bool prealloc, const uint32_t *slab_sizes) 
{
    int i = POWER_SMALLEST - 1;
    unsigned int size = sizeof(item) + settings.chunk_size;

    size = 96;
    
    mem_limit_nvm = limit;
    
    if (prealloc) {
        mem_base_nvm = malloc_nvm(mem_limit_nvm);               
        if (mem_base_nvm != NULL) {
            mem_current_nvm = mem_base_nvm;
            mem_avail_nvm = mem_limit_nvm;
        } else {
            fprintf(stderr, "Warning: Failed to allocate requested memory in"
                    " one large chunk.\nWill allocate in smaller chunks\n");
        }
    }
    
    memset(slabclass_nvm, 0, sizeof(slabclass_nvm));

    assert(sizeof(item) >= sizeof(item_nvm));
    int diff_size = sizeof(item) - sizeof(item_nvm);

    while (++i < MAX_NUMBER_OF_SLAB_CLASSES - 1) {
        if (slab_sizes != NULL) {
            if (slab_sizes[i-1] == 0)
                break;
            size = slab_sizes[i-1];
        } else if (size >= settings.slab_chunk_size_max / factor) {
            break;
        }
        
        if (size % CHUNK_ALIGN_BYTES)
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
    
        slabclass_nvm[i].size = slabclass[i].size - diff_size;
        slabclass_nvm[i].perslab = settings.slab_page_size / slabclass_nvm[i].size;
        pthread_mutex_init(&decay_lock_nvm[i], NULL);

        if (slab_sizes == NULL)
              size *= factor;
    }
    
    power_largest_nvm = i;
    slabclass_nvm[power_largest].size = slabclass[power_largest].size - diff_size;
    slabclass_nvm[power_largest].perslab = settings.slab_page_size / slabclass_nvm[power_largest].size;
    pthread_mutex_init(&decay_lock_nvm[power_largest], NULL);

    if (prealloc) {
        slabs_preallocate_nvm(power_largest);
    }
}

void slabs_prefill_global_nvm(void) {
    void *ptr;
    slabclass_t *p = &slabclass_nvm[0];
    int len = settings.slab_page_size;
    
    while (mem_malloced_nvm < mem_limit_nvm
            && (ptr = memory_allocate_nvm(len)) != NULL) {
        grow_slab_list_nvm(0);
        
        p->slab_list[p->slabs++] = ptr;
    }
    mem_limit_reached_nvm = true;
}

static void slabs_preallocate_nvm(const unsigned int maxslabs) {
    int i;
    unsigned int prealloc = 0;
    
    for (i = POWER_SMALLEST; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
        if (++prealloc > maxslabs)
            return;
        if (do_slabs_newslab_nvm(i) == 0) {
            fprintf(stderr, "Error while preallocating slab memory!\n"
                    "If using -L or other prealloc options, max memory must be "
                    "at least %d megabytes.\n", power_largest_nvm);
            exit(1);
        }
    }
}

static int grow_slab_list_nvm(const unsigned int id) {
    slabclass_t *p = &slabclass_nvm[id];
    if (p->slabs == p->list_size) {
        size_t new_size = (p->list_size != 0) ? p->list_size * 2 : 16;
        void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
        if (new_list == 0) return 0;

        pthread_mutex_lock(&decay_lock_nvm[id]);
        p->list_size = new_size;
        p->slab_list = new_list;
        pthread_mutex_unlock(&decay_lock_nvm[id]);
    }
    return 1;
}

static void split_slab_page_into_freelist_nvm(char *ptr, const unsigned int id) {
    slabclass_t *p = &slabclass_nvm[id];
    int x;
    for (x = 0; x < p->perslab; x++) {
        do_slabs_free_nvm(ptr, 0, id);
        ptr += p->size;
    }
}

/*  Fast FIFO queue */
static void *get_page_from_global_pool_nvm(void) {
    slabclass_t *p = &slabclass_nvm[SLAB_GLOBAL_PAGE_POOL];
    if (p->slabs < 1) {
        return NULL;
    }
    char *ret = p->slab_list[p->slabs - 1];
    p->slabs--;
    return ret;
}

static int do_slabs_newslab_nvm(const unsigned int id) {
    slabclass_t *p = &slabclass_nvm[id];
    slabclass_t *g = &slabclass_nvm[SLAB_GLOBAL_PAGE_POOL];
    int len = (settings.slab_reassign || settings.slab_chunk_size_max != settings.slab_page_size)
                ? settings.slab_page_size : p->size * p->perslab;
    char *ptr;
    
    if ((mem_limit_nvm && mem_malloced_nvm + len > mem_limit_nvm && p->slabs > 0 && g->slabs == 0)) {
        mem_limit_reached_nvm = true;
        return 0;
    }
    
    if ((grow_slab_list_nvm(id) == 0)
            || (((ptr = get_page_from_global_pool_nvm()) == NULL)
                && ((ptr = memory_allocate_nvm((size_t)len)) == 0))) {
        return 0;
    }
    
    memset(ptr, 0, (size_t)len);
    split_slab_page_into_freelist_nvm(ptr, id);
    
    pthread_mutex_lock(&decay_lock_nvm[id]);
    p->slab_list[p->slabs++] = ptr;
    pthread_mutex_unlock(&decay_lock_nvm[id]);

    return 1;
}

static void *do_slabs_alloc_nvm(const size_t size, unsigned int id, 
                                uint64_t *total_bytes, unsigned int flags)
{
    slabclass_t *p;
    void *ret = NULL;
    slabbed_item_nvm *it = NULL;
    
    if (id < POWER_SMALLEST || id > power_largest_nvm)
        return NULL;
    
    p = &slabclass_nvm[id];
    
    assert(p->sl_curr == 0 || ((slabbed_item_nvm *)p->slots)->slabs_clsid == 0);
    if (total_bytes != NULL) {
        *total_bytes = p->requested;
    }
    
    assert(size <= p->size);
    if (p->sl_curr == 0 && flags != SLABS_ALLOC_NO_NEWPAGE_NVM) {
        do_slabs_newslab_nvm(id);
    }
    
    if (p->sl_curr != 0) {
        /* return off our freelist */
        it = (slabbed_item_nvm *)p->slots;
        p->slots = it->next;
        if (it->next) it->next->prev = 0;
        /* Kill flag and initialize refcount here for lock safety in slab
         * mover's freeness detection. */
        it->it_flags &= ~ITEM_SLABBED;
        // it->refcount = 1;
        p->sl_curr--;
        ret = (void *)it;
    } else {
        ret = NULL;
    }

    if (ret) {
        ((item_nvm *)ret)->memory_is_dram = 0;
        p->requested += size;
    }

    return ret;
}

static void do_slabs_free_nvm(void *ptr, const size_t size, unsigned int id) {
    slabclass_t *p;
    slabbed_item_nvm *it;
    
    assert(id >= POWER_SMALLEST && id <= power_largest_nvm);
    if (id < POWER_SMALLEST || id > power_largest_nvm)
        return;
    
    p = &slabclass_nvm[id];
    it = (slabbed_item_nvm *)ptr;
    if ((it->it_flags & ITEM_CHUNKED) == 0) {
        it->it_flags = ITEM_SLABBED;
        it->slabs_clsid = 0;
        it->prev = 0;
        it->next = p->slots;
        if (it->next) it->next->prev = it;
        p->slots = it;
        p->sl_curr++;
        p->requested -= size;
    } else {
        printf("error in do_slabs_free_nvm()\n");
    }
    return;
}

unsigned int global_page_pool_size_nvm(bool *mem_flag) {
    unsigned int ret = 0;
    pthread_mutex_lock(&slabs_lock_nvm);
    if (mem_flag != NULL)
        *mem_flag = mem_malloced_nvm >= mem_limit_nvm ? true : false;
    ret = slabclass_nvm[SLAB_GLOBAL_PAGE_POOL].slabs;
    pthread_mutex_unlock(&slabs_lock_nvm);
    return ret;
}

static void *memory_allocate_nvm(size_t size) {
    void *ret;
    if (mem_base_nvm == NULL) {
        ret = malloc_nvm(size);
    } else {
        ret = mem_current_nvm;
        if (size > mem_avail_nvm) {                        
            return NULL;                        
        }            
        if (size % CHUNK_ALIGN_BYTES) {                        
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }
        
        mem_current_nvm = ((char*)mem_current_nvm) + size;
        if (size < mem_avail_nvm) {
            mem_avail_nvm -= size;
        } else {
            mem_avail_nvm = 0;
        }
    }
    mem_malloced_nvm += size;
    return ret;
}

static void memory_release_nvm() {
    void *p = NULL;
    if (mem_base_nvm != NULL)
        return;
    
    if (!settings.slab_reassign)
        return;
    
    while (mem_malloced_nvm > mem_limit_nvm &&
            (p = get_page_from_global_pool_nvm()) != NULL) {
        free(p);
        mem_malloced_nvm -= settings.slab_page_size;
    }
}
    
void *slabs_alloc_nvm(size_t size, unsigned int id,
                      uint64_t *total_bytes, unsigned int flags) 
{
    void *ret;
    pthread_mutex_lock(&slabs_lock_nvm);
    ret = do_slabs_alloc_nvm(size, id, total_bytes, flags);
    if (ret) {
        index_nvm *index = slabs_alloc_index();
        if (index) {
            ((item_nvm *)ret)->index = index;
            index->kvitem = (item_nvm *)ret;
            index->refcount = 1;
        } else {
            printf("error in slabs_alloc_nvm()\n");
            do_slabs_free_nvm(ret, size, id);
            ret = NULL;
        }
    }
    pthread_mutex_unlock(&slabs_lock_nvm);
    return ret;
}
    
void slabs_free_nvm(void *ptr, size_t size, unsigned int id) {
    assert(ptr && (((item_nvm *)ptr)->memory_is_dram == 0));
    pthread_mutex_lock(&slabs_lock_nvm);
    slabs_free_index(((item_nvm *)ptr)->index);
    ((item_nvm *)ptr)->index = NULL;
    do_slabs_free_nvm(ptr, size, id);
    pthread_mutex_unlock(&slabs_lock_nvm);
}

static bool do_slabs_adjust_mem_limit_nvm(size_t new_mem_limit) {
    if (mem_base_nvm != NULL)
        return false;
    //settings.maxbytes = new_mem_limit;
    mem_limit_nvm = new_mem_limit;
    mem_limit_reached_nvm = false;
    memory_release_nvm();
    return true;
}
    
bool slabs_adjust_mem_limit_nvm(size_t new_mem_limit) {
    bool ret;
    pthread_mutex_lock(&slabs_lock_nvm);
    ret = do_slabs_adjust_mem_limit_nvm(new_mem_limit);
    pthread_mutex_unlock(&slabs_lock_nvm);
    return ret;
}
    
void slabs_adjust_mem_requested_nvm(unsigned int id, size_t old, size_t ntotal)
{
    pthread_mutex_lock(&slabs_lock_nvm);
    slabclass_t *p;
    if (id < POWER_SMALLEST || id > power_largest_nvm) {
        fprintf(stderr, "Internal error! Invalid slab class\n");
        abort();
    }

    p = &slabclass_nvm[id];
    p->requested = p->requested - old + ntotal;
    pthread_mutex_unlock(&slabs_lock_nvm);
}

unsigned int slabs_available_chunks_nvm(const unsigned int id, bool *mem_flag,
        uint64_t *total_bytes, unsigned int *chunks_perslab) 
{
    unsigned int ret;
    slabclass_t *p;
    
    pthread_mutex_lock(&slabs_lock_nvm);
    p = &slabclass_nvm[id];
    ret = p->sl_curr;
    if (mem_flag != NULL)
        *mem_flag = mem_malloced_nvm >= mem_limit_nvm ? true : false;
    if (total_bytes != NULL)
        *total_bytes = p->requested;
    if (chunks_perslab != NULL)
        *chunks_perslab = p->perslab;
    pthread_mutex_unlock(&slabs_lock_nvm);
    return ret;
}

static void do_slabs_stats_nvm(ADD_STAT add_stats, void *c)
{
    int i;
    for(i = POWER_SMALLEST; i <= power_largest_nvm; i++) {
        slabclass_t *p = &slabclass_nvm[i];
        if (p->slabs != 0) {
            uint32_t perslab, slabs;
            slabs = p->slabs;
            perslab = p->perslab; 
            char key_str[STAT_KEY_LEN];
            char val_str[STAT_VAL_LEN];
            int klen = 0, vlen = 0;
            APPEND_NUM_STAT(i, "NVM_ZONE", "%u", 0);
            APPEND_NUM_STAT(i, "chunk_size", "%u", p->size);
            APPEND_NUM_STAT(i, "chunks_per_page", "%u", perslab);
            APPEND_NUM_STAT(i, "total_pages", "%u", slabs);
            APPEND_NUM_STAT(i, "total_chunks", "%u", slabs * perslab);
            APPEND_NUM_STAT(i, "used_chunks", "%u", slabs * perslab - p->sl_curr);
            APPEND_NUM_STAT(i, "free_chunks", "%u", p->sl_curr);
            APPEND_NUM_STAT(i, "free_chunks_end", "%u", 0);
            APPEND_NUM_STAT(i, "mem_requested", "%llu", (unsigned long long)p->requested);
        }
    }
}

void slabs_stats_nvm(ADD_STAT add_stats, void *c) {
    pthread_mutex_lock(&slabs_lock_nvm);
    do_slabs_stats_nvm(add_stats, c);
    pthread_mutex_unlock(&slabs_lock_nvm);
}

// TODO  2018.10.08
item_nvm *item_evict_use_clock(const size_t ntotal, const unsigned int id)
{
    return NULL;
    /*
    // printf("LEEZW: item_evict_use_clock()\n");
    // int tries = 100;
    item_nvm *search = NULL;
    item_meta_nvm *m_search = NULL;
    void *hold_lock = NULL;
    slabclass_t *p = &slabclass_nvm[id];
    slabclass_meta_t *mp = &slabclass_meta_nvm[id];

    pthread_mutex_lock(&clock_lock[id]);

    unsigned int slabs_ptr = mp->slabs_ptr;
    unsigned int perslab_ptr = mp->perslab_ptr;
    unsigned int slabs_max = mp->m_slabs;
    unsigned int perslab_max = mp->m_perslab;

    if (slabs_ptr >= slabs_max || perslab_ptr >= perslab_max) {
        pthread_mutex_unlock(&clock_lock[id]);
        return NULL;
    }

    while (slabs_ptr < slabs_max && perslab_ptr < perslab_max) {
        m_search = (item_meta_nvm *)((char *)mp->m_slab_list[slabs_ptr] + perslab_ptr * mp->m_size);
        
        if (m_search->in_hashtable == 1) {
            if (m_search->clock_bit == 1) {
                m_search->clock_bit = 0;
            } else {
                search = (item_nvm *)((char *)p->slab_list[slabs_ptr] + perslab_ptr * p->size);
                uint32_t hv = hash(ITEM_key(search), search->nkey);
                if ((hold_lock = item_trylock(hv)) != NULL) {
                    if (search->meta->refcount == 1) {
                        assoc_delete_nvm(ITEM_key(search), search->nkey, hv);
                        m_search->in_hashtable = 0;
                        item_trylock_unlock(hold_lock);
                        break;
                    }
                }
            }
        }

        perslab_ptr++;
        if (perslab_ptr == perslab_max) {
            perslab_ptr = 0;
            slabs_ptr++;
            if (slabs_ptr == slabs_max)
                slabs_ptr = 0;
        }
    }

    mp->slabs_ptr = slabs_ptr;
    mp->perslab_ptr = perslab_ptr;

    pthread_mutex_unlock(&clock_lock[id]);

    //  TODO TODO TODO 
    search->meta->refcount = 0;
    return search;
    */
}

unsigned get_dram_capacity() {
    return mem_limit / 1024 / 1024;
}

unsigned get_dram_of_slabclass(unsigned slabs_clsid) {
    return slabclass[slabs_clsid].slabs;
}

unsigned get_items_perslab(unsigned slabs_clsid) {
    return slabclass[slabs_clsid].perslab;
}

unsigned get_itemsize_of_slabclass(unsigned slabs_clsid) {
    return slabclass[slabs_clsid].size;
}

void set_decay_counter_time(void) {
    settings.decay_counter_time = current_time;
}


static volatile int do_run_dram_rebalance_thread = 1;
static volatile int dram_rebalance_signal = 0;
static pthread_mutex_t dram_rebalance_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t dram_rebalance_cond = PTHREAD_COND_INITIALIZER;
static pthread_t dram_rebalance_tid;

static decayer decayers[MQ_COUNT * 64];
static uint64_t decay_counter_times;
static uint64_t decay_counter_nvm_times;

static volatile int cur_dram_repartition_count;

bool decay_counter(int step);
bool decay_counter_nvm(int step);
void insert_decayers(uint32_t sid);
void insert_decayers_nvm(uint32_t sid);
bool compute_dram_repartition(int *src, int *dst);


void dram_repartition(void)
{
    if (pthread_mutex_trylock(&dram_rebalance_lock) != 0)
        return;

    if (dram_rebalance_signal == 0) {
        dram_rebalance_signal = 1;
        pthread_cond_signal(&dram_rebalance_cond);
    }

    pthread_mutex_unlock(&dram_rebalance_lock);
}


static uint64_t zero_objects[MAX_NUMBER_OF_SLAB_CLASSES];

bool decay_counter(int step)
{
    item *search = NULL;
    void *hold_lock = NULL;
    bool res = true;
    int i, j;
    int cur_step = 0;

    for (i = 1; i < 64; i++) {
        for (j = 0; j < MQ_COUNT; j++) {
            cur_step = step;
            uint16_t id = i * MQ_COUNT + j;
            
            while (cur_step) {
                cur_step--;
                if (decayers[id].it_flags != 1)
                    continue;

                pthread_mutex_lock(&lru_locks[id]);

                search = do_item_decayer_q((item *)&decayers[id]);
                
                if (search == NULL) {
                    decayers[id].it_flags = 0;
                    do_item_unlinktail_q((item *)&decayers[id]);
                    pthread_mutex_unlock(&lru_locks[id]);
                    continue;
                }

                uint32_t hv = hash(ITEM_key(search), search->nkey);
                if ((hold_lock = item_trylock(hv)) == NULL) {
                    pthread_mutex_unlock(&lru_locks[id]);
                    continue;
                }

                if (search->time >= settings.decay_counter_time) {
                    decayers[id].it_flags = 0;
                    do_item_unlinktail_q((item *)&decayers[id]);

                    if (hold_lock)
                        item_trylock_unlock(hold_lock);
                    pthread_mutex_unlock(&lru_locks[id]);
                    continue;
                }

                res = false;
                decay_counter_times++;

                if (refcount_incr(search) != 2) {
                    refcount_decr(search);
                    if (hold_lock)
                        item_trylock_unlock(hold_lock);
                    pthread_mutex_unlock(&lru_locks[id]);
                    continue;
                }

                assert(search->MQ_id != id);
                do_item_unlink_q(search);  // remove from current link

                pthread_mutex_unlock(&lru_locks[id]);

                search->time = current_time;
                
                if (search->counter == 0)
                    zero_objects[i]++;
                
                if (search->idle_cycle == 3) {
                    while (!lockfree_push(ITEM_clsid(search), search->counter, 0));
                    search->counter = 0;
                } else {
                    uint64_t new_counter = search->counter >> settings.divisors[search->idle_cycle];
                    while (!lockfree_push(ITEM_clsid(search), search->counter, new_counter));
                    search->counter = new_counter;
                    search->idle_cycle++;
                }
                
                uint16_t tmp = get_MQ_num(search->counter);
                if (search->MQ_num != tmp) {
                    search->MQ_num = tmp;
                    search->MQ_id = search->MQ_num + ITEM_clsid(search) * MQ_COUNT;
                }

                pthread_mutex_lock(&lru_locks[search->MQ_id]);
                do_item_link_q(search);
                pthread_mutex_unlock(&lru_locks[search->MQ_id]);

                refcount_decr(search);

                if (hold_lock)
                    item_trylock_unlock(hold_lock);
            }
        }
    }

    return res;
}

bool decay_counter_nvm(int step)
{
    item_nvm *search = NULL;
    bool res = true;
    int cur_step = 0;

    slabclass_t *p = NULL;
    uint32_t slabs_ptr = 0;
    uint32_t perslab_ptr = 0;
    uint32_t slabs_max = 0;
    uint32_t perslab_max = 0;

    // POWER_SMALLEST  -->>  power_largest_nvm
    for (uint32_t i = POWER_SMALLEST; i <= power_largest_nvm; i++) {
        pthread_mutex_lock(&decay_lock_nvm[i]);
        
        p = &slabclass_nvm[i];
        if (p->decay_counter_done) {
            pthread_mutex_unlock(&decay_lock_nvm[i]);
            continue;
        }

        cur_step = step;
        slabs_ptr = p->decay_slabs_ptr;
        perslab_ptr = p->decay_perslab_ptr;
        slabs_max = p->slabs;
        perslab_max = p->perslab;

        while (cur_step > 0 && slabs_ptr < slabs_max && perslab_ptr < perslab_max) {
            search = (item_nvm *)((char *)p->slab_list[slabs_ptr] + perslab_ptr * p->size);
            index_nvm *index = search->index;
            if (((search->it_flags & ITEM_LINKED) != 0) && (index->time < settings.decay_counter_time)) {
                __sync_lock_test_and_set(&index->time, current_time);
                // index->time = current_time;
                if (index->idle_cycle == 3) {
                    while (!lockfree_push(i, index->counter, 0));
                    //update_reallocate_counter(i, index->counter, 0);
                    index->counter = 0;
                } else {
                    int idle_cycle = index->idle_cycle;
                    uint32_t counter = index->counter;
                    while (!lockfree_push(i, counter, counter >> settings.divisors[idle_cycle]));
                    //update_reallocate_counter(i, counter, counter >> settings.divisors[idle_cycle]);
                    index->counter = counter >> settings.divisors[idle_cycle];
                    index->idle_cycle++;
                }
                cur_step--;
                res = false;
                decay_counter_nvm_times++;
            }

            perslab_ptr++;
            if (perslab_ptr == perslab_max) {
                perslab_ptr = 0;
                slabs_ptr++;
                if (slabs_ptr == slabs_max) {
                    p->decay_counter_done = true;
                    break;
                }
            }
        }
        p->decay_slabs_ptr = slabs_ptr;
        p->decay_perslab_ptr = perslab_ptr;

        pthread_mutex_unlock(&decay_lock_nvm[i]);
    }
    
    return res;
}

void insert_decayers(uint32_t sid)
{
    pthread_mutex_lock(&lru_locks[sid]);

    if (decayers[sid].it_flags == 0) {
        decayers[sid].nbytes = 1;
        decayers[sid].nkey = 0;
        decayers[sid].it_flags = 1;
        decayers[sid].prev = 0;
        decayers[sid].next = 0;
        decayers[sid].time = 0;
        decayers[sid].slabs_clsid = 0;
        decayers[sid].MQ_id = sid;
        decayers[sid].MQ_num = 0;
        do_item_linktail_q((item *)&decayers[sid]);
    }

    pthread_mutex_unlock(&lru_locks[sid]);
}

void insert_decayers_nvm(uint32_t sid)
{
    pthread_mutex_lock(&decay_lock_nvm[sid]);
    slabclass_t *p = &slabclass_nvm[sid];
    p->decay_slabs_ptr = 0;
    p->decay_perslab_ptr = 0;
    p->decay_counter_done = false;
    pthread_mutex_unlock(&decay_lock_nvm[sid]);
}


static unsigned int slabs_new[MAX_NUMBER_OF_SLAB_CLASSES];

static void *dram_rebalance_thread(void *arg)
{
    pthread_mutex_lock(&dram_rebalance_lock);
    bool decay_flag = false;
    bool decay_flag_nvm = false;
    bool dram_repartition_done = true;
    int sleep_time = 0;

    while (do_run_dram_rebalance_thread) {
        if (dram_rebalance_signal == 1) {

            decay_counter_times = 0;
            decay_counter_nvm_times = 0;
            do_dram_repartition(slabs_new);

            pthread_mutex_lock(&slabs_lock);            

#ifdef DRAM_REPARTITION_DEBUG
            printf("=== Optimal Allocation: ===\n");
            uint32_t current_dram = 0;
            uint32_t current_nvm  = 0;
            for (uint32_t i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
                if (slabs_new[i] != 0 || slabclass[i].slabs != 0) {
                    printf("Class %2d: %3u MB; Curr: %3u MB, %0.3f / %0.2f / %0.3f\n",
                            i, slabs_new[i], slabclass[i].slabs, dram_cmd_ratio(i), 
                            1.0 * slabs_new[i] / slabclass[i].slabs,
                            1.0 * slabclass[i].sl_curr / (slabclass[i].slabs * slabclass[i].perslab));
                }
                current_dram += slabclass[i].slabs;
                current_nvm  += slabclass_nvm[i].slabs;
            }
            printf("DRAM Current Total: %u MB\n", current_dram);
            printf("NVM  Current Total: %u MB\n", current_nvm);
#endif
            pthread_mutex_unlock(&slabs_lock);

            if (mem_limit_reached) {
                dram_repartition_done = false;
                DRAM_Repartition_number = 1;


#ifdef DRAM_REPARTITION_DEBUG
                printf("=== start dram_repartition ===\n");
#endif
            } else {
                // Still have available DRAM memory, so don't need dram repartition,
                // but has to decay counter.
                dram_repartition_done = true;
            }

            cur_dram_repartition_count = settings.max_dram_repartition;
            dram_rebalance_signal = 2;

            for (uint32_t i = POWER_SMALLEST; i < MQ_COUNT * 64; i++)
                insert_decayers(i);
            for (uint32_t i = POWER_SMALLEST; i <= power_largest_nvm; i++)
                insert_decayers_nvm(i);

            decay_flag = true;
            decay_flag_nvm = true;

            set_decay_counter_time();

            int k;
            for (k = 0; k < MAX_NUMBER_OF_SLAB_CLASSES; k++)
                zero_objects[k] = 0;
            
            sleep_time = 500;
        }

        if (dram_rebalance_signal == 2) {
            usleep(sleep_time);

            if (decay_flag == true || decay_flag_nvm == true) {

                if (decay_flag)
                    if (decay_counter(2))
                        decay_flag = false;

                if (decay_flag_nvm)
                    if (decay_counter_nvm(10))
                        decay_flag_nvm = false;

                if (decay_flag == false && decay_flag_nvm == false) {
#ifdef DRAM_REPARTITION_DEBUG
                    printf("decay_counter_times:     %8zu\n", decay_counter_times);
                    printf("decay_counter_nvm_times: %8zu\n", decay_counter_nvm_times);
                    printf("total_decay_times:       %8zu\n", decay_counter_times + decay_counter_nvm_times);
                    printf("decay_counter_done at %d\n", current_time);
#endif
                }
            }

            if (dram_repartition_done == false) {
                int src = 0, dst = 0;
                bool res = false;
                res = compute_dram_repartition(&src, &dst);
                
                if (res) {
                    if (slabs_reassign(src, dst) != REASSIGN_RUNNING) {
                        cur_dram_repartition_count--;
                        if (cur_dram_repartition_count == 0)
                            dram_repartition_done = true;
                    }
                } else {
                    dram_repartition_done = true;
                }
            }

            if (decay_flag == false && decay_flag_nvm == false && dram_repartition_done == true) {
                dram_rebalance_signal = 0;
#ifdef DRAM_REPARTITION_DEBUG
                printf("dram_repartition finish at %d\n", current_time);
                printf("========================\n\n");
#endif
            }
        }

        if (dram_rebalance_signal == 0)
            pthread_cond_wait(&dram_rebalance_cond, &dram_rebalance_lock);
    }

    return NULL;
}


int start_dram_maintenance_thread(void)
{
    int ret = 0;
    dram_rebalance_signal = 0;
    if (pthread_cond_init(&dram_rebalance_cond, NULL) != 0) {
        fprintf(stderr, "Can't initialize rebalance condition for DRAM\n");
        return -1;
    }
    pthread_mutex_init(&dram_rebalance_lock, NULL);

    if ((ret = pthread_create(&dram_rebalance_tid, NULL,
                              dram_rebalance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create rebalance thread for DRAM: %s\n", strerror(ret));
        return -1;
    }

    printf("start_dram_maintenance_thread\n");

    return 0;
}


void stop_dram_maintenance_thread()
{
    pthread_mutex_lock(&dram_rebalance_lock);
    do_run_dram_rebalance_thread = 0;
    pthread_cond_signal(&dram_rebalance_cond);
    pthread_mutex_unlock(&dram_rebalance_lock);
    pthread_join(dram_rebalance_tid, NULL);
}


bool compute_dram_repartition(int *src, int *dst)
{
    int i;
    int max = 0, min = 0;
    int s = 0, d = 0;

    pthread_mutex_lock(&slabs_lock);
    for (i = 1; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
        int tmp = slabs_new[i] - slabclass[i].slabs;
        if (tmp == 0)
            continue;
        if (tmp > max) {
            max = tmp;
            d = i;
        }
        if (tmp < min) {
            min = tmp;
            s = i;
        }
    }
    pthread_mutex_unlock(&slabs_lock);

    if (max > 0 && min < 0 && s != d) {
        *src = s;
        *dst = d;
        return true;
    }
    return false;
}

unsigned int get_dram_capacity_slab(unsigned int i)
{
    return slabclass[i].slabs;
}

// BUCKET POOL
void slabs_init_bucket(void) {
    memset(&slabbucket, 0, sizeof(slabbucket));
    slabbucket.size = sizeof(bucket_nvm);
    slabbucket.perslab = settings.slab_page_size / slabbucket.size;
}

static int grow_slab_list_bucket(void) {
    slabclass_t *p = &slabbucket;
    if (p->slabs == p->list_size) {
        size_t new_size = (p->list_size != 0) ? p->list_size * 2 : 16;
        void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
        if (new_list == 0) return 0;
        
        p->list_size = new_size;
        p->slab_list = new_list;
    }
    return 1;
}

static void split_slab_page_into_freelist_bucket(char *ptr) {
    slabclass_t *p = &slabbucket;
    int x;
    for (x = 0; x < p->perslab; x++) {
        do_slabs_free_bucket(ptr);
        ptr += p->size;
    }
}

static int do_slabs_newslab_bucket(void) {
    slabclass_t *p = &slabbucket;   
    int len = p->size * p->perslab;
    char *ptr;
    if ((grow_slab_list_bucket() == 0) ||
            ((ptr = numa_alloc_onnode((size_t)len, 0)) == 0)) {
        return 0;
    }

    mem_alloc_bucket += len;

    memset(ptr, 0, (size_t)len);
    split_slab_page_into_freelist_bucket(ptr);
    p->slab_list[p->slabs++] = ptr;
    return 1;
}

static void *do_slabs_alloc_bucket(void) {
    slabclass_t *p = &slabbucket;
    void *ret = NULL;
    slabbed_bucket_nvm *bn = NULL;

    if (p->sl_curr == 0)
        do_slabs_newslab_bucket();
    
    if (p->sl_curr != 0) {
        bn = (slabbed_bucket_nvm *)p->slots;
        p->slots = bn->next;
        if (bn->next) bn->next->prev = 0;
        p->sl_curr--;
        ret = (void *)bn;    
    } else {
        ret = NULL;
    }

    return ret;
}

static void do_slabs_free_bucket(void *ptr) {
    slabclass_t *p = &slabbucket;
    slabbed_bucket_nvm *bn = (slabbed_bucket_nvm *)ptr;
    bn->prev = 0;
    bn->next = p->slots;
    if (bn->next) bn->next->prev = bn;
    p->slots = bn;
    p->sl_curr++;
    return;
}

void *slabs_alloc_bucket(void) {
    void *ret;
    pthread_mutex_lock(&slabs_lock_bucket);
    ret = do_slabs_alloc_bucket();
    pthread_mutex_unlock(&slabs_lock_bucket);
    return ret;
}

void slabs_free_bucket(void *ptr) {
    pthread_mutex_lock(&slabs_lock_bucket);
    do_slabs_free_bucket(ptr);
    pthread_mutex_unlock(&slabs_lock_bucket);
}

uint64_t get_mem_alloc_bucket(void) {
    return mem_alloc_bucket;
}


// ***** INDEX POOL *****
void slabs_init_index(void) {
    memset(&slabindex, 0, sizeof(slabindex));
    slabindex.size = sizeof(index_nvm);
    slabindex.perslab = settings.slab_page_size / slabindex.size;
}

static int grow_slab_list_index(void) {
    slabclass_t *p = &slabindex;
    if (p->slabs == p->list_size) {
        size_t new_size = (p->list_size != 0) ? p->list_size * 2 : 16;
        void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
        if (new_list == 0) return 0;
        p->list_size = new_size;
        p->slab_list = new_list;
    }
    return 1;
}

static void split_slab_page_into_freelist_index(char *ptr) {
    slabclass_t *p = &slabindex;
    int x;
    for (x = 0; x < p->perslab; x++) {
        do_slabs_free_index(ptr);
        ptr += p->size;
    }
}

static int do_slabs_newslab_index(void) {
    slabclass_t *p = &slabindex;   
    int len = p->size * p->perslab; // settings.slab_page_size;
    char *ptr;

    if ((grow_slab_list_index() == 0) ||
            ((ptr = numa_alloc_onnode((size_t)len, 0)) == 0)) {
        return 0;
    }

    mem_alloc_index += len;
    
    memset(ptr, 0, (size_t)len);
    split_slab_page_into_freelist_index(ptr);
    p->slab_list[p->slabs++] = ptr;
    return 1;
}

static void *do_slabs_alloc_index(void) {
    slabclass_t *p = &slabindex;
    void *ret = NULL;
    slabbed_index_nvm *in = NULL;

    if (p->sl_curr == 0)
        do_slabs_newslab_index();
                        
    if (p->sl_curr != 0) {
        in = (slabbed_index_nvm *)p->slots;
        p->slots = in->next;
        if (in->next) in->next->prev = 0;
        p->sl_curr--;
        ret = (void *)in;    
    } else {
        ret = NULL;
    }
    return ret;
}

static void do_slabs_free_index(void *ptr) {
    slabclass_t *p = &slabindex;
    slabbed_index_nvm *in = (slabbed_index_nvm *)ptr;
    in->prev = 0;
    in->next = p->slots;
    if (in->next) in->next->prev = in;
    p->slots = in;
    p->sl_curr++;
    return;
}

void *slabs_alloc_index(void) {
    void *ret;
    pthread_mutex_lock(&slabs_lock_index);
    ret = do_slabs_alloc_index();
    pthread_mutex_unlock(&slabs_lock_index);
    return ret;
}

void slabs_free_index(void *ptr) {
    pthread_mutex_lock(&slabs_lock_index);
    do_slabs_free_index(ptr);
    pthread_mutex_unlock(&slabs_lock_index);
}

uint64_t get_mem_alloc_index(void) {
    return mem_alloc_index;
}

uint64_t get_dram_free_chunks(unsigned int i) {
    assert(i >= 0 && i < MAX_NUMBER_OF_SLAB_CLASSES);
    return slabclass[i].sl_curr;
}

uint64_t get_dram_free_slabs(unsigned int i) {
    assert(i >= 0 && i < MAX_NUMBER_OF_SLAB_CLASSES);
    return (mem_limit - mem_malloced) / 1024 / 1024;
}

uint64_t get_dram_perslab(unsigned int i) {
    assert(i >= 0 && i < MAX_NUMBER_OF_SLAB_CLASSES);
    return slabclass[i].perslab;
}


// ***** LOCKFREE *****
#define CAS(ptr, oldvalue, newvalue) __sync_bool_compare_and_swap(ptr, oldvalue, newvalue)
#define FULL  false
#define EMPTY 0
#define LOCKFREE_ARRAY_SIZE 500000

uint64_t lockfree_array_size = 0;
uint64_t m_Queue[MAX_NUMBER_OF_SLAB_CLASSES][LOCKFREE_ARRAY_SIZE];
uint64_t m_CurrentWriteIndex[MAX_NUMBER_OF_SLAB_CLASSES];
uint64_t m_CurrentReadIndex[MAX_NUMBER_OF_SLAB_CLASSES];
uint64_t m_MaxReadIndex[MAX_NUMBER_OF_SLAB_CLASSES];

uint64_t count_to_index(uint64_t);

void lockfree_array_init(void) {
    int i;
    for (i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
        m_CurrentWriteIndex[i] = 0;
        m_CurrentReadIndex[i] = 0;
        m_MaxReadIndex[i] = 0;
    }
    lockfree_array_size = LOCKFREE_ARRAY_SIZE;
}

uint64_t count_to_index(uint64_t count) {
    return (count % lockfree_array_size);
}

bool lockfree_push(unsigned int i, uint64_t oldval, uint64_t newval) {
    assert(i >= 0 && i < MAX_NUMBER_OF_SLAB_CLASSES);

    uint64_t old = (oldval << 32);
    uint64_t new = newval;
    uint64_t element = old | new;
    
    uint64_t CurrentWriteIndex;
    uint64_t CurrentReadIndex;
    do {
        CurrentReadIndex = m_CurrentReadIndex[i];
        CurrentWriteIndex = m_CurrentWriteIndex[i];
        if (count_to_index(CurrentWriteIndex + 1) == count_to_index(CurrentReadIndex))
            return FULL;
        if (!CAS(&m_CurrentWriteIndex[i], CurrentWriteIndex, CurrentWriteIndex + 1))
            continue;
        m_Queue[i][count_to_index(CurrentWriteIndex)] = element;
        break;
    } while (1);

    while (!CAS(&m_MaxReadIndex[i], CurrentWriteIndex, CurrentWriteIndex + 1))
        sched_yield();
    return true;
}

uint64_t lockfree_pop(unsigned int i) {
    assert(i >= 0 && i < MAX_NUMBER_OF_SLAB_CLASSES);

    uint64_t CurrentReadIndex;
    uint64_t result;
    int j = 5;
    do {
        CurrentReadIndex = m_CurrentReadIndex[i];
        if (count_to_index(CurrentReadIndex) == count_to_index(m_MaxReadIndex[i]))
            return EMPTY;
        result = m_Queue[i][count_to_index(CurrentReadIndex)];
        if (!CAS(&m_CurrentReadIndex[i], CurrentReadIndex, CurrentReadIndex + 1))
            continue;
        return result;
    } while (j--);

    return 0;
}

#define UPDATE_COUNTER_THREADS 1
static volatile int do_run_update_counter_thread = 1;
static pthread_t update_counter_tid[UPDATE_COUNTER_THREADS];

static void *update_counter_thread(void *arg)
{
    while (do_run_update_counter_thread) {
        int i;
        bool empty = true;
        for (i = 1; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
            uint64_t element = lockfree_pop(i);
            if (element != 0) {
                empty = false;
                uint32_t tmp = element >> 32;
                uint64_t oldval = tmp;
                tmp = element;
                uint64_t newval = tmp;
                update_repartition_counter(i, oldval, newval);
            }
        }
        if (empty)
            usleep(10000);
    }

    return NULL;
}

int start_update_counter_thread(void)
{
    int ret = 0;
    int i;
    for (i = 0; i < UPDATE_COUNTER_THREADS; i++) {
        if ((ret = pthread_create(&update_counter_tid[i], NULL,
                        update_counter_thread, NULL)) != 0) {
            fprintf(stderr, "Can't create update counter thread: %s\n", strerror(ret));
            return -1;
        }
        // printf("start_update_counter_thread: %d\n", i);
    }
    
    return 0;
}

void stop_update_counter_thread()
{
    //pthread_mutex_lock(&update_counter_lock);
    do_run_update_counter_thread = 0;
    //pthread_cond_signal(&update_counter_cond);
    //pthread_mutex_unlock(&update_counter_lock);
    int i;
    for (i = 0; i < UPDATE_COUNTER_THREADS; i++)
        pthread_join(update_counter_tid[i], NULL);
}
