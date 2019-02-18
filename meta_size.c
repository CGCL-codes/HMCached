#include <stdio.h>
#include <stdint.h>



typedef struct __attribute__ ((__packed__)) _stritem {
    uint32_t        memory_is_dram:1;
    uint32_t        padding_1:7;
    uint32_t        nsuffix:8;
    uint32_t        it_flags:8;
    uint32_t        slabs_clsid:8;

    uint32_t        nkey:8;
    uint32_t        nbytes:23;
    uint32_t        padding_2:1;

    uint32_t        refcount:6;
    uint32_t        MQ_id:11;   // MQ_id = MQ_num + ITEM_clsid(it) *MQ_COUNT
    uint32_t        MQ_num:5;   // MQ_COUNT = 16
    uint32_t        reset:2;
    uint32_t        padding_3:8;
    uint32_t        counter;

    uint32_t      time;
    uint32_t      exptime;

    struct _stritem *next;
    struct _stritem *prev;
    struct _stritem *h_next;
    // bool         has_extra_copy; // TODO

    union {
        uint64_t cas;
        char end;
    } data[];
} item;


typedef struct __attribute__ ((__packed__)) _stritem_nvm {
    uint32_t            memory_is_dram:1;
    uint32_t            padding:7;
    uint32_t            nsuffix:8;

    uint32_t            it_flags:8;
    uint32_t            slabs_clsid:8;

    uint32_t            nkey:8;
    uint32_t            nbytes:23;
    uint32_t            has_dram_copy:1;
    uint32_t            exptime;

    // void                *extra_meta;

    //struct _stritem_nvm *h_next;

    //union {
    //    uint64_t cas;
    //    char end;
    //} data[];
} item_nvm;


int main()
{
    printf("dram size: %zu\n", sizeof(item));
    printf("nvm size: %zu\n", sizeof(item_nvm));
    return 0;
}
