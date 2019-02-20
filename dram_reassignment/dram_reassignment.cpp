#include <iostream>
#include <vector>
#include <map>
#include <climits>
#include <pthread.h>
#include <cstdio>
#include <stdint.h> // <cstdint>
#include <fstream>
#include <cmath>
using namespace std;

#define SLABCLASSNUM (63 + 1)

static map<uint32_t, uint32_t> counter_mapping_set[SLABCLASSNUM];
static pthread_mutex_t counter_mapping_set_mutex[SLABCLASSNUM];

extern "C" uint64_t get_dram_capacity(void);
extern "C" uint64_t get_dram_of_slabclass(unsigned slabs_clsid);
extern "C" uint64_t get_items_perslab(unsigned slabs_clsid);
extern "C" uint64_t get_itemsize_of_slabclass(unsigned slabs_clsid);


extern "C" void init_counter_mapping_set();
extern "C" void update_counter_mapping_set(unsigned slabs_clsid, uint64_t oldval, uint64_t newval);
extern "C" void compute_dram_reassignment(unsigned int *slabs_new);


void init_counter_mapping_set() 
{
    for (uint64_t i = 0; i < SLABCLASSNUM; i++)
        pthread_mutex_init(&counter_mapping_set_mutex[i], NULL);

    for (uint64_t i = 0; i < SLABCLASSNUM; i++)
        counter_mapping_set[i].clear();
}


void update_counter_mapping_set(unsigned slabs_clsid, uint64_t oldval, uint64_t newval)
{
    unsigned id = slabs_clsid;
    if ((id == 0) || (oldval == newval))
        return;

    pthread_mutex_lock(&counter_mapping_set_mutex[id]);

    if (oldval != 0) {
        map<uint32_t, uint32_t>::iterator iter;
        iter = counter_mapping_set[id].find(oldval);
        if (iter != counter_mapping_set[id].end()) {
            if (iter->second == 1)
                counter_mapping_set[id].erase(iter);
            else if (iter->second > 1)
                iter->second--;
        } else {
            printf("*** oldval: %d ***\n", oldval);
        }
    }

    if (newval != 0) 
        counter_mapping_set[id][newval]++;
    
    pthread_mutex_unlock(&counter_mapping_set_mutex[id]);
}


/*
void print_reallocate_counter(unsigned slabs_clsid)
{
    unsigned id = slabs_clsid;
    map<uint32_t, uint32_t>::reverse_iterator iter;

    pthread_mutex_lock(&counter_mutex[id]);

    for (iter = reallocate_counter[id].rbegin(); iter != reallocate_counter[id].rend(); iter++)
        cout << "val, " << iter->first << ", " << "count, " << iter->second << endl;

    pthread_mutex_unlock(&counter_mutex[id]);
}
*/

void optimal_allocation(vector<vector<uint64_t> > &Cost, uint64_t dram_capacity, vector<uint64_t> &Snew)
{
    vector<vector<uint64_t> > min_cost(SLABCLASSNUM, vector<uint64_t>(dram_capacity + 1, UINT64_MAX));
    vector<vector<uint64_t> > Save(SLABCLASSNUM, vector<uint64_t>(dram_capacity + 1, 0));

    for (uint64_t i = 0; i <= dram_capacity; i++)
        min_cost[0][i] = 0;

    for (uint64_t i = 1; i <= SLABCLASSNUM - 1; i++)
        min_cost[i][0] = min_cost[i-1][0] + Cost[i][0];

    uint64_t temp;
    for (uint64_t i = 1; i <= SLABCLASSNUM - 1; i++) {
        for (uint64_t j = 1; j <= dram_capacity; j++) {
            for (uint64_t k = 0; k <= j; k++) {
                temp = min_cost[i - 1][j - k] + Cost[i][k];
                if (temp < min_cost[i][j]) {
                    min_cost[i][j] = temp;
                    Save[i][j] = k;
                }
            }
        }
    }

    temp = dram_capacity;
    for (uint64_t i = SLABCLASSNUM - 1; i >= 1; i--) {
        Snew[i] = Save[i][temp];
        temp = temp - Save[i][temp];
    }
}


uint64_t get_dram_capacity() {
    return 0; 
}


uint64_t get_dram_of_slabclass(unsigned slabs_clsid) { 
    return 0; 
}


uint64_t get_items_perslab(unsigned slabs_clsid) {
    return 0;
}


uint64_t get_itemsize_of_slabclass(unsigned slabs_clsid) {
    return 0;
}


void compute_dram_reassignment(unsigned int *slabs_new)
{
    map<uint32_t, uint32_t> counter_mapping_set_copy[SLABCLASSNUM];
    for (uint64_t i = 0; i < SLABCLASSNUM; i++) {
        pthread_mutex_lock(&counter_mapping_set_mutex[i]);
        counter_mapping_set_copy[i] = counter_mapping_set[i];
        pthread_mutex_unlock(&counter_mapping_set_mutex[i]);
    }

    uint64_t dram_capacity = get_dram_capacity();
    cout << "total dram capacity: " << dram_capacity << " MB" << endl;

    vector<uint64_t> Sold(SLABCLASSNUM, 0);
    vector<uint64_t> perslab(SLABCLASSNUM, 0);
    for (uint64_t i = 0; i < SLABCLASSNUM; i++) {
        Sold[i] = get_dram_of_slabclass(i);
        perslab[i] = get_items_perslab(i);
    }

    map<uint32_t, uint32_t>::reverse_iterator iter;

    vector<uint64_t> total_access(SLABCLASSNUM, 0);
    for (uint64_t i = 0; i < SLABCLASSNUM; i++) {
        if (counter_mapping_set_copy[i].empty())
            continue;
        for (iter = counter_mapping_set_copy[i].rbegin(); iter != counter_mapping_set_copy[i].rend(); iter++)
            total_access[i] += (iter->first * iter->second);
    }

    vector<vector<uint64_t> > accumu_access(SLABCLASSNUM, vector<uint64_t>(dram_capacity + 1, 0));
    for (uint64_t i = 0; i < SLABCLASSNUM; i++) {
        if (total_access[i] == 0)
            continue;

        iter = counter_mapping_set_copy[i].rbegin();
        for (uint64_t j = 1; j <= dram_capacity; j++) {
            accumu_access[i][j] = accumu_access[i][j - 1];
            uint64_t cap = perslab[i];
            while (iter != counter_mapping_set_copy[i].rend()) {
                if (iter->second < cap) {
                    accumu_access[i][j] += (iter->first * iter->second);
                    cap -= iter->second;
                    iter++;
                } else if (iter->second == cap) {
                    accumu_access[i][j] += (iter->first * iter->second);
                    iter++;
                    break;
                } else if (iter->second > cap) {
                    accumu_access[i][j] += (iter->first * cap);
                    iter->second -= cap;
                    break;
                }
            }
        }
    }

    vector<vector<uint64_t> > cost(SLABCLASSNUM, vector<uint64_t>(dram_capacity + 1, 0));
    for (uint64_t i = 0; i < SLABCLASSNUM; i++) {
        if (total_access[i] == 0)
            continue;
        
        uint64_t size = get_itemsize_of_slabclass(i);
        for (uint64_t j = 0; j <= dram_capacity; j++) {
            cost[i][j] = (total_access[i] - accumu_access[i][j]) * size;
        }
    }

    vector<uint64_t> Snew(SLABCLASSNUM, 0);
    optimal_allocation(cost, dram_capacity, Snew);

    for (uint64_t i = 0; i < SLABCLASSNUM; i++)
        slabs_new[i] = Snew[i];
}
