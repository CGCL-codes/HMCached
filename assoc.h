/* associative array */
void assoc_init(const int hashpower_init);
item *assoc_find(const char *key, const size_t nkey, const uint32_t hv);
int assoc_insert(item *item, const uint32_t hv);
void assoc_delete(const char *key, const size_t nkey, const uint32_t hv);
void do_assoc_move_next_bucket(void);
int start_assoc_maintenance_thread(void);
void stop_assoc_maintenance_thread(void);
void assoc_start_expand(uint64_t curr_items);
extern unsigned int hashpower;
extern unsigned int item_lock_hashpower;

void assoc_init_nvm(const int hashpower_init);
item_nvm *assoc_find_nvm(const char *key, const size_t nkey, const uint32_t hv);
int assoc_insert_nvm(item_nvm *item, const uint32_t hv);
void assoc_delete_nvm(const char *key, const size_t nkey, const uint32_t hv);
void do_assoc_move_next_bucket_nvm(void);
int start_assoc_maintenance_thread_nvm(void);
void stop_assoc_maintenance_thread_nvm(void);
void assoc_start_expand_nvm(uint64_t curr_items);
extern unsigned int hashpower_nvm;
extern unsigned int item_lock_hashpower_nvm;
