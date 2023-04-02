#include <stdint.h>

#define MAX_NUM_OF_PAGES 100

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[MAX_NUM_OF_PAGES];
    uint32_t root_page_num;
} Pager;

int binary_search(void* node, uint32_t num_cells, uint32_t key);
void search(Pager* pager, uint32_t key);
int _search(Pager* pager, uint32_t key);

void* get_page(Pager* pager, uint32_t page_num);

void set_root_page(Pager* pager, uint32_t root_page_num);
uint32_t get_root_page(Pager* pager);

#define _insert(pager, node, key, value) \
    _Generic((value), \
        uint32_t: _insert_into_leaf, void*: _insert_into_internal)(pager, node, key, value)

void _insert_into_leaf(Pager* pager, void* node, uint32_t key, uint32_t value);
void _insert_into_internal(Pager* pager, void* node, uint32_t key, void* child_pointer);

void print_node(void* node);