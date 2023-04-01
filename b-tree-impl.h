#include <stdint.h>

#define MAX_NUM_OF_PAGES 100

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[MAX_NUM_OF_PAGES];
} Pager;

void* get_page(Pager* pager, uint32_t page_num);

#define _insert(pager, node, key, value) \
    _Generic((value), \
        uint32_t: _insert_into_leaf, void*: _insert_into_internal)(pager, node, key, value)

void _insert_into_leaf(Pager* pager, void* node, uint32_t key, uint32_t value);
void _insert_into_internal(Pager* pager, void* node, uint32_t key, void* child_pointer);