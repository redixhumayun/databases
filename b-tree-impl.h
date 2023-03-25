#include <stdint.h>

#define MAX_NUM_OF_PAGES 100

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[MAX_NUM_OF_PAGES];
} Pager;

void* get_page(Pager* pager, uint32_t page_num);