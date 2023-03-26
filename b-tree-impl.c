#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "./b-tree-impl.h"

const uint32_t PAGE_SIZE = 16384;

/**
 * Leaf Node Header Layout 
 */
const char PAGE_TYPE_SIZE = sizeof(char);
const char NODE_INITIALIZED = 'Y';
const uint32_t NODE_INITIALIZED_SIZE = sizeof(char);
const uint32_t FREE_BLOCK_OFFSET_OFFSET = PAGE_TYPE_SIZE + NODE_INITIALIZED_SIZE;
const uint16_t FREE_BLOCK_OFFSET_SIZE = sizeof(uint16_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = PAGE_TYPE_SIZE + NODE_INITIALIZED_SIZE + FREE_BLOCK_OFFSET_SIZE;
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_HEADER_SIZE = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;

/**
 * Leaf Node Body Layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = LEAF_NODE_HEADER_SIZE;
const uintptr_t LEAF_NODE_KEY_POINTER_SIZE = sizeof(uintptr_t);
const uint32_t LEAF_NODE_KEY_POINTER_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_VALUE_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_VALUE_OFFSET = PAGE_SIZE - LEAF_NODE_VALUE_SIZE;

void* leaf_node_free_block_offset(void* node) {
    return node + FREE_BLOCK_OFFSET_OFFSET;
}

void* leaf_node_num_of_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * (LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE);
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

void* leaf_node_key_pointer(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void* leaf_node_value(void* node) {
    uint32_t num_of_cells = *(uint32_t*)(leaf_node_num_of_cells(node));
    if (num_of_cells == 0) {
        return node + LEAF_NODE_VALUE_OFFSET;
    }
    void* start_point = node + LEAF_NODE_VALUE_OFFSET;
    for (int i = 0; i < num_of_cells; i++) {
        start_point -= LEAF_NODE_VALUE_SIZE;
    }
    return start_point;
}

int binary_search(void* node, uint32_t num_cells, uint32_t key) {
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells - 1;

    if (num_cells == 0) {
        return 0;
    }

    while (one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index) {
            return index;
        }
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    return min_index;
}

int _search(Pager* pager, uint32_t key) {
    //  get the root node
    void* node = get_page(pager, 0);
    printf("The root node is %p\n", node);

    uint32_t num_cells = *(uint32_t*)leaf_node_num_of_cells(node);
    printf("The number of cells is %d\n", num_cells);

    uint32_t key_index = binary_search(node, num_cells, key);
    printf("The key index is %d\n", key_index);

    uint32_t key_at_index = *leaf_node_key(node, key_index);
    printf("The key at index is %d\n", key_at_index);

    if (key_at_index == key) {
        return 1;
    }
    return -1;
}

void search(Pager* pager, uint32_t key) {
    //  get the root node
    void* node = get_page(pager, 0);
    printf("The root node is %p\n", node);

    uint32_t num_cells = *(uint32_t*)leaf_node_num_of_cells(node);
    printf("The number of cells is %d\n", num_cells);

    uint32_t key_index = binary_search(node, num_cells, key);
    printf("The key index is %d\n", key_index);

    uint32_t key_at_index = *leaf_node_key(node, key_index);
    printf("The key at index is %d\n", key_at_index);

    void** key_pointer_address = leaf_node_key_pointer(node, key_index);
    printf("The key pointer is %p\n", key_pointer_address);

    void* value = *key_pointer_address;
    printf("The value is %d\n", *(uint32_t*)value);
    printf("\n");
}

/**
 * @brief This function is used to insert into the free block list
 * Starting at the page header, traverse the free block list, until the first empty one is found. 
 * Store the offset of the free block in the empty one.
 * 
 * @param node 
 * @param offset 
 */
void _insert_into_free_block_list(void* node, void* deleted_memory_address, uint16_t deleted_memory_size) {
    //  If the free block list is empty, store the offset in the page header

    printf("The deleted memory address is %p\n", deleted_memory_address);
    uint16_t free_block_offset = *(uint16_t*)leaf_node_free_block_offset(node);
    uint16_t offset_between_deleted_and_header = deleted_memory_address - node;
    printf("The offset between the deleted memory address and the header is %d\n", offset_between_deleted_and_header);
    if (free_block_offset == 0) {
        printf("The free block offset is 0\n");
        *(uint16_t*)leaf_node_free_block_offset(node) = offset_between_deleted_and_header;
        return;
    }

    //  If  the free block list is not empty, traverse the list until the first empty one is found, which is
    //  past the address of offset
    //  The new free block will be situated between the previous free block and the current free block
    void* free_block = node + free_block_offset;
    void* temp = node;
    printf("Finding the next free block\n");
    while(*(uint16_t*)free_block != 0 && free_block < deleted_memory_address) {
        temp = free_block;
        free_block = node + *(uint16_t*)free_block;
    }

    printf("The free block is %p\n", free_block);

    printf("Updating the previous freeblock\n");
    uint16_t offset_of_deleted_from_prev = (uint16_t)(deleted_memory_address - temp);
    printf("Updating the previous freeblock with the offset to deleted memory: %d\n", offset_of_deleted_from_prev);
    *(uint16_t*)temp = offset_of_deleted_from_prev;

    printf("Setting the data for the new free block\n");
    uint16_t offset_of_deleted_from_next = (uint16_t)(free_block - deleted_memory_address);
    printf("Updating the new freeblock with the offset to the next free block: %d", offset_of_deleted_from_next);
    uint16_t* deleted_memory_location = (uint16_t*) deleted_memory_address;
    deleted_memory_location[0] = offset_of_deleted_from_next;
    deleted_memory_location[1] = deleted_memory_size;
    return;
}

void delete(Pager* pager, uint32_t key) {
    //  Check if the key exists first
    if (_search(pager, key) == -1) {
        printf("The key does not exist\n");
        return;
    }

    //  Get the root node
    void* node = get_page(pager, 0);
    printf("The root node is %p\n", node);

    uint32_t num_cells = *(uint32_t*)leaf_node_num_of_cells(node);
    printf("The number of cells is %d\n", num_cells);

    uint32_t key_index = binary_search(node, num_cells, key);
    printf("The key index is %d\n", key_index);

    uint32_t key_at_index = *leaf_node_key(node, key_index);
    printf("The key at index is %d\n", key_at_index);

    void** key_pointer_address = leaf_node_key_pointer(node, key_index);
    printf("The key pointer is %p\n", key_pointer_address);

    void* value = *key_pointer_address;
    printf("The value is %d\n", *(uint32_t*)value);

    //  Shift the cells starting at one past the key pointer address to the left
    //  This will erase the contents of the current key and key pointer
    uint32_t num_of_cells_to_move = num_cells - key_index - 1;
    uint32_t size_of_data_to_move = num_of_cells_to_move * (LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE);
    void* destination = leaf_node_cell(node, key_index);
    void* source = leaf_node_cell(node, key_index + 1);
    memmove(destination, source, size_of_data_to_move);

    //  Update the number of cells
    *(uint32_t*)leaf_node_num_of_cells(node) = num_cells - 1;

    //  Update the free block list with the address of the deleted value
    _insert_into_free_block_list(node, value, LEAF_NODE_VALUE_SIZE);
}

void _insert(void* node, uint32_t key, uint32_t value) {
    uint32_t num_cells = *(uint32_t*)leaf_node_num_of_cells(node);
    printf("The number of cells is %d\n", num_cells);

    uint32_t key_index = binary_search(node, num_cells, key);
    printf("The key index is %d\n", key_index);

    void* destination = leaf_node_cell(node, key_index);
    int num_of_cells_to_move = num_cells - key_index;
    printf("The number of cells to move is %d\n", num_of_cells_to_move);

    int size_of_data_to_move = num_of_cells_to_move * (LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE);
    printf("The size of data to move is %d\n", size_of_data_to_move);

    printf("Moving %d bytes of data from %p to %p\n", size_of_data_to_move, destination, destination + LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE);
    memmove(destination + LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE, destination, size_of_data_to_move);

    printf("Setting the value of %p to %d\n", leaf_node_key(node, key_index), key);
    *(uint32_t*)leaf_node_key(node, key_index) = key;

    void* value_destination = leaf_node_value(node);

    printf("Setting the value of %p to %d\n", value_destination, value);
    *(uint32_t*)value_destination = value;

    void** key_pointer_address = leaf_node_key_pointer(node, key_index);
    printf("Setting the key pointer address: %p to %p\n", key_pointer_address, value_destination);
    *key_pointer_address = value_destination;

    printf("Incrementing the number of cells from %d to %d\n", num_cells, num_cells + 1);
    *(uint32_t*)leaf_node_num_of_cells(node) = num_cells + 1;
    printf("\n");
    return;
}

void insert(Pager* pager, uint32_t key, uint32_t value) {
    //  get the root node
    void* node = get_page(pager, 0);
    printf("The root node is at %p\n", node);

    //  make sure the page is initialized
    //  check if the first byte of the node is set to NODE_INITIALIZED
    printf("The first byte is set to: %c\n", *(char*)node);
    if (*(char*)node != NODE_INITIALIZED) {
        printf("Initializing node, setting the first byte to 'Y' and the number of cells to 0\n");

        //  if not, set it to NODE_INITIALIZED
        *(char*)node = NODE_INITIALIZED;
        printf("First byte is set to: %c\n", *(char*)node);

        //  set the number of cells to 0
        void* num_of_cells = leaf_node_num_of_cells(node);
        *(uint32_t*)num_of_cells = 0;

        _insert(node, key, value);
        return;
    }

    //  the page is alreay initialized
    _insert(node, key, value);
    printf("\n");
    return;
}

Pager* open_database_file(const char* filename) {
    int fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        fprintf(stderr, "Unable to open file\n");
        exit(EXIT_FAILURE);
    }
    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;

    for(uint32_t i = 0; i < MAX_NUM_OF_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        fprintf(stderr, "Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        fprintf(stderr, "Error seeking: %d", page_num * PAGE_SIZE);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    if (bytes_written == -1) {
        fprintf(stderr, "Error writing: %d", size);
        exit(EXIT_FAILURE);
    }
    return;
}

void close_database_file(Pager* pager) {
    for(uint32_t i = 0; i < MAX_NUM_OF_PAGES; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
    int result = close(pager->file_descriptor);
    if (result == -1) {
        fprintf(stderr, "Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for(uint32_t i = 0; i < MAX_NUM_OF_PAGES; i++) {
        free(pager->pages[i]);
    }
    free(pager);
}

void* get_page(Pager* pager, uint32_t page_num) {
    if (pager->pages[page_num] == NULL) {
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        
        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                fprintf(stderr, "Error reading file: %d", page_num);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

int main() {
    Pager* pager = open_database_file("test.db");
    insert(pager, 999, 999);
    search(pager, 999);
    insert(pager, 2, 2);
    search(pager, 2);
    search(pager, 999);
    delete(pager, 2);
    close_database_file(pager);
    return 0;
}