#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

#include "./b-tree-impl.h"
#include "./wal.h"
#include "./utils.h"

const uint32_t PAGE_SIZE = 4096;
const int NODE_ORDER = 10;

typedef enum PageType
{
    INTERNAL_NODE,
    LEAF_NODE
} PageType;

const char NODE_INITIALIZED = 'Y';

//  Define the required mutexes here
pthread_mutex_t row_insert_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t row_update_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t pager_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct Transaction
{
    uint32_t tx_id;
    TransactionType transaction_type;
    uint32_t key;
    uint32_t value;
    Pager *pager;
} Transaction;

/*
 * Common Node Header Layout
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint32_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t NODE_INITIALIZED_SIZE = sizeof(char);
const uint32_t NODE_INITIALIZED_OFFSET = NODE_TYPE_SIZE;
const uint32_t IS_ROOT_SIZE = sizeof(uint32_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE + NODE_INITIALIZED_SIZE;
const uintptr_t PARENT_POINTER_SIZE = sizeof(uintptr_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t FREE_BLOCK_OFFSET_SIZE = sizeof(uint32_t);
const uint32_t FREE_BLOCK_OFFSET_OFFSET = PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + NODE_INITIALIZED_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE + FREE_BLOCK_OFFSET_SIZE;

/**
 * @brief Internal Node Header Layout
 */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uintptr_t INTERNAL_NODE_RIGHT_CHILD_POINTER_SIZE = sizeof(uintptr_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_POINTER_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_POINTER_SIZE;

/**
 * Internal Node Body Layout
 */
const uintptr_t INTERNAL_NODE_CHILD_POINTER_SIZE = sizeof(uintptr_t);
const uint32_t INTERNAL_NODE_CHILD_POINTER_OFFSET = 0;
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_KEY_OFFSET = INTERNAL_NODE_CHILD_POINTER_OFFSET + INTERNAL_NODE_CHILD_POINTER_SIZE;
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_POINTER_SIZE + INTERNAL_NODE_KEY_SIZE;

/**
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_RIGHT_SIBLING_POINTER_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_RIGHT_SIBLING_POINTER_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_RIGHT_SIBLING_POINTER_SIZE;

/**
 * Leaf Node Body Layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = LEAF_NODE_HEADER_SIZE;
const uintptr_t LEAF_NODE_KEY_POINTER_SIZE = sizeof(uintptr_t);
const uint32_t LEAF_NODE_KEY_POINTER_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_VALUE_SIZE = sizeof(Row);
const uint32_t LEAF_NODE_VALUE_OFFSET = PAGE_SIZE - LEAF_NODE_VALUE_SIZE;

/**
 * Common methods for all nodes
 * */
uint8_t *node_type(void *node)
{
    return node + NODE_TYPE_OFFSET;
}

char *node_initialized(void *node)
{
    return node + NODE_INITIALIZED_OFFSET;
}

uint8_t *node_is_root(void *node)
{
    return node + IS_ROOT_OFFSET;
}

void **node_parent_pointer(void *node)
{
    return node + PARENT_POINTER_OFFSET;
}

uint16_t *node_free_block_offset(void *node)
{
    return node + FREE_BLOCK_OFFSET_OFFSET;
}

/**
 * Internal node methods
 */
uint32_t *internal_node_num_keys(void *node)
{
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

void **internal_node_right_child_pointer(void *node)
{
    return node + INTERNAL_NODE_RIGHT_CHILD_POINTER_OFFSET;
}

void **internal_node_child_pointer(void *node, uint32_t child_num)
{
    return node + INTERNAL_NODE_HEADER_SIZE + child_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t *internal_node_key(void *node, uint32_t key_num)
{
    return node + INTERNAL_NODE_HEADER_SIZE + (key_num * INTERNAL_NODE_CELL_SIZE) + INTERNAL_NODE_CHILD_POINTER_SIZE;
}

/**
 * Leaf node methods
 */
uint32_t *leaf_node_num_cells(void *node)
{
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t *leaf_node_right_sibling_pointer(void *node)
{
    return node + LEAF_NODE_RIGHT_SIBLING_POINTER_OFFSET;
}

uint32_t *leaf_node_key(void *node, uint32_t cell_num)
{
    return node + LEAF_NODE_HEADER_SIZE + cell_num * (LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE);
}

uintptr_t **leaf_node_key_pointer(void *node, uint32_t cell_num)
{
    return node + LEAF_NODE_HEADER_SIZE + cell_num * (LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE) + LEAF_NODE_KEY_SIZE;
}

/**
 * @brief This method is used to find the next available cell in a leaf node. Leaf nodes grow backwards from the end and there is a pointer to each cell.
 * If there are no cells, the next available cell is the first cell.
 * If there are cells, the next available cell is the cell before the last cell.
 * @param node
 * @return Row*
 */
Row *next_available_leaf_node_cell(void *node)
{
    uint32_t num_of_cells = *(uint32_t *)(leaf_node_num_cells(node));
    if (num_of_cells == 0)
    {
        return (Row *)(node + LEAF_NODE_VALUE_OFFSET);
    }

    // Check if there is a valid free block in this page
    uint16_t *free_block_offset = (uint16_t *)(node_free_block_offset(node));
    uint16_t *prev = NULL;

    // If there is a valid free block, traverse the linked list to find the last free block
    // Return the last free block and update the previous one
    if (*free_block_offset != 0)
    {
        while (*free_block_offset != 0)
        {
            prev = free_block_offset;
            free_block_offset = (uint16_t *)((uint8_t *)node + *free_block_offset);
        }
        prev[0] = 0;
        prev[1] = 0;
        return (Row *)free_block_offset;
    }

    void *start_point = node + LEAF_NODE_VALUE_OFFSET;
    for (int i = 0; i < num_of_cells; i++)
    {
        start_point = (uint8_t *)start_point - LEAF_NODE_VALUE_SIZE;
    }
    return (Row *)start_point;
}

/**
 * @brief This method will determine what type of node is passed in
 *
 * @param node
 * @return int
 */
int check_type_of_node(void *node)
{
    uint32_t page_type = *node_type(node);
    printf("Page type: %d\n", page_type);
    switch (page_type)
    {
    case INTERNAL_NODE:
        printf("This is an internal node\n");
        return 0;
    case LEAF_NODE:
        printf("This is a leaf node\n");
        return 1;
    default:
        printf("This is an unknown node\n");
        return -1;
    }
}

/**
 * @brief This function is used to insert into the free block list
 * Starting at the page header, traverse the free block list, until the first empty one is found.
 * Store the offset of the free block in the empty one.
 *
 * @param node
 * @param deleted_memory_address
 * @param deleted_memory_size
 */
void _insert_into_free_block_list(void *node, void *deleted_memory_address, uint16_t deleted_memory_size)
{
    printf("***SETTING FREE BLOCK***\n");
    printf("Deleting memory address %p of size %d\n", deleted_memory_address, deleted_memory_size);

    //  If the free block list is empty, store the offset in the page header
    printf("The deleted memory address is %p\n", deleted_memory_address);
    uint16_t free_block_offset = *(uint16_t *)node_free_block_offset(node);
    uint16_t offset_between_deleted_and_header = deleted_memory_address - node;
    printf("The offset between the deleted memory address and the header is %d\n", offset_between_deleted_and_header);
    if (free_block_offset == 0)
    {
        printf("The free block offset is 0\n");
        uint16_t *free_block_offset_address = node_free_block_offset(node);
        free_block_offset_address[0] = offset_between_deleted_and_header;
        free_block_offset_address[1] = deleted_memory_size;
        printf("***DONE SETTING FREE BLOCK***\n");
        return;
    }

    //  If the free block list is not empty, traverse the list until the first empty one is found, which is
    //  past the address of offset
    //  The new free block will be situated between the previous free block and the current free block
    void *free_block = node + free_block_offset;
    void *temp = node;
    printf("Finding the next free block\n");
    while (*(uint16_t *)free_block != 0 && free_block < deleted_memory_address)
    {
        temp = free_block;
        free_block = node + *(uint16_t *)free_block;
    }

    printf("The free block is %p\n", free_block);

    printf("Updating the previous freeblock\n");
    uint16_t offset_of_deleted_from_prev = (uint16_t)(deleted_memory_address - temp);
    printf("Updating the previous freeblock with the offset to deleted memory: %d\n", offset_of_deleted_from_prev);
    *(uint16_t *)temp = offset_of_deleted_from_prev;

    printf("Setting the data for the new free block\n");
    uint16_t offset_of_deleted_from_next = (uint16_t)(free_block - deleted_memory_address);
    printf("Updating the new freeblock with the offset to the next free block: %d\n", offset_of_deleted_from_next);
    uint16_t *deleted_memory_location = (uint16_t *)deleted_memory_address;
    deleted_memory_location[0] = offset_of_deleted_from_next;
    deleted_memory_location[1] = deleted_memory_size;
    printf("Done deleting memory address %p of size %d\n", deleted_memory_address, deleted_memory_size);
    printf("***DONE SETTING FREE BLOCK***\n");
    return;
}

/**
 * @brief This function is used to delete a row from the table
 *
 * @param pager
 * @param key
 * @param tx_id
 */
void delete(Pager *pager, uint32_t key, uint32_t tx_id)
{
    printf("***DELETE***\n");
    printf("Deleting key %d\n", key);

    void *node = get_page(pager, pager->root_page_num);

    //  Check if the key exists first
    if (search(pager, &node, key) == -1)
    {
        printf("The key does not exist\n");
        return;
    }

    uint32_t key_index = binary_search_modify_pointer(&node, key);
    printf("The key index is %d\n", key_index);

    uint32_t num_cells = *(uint32_t *)leaf_node_num_cells(node);
    printf("The number of cells in the node is %d\n", num_cells);

    uint32_t key_at_index = *leaf_node_key(node, key_index);
    printf("The key at index is %d\n", key_at_index);

    uintptr_t **key_pointer_address = leaf_node_key_pointer(node, key_index);
    printf("The key pointer is %p\n", key_pointer_address);

    Row *row = (Row *)*key_pointer_address;
    printf("The value is %d\n", row->data);

    //  Shift the cells starting at one past the key pointer address to the left
    //  This will erase the contents of the current key and key pointer
    uint32_t num_of_cells_to_move = num_cells - key_index - 1;
    uint32_t size_of_data_to_move = num_of_cells_to_move * (LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE);
    void *destination = leaf_node_key(node, key_index);
    void *source = leaf_node_key(node, key_index + 1);
    memmove(destination, source, size_of_data_to_move);

    //  Update the number of cells
    *(uint32_t *)leaf_node_num_cells(node) = num_cells - 1;

    //  Zeroing out the cell whose pointer was deleted
    memset(row, 0, LEAF_NODE_VALUE_SIZE);

    //  Update the free block list with the address of the deleted value
    _insert_into_free_block_list(node, row, LEAF_NODE_VALUE_SIZE);

    printf("***DONE DELETE***\n");
}

void update(Pager *pager, void *node, uint32_t key, uint32_t value, uint32_t tx_id)
{
    printf("***ROW UPDATE***\n");
    //  Write to the WAL first
    int result = wal_write(tx_id, value);
    if (result == -1)
    {
        perror("Failed to write to the WAL\n");
        return;
    }

    uint32_t key_index = binary_search_modify_pointer(&node, key);
    uint32_t key_at_index = *leaf_node_key(node, key_index);
    uintptr_t **key_pointer_address = leaf_node_key_pointer(node, key_index);
    Row *row = (Row *)*key_pointer_address;

    if (row->xmin > tx_id)
    {
        printf("The transaction id of the row is %d, which is greater than the current transaction id %d. Cannot update the row\n", row->xmin, tx_id);
        return;
    }

    //  Acquire a lock to update a row
    //  NOTE: The locking currently uses a global lock. Ideally, each row struct should have it's own lock, so updating one row does not block other rows
    pthread_mutex_lock(&row_update_lock);
    Row *new_row = next_available_leaf_node_cell(node);
    new_row->id = row->id;
    new_row->is_deleted = false;
    new_row->xmin = tx_id;
    new_row->xmax = MAX_TRANSACTION_ID;
    new_row->data = value;
    new_row->prev_row = row;

    //  update the original row with the new row data
    *key_pointer_address = (uintptr_t *)new_row;

    //  mark the old row for deletion
    row->xmax = tx_id;
    row->is_deleted = true;

    //  update the free block list with the address of the deleted value
    _insert_into_free_block_list(node, row, LEAF_NODE_VALUE_SIZE);

    //  Release the acquired lock
    pthread_mutex_unlock(&row_update_lock);
    printf("***END ROW UPDATE***\n");
    return;
}

void initialize_leaf_node(void *node)
{
    printf("Initializing the node as a leaf node\n");
    *(uint32_t *)node = LEAF_NODE;
    *(char *)node_initialized(node) = NODE_INITIALIZED;
    *(uint32_t *)node_free_block_offset(node) = 0;
    *(uint32_t *)leaf_node_num_cells(node) = 0;
    printf("Done initializing the leaf node\n");
    printf("***\n");
}

void initialize_internal_node(void *node)
{
    printf("Initializing the node as an internal node\n");
    *(uint32_t *)node = INTERNAL_NODE;
    *(char *)node_initialized(node) = NODE_INITIALIZED;
    *(uint32_t *)node_free_block_offset(node) = 0;
    *(uint32_t *)internal_node_num_keys(node) = 0;
    printf("Done initializing the internal node\n");
}

void split_internal_node(Pager *pager, void *node, void *sibling_node, uint32_t key, void *child_pointer, uint32_t tx_id)
{
    return;
}

void split_leaf_node(Pager *pager, void *node, void *sibling_node, uint32_t key, uint32_t value, uint32_t tx_id)
{
    printf("****\n");
    printf("Splitting the leaf node\n");

    printf("The new node is %p\n", sibling_node);

    //  Initialize the new node
    initialize_leaf_node(sibling_node);

    //  Copy the second half of the keys and their corresponding values to the new node
    uint32_t num_cells = *(uint32_t *)leaf_node_num_cells(node);
    printf("The number of cells is %d\n", num_cells);

    uint32_t start_index_of_cells_to_move = num_cells / 2;
    printf("The number of cells to move is %d\n", num_cells - start_index_of_cells_to_move);

    for (int i = start_index_of_cells_to_move; i < num_cells; i++)
    {
        uint32_t key = *leaf_node_key(node, i);
        uintptr_t **key_pointer_address = leaf_node_key_pointer(node, i);
        uintptr_t *value = *key_pointer_address;
        printf("Copying the key: %d\n", key);
        printf("Copying the value: %d\n", *(uint32_t *)value);
        _insert(pager, sibling_node, key, *(uint32_t *)value, tx_id);
    }

    //  Update the number of cells in the original node
    *(uint32_t *)leaf_node_num_cells(node) = start_index_of_cells_to_move;

    //  Update the number of cells in the new node
    *(uint32_t *)leaf_node_num_cells(sibling_node) = num_cells - start_index_of_cells_to_move;

    //  Insert the new key and value into one of the nodes
    if (key <= *leaf_node_key(sibling_node, 0))
    {
        printf("Inserting the key %d into the original node\n", key);
        _insert(pager, node, key, value, tx_id);
    }
    else
    {
        printf("Inserting the key %d into the new node\n", key);
        _insert(pager, sibling_node, key, value, tx_id);
    }
    return;
}

void _insert_key_value_pair_to_internal_node(void *node, uint32_t key, void *child_pointer, uint32_t tx_id)
{
    uint32_t num_keys = *(uint32_t *)internal_node_num_keys(node);
    printf("The number of keys is %d\n", num_keys);

    uint32_t key_index = binary_search(node, key);
    printf("The key index is %d\n", key_index);

    uint32_t *destination = internal_node_key(node, key_index);
    uint32_t num_of_cells_to_move = num_keys - key_index;
    printf("The number of cells to move is %d\n", num_of_cells_to_move);

    uint64_t size_of_data_to_move = num_of_cells_to_move * (INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_POINTER_SIZE);
    printf("The size of data to move is: %llu\n", size_of_data_to_move);

    printf("Moving %llu bytes from %p to %p\n", size_of_data_to_move, destination, destination + INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_POINTER_SIZE);
    memmove((void *)destination + INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_POINTER_SIZE, destination, size_of_data_to_move);

    //  Insert the new key and child pointer
    *internal_node_key(node, key_index) = key;
    void **current_child_pointer = internal_node_child_pointer(node, key_index);
    *current_child_pointer = child_pointer;

    //  Update the number of keys
    *(uint32_t *)internal_node_num_keys(node) = num_keys + 1;
}

void _insert_key_value_pair_to_leaf_node(void *node, uint32_t key, uint32_t value, uint32_t tx_id)
{
    //  Write the key and value to the WAL
    int result = wal_write(tx_id, value);
    if (result == -1)
    {
        perror("Failed to write to the WAL\n");
        return;
    }

    //  Acquire the lock for inserting a row
    pthread_mutex_lock(&row_insert_lock);

    uint32_t num_cells = *(uint32_t *)leaf_node_num_cells(node);
    printf("The number of cells is %d\n", num_cells);

    uint32_t key_index = binary_search(node, key);
    printf("The key index is %d\n", key_index);

    uint32_t *destination = leaf_node_key(node, key_index);
    uint32_t num_of_cells_to_move = num_cells - key_index;
    printf("The number of cells to move is %d\n", num_of_cells_to_move);

    uint64_t size_of_data_to_move = num_of_cells_to_move * (LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE);
    printf("The size of data to move is %llu\n", size_of_data_to_move);

    printf("Moving %llu bytes of data from %p to %p\n", size_of_data_to_move, destination, destination + LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE);
    memmove((void *)destination + LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_POINTER_SIZE, destination, size_of_data_to_move);

    *(uint32_t *)leaf_node_key(node, key_index) = key;
    printf("Set the key as %d\n", key);

    Row *row = next_available_leaf_node_cell(node);
    row->id = generate_random_uint32();
    row->xmin = tx_id;
    row->xmax = MAX_TRANSACTION_ID;
    row->data = value;
    row->is_deleted = false;
    row->prev_row = NULL;
    printf("Created a row with a random id of: %d\n", row->id);

    uintptr_t **key_pointer_address = leaf_node_key_pointer(node, key_index);
    *key_pointer_address = (uintptr_t *)row;

    *(uint32_t *)leaf_node_num_cells(node) = num_cells + 1;
    printf("\n");
    pthread_mutex_unlock(&row_insert_lock);
    return;
}

void _insert_into_internal(Pager *pager, void *node, uint32_t key, void *child_pointer, uint32_t tx_id)
{
    uint32_t num_keys = *(uint32_t *)internal_node_num_keys(node);
    printf("The number of keys is %d\n", num_keys);

    //  Check if the node needs to be split
    if (num_keys < NODE_ORDER - 1)
    {
        printf("The internal node does not need to be split\n");
        _insert_key_value_pair_to_internal_node(node, key, child_pointer, tx_id);
        return;
    }

    //  The node needs to be split

    //  Check if this node has a parent that has been initialized
    void **parent_page = node_parent_pointer(node);

    //  Create a new root if the parent has not been initialized
    if (*(char *)node_initialized(*parent_page) != NODE_INITIALIZED)
    {
        void *new_root = get_page(pager, pager->num_pages);
        *parent_page = new_root;
        initialize_internal_node(*parent_page);
        *(uint8_t *)node_is_root(parent_page) = 1;
        set_root_page(pager, pager->num_pages - 1);
    }

    printf("The internal node needs to be split\n");
    void *sibling_node = get_page(pager, pager->num_pages);
    split_internal_node(pager, node, sibling_node, key, child_pointer, tx_id);
    if (key <= *internal_node_key(sibling_node, 0))
    {
        _insert(pager, node, key, child_pointer, tx_id);
    }
    else
    {
        _insert(pager, sibling_node, key, child_pointer, tx_id);
    }

    int key_to_promote = *internal_node_key(sibling_node, 0);
    _insert(pager, parent_page, key_to_promote, sibling_node, tx_id);
    *node_parent_pointer(sibling_node) = parent_page;
    *node_parent_pointer(node) = parent_page;

    int parent_insertion_index = binary_search(parent_page, key_to_promote);
    int parent_num_keys = *(uint32_t *)internal_node_num_keys(parent_page);
    if (parent_insertion_index < parent_num_keys)
    {
        void **parent_child_pointer = internal_node_child_pointer(parent_page, parent_insertion_index);
        *parent_child_pointer = sibling_node;
    }
    else
    {
        void **parent_right_child_pointer = internal_node_right_child_pointer(parent_page);
        *parent_right_child_pointer = sibling_node;
    }

    //  For an internal node, remove the node's right child pointer
    void **right_child_pointer = internal_node_right_child_pointer(node);
    *right_child_pointer = NULL;
    return;
}

void _insert_into_leaf(Pager *pager, void *node, uint32_t key, uint32_t value, uint32_t tx_id)
{
    uint32_t num_cells = *(uint32_t *)leaf_node_num_cells(node);
    printf("The number of cells is %d\n", num_cells);

    //  Check if the node needs to be split
    if (num_cells < NODE_ORDER)
    {
        //  this leaf node does not need to be split
        printf("The leaf node does not need to be split\n");
        _insert_key_value_pair_to_leaf_node(node, key, value, tx_id);
        return;
    }

    //  The node needs to split
    printf("The leaf node needs to be split\n");

    //  Check if this node has a parent that has been initialized
    void **parent_page_pointer = node_parent_pointer(node);

    if (*parent_page_pointer == NULL || *(char *)node_initialized(*parent_page_pointer) != NODE_INITIALIZED)
    {
        void *new_root = get_page(pager, pager->num_pages);
        *parent_page_pointer = new_root;
        initialize_internal_node(*parent_page_pointer);
        *(uint8_t *)node_is_root(*parent_page_pointer) = 1;
        set_root_page(pager, pager->num_pages - 1);
    }

    void *sibling_node = get_page(pager, pager->num_pages);
    split_leaf_node(pager, node, sibling_node, key, value, tx_id);

    int key_to_promote = *leaf_node_key(sibling_node, 0);
    _insert(pager, *parent_page_pointer, key_to_promote, node, tx_id);
    *node_parent_pointer(sibling_node) = *parent_page_pointer;
    *node_parent_pointer(node) = *parent_page_pointer;

    int parent_insertion_index = binary_search(*parent_page_pointer, key_to_promote);
    int parent_num_keys = *(uint32_t *)internal_node_num_keys(*parent_page_pointer);
    if (parent_insertion_index < parent_num_keys)
    {
        //  Set the left child pointer of the parent at  insertion index equal to the sibling node
        *internal_node_child_pointer(*parent_page_pointer, parent_insertion_index) = sibling_node;
    }
    else
    {
        //  Set the right child pointer of the parent at insertion index equal to the sibling node
        *internal_node_right_child_pointer(*parent_page_pointer) = sibling_node;
    }

    return;
}

void insert(Pager *pager, uint32_t key, uint32_t value, uint32_t tx_id)
{
    printf("***INSERT***\n");
    //  get the root node
    void *node = get_page(pager, pager->root_page_num);
    printf("The root node is at %p\n", node);

    //  Check if the root node is initialized
    if (*(char *)node_initialized(node) != NODE_INITIALIZED)
    {
        initialize_leaf_node(node);
    }

    //  Check if this key already exists. If it does, run the update function instead
    if (search(pager, &node, key) != -1)
    {
        update(pager, node, key, value, tx_id);
        return;
    }

    _insert(pager, node, key, value, tx_id);
    printf("***END INSERT***\n\n");
    return;
}

void _select_all_rows(Pager *pager, void *node, uint32_t tx_id)
{
    int node_type = check_type_of_node(node);
    if (node_type == INTERNAL_NODE)
    {
        uint32_t num_keys = *(uint32_t *)internal_node_num_keys(node);
        for (int i = 0; i < num_keys; i++)
        {
            void **child_pointer = internal_node_child_pointer(node, i);
            _select_all_rows(pager, *child_pointer, tx_id);
        }
        return;
    }

    //  handle leaf node
    uint32_t num_cells = *(uint32_t *)leaf_node_num_cells(node);
    for (int i = 0; i < num_cells; i++)
    {
        uint32_t *key = leaf_node_key(node, i);
        uintptr_t **key_pointer_address = leaf_node_key_pointer(node, i);
        Row *row = (Row *)*key_pointer_address;
        //  This row is at the head of the linked list
        //  Iterate through the linked list and print all versions of this row
        while (row != NULL)
        {
            if (row->xmin <= tx_id && tx_id <= row->xmax)
            {
                //  visibile to the transaction
                printf("The key is %d and the value is %d\n", *key, row->data);
            }
            else
            {
                //  not visible to the transaction
                printf("The key is %d and the value is %d but it is not visible to the transaction\n", *key, row->data);
            }
            row = row->prev_row;
        }
    }
}

/**
 * @brief This method will attempt to read all the rows that have been inserted into
 * the database so far.
 *
 * @param pager
 */
void select_all_rows(Pager *pager, uint32_t tx_id)
{
    void *node = get_page(pager, pager->root_page_num);
    printf("The root node is at %p\n", node);

    //  Check if the root node is initialized
    if (*(char *)node_initialized(node) != NODE_INITIALIZED)
    {
        printf("The root node is not initialized\n");
        return;
    }

    _select_all_rows(pager, node, tx_id);
}

/**
 * @brief This method will take a pointer to a page and a key and will return the index of the key in the page
 * It will also modify the pointer to the page to point to the correct child node
 *
 * @param node
 * @param key
 * @return int
 */
int binary_search_modify_pointer(void **node, uint32_t key)
{
    int node_type = check_type_of_node(*node);
    uint32_t num_cells;
    if (node_type == LEAF_NODE)
    {
        num_cells = *(uint32_t *)leaf_node_num_cells(*node);
    }
    else if (node_type == INTERNAL_NODE)
    {
        num_cells = *(uint32_t *)internal_node_num_keys(*node);
    }
    if (num_cells == 0)
    {
        return 0;
    }
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;

    //  Perform binary search on an internal node
    while (node_type != LEAF_NODE && one_past_max_index > min_index)
    {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *internal_node_key(*node, index);
        if (key < key_at_index)
        {
            //  search the left side of the node
            one_past_max_index = index;
        }
        else if (key > key_at_index)
        {
            //  search the right side of the node
            min_index = index + 1;
        }
        else
        {
            *node = *internal_node_right_child_pointer(*node);
            return binary_search_modify_pointer(node, key);
        }
    }

    //  Decide which child pointer to follow
    if (node_type == INTERNAL_NODE)
    {
        if (min_index < num_cells)
        {
            *node = *internal_node_child_pointer(*node, min_index);
        }
        else
        {
            *node = *internal_node_right_child_pointer(*node);
        }
        return binary_search_modify_pointer(node, key);
    }

    //  Perform binary search on a leaf node
    while (one_past_max_index != min_index)
    {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(*node, index);
        if (key == key_at_index)
        {
            return index;
        }
        if (key < key_at_index)
        {
            one_past_max_index = index;
        }
        else
        {
            min_index = index + 1;
        }
    }
    return min_index;
}

int binary_search(void *node, uint32_t key)
{
    int node_type = check_type_of_node(node);
    uint32_t num_cells;
    if (node_type == LEAF_NODE)
    {
        num_cells = *(uint32_t *)leaf_node_num_cells(node);
    }
    else if (node_type == INTERNAL_NODE)
    {
        num_cells = *(uint32_t *)internal_node_num_keys(node);
    }
    if (num_cells == 0)
    {
        return 0;
    }
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;

    while (node_type != LEAF_NODE && one_past_max_index > min_index)
    {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *internal_node_key(node, index);
        if (key < key_at_index)
        {
            //  search the left side of the node
            one_past_max_index = index;
        }
        else if (key > key_at_index)
        {
            //  search the right side of the node
            min_index = index + 1;
        }
        else
        {
            //  get the page the left child pointer points to
            node = *internal_node_child_pointer(node, index);
            node_type = check_type_of_node(node);
            if (node_type == INTERNAL_NODE)
            {
                num_cells = *(uint32_t *)internal_node_num_keys(node);
                min_index = 0;
            }
            else
            {
                break;
            }
        }
    }

    //  If the node is still an internal node, call binary_search with the right child pointer
    //  If the node is a leaf node, find the key and return the index
    if (node_type == INTERNAL_NODE)
    {
        return binary_search(*internal_node_right_child_pointer(node), key);
    }
    else
    {
        while (one_past_max_index != min_index)
        {
            uint32_t index = (min_index + one_past_max_index) / 2;
            uint32_t key_at_index = *leaf_node_key(node, index);
            if (key == key_at_index)
            {
                return index;
            }
            if (key < key_at_index)
            {
                one_past_max_index = index;
            }
            else
            {
                min_index = index + 1;
            }
        }
        return min_index;
    }
}

/**
 * @brief This method is responsible for searching for a key in the B+ tree
 * It handles two cases, one where the node passed is an internal node and the other where it is a leaf node
 * Returns -1 if the key is not found. Else, returns the key
 * @param pager
 * @param key
 * @return int
 */
int search(Pager *pager, void **node, uint32_t key)
{
    int node_type = check_type_of_node(*node);

    if (node_type == INTERNAL_NODE)
    {
        uint32_t num_keys = *(uint32_t *)internal_node_num_keys(node);
        uint32_t key_index = binary_search_modify_pointer(node, key);
        if (key_index == -1)
        {
            return -1;
        }
        uint32_t key_at_index = *leaf_node_key(*node, key_index);
        if (key_at_index != key)
        {
            return -1;
        }
        uintptr_t **key_pointer_address = leaf_node_key_pointer(*node, key_index);
        uintptr_t *value = *key_pointer_address;
        printf("The value is %d\n", *(uint32_t *)value);
        return 1;
    }
    else if (node_type == LEAF_NODE)
    {
        uint32_t num_cells = *(uint32_t *)leaf_node_num_cells(*node);
        uint32_t key_index = binary_search(*node, key);
        if (key_index == -1)
        {
            return -1;
        }
        uint32_t key_at_index = *leaf_node_key(*node, key_index);
        if (key_at_index != key)
        {
            return -1;
        }
        uintptr_t **key_pointer_address = leaf_node_key_pointer(*node, key_index);
        uintptr_t *value = *key_pointer_address;
        printf("The value is %d\n", *(uint32_t *)value);
        return key_at_index;
    }
    else
    {
        printf("Unknown node type.\n");
        exit(1);
    }
}

void *start_transaction(void *t)
{
    //  cast the pointer to a transaction pointer
    Transaction *transaction = (Transaction *)t;

    //  Get the transaction ID for this transaction
    uint32_t tx_id = get_next_xid();
    transaction->tx_id = tx_id;

    switch (transaction->transaction_type)
    {
    case INSERT:
        insert(transaction->pager, transaction->key, transaction->value, transaction->tx_id);
        break;
    case DELETE:
        delete (transaction->pager, transaction->key, tx_id);
        break;
    default:
        printf("Unknown transaction type.\n");
        exit(1);
    }
    return NULL;
}

Pager *open_database_file(const char *filename)
{
    int fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1)
    {
        fprintf(stderr, "Unable to open file\n");
        exit(EXIT_FAILURE);
    }
    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;
    pager->root_page_num = 0;

    for (uint32_t i = 0; i < MAX_NUM_OF_PAGES; i++)
    {
        pager->pages[i] = NULL;
    }
    wal_init("wal.txt");
    return pager;
}

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size)
{
    if (pager->pages[page_num] == NULL)
    {
        fprintf(stderr, "Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1)
    {
        fprintf(stderr, "Error seeking: %d", page_num * PAGE_SIZE);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    if (bytes_written == -1)
    {
        fprintf(stderr, "Error writing: %d", size);
        exit(EXIT_FAILURE);
    }
    return;
}

void close_database_file(Pager *pager)
{
    for (uint32_t i = 0; i < MAX_NUM_OF_PAGES; i++)
    {
        if (pager->pages[i] == NULL)
        {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
    int result = close(pager->file_descriptor);
    if (result == -1)
    {
        fprintf(stderr, "Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < MAX_NUM_OF_PAGES; i++)
    {
        free(pager->pages[i]);
    }
    free(pager);
    wal_close();
}

uint32_t get_root_page(Pager *pager)
{
    return pager->root_page_num;
}

void set_root_page(Pager *pager, uint32_t root_page_num)
{
    pager->root_page_num = root_page_num;
}

void *get_page(Pager *pager, uint32_t page_num)
{
    pthread_mutex_lock(&pager_lock);
    if (pager->pages[page_num] == NULL)
    {
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        if (page_num <= num_pages)
        {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1)
            {
                fprintf(stderr, "Error reading file: %d", page_num);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages)
        {
            pager->num_pages = page_num + 1;
        }
    }
    pthread_mutex_unlock(&pager_lock);
    return pager->pages[page_num];
}

void print_internal_node(void *node)
{
    printf("Printing internal node\n");
    uint32_t num_keys = *internal_node_num_keys(node);
    printf("The number of cells is %d\n", num_keys);
    for (uint32_t i = 0; i < num_keys; i++)
    {
        printf("The key is %d\n", *internal_node_key(node, i));
        printf("The child pointer is %p\n", internal_node_child_pointer(node, i));
        print_node(*internal_node_child_pointer(node, i));
    }
    printf("The right child pointer is %p\n", internal_node_right_child_pointer(node));
    print_node(*internal_node_right_child_pointer(node));
}

void print_leaf_node(void *node)
{
    printf("Printing leaf node\n");
    uint32_t num_cells = *(uint32_t *)leaf_node_num_cells(node);
    printf("The number of cells is %d\n", num_cells);
    for (uint32_t i = 0; i < num_cells; i++)
    {
        printf("The key is %d\n", *leaf_node_key(node, i));
        uintptr_t **key_pointer_address = leaf_node_key_pointer(node, i);
        Row *row = (Row *)*key_pointer_address;
        printf("The value is %d\n", row->data);
    }
}

void print_node(void *node)
{
    printf("Printing node\n");
    uint32_t num_cells = *(uint32_t *)leaf_node_num_cells(node);
    int node_type = check_type_of_node(node);
    if (node_type == INTERNAL_NODE)
    {
        //  print internal node
        print_internal_node(node);
    }
    else if (node_type == LEAF_NODE)
    {
        //  print leaf node
        print_leaf_node(node);
    }
}

void print_all_pages(Pager *pager)
{
    printf("***PRINT FILE***\n");
    uint32_t root_page_num = pager->root_page_num;
    void *root_node = get_page(pager, root_page_num);
    print_node(root_node);
    printf("***END OF PRINT FILE***\n");
}

int main()
{
    Pager *pager = open_database_file("test.db");
    Transaction *t1 = malloc(sizeof(Transaction));
    t1->tx_id = -1;
    t1->transaction_type = INSERT;
    t1->key = 3;
    t1->value = 3;
    t1->pager = pager;
    pthread_t thread1;

    Transaction *t2 = malloc(sizeof(Transaction));
    t2->tx_id = -1;
    t2->transaction_type = INSERT;
    t2->key = 6;
    t2->value = 6;
    t2->pager = pager;
    pthread_t thread2;

    Transaction *t3 = malloc(sizeof(Transaction));
    t3->tx_id = -1;
    t3->transaction_type = INSERT;
    t3->key = 9;
    t3->value = 9;
    t3->pager = pager;
    pthread_t thread3;

    Transaction *delete_tx = malloc(sizeof(Transaction));
    delete_tx->tx_id = -1;
    delete_tx->transaction_type = DELETE;
    delete_tx->key = 3;
    delete_tx->pager = pager;
    pthread_t thread4;

    Transaction *t5 = malloc(sizeof(Transaction));
    t5->tx_id = -1;
    t5->transaction_type = INSERT;
    t5->key = 12;
    t5->value = 12;
    t5->pager = pager;
    pthread_t thread5;

    Transaction *t6 = malloc(sizeof(Transaction));
    t6->tx_id = -1;
    t6->transaction_type = INSERT;
    t6->key = 15;
    t6->value = 15;
    t6->pager = pager;
    pthread_t thread6;

    pthread_create(&thread1, NULL, start_transaction, t1);
    pthread_create(&thread2, NULL, start_transaction, t2);
    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);

    pthread_create(&thread3, NULL, start_transaction, t3);
    pthread_join(thread3, NULL);

    pthread_create(&thread4, NULL, start_transaction, delete_tx);
    pthread_join(thread4, NULL);

    pthread_create(&thread5, NULL, start_transaction, t5);
    pthread_join(thread5, NULL);

    pthread_create(&thread6, NULL, start_transaction, t6);
    pthread_join(thread6, NULL);

    print_all_pages(pager);
    close_database_file(pager);
    return 0;
}