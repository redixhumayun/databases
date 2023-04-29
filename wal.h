#ifndef WAL_H
#define WAL_H

#include <stdint.h>

typedef enum TransactionType
{
    INSERT,
    DELETE
} TransactionType;

/**
 * WAL Header Layout
 * The size of this struct is 4 bytes
 */
typedef struct WalHeader
{
    uint32_t num_of_records;
} WalHeader;

/**
 * WAL Record Layout
 * The size of this struct is 16 bytes
 */
typedef struct WalRecord
{
    uint32_t size;                    // Size of the record in bytes (4 bytes)
    TransactionType transaction_type; // Transaction type (4 bytes)
    uint32_t tx_id;                   //  Transaction ID (4 bytes)
    uint32_t value;                   //  Value (4 bytes)
} WalRecord;

void wal_init(const char *wal_path);
int wal_write(uint32_t tx_id, uint32_t data);
void wal_close();
uint32_t get_next_xid();

#endif