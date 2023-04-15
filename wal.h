#ifndef WAL_H
#define WAL_H

#include <stdint.h>

typedef enum TransactionType {
    INSERT,
    DELETE
} TransactionType;

/**
 * WAL Header Layout
 */
typedef struct WalHeader {
    uint32_t num_of_records;
} WalHeader;

typedef struct WalRecord {
    uint32_t size;      // Size of the record in bytes
    TransactionType transaction_type;
    uint32_t tx_id; // Transaction ID
    uint32_t value;
} WalRecord;

void wal_init(const char *wal_path);
int wal_write(uint32_t data);
void wal_close();

#endif