#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

#include "./wal.h"

int fd = 0;

void wal_init(const char* wal_path) {
    fd = open(wal_path, O_RDWR | O_CREAT, 0644);
    if (fd == -1) {
        perror("Error opening file for writing WAL");
        exit(EXIT_FAILURE);
    }
    WalHeader* wal_header = (WalHeader*)malloc(sizeof(WalHeader));
    wal_header->num_of_records = 0;
    return;
}

int wal_write(uint32_t value) {
    if (fd == 0) {
        perror("WAL file not initialized");
        return 1;
    }
    //  Read the beginning of the file and increment the number of records
    WalHeader* wal_header = (WalHeader*)malloc(sizeof(WalHeader));
    printf("Size of WalHeader: %lu\n", sizeof(WalHeader));
    int read_result = read(fd, wal_header, sizeof(WalHeader));
    if (read_result == -1) {
        perror("Error reading WAL header");
        return -1;
    }
    wal_header->num_of_records++;
    //  Write the header back to the file
    int write_result = write(fd, wal_header, sizeof(WalHeader));
    if (write_result == -1) {
        perror("Error writing header to WAL");
        return -1;
    }

    //  First get the last record written to read the previous tx_id
    int offset = sizeof(WalHeader) + ((wal_header->num_of_records - 1) * sizeof(WalRecord));
    WalRecord* last_record = (WalRecord*)malloc(sizeof(WalRecord));
    read_result = pread(fd, last_record, sizeof(WalRecord), offset);
    if (read_result == -1) {
        perror("Error reading previous WAL record");
        return -1;
    }

    WalRecord* record = (WalRecord*)malloc(sizeof(WalRecord));
    printf("Size of WalRecord: %lu\n", sizeof(WalRecord));
    record->size = sizeof(value);
    record->transaction_type = INSERT;
    record->tx_id = last_record->tx_id + 1;
    record->value = value;

    //  Write the record to the file by first finding the offset to write to
    //  The offset is the size of the header plus the size of the records
    //  already written
    offset = sizeof(WalHeader) + (wal_header->num_of_records * sizeof(WalRecord));
    write_result = pwrite(fd, record, sizeof(WalRecord), offset);
    if (write_result == -1) {
        perror("Error writing new record to WAL");
        return -1;
    }
    int stored_tx_id = record->tx_id;
    free(wal_header);
    free(record);
    return stored_tx_id;
}

void wal_close() {
    if (fd == 0) {
        perror("WAL file not initialized");
        exit(EXIT_FAILURE);
    }
    int close_result = close(fd);
    fd = 0;
    if (close_result == -1) {
        perror("Error closing WAL");
        exit(EXIT_FAILURE);
    }
    return;
}