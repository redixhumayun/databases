#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "./wal.h"

pthread_mutex_t wal_write_mutex = PTHREAD_MUTEX_INITIALIZER;  //  the PTHREAD_MUTEX_INITIALIZER macro initializes the mutex to a default value
pthread_mutex_t wal_increment_xid_mutex = PTHREAD_MUTEX_INITIALIZER;

int fd = 0;
int nextXid = 0;

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

uint32_t get_next_xid() {
    uint32_t result;
    WalHeader* wal_header = NULL;
    WalRecord* wal_record = NULL;

    pthread_mutex_lock(&wal_increment_xid_mutex);
    do {
        if (nextXid != 0) {
            result = nextXid++;
            break;
        }

        wal_header = (WalHeader*)malloc(sizeof(WalHeader));
        int read_result = read(fd, wal_header, sizeof(WalHeader));
        if (read_result != sizeof(WalHeader)) {
            perror("Error reading WAL header");
            result = 0;
            break;
        }
        
        int offset = sizeof(WalHeader) + ((wal_header->num_of_records - 1) * sizeof(WalRecord));
        lseek(fd, offset, SEEK_SET);

        wal_record = (WalRecord*)malloc(sizeof(WalRecord));
        read_result = read(fd, wal_record, sizeof(WalRecord));
        if (read_result != sizeof(WalRecord)) {
            perror("Error reading WAL record");
            result = 0;
            break;
        }
        
        nextXid = wal_record->tx_id;
        //  Start with a transaction ID of 1, not 0
        if (nextXid == 0) {
            nextXid = 1;
        }
        result = nextXid++;
    } while (0);

    if (wal_header) {
        free(wal_header);
    }
    if (wal_record) {
        free(wal_record);
    }
    pthread_mutex_unlock(&wal_increment_xid_mutex);
    
    return result;
}


int wal_write(uint32_t value) {
    if (fd == 0) {
        perror("WAL file not initialized");
        return -1;
    }
    //  Read the beginning of the file and increment the number of records
    pthread_mutex_lock(&wal_write_mutex);
    WalHeader* wal_header = (WalHeader*)malloc(sizeof(WalHeader));
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
    pthread_mutex_unlock(&wal_write_mutex);
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