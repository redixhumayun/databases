#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>

#include "./wal.h"

struct stat file_info;

pthread_mutex_t wal_write_mutex = PTHREAD_MUTEX_INITIALIZER; //  the PTHREAD_MUTEX_INITIALIZER macro initializes the mutex to a default value

int fd = 0;
int nextXid = -1;

void wal_init(const char *wal_path)
{
    fd = open(wal_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd == -1)
    {
        perror("Error opening file for writing WAL");
        exit(EXIT_FAILURE);
    }
    WalHeader *wal_header = (WalHeader *)malloc(sizeof(WalHeader));
    wal_header->num_of_records = 0;
    return;
}

/**
 * @brief This method will find the last record in the WAL and check it's transaction ID. It will then increment the transaction ID and return it.
 *
 * @return uint32_t
 */

//  NOTE: This method shouldn't read from the WAL file each time. It should only read once when the WAL is initialized and then increment the transaction ID in memory.
uint32_t get_next_xid()
{
    uint32_t result;
    WalHeader *wal_header = NULL;
    WalRecord *wal_record = NULL;

    pthread_mutex_lock(&wal_write_mutex);
    printf("***GETTING NEXT XID***\n");
    do
    {
        if (nextXid != -1)
        {
            result = ++nextXid;
            break;
        }

        wal_header = (WalHeader *)malloc(sizeof(WalHeader));

        //  Handle the situation where the file is empty
        if (fstat(fd, &file_info) == -1)
        {
            perror("Error getting file info");
            exit(1);
        }
        else if (file_info.st_size <= 1)
        {
            printf("The WAL file is empty\n");
            //  The first tx_id should be 1
            nextXid = 1;
            result = nextXid;
            break;
        }
        printf("The size of the file is: %lld\n", file_info.st_size);

        int read_result = read(fd, wal_header, sizeof(WalHeader));
        if (read_result != sizeof(WalHeader))
        {
            perror("Error reading WAL header");
            exit(1);
        }

        printf("The number of records in the wal currently is: %d\n", wal_header->num_of_records);
        int offset = sizeof(WalHeader) + ((wal_header->num_of_records - 1) * sizeof(WalRecord));
        printf("Setting the offset to: %d\n", offset);
        lseek(fd, offset, SEEK_SET);

        wal_record = (WalRecord *)malloc(sizeof(WalRecord));
        read_result = read(fd, wal_record, sizeof(WalRecord));
        if (read_result != sizeof(WalRecord))
        {
            perror("Error reading WAL record");
            exit(1);
        }

        nextXid = wal_record->tx_id;
        result = ++nextXid;
    } while (0);

    if (wal_header)
    {
        free(wal_header);
    }
    if (wal_record)
    {
        free(wal_record);
    }
    printf("The value of nextXid is: %d\n", nextXid);
    printf("***END OF GETTING NEXT XID***\n");
    pthread_mutex_unlock(&wal_write_mutex);

    return result;
}

/**
 * @brief This method will write a new record to the WAL. It will first read the WAL header to find the number of records. It will then create a new record
 * and write it to the end of the WAL. It will then update the WAL header with the new number of records.
 * It returns 1 on success and -1 on failure.
 * @param tx_id
 * @param value
 * @return int
 */
int wal_write(uint32_t tx_id, uint32_t value)
{
    if (fd == 0)
    {
        perror("WAL file not initialized");
        return -1;
    }
    //  Acquire the mutex required for writing to the WAL
    pthread_mutex_lock(&wal_write_mutex);
    printf("\n");
    printf("***WRITING TO THE WAL***\n");
    printf("Writing the following values to the WAL:\n");
    printf("\ttx_id: %d\n", tx_id);
    printf("\tvalue: %d\n", value);
    int result = 1;

    do
    {
        //  Read the beginning of the file and find the number of records
        WalHeader *wal_header = (WalHeader *)malloc(sizeof(WalHeader));
        lseek(fd, 0, SEEK_SET); //  Move the offset pointer to the beginning of the file (just in case it's not already there)
        int read_result = read(fd, wal_header, sizeof(WalHeader));
        if (read_result == -1)
        {
            perror("Error reading WAL header");
            result = -1;
            break;
        }

        //  Move the offset pointer to the last record
        int offset;
        if (wal_header->num_of_records == 0)
        {
            offset = sizeof(WalHeader);
        }
        else
        {
            offset = sizeof(WalHeader) + ((wal_header->num_of_records) * sizeof(WalRecord));
        }

        //  Create the new WAL record
        WalRecord *record = (WalRecord *)malloc(sizeof(WalRecord));
        record->size = sizeof(value);
        record->transaction_type = INSERT;
        record->tx_id = tx_id;
        record->value = value;

        int write_result = pwrite(fd, record, sizeof(WalRecord), offset);
        if (write_result == -1)
        {
            perror("Error writing WAL record");
            result = -1;
            break;
        }

        //  Update the WAL header
        printf("The number of records in the wal currently is: %d\n", wal_header->num_of_records);
        uint32_t new_num_of_records = wal_header->num_of_records + 1;
        printf("The new number of records in the wal is: %d\n", new_num_of_records);
        wal_header->num_of_records = new_num_of_records;
        printf("Increment wal_header to: %d\n", wal_header->num_of_records);
        write_result = pwrite(fd, wal_header, sizeof(WalHeader), 0);
        if (write_result == -1)
        {
            perror("Error writing WAL header");
            result = -1;
            break;
        }
    } while (0);
    printf("***END OF WRITING TO THE WAL***\n");
    printf("\n");
    pthread_mutex_unlock(&wal_write_mutex);
    return result;
}

void wal_close()
{
    if (fd == 0)
    {
        perror("WAL file not initialized");
        exit(EXIT_FAILURE);
    }
    int close_result = close(fd);
    fd = 0;
    if (close_result == -1)
    {
        perror("Error closing WAL");
        exit(EXIT_FAILURE);
    }
    return;
}