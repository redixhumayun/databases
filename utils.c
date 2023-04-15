#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include "utils.h"

uint32_t generate_random_uint32() {
    // Seed the random number generator
    srand(time(NULL));

    // Generate a random 4-byte integer and return it as a uint32_t
    return (uint32_t) rand();
}