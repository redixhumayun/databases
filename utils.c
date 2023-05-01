#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include <stdbool.h>

static bool random_seed_initialized = false;

static void ensure_random_seed_initialized()
{
    if (!random_seed_initialized)
    {
        srand(time(NULL));
        random_seed_initialized = true;
    }
}

uint32_t generate_random_uint32()
{
    ensure_random_seed_initialized();
    return ((uint32_t)rand() << 16) | (uint32_t)rand();
}
