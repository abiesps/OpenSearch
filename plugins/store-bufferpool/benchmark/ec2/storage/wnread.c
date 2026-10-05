/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
// wnread: model of the bufferpool's storage read (ec2-bench eb26156111a): per IO window,
// posix_fadvise(hint_fd, off, len, POSIX_FADV_WILLNEED) on a second read-only fd, then ONE buffered
// pread(read_fd, len, off). N threads issue synchronous window reads, so in-flight IOs = threads.
// Each thread owns a disjoint region of the file (no block is read twice in a run, so no page-cache hit
// after the caller drops the cache). Latency per window = fadvise + pread wall time (CLOCK_MONOTONIC).
//
// Modes mmap / mmap-random model stock MMapDirectory instead: the whole file is mapped once (PROT_READ, MAP_SHARED),
// madvise(MADV_NORMAL) or madvise(MADV_RANDOM) on the mapping, and each op copies the window out of the mapping
// (page faults + kernel readahead/read-around decide the device IO).
//
// usage: wnread FILE BS rand|seq THREADS RUNTIME_S RAMP_S none|willneed|random|mmap|mmap-random
// output: one JSON object on stdout.
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static const char *path;
static long bs;
static int seq;
static int nthreads;
static double runtime_s, ramp_s;
static int hint; // 0 none, 1 willneed, 2 random (FADV_RANDOM on the read fd at open), 3 mmap, 4 mmap-random
static char *map;
static int read_fd, hint_fd;
static off_t fsize;
static struct timespec t_start;

typedef struct {
    int id;
    uint64_t *lat;  // ns, measured phase only
    size_t n, cap;
    uint64_t bytes;
    uint64_t short_reads, errors, hint_errors;
} worker_t;

static double now_s(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (t.tv_sec - t_start.tv_sec) + (t.tv_nsec - t_start.tv_nsec) / 1e9;
}

static uint64_t xs(uint64_t *s) {
    uint64_t x = *s;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    return *s = x;
}

static void *run(void *arg) {
    worker_t *w = arg;
    uint64_t nblocks_total = fsize / bs;
    uint64_t per = nblocks_total / nthreads;
    uint64_t first = per * w->id;
    uint8_t *seen = calloc((per + 7) / 8, 1);
    char *buf;
    if (posix_memalign((void **)&buf, 4096, bs) != 0) { perror("memalign"); exit(1); }
    uint64_t rng = 0x9E3779B97F4A7C15ULL ^ ((uint64_t)w->id * 0xD1B54A32D192ED03ULL) ^ (uint64_t)getpid();
    uint64_t used = 0, next = 0;
    while (1) {
        double t = now_s();
        if (t >= ramp_s + runtime_s) break;
        if (used >= per) break; // region exhausted (reported)
        uint64_t idx;
        if (seq) {
            idx = next++;
        } else {
            do { idx = xs(&rng) % per; } while (seen[idx >> 3] & (1u << (idx & 7)));
            seen[idx >> 3] |= (1u << (idx & 7));
        }
        used++;
        off_t off = (off_t)(first + idx) * bs;
        struct timespec a, b;
        clock_gettime(CLOCK_MONOTONIC, &a);
        if (hint == 1) {
            if (posix_fadvise(hint_fd, off, bs, POSIX_FADV_WILLNEED) != 0) w->hint_errors++;
        }
        ssize_t r;
        if (map) {
            memcpy(buf, map + off, bs);
            r = bs;
        } else {
            r = pread(read_fd, buf, bs, off);
        }
        clock_gettime(CLOCK_MONOTONIC, &b);
        if (r < 0) { w->errors++; continue; }
        if (r != bs) w->short_reads++;
        if (t >= ramp_s) {
            if (w->n == w->cap) {
                w->cap = w->cap ? w->cap * 2 : 65536;
                w->lat = realloc(w->lat, w->cap * sizeof(uint64_t));
            }
            w->lat[w->n++] = (uint64_t)(b.tv_sec - a.tv_sec) * 1000000000ULL + (b.tv_nsec - a.tv_nsec);
            w->bytes += r;
        }
    }
    free(seen);
    free(buf);
    return NULL;
}

static int cmp(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}

int main(int argc, char **argv) {
    if (argc != 8) {
        fprintf(stderr, "usage: wnread FILE BS rand|seq THREADS RUNTIME_S RAMP_S none|willneed|random\n");
        return 2;
    }
    path = argv[1];
    bs = atol(argv[2]);
    seq = strcmp(argv[3], "seq") == 0;
    nthreads = atoi(argv[4]);
    runtime_s = atof(argv[5]);
    ramp_s = atof(argv[6]);
    hint = strcmp(argv[7], "willneed") == 0 ? 1 : strcmp(argv[7], "random") == 0 ? 2
         : strcmp(argv[7], "mmap") == 0 ? 3 : strcmp(argv[7], "mmap-random") == 0 ? 4 : 0;
    read_fd = open(path, O_RDONLY);
    hint_fd = open(path, O_RDONLY);
    if (read_fd < 0 || hint_fd < 0) { perror("open"); return 1; }
    if (hint == 2 && posix_fadvise(read_fd, 0, 0, POSIX_FADV_RANDOM) != 0) { perror("fadvise random"); return 1; }
    struct stat st;
    fstat(read_fd, &st);
    fsize = st.st_size;
    if (hint >= 3) {
        map = mmap(NULL, fsize, PROT_READ, MAP_SHARED, read_fd, 0);
        if (map == MAP_FAILED) { perror("mmap"); return 1; }
        if (madvise(map, fsize, hint == 4 ? MADV_RANDOM : MADV_NORMAL) != 0) { perror("madvise"); return 1; }
    }
    worker_t *ws = calloc(nthreads, sizeof(worker_t));
    pthread_t *th = calloc(nthreads, sizeof(pthread_t));
    clock_gettime(CLOCK_MONOTONIC, &t_start);
    for (int i = 0; i < nthreads; i++) { ws[i].id = i; pthread_create(&th[i], NULL, run, &ws[i]); }
    for (int i = 0; i < nthreads; i++) pthread_join(th[i], NULL);
    double elapsed = now_s() - ramp_s;
    if (elapsed > runtime_s) elapsed = runtime_s;
    size_t total = 0;
    uint64_t bytes = 0, shorts = 0, errs = 0, herrs = 0;
    for (int i = 0; i < nthreads; i++) { total += ws[i].n; bytes += ws[i].bytes; shorts += ws[i].short_reads; errs += ws[i].errors; herrs += ws[i].hint_errors; }
    uint64_t *all = malloc((total ? total : 1) * sizeof(uint64_t));
    size_t k = 0;
    double sum = 0;
    for (int i = 0; i < nthreads; i++) for (size_t j = 0; j < ws[i].n; j++) { all[k++] = ws[i].lat[j]; sum += ws[i].lat[j]; }
    qsort(all, total, sizeof(uint64_t), cmp);
#define P(q) (total ? all[(size_t)((q) * (total - 1))] / 1000.0 : 0)
    printf("{\"tool\":\"wnread\",\"file\":\"%s\",\"file_size\":%lld,\"bs\":%ld,\"pattern\":\"%s\",\"threads\":%d,"
           "\"hint\":\"%s\",\"runtime_s\":%.3f,\"ramp_s\":%.1f,\"ops\":%zu,\"bytes\":%llu,\"iops\":%.1f,\"MBps\":%.2f,"
           "\"lat_us\":{\"mean\":%.1f,\"min\":%.1f,\"p50\":%.1f,\"p90\":%.1f,\"p99\":%.1f,\"p999\":%.1f,\"max\":%.1f},"
           "\"short_reads\":%llu,\"errors\":%llu,\"hint_errors\":%llu}\n",
           path, (long long)fsize, bs, seq ? "seq" : "rand", nthreads, argv[7], elapsed, ramp_s, total,
           (unsigned long long)bytes, total / elapsed, bytes / elapsed / 1e6, total ? sum / total / 1000.0 : 0,
           P(0.0), P(0.50), P(0.90), P(0.99), P(0.999), P(1.0), (unsigned long long)shorts,
           (unsigned long long)errs, (unsigned long long)herrs);
    return 0;
}
