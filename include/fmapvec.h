#pragma once

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#if !defined(__x86_64__) && !defined(__aarch64__)
#error "Unsupported architecture, only x86_64 and aarch64 are supported"
#endif

static const size_t EXPECTED_PAGESIZE = 4096;
static const size_t MMAP_SIZE = 1ull << 40; // 1 TiB, max table size

typedef uint64_t sent_magic_t;
const char *_SENTINEL_MAGIC = "FMAPVEC";
const sent_magic_t SENTINEL_MAGIC = *(sent_magic_t *)_SENTINEL_MAGIC;

typedef struct {
  // spans one page
  sent_magic_t magic;
  size_t size;

  uint64_t pad[510];
} sentinel_t;

typedef struct {
  size_t elem_size;
  size_t capacity;

  int fd;

  void *region_start;
  void *data_start;
} fmapvec_t;

static inline size_t fmapvec_size(fmapvec_t *vec) {
  assert(vec != NULL);

  sentinel_t *sentinel = (sentinel_t *)vec->region_start;
  return sentinel->size;
}

// exits with a message if the file is not a valid fmapvec file
size_t fmapvec_check_file(fmapvec_t *vec) {
  assert(vec != NULL);

  const int fd = vec->fd;
  const size_t elem_size = vec->elem_size;

  struct stat st;
  fstat(fd, &st);

  assert(st.st_size > 0);

  // check file size
  if ((size_t)st.st_size < sizeof(sentinel_t)) {
    fprintf(stderr, "Error: invalid fmapvec file, too short\n");
    exit(1);
  }

  // check sentinel
  lseek(fd, 0, SEEK_SET);
  sentinel_t sentinel;
  read(fd, &sentinel, sizeof(sentinel_t));

  if (sentinel.magic != SENTINEL_MAGIC) {
    fprintf(stderr, "Error: invalid fmapvec file, bad magic number\n");
    exit(1);
  }

  // check data size
  const size_t data_size = st.st_size - sizeof(sentinel_t);
  const size_t min_data_size = sentinel.size * elem_size;

  if (data_size < min_data_size) {
    fprintf(stderr,
            "Error: invalid fmapvec file, some data lost."
            " Expected at least %zu bytes, got %zu bytes\n",
            min_data_size, data_size);
    exit(1);
  }

  if (data_size % elem_size != 0) {
    fprintf(stderr,
            "Error: invalid fmapvec file, data size (%zu) is not a multiple "
            "of element size (%zu)\n",
            data_size, elem_size);
    exit(1);
  }

  return (data_size / elem_size);
}

static void fmapvec_init(fmapvec_t *vec, int fd, size_t elem_size) {
  assert(vec != NULL);
  assert(fd >= 0);

  // elements must be word-aligned
  assert(elem_size % 8 == 0);

  memset(vec, 0, sizeof(fmapvec_t));

  vec->fd = fd;
  vec->elem_size = elem_size;

  // sanity asserts
  assert(sizeof(size_t) == sizeof(uint64_t));
  assert(sizeof(sent_magic_t) == sizeof(uint64_t));
  assert(getpagesize() == EXPECTED_PAGESIZE);
  assert(sizeof(sentinel_t) == EXPECTED_PAGESIZE);
  assert(strnlen(_SENTINEL_MAGIC, sizeof(sent_magic_t)) + 1 ==
         sizeof(sent_magic_t));

  // get file size
  {
    struct stat st;
    fstat(fd, &st);

    // new file, initialize
    if (st.st_size == 0) {
      int ret = ftruncate(fd, sizeof(sentinel_t));
      if (ret != 0) {
        perror("ftruncate");
        exit(1);
      }

      // initialize sentinel
      sentinel_t init_sentinel = {
          .magic = SENTINEL_MAGIC,
          .size = 0,
          .pad = {0},
      };

      // write sentinel
      ret = lseek(fd, 0, SEEK_SET);
      if (ret != 0) {
        perror("lseek");
        exit(1);
      }

      ret = write(fd, &init_sentinel, sizeof(sentinel_t));
      if (ret != sizeof(sentinel_t)) {
        perror("write");
        exit(1);
      }

      ret = fallocate(vec->fd, 0,
                      sizeof(sentinel_t),
                      1024 * elem_size);
    }
  }

  size_t capacity = fmapvec_check_file(vec);
  vec->capacity = capacity;

  // map file
  lseek(fd, 0, SEEK_SET);
  vec->region_start =
      mmap(NULL, MMAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (vec->region_start == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }

  // keep size counter always resident
  madvise(vec->region_start, sizeof(sent_magic_t), MADV_WILLNEED);

  vec->data_start =
      (void *)((uintptr_t)(vec->region_start) + sizeof(sentinel_t));
}

static inline void fmapvec_destroy(fmapvec_t *vec) {
  assert(vec != NULL);

  int ret = munmap(vec->region_start, MMAP_SIZE);
  if (ret != 0) {
    perror("munmap");
    exit(1);
  }

  ret = close(vec->fd);
  if (ret != 0) {
    perror("close");
    exit(1);
  }
}

static inline void *fmapvec_get(fmapvec_t *vec, size_t loc) {
  assert(vec != NULL);
  assert(loc % vec->elem_size == 0);
  assert(loc / vec->elem_size < vec->capacity);

  return (void *)((uintptr_t)(vec->data_start) + loc);
}

static inline void fmapvec_append(fmapvec_t *vec, const void *elem) {
  assert(vec != NULL);
  assert(elem != NULL);

  sentinel_t *const sentinel = (sentinel_t *)vec->region_start;
  const size_t elem_size = vec->elem_size;
  assert(sentinel->size <= vec->capacity);

  if (sentinel->size == vec->capacity) {
    // expand file
    const size_t new_capacity = vec->capacity == 0 ? 1 : vec->capacity * 2;
    const size_t diff = new_capacity - vec->capacity;

    int ret =
        fallocate(vec->fd, 0, sizeof(sentinel_t) + (vec->capacity * elem_size),
                  diff * elem_size);

    if (ret != 0) {
      perror("fallocate");
      exit(1);
    }

    vec->capacity = new_capacity;
  }

  const size_t loc = sentinel->size * elem_size;
  void *dest = fmapvec_get(vec, loc);
  memcpy(dest, elem, elem_size);

  sentinel->size++;
}

static inline void fmapvec_sync(fmapvec_t *vec) {
  assert(vec != NULL);

  sentinel_t *sentinel = (sentinel_t *)vec->region_start;
  int ret =
      msync(vec->region_start,
            sizeof(sentinel_t) + (sentinel->size * vec->elem_size), MS_ASYNC);

  if (ret != 0) {
    perror("msync");
    exit(1);
  }
}