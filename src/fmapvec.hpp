#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>

template<typename T>
class FileMappedVector {
    size_t size;
    size_t capacity;

    const char* sentinel_magic = "FMAPVEC";

    struct sentinel {
        char magic[8];
        size_t used_size;
    };

    union elem {
        T data;
        sentinel sent;
    };

    int fd;
    elem* data;

    public:
    FileMappedVector(std::string loc) {
        size_t init_size = sizeof(elem) * 32;

        // create file if it doesn't exist
        fd = open(loc.c_str(), O_RDWR | O_CREAT, 0644);

        if (fd == -1) {
            std::cerr << "Error: could not open file " << loc << std::endl;
            return;
        }

        // get file size
        struct stat st;
        fstat(fd, &st);

        if(st.st_size == 0) {
            // initialize file
            ftruncate(fd, init_size);
            st.st_size = init_size;
        }

        capacity = st.st_size / sizeof(elem);

        if (capacity == 0) {
            // invalid file
            std::cerr << "Error: file " << loc << " is invalid" << std::endl;
            return;
        }

        // map file to memory
        data = (elem*)mmap(NULL, capacity * sizeof(elem), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Error: could not map file " << loc << " to memory" << std::endl;
            return;
        }

        // last element is a sentinel, check magic
        elem* last = &data[capacity - 1];
        if(strncmp(last->sent.magic, sentinel_magic, 8) != 0) {
            // initialize sentinel
            last->sent.used_size = 0;
            memcpy(last->sent.magic, sentinel_magic, 8);
        }

        size = last->sent.used_size;
    };

    ~FileMappedVector() {
        msync(data, capacity * sizeof(elem), MS_SYNC);
        munmap(data, capacity * sizeof(elem));
        close(fd);
    };

    void append(T new_elem) {
        if (size >= capacity - 1) {
            // resize
            auto new_capacity = capacity * 2;

            // unmap old memory
            munmap(data, capacity * sizeof(elem));

            // ftrunc
            ftruncate(fd, new_capacity * sizeof(elem));


            // remap
            data = (elem*)mmap(NULL, new_capacity * sizeof(elem), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (data == MAP_FAILED) {
                std::cerr << "Error: could not remap file to memory" << std::endl;
                return;
            }

            capacity = new_capacity;

            // update sentinel
            elem* last = &data[capacity - 1];
            memcpy(last->sent.magic, sentinel_magic, 8);
            last->sent.used_size = size;
        }

        data[size].data = new_elem;
        size++;

        // update sentinel
        elem* last = &data[capacity - 1];
        last->sent.used_size = size;
    };

    T operator[](size_t i) {
        if (i >= size) {
            std::cerr << "Error: index out of bounds" << std::endl;
            return T();
        }

        return data[i].data;
    };
};
