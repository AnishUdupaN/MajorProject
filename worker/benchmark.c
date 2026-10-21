#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <dlfcn.h>

#if defined(__APPLE__)
#include <sys/types.h>
#include <sys/sysctl.h>
#elif defined(__linux__)
#include <sys/sysinfo.h>
#endif

#define CPU_OPS_PER_THREAD 800000000ULL
#define GPU_ARRAY_SIZE     (1024 * 1024 * 8) // 8M elements
#define GPU_KERNEL_ITERS   64

static bool g_debug = false;

#define DEBUG_LOG(fmt, ...) \
    do { \
        if (g_debug) { \
            fprintf(stderr, "[GPU Debug] " fmt "\n", ##__VA_ARGS__); \
        } \
    } while (0)

// OpenCL minimal standard type definitions
typedef intptr_t cl_int;
typedef uint32_t cl_uint;
typedef uint64_t cl_ulong;
typedef uint64_t cl_bitfield;
typedef cl_bitfield cl_device_type;
typedef cl_uint cl_device_info;
typedef void* cl_platform_id;
typedef void* cl_device_id;
typedef void* cl_context;
typedef void* cl_command_queue;
typedef void* cl_mem;
typedef void* cl_program;
typedef void* cl_kernel;

#define CL_SUCCESS                     0
#define CL_DEVICE_TYPE_DEFAULT         (1 << 0)
#define CL_DEVICE_TYPE_CPU             (1 << 1)
#define CL_DEVICE_TYPE_GPU             (1 << 2)
#define CL_DEVICE_TYPE_ALL             0xFFFFFFFF
#define CL_DEVICE_GLOBAL_MEM_SIZE      0x101F
#define CL_MEM_READ_WRITE              (1 << 0)
#define CL_MEM_COPY_HOST_PTR           (1 << 5)

// Dynamic function pointer prototypes
typedef cl_int (*pfn_clGetPlatformIDs)(cl_uint, cl_platform_id*, cl_uint*);
typedef cl_int (*pfn_clGetDeviceIDs)(cl_platform_id, cl_device_type, cl_uint, cl_device_id*, cl_uint*);
typedef cl_int (*pfn_clGetDeviceInfo)(cl_device_id, cl_device_info, size_t, void*, size_t*);
typedef cl_context (*pfn_clCreateContext)(const intptr_t*, cl_uint, const cl_device_id*, void*, void*, cl_int*);
typedef cl_command_queue (*pfn_clCreateCommandQueue)(cl_context, cl_device_id, uint64_t, cl_int*);
typedef cl_program (*pfn_clCreateProgramWithSource)(cl_context, cl_uint, const char**, const size_t*, cl_int*);
typedef cl_int (*pfn_clBuildProgram)(cl_program, cl_uint, const cl_device_id*, const char*, void*, void*);
typedef cl_kernel (*pfn_clCreateKernel)(cl_program, const char*, cl_int*);
typedef cl_mem (*pfn_clCreateBuffer)(cl_context, uint64_t, size_t, void*, cl_int*);
typedef cl_int (*pfn_clSetKernelArg)(cl_kernel, cl_uint, size_t, const void*);
typedef cl_int (*pfn_clEnqueueNDRangeKernel)(cl_command_queue, cl_kernel, cl_uint, const size_t*, const size_t*, const size_t*, cl_uint, const void*, void*);
typedef cl_int (*pfn_clFinish)(cl_command_queue);
typedef cl_int (*pfn_clReleaseMemObject)(cl_mem);
typedef cl_int (*pfn_clReleaseKernel)(cl_kernel);
typedef cl_int (*pfn_clReleaseProgram)(cl_program);
typedef cl_int (*pfn_clReleaseCommandQueue)(cl_command_queue);
typedef cl_int (*pfn_clReleaseContext)(cl_context);

static inline uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

// -------------------------------------------------------------
// System RAM Detection (Cross-Platform)
// -------------------------------------------------------------
uint64_t get_system_ram_mb(void) {
#if defined(__APPLE__)
    int mib[2] = {CTL_HW, HW_MEMSIZE};
    uint64_t mem = 0;
    size_t len = sizeof(mem);
    if (sysctl(mib, 2, &mem, &len, NULL, 0) == 0) {
        return mem / (1024 * 1024);
    }
#elif defined(__linux__)
    struct sysinfo si;
    if (sysinfo(&si) == 0) {
        return ((uint64_t)si.totalram * (uint64_t)si.mem_unit) / (1024 * 1024);
    }
#endif
    // POSIX fallback
    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    if (pages > 0 && page_size > 0) {
        return ((uint64_t)pages * (uint64_t)page_size) / (1024 * 1024);
    }
    return 0;
}

// -------------------------------------------------------------
// CPU Benchmark (Fused Multiply-Add & Bit Mixing)
// -------------------------------------------------------------
void *cpu_worker(void *arg) {
    uint64_t iters = *(uint64_t *)arg;
    volatile double a = 1.0000001, b = 1.0000002, c = 0.5;

    for (uint64_t i = 0; i < iters; i++) {
        a = a * b + c;
        b = b * c + a;
    }

    *(double *)arg = a + b;
    return NULL;
}

double run_cpu_benchmark(int thread_count) {
    pthread_t threads[thread_count];
    uint64_t args[thread_count];

    for (int i = 0; i < thread_count; i++) {
        args[i] = CPU_OPS_PER_THREAD;
    }

    uint64_t start = get_time_ns();
    for (int i = 0; i < thread_count; i++) {
        pthread_create(&threads[i], NULL, cpu_worker, &args[i]);
    }
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    uint64_t end = get_time_ns();

    double elapsed_sec = (double)(end - start) / 1e9;
    double total_ops = (double)CPU_OPS_PER_THREAD * (double)thread_count * 2.0;
    return (total_ops / elapsed_sec) / 1e6; // MOps/sec
}

// -------------------------------------------------------------
// GPU Benchmark & VRAM Query
// -------------------------------------------------------------
const char *gpu_kernel_src =
"__kernel void gpu_fma_bench(__global float *data, int iters) {       \n"
"    int gid = get_global_id(0);                                      \n"
"    float x = data[gid];                                             \n"
"    float y = 1.0001f;                                               \n"
"    for (int i = 0; i < iters; i++) {                                \n"
"        x = x * y + 0.5f;                                            \n"
"        y = y * x + 0.25f;                                           \n"
"    }                                                                \n"
"    data[gid] = x + y;                                               \n"
"}                                                                    \n";

double run_gpu_benchmark(uint64_t *out_vram_mb) {
    if (out_vram_mb) *out_vram_mb = 0;

    void *handle = NULL;

#if defined(__APPLE__)
    handle = dlopen("/System/Library/Frameworks/OpenCL.framework/OpenCL", RTLD_LAZY);
#else
    const char *linux_libs[] = {
        "libOpenCL.so.1",
        "libOpenCL.so",
        "/usr/lib64/libOpenCL.so.1",
        "/usr/lib/x86_64-linux-gnu/libOpenCL.so.1",
        "/usr/lib64/libOpenCL.so",
        "/usr/lib/libOpenCL.so.1",
        NULL
    };
    for (int i = 0; linux_libs[i] != NULL; i++) {
        handle = dlopen(linux_libs[i], RTLD_LAZY);
        if (handle) break;
    }
#endif

    if (!handle) {
        DEBUG_LOG("dlopen could not load OpenCL runtime: %s", dlerror());
        return -1.0;
    }

    #define LOAD_SYM(name) pfn_##name fn_##name = (pfn_##name)dlsym(handle, #name); \
        if (!fn_##name) { DEBUG_LOG("Missing symbol: %s", #name); dlclose(handle); return -1.0; }
    LOAD_SYM(clGetPlatformIDs);
    LOAD_SYM(clGetDeviceIDs);
    LOAD_SYM(clGetDeviceInfo);
    LOAD_SYM(clCreateContext);
    LOAD_SYM(clCreateCommandQueue);
    LOAD_SYM(clCreateProgramWithSource);
    LOAD_SYM(clBuildProgram);
    LOAD_SYM(clCreateKernel);
    LOAD_SYM(clCreateBuffer);
    LOAD_SYM(clSetKernelArg);
    LOAD_SYM(clEnqueueNDRangeKernel);
    LOAD_SYM(clFinish);
    LOAD_SYM(clReleaseMemObject);
    LOAD_SYM(clReleaseKernel);
    LOAD_SYM(clReleaseProgram);
    LOAD_SYM(clReleaseCommandQueue);
    LOAD_SYM(clReleaseContext);
    #undef LOAD_SYM

    cl_platform_id platforms[8];
    cl_uint num_platforms = 0;
    cl_int err = fn_clGetPlatformIDs(8, platforms, &num_platforms);
    if (err != CL_SUCCESS || num_platforms == 0) {
        DEBUG_LOG("clGetPlatformIDs returned err: %ld (platforms found: %u). Check /etc/OpenCL/vendors/.", (long)err, num_platforms);
        dlclose(handle);
        return -1.0;
    }

    cl_device_id device = NULL;
    for (cl_uint i = 0; i < num_platforms; i++) {
        cl_uint num_devs = 0;
        if (fn_clGetDeviceIDs(platforms[i], CL_DEVICE_TYPE_GPU, 1, &device, &num_devs) == CL_SUCCESS && num_devs > 0) {
            break;
        }
        if (fn_clGetDeviceIDs(platforms[i], CL_DEVICE_TYPE_ALL, 1, &device, &num_devs) == CL_SUCCESS && num_devs > 0) {
            break;
        }
    }

    if (!device) {
        DEBUG_LOG("No suitable OpenCL compute device detected across %u platform(s).", num_platforms);
        dlclose(handle);
        return -1.0;
    }

    // Query VRAM size
    cl_ulong global_mem = 0;
    if (fn_clGetDeviceInfo(device, CL_DEVICE_GLOBAL_MEM_SIZE, sizeof(global_mem), &global_mem, NULL) == CL_SUCCESS) {
        if (out_vram_mb) {
            *out_vram_mb = global_mem / (1024 * 1024);
        }
    }

    cl_context ctx = fn_clCreateContext(NULL, 1, &device, NULL, NULL, &err);
    if (err != CL_SUCCESS || !ctx) {
        DEBUG_LOG("clCreateContext failed (err: %ld)", (long)err);
        dlclose(handle);
        return -1.0;
    }

    cl_command_queue queue = fn_clCreateCommandQueue(ctx, device, 0, &err);
    if (err != CL_SUCCESS || !queue) {
        DEBUG_LOG("clCreateCommandQueue failed (err: %ld)", (long)err);
        fn_clReleaseContext(ctx);
        dlclose(handle);
        return -1.0;
    }

    cl_program program = fn_clCreateProgramWithSource(ctx, 1, &gpu_kernel_src, NULL, &err);
    err = fn_clBuildProgram(program, 1, &device, NULL, NULL, NULL);
    if (err != CL_SUCCESS) {
        DEBUG_LOG("clBuildProgram failed (err: %ld)", (long)err);
        fn_clReleaseProgram(program);
        fn_clReleaseCommandQueue(queue);
        fn_clReleaseContext(ctx);
        dlclose(handle);
        return -1.0;
    }

    cl_kernel kernel = fn_clCreateKernel(program, "gpu_fma_bench", &err);
    if (err != CL_SUCCESS || !kernel) {
        DEBUG_LOG("clCreateKernel failed (err: %ld)", (long)err);
        fn_clReleaseProgram(program);
        fn_clReleaseCommandQueue(queue);
        fn_clReleaseContext(ctx);
        dlclose(handle);
        return -1.0;
    }

    size_t bytes = sizeof(float) * GPU_ARRAY_SIZE;
    float *h_data = (float *)malloc(bytes);
    if (!h_data) {
        DEBUG_LOG("Host malloc failed");
        fn_clReleaseKernel(kernel);
        fn_clReleaseProgram(program);
        fn_clReleaseCommandQueue(queue);
        fn_clReleaseContext(ctx);
        dlclose(handle);
        return -1.0;
    }
    for (size_t i = 0; i < GPU_ARRAY_SIZE; i++) {
        h_data[i] = 1.0f;
    }

    cl_mem d_data = fn_clCreateBuffer(ctx, CL_MEM_READ_WRITE | CL_MEM_COPY_HOST_PTR, bytes, h_data, &err);
    if (err != CL_SUCCESS) {
        DEBUG_LOG("clCreateBuffer failed (err: %ld)", (long)err);
        free(h_data);
        fn_clReleaseKernel(kernel);
        fn_clReleaseProgram(program);
        fn_clReleaseCommandQueue(queue);
        fn_clReleaseContext(ctx);
        dlclose(handle);
        return -1.0;
    }

    int iters = GPU_KERNEL_ITERS;
    fn_clSetKernelArg(kernel, 0, sizeof(cl_mem), &d_data);
    fn_clSetKernelArg(kernel, 1, sizeof(int), &iters);

    size_t global_size = GPU_ARRAY_SIZE;

    uint64_t start = get_time_ns();
    err = fn_clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_size, NULL, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        DEBUG_LOG("clEnqueueNDRangeKernel failed (err: %ld)", (long)err);
        free(h_data);
        fn_clReleaseMemObject(d_data);
        fn_clReleaseKernel(kernel);
        fn_clReleaseProgram(program);
        fn_clReleaseCommandQueue(queue);
        fn_clReleaseContext(ctx);
        dlclose(handle);
        return -1.0;
    }

    fn_clFinish(queue);
    uint64_t end = get_time_ns();

    double elapsed_sec = (double)(end - start) / 1e9;
    double total_flops = (double)GPU_ARRAY_SIZE * (double)GPU_KERNEL_ITERS * 4.0;
    double score = (total_flops / elapsed_sec) / 1e6;

    free(h_data);
    fn_clReleaseMemObject(d_data);
    fn_clReleaseKernel(kernel);
    fn_clReleaseProgram(program);
    fn_clReleaseCommandQueue(queue);
    fn_clReleaseContext(ctx);
    dlclose(handle);

    return score;
}

// -------------------------------------------------------------
// Entry Point & CLI Argument Parsing
// -------------------------------------------------------------
int main(int argc, char **argv) {
    bool run_single = false;
    bool run_multi  = false;
    bool run_gpu    = false;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--debug") == 0) {
            g_debug = true;
        } else if (strcmp(argv[i], "--singlecore") == 0) {
            run_single = true;
        } else if (strcmp(argv[i], "--multicore") == 0) {
            run_multi = true;
        } else if (strcmp(argv[i], "--gpu") == 0) {
            run_gpu = true;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--singlecore] [--multicore] [--gpu] [--debug]\n", argv[0]);
            printf("If no benchmark target flags are passed, all benchmarks are run by default.\n");
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    if (!run_single && !run_multi && !run_gpu) {
        run_single = true;
        run_multi  = true;
        run_gpu    = true;
    }

    uint64_t ram_mb = get_system_ram_mb();
    uint64_t vram_mb = 0;

    long num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (num_cores < 1) num_cores = 1;

    double single_score = -1.0;
    double multi_score  = -1.0;
    double gpu_score    = -1.0;

    if (run_single) {
        single_score = run_cpu_benchmark(1);
    }
    if (run_multi) {
        multi_score = run_cpu_benchmark((int)num_cores);
    }
    if (run_gpu) {
        gpu_score = run_gpu_benchmark(&vram_mb);
    }

    // Output JSON
    printf("{\n");
    printf("  \"ram_mb\": %llu,\n", (unsigned long long)ram_mb);
    printf("  \"vram_mb\": %llu", (unsigned long long)vram_mb);

    if (run_single) {
        printf(",\n  \"single_core_score\": %.2f", single_score);
    }
    if (run_multi) {
        printf(",\n  \"multi_core_score\": %.2f", multi_score);
    }
    if (run_gpu) {
        if (gpu_score >= 0.0) {
            printf(",\n  \"gpu_score\": %.2f", gpu_score);
        } else {
            printf(",\n  \"gpu_score\": 0.00");
        }
    }
    printf("\n}\n");

    return 0;
}
