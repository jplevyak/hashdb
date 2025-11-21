#ifndef THREAD_POOL_HPP
#define THREAD_POOL_HPP

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <algorithm>
#include <utility>
#include <functional>
#include <stdexcept>


class ThreadPool;

class Worker {
  public:
    Worker(ThreadPool& s) : pool(s) { }

    void operator()();

  private:
    ThreadPool& pool;
};

class ThreadPool {
  public:
    typedef std::vector<std::thread>::size_type size_type;

    ThreadPool() : ThreadPool(std::max(1u, std::thread::hardware_concurrency()))
 { }
    ThreadPool(size_type);
    ~ThreadPool();

    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<typename std::result_of<F(Args...)>::type>;
    void add_job(void *(*pfn)(void *), void *arg);
    static pthread_t thread_create(void *(*pfn)(void *), void *arg);

  private:
    friend class Worker;

    // need to keep track of threads so we can join them
    std::vector<std::thread> workers;

    typedef std::pair<int, std::function<void()>> priority_task;

    // emulate 'nice'
    struct task_comp {
      bool operator()(const priority_task& lhs, const priority_task& rhs) const
{
        return lhs.first > rhs.first;
      }
    };

    // the prioritized task queue
    std::priority_queue<priority_task, std::vector<priority_task>, task_comp> tasks;

    // synchronization
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

inline void Worker::operator()() {
  std::function<void()> task;
  while(true) {
    std::unique_lock<std::mutex> lock(pool.queue_mutex);

    while(!pool.stop && pool.tasks.empty())
      pool.condition.wait(lock);

    if(pool.stop && pool.tasks.empty())
      return;

    task = pool.tasks.top().second;
    pool.tasks.pop();

    lock.unlock();

    task();
  }
}

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(ThreadPool::size_type threads) : stop(false) {
  workers.reserve(threads);

  for(ThreadPool::size_type i = 0; i < threads; ++i)
    workers.emplace_back(Worker(*this));
}

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type>
{
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared< std::packaged_task<return_type()> >(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if(stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");

        tasks.emplace(0, [task](){ (*task)(); });
    }
    condition.notify_one();
    return res;
}

inline void ThreadPool::add_job(void *(*pfn)(void *), void *arg) {
  enqueue((std::function<void *()>)std::bind(pfn, arg));
}

inline pthread_t ThreadPool::thread_create(void *(*pfn)(void *), void *arg) {
  pthread_t thread;
  pthread_create(&thread, NULL, pfn, arg);
  return thread;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
  stop = true;

  condition.notify_all();

  for(ThreadPool::size_type i = 0; i < workers.size(); ++i)
    workers[i].join();
}

#endif
