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
#include <functional>
#include <stdexcept>
#include <type_traits>

class ThreadPool;

class Worker {
 public:
  Worker(ThreadPool &s) : pool_(s) {}

  void operator()();

 private:
  ThreadPool &pool_;
};

class ThreadPool {
 public:
  typedef std::vector<std::thread>::size_type size_type;

  ThreadPool() : ThreadPool(std::max(1u, std::thread::hardware_concurrency())) {}
  ThreadPool(size_type);
  ~ThreadPool();

  template <class F, class... Args>
  auto enqueue(F &&f, Args &&...args) -> std::future<std::invoke_result_t<F, Args...>>;
  void add_job(void *(*pfn)(void *), void *arg);
  static std::thread thread_create(void *(*pfn)(void *), void *arg);

 private:
  friend class Worker;

  // need to keep track of threads so we can join them
  std::vector<std::thread> workers_;

  typedef std::pair<int, std::function<void()>> priority_task;

  // emulate 'nice'
  struct task_comp {
    bool operator()(const priority_task &lhs, const priority_task &rhs) const { return lhs.first > rhs.first; }
  };

  // the prioritized task queue
  std::priority_queue<priority_task, std::vector<priority_task>, task_comp> tasks_;

  // synchronization
  std::mutex queue_mutex_;
  std::condition_variable condition_;
  bool stop_;
};

inline void Worker::operator()() {
  std::function<void()> task;
  while (true) {
    std::unique_lock<std::mutex> lock(pool_.queue_mutex_);

    while (!pool_.stop_ && pool_.tasks_.empty()) pool_.condition_.wait(lock);

    if (pool_.stop_ && pool_.tasks_.empty()) return;

    task = pool_.tasks_.top().second;
    pool_.tasks_.pop();

    lock.unlock();
    task();
  }
}

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(ThreadPool::size_type threads) : stop_(false) {
  workers_.reserve(threads);

  for (ThreadPool::size_type i = 0; i < threads; ++i) workers_.emplace_back(Worker(*this));
}

// add new work item to the pool
template <class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&...args) -> std::future<std::invoke_result_t<F, Args...>> {
  using return_type = std::invoke_result_t<F, Args...>;

  auto task =
      std::make_shared<std::packaged_task<return_type()>>(std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  std::future<return_type> res = task->get_future();
  {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // don't allow enqueueing after stopping the pool
    if (stop_) throw std::runtime_error("enqueue on stopped ThreadPool");

    tasks_.emplace(0, [task]() { (*task)(); });
  }
  condition_.notify_one();
  return res;
}

inline void ThreadPool::add_job(void *(*pfn)(void *), void *arg) {
  enqueue((std::function<void *()>)std::bind(pfn, arg));
}

inline std::thread ThreadPool::thread_create(void *(*pfn)(void *), void *arg) { return std::thread(pfn, arg); }

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
  stop_ = true;

  condition_.notify_all();

  for (ThreadPool::size_type i = 0; i < workers_.size(); ++i) workers_[i].join();
}

#endif
