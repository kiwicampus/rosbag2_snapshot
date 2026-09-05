#ifndef ROSBAG2_SNAPSHOT__SHARED_MEMORY_BUDGET_HPP_
#define ROSBAG2_SNAPSHOT__SHARED_MEMORY_BUDGET_HPP_

#include <cstdint>
#include <mutex>

namespace rosbag2_snapshot
{

// Total buffered bytes across every topic's MessageQueue, shared by all of
// them so one noisy topic can't exhaust memory alone. Accounting only
// (fits/add/release); picking which queue to trim when full is the caller's
// job.
class SharedMemoryBudget
{
public:
  SharedMemoryBudget() = default;
  explicit SharedMemoryBudget(int64_t limit_bytes)
  : limit_bytes_(limit_bytes) {}

  int64_t limit() const {return limit_bytes_;}

  // Set once at startup, before any topic is subscribed; no lock needed.
  void setLimit(int64_t limit_bytes) {limit_bytes_ = limit_bytes;}

  int64_t used() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_bytes_;
  }

  // <= 0 = unlimited, matching the rest of this option's "<=0 disables it".
  bool fits(int64_t bytes) const
  {
    if (limit_bytes_ <= 0) {
      return true;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    return used_bytes_ + bytes <= limit_bytes_;
  }

  void add(int64_t bytes)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    used_bytes_ += bytes;
  }

  void release(int64_t bytes)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    used_bytes_ = bytes > used_bytes_ ? 0 : used_bytes_ - bytes;
  }

private:
  int64_t limit_bytes_{0};
  mutable std::mutex mutex_;
  int64_t used_bytes_{0};
};

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__SHARED_MEMORY_BUDGET_HPP_
