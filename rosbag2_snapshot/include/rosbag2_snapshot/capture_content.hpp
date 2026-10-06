#ifndef ROSBAG2_SNAPSHOT__CAPTURE_CONTENT_HPP_
#define ROSBAG2_SNAPSHOT__CAPTURE_CONTENT_HPP_

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace rosbag2_snapshot
{

// What one capture wrote: per-topic message counts and the receipt-time span
// of the messages. One instance per capture, used from its worker only.
// ROS-free header: see test/test_capture_content.cpp.
class CaptureContent
{
public:
  // Declares a topic of the bag, so a topic with no message still shows a 0.
  void addTopic(const std::string & topic)
  {
    if (index_.count(topic) == 0) {
      index_[topic] = topics_.size();
      topics_.push_back(topic);
      counts_.push_back(0);
    }
  }

  // One message written on topic, received at receipt_ns.
  void recordMessage(const std::string & topic, int64_t receipt_ns)
  {
    addTopic(topic);
    ++counts_[index_[topic]];
    if (total_ == 0 || receipt_ns < first_ns_) {
      first_ns_ = receipt_ns;
    }
    if (total_ == 0 || receipt_ns > last_ns_) {
      last_ns_ = receipt_ns;
    }
    ++total_;
  }

  // In declaration order; counts() is parallel to it.
  const std::vector<std::string> & topics() const {return topics_;}
  const std::vector<uint64_t> & counts() const {return counts_;}
  uint64_t totalMessages() const {return total_;}
  // 0 when no message was written.
  int64_t firstReceiptNs() const {return total_ == 0 ? 0 : first_ns_;}
  int64_t lastReceiptNs() const {return total_ == 0 ? 0 : last_ns_;}

private:
  std::vector<std::string> topics_;
  std::vector<uint64_t> counts_;
  std::map<std::string, size_t> index_;
  uint64_t total_{0};
  int64_t first_ns_{0};
  int64_t last_ns_{0};
};

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__CAPTURE_CONTENT_HPP_
