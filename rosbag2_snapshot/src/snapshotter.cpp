// Copyright (c) 2018-2021, Open Source Robotics Foundation, Inc., GAIA Platform, Inc., All rights reserved.  // NOLINT
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//
//    * Neither the name of the {copyright_holder} nor the names of its
//      contributors may be used to endorse or promote products derived from
//      this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#include <builtin_interfaces/msg/time.hpp>
#include <rcpputils/scope_exit.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rmw/rmw.h>
#include <rosbag2_cpp/typesupport_helpers.hpp>
#include <rosbag2_snapshot/snapshotter.hpp>
#include <rosidl_typesupport_introspection_cpp/field_types.hpp>
#include <rosidl_typesupport_introspection_cpp/message_introspection.hpp>

#include <filesystem>

#include <climits>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <iomanip>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <thread>
#include <fstream>

namespace rosbag2_snapshot
{

using namespace std::chrono_literals;  // NOLINT

using rclcpp::Time;
using rosbag2_snapshot_msgs::srv::TriggerSnapshot;
using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;
using std::shared_ptr;
using std::string;
using std_srvs::srv::SetBool;

namespace
{
int64_t abs_duration_ns(const rclcpp::Time & a, const rclcpp::Time & b)
{
  return std::llabs((a - b).nanoseconds());
}

rclcpp::Time semantic_time_or_receive(
  const builtin_interfaces::msg::Time & stamp,
  const rclcpp::Time & receive_time)
{
  if (stamp.sec != 0 || stamp.nanosec != 0) {
    return rclcpp::Time(stamp);
  }
  return receive_time;
}

bool builtin_time_nonzero(const builtin_interfaces::msg::Time & t)
{
  return t.sec != 0 || t.nanosec != 0;
}

bool builtin_time_equal(const builtin_interfaces::msg::Time & a, const builtin_interfaces::msg::Time & b)
{
  return a.sec == b.sec && a.nanosec == b.nanosec;
}

namespace introspection = rosidl_typesupport_introspection_cpp;

const introspection::MessageMember * find_member(
  const introspection::MessageMembers * members, const char * name)
{
  if (members == nullptr) {
    return nullptr;
  }
  for (uint32_t i = 0; i < members->member_count_; ++i) {
    if (std::string(members->members_[i].name_) == name) {
      return &members->members_[i];
    }
  }
  return nullptr;
}

/// True if `member` is a non-array nested message of exactly the given type.
bool is_message_of_type(
  const introspection::MessageMember * member, const char * ns, const char * name)
{
  if (member == nullptr || member->type_id_ != introspection::ROS_TYPE_MESSAGE ||
    member->is_array_)
  {
    return false;
  }
  auto members = static_cast<const introspection::MessageMembers *>(member->members_->data);
  return std::string(members->message_namespace_) == ns &&
         std::string(members->message_name_) == name;
}

/**
 * Reads header.stamp out of a serialized message, given only its type name, no compile-time
 * knowledge of the type needed. Only works if the message really has a std_msgs/Header with a
 * builtin_interfaces/Time stamp; a field just named "header" or "stamp" doesn't count, we
 * check the actual type. Anything else is cleanly rejected.
 *
 * This package is meant to work on any robot, so it can't depend on a robot's own message
 * packages just to read a timestamp, the way deserializing into a hardcoded C++ type would
 * require. Looking the type up at runtime avoids that, and works no matter where header
 * sits in the message (some types put it last, not first).
 *
 * Type lookups load a shared library, so we cache the result per type name.
 */
class HeaderStampReader
{
public:
  std::optional<builtin_interfaces::msg::Time> read(
    const std::string & type, const rclcpp::SerializedMessage & serialized)
  {
    const Layout * layout = layoutFor(type);
    if (layout == nullptr) {
      return std::nullopt;
    }

    std::vector<uint8_t> storage(layout->members->size_of_);
    layout->members->init_function(
      storage.data(), rosidl_runtime_cpp::MessageInitialization::ALL);
    RCPPUTILS_SCOPE_EXIT(layout->members->fini_function(storage.data()));

    if (rmw_deserialize(
        &serialized.get_rcl_serialized_message(), layout->type_support,
        storage.data()) != RMW_RET_OK)
    {
      return std::nullopt;
    }

    const uint8_t * stamp_base = storage.data() + layout->stamp_offset;
    builtin_interfaces::msg::Time stamp;
    stamp.sec = *reinterpret_cast<const int32_t *>(stamp_base + layout->sec_offset);
    stamp.nanosec = *reinterpret_cast<const uint32_t *>(stamp_base + layout->nanosec_offset);
    return stamp;
  }

  /// Whether this type carries a std_msgs/Header with a builtin_interfaces/Time stamp,
  /// i.e. whether read() can work on it.
  bool hasHeader(const std::string & type) {return layoutFor(type) != nullptr;}

private:
  struct Layout
  {
    const introspection::MessageMembers * members{nullptr};
    const rosidl_message_type_support_t * type_support{nullptr};
    uint32_t stamp_offset{0};
    uint32_t sec_offset{0};
    uint32_t nanosec_offset{0};
    // Keep the libraries alive for as long as the handles taken from them are used.
    std::shared_ptr<rcpputils::SharedLibrary> introspection_library;
    std::shared_ptr<rcpputils::SharedLibrary> type_support_library;
  };

  // Guards every access to layouts_ (lookup and insert-on-miss alike): this reader is a
  // single shared instance, and Snapshotter runs each accepted TriggerSnapshot goal on
  // its own detached thread (see handle_accepted), so concurrent snapshots can call in
  // here at once. unordered_map gives no thread-safety for a concurrent insert against
  // any other operation, including reads of a different key: an insert can rehash and
  // touch every bucket. The lock is released before using the returned pointer: once an
  // entry exists it is never mutated or erased again, and unordered_map guarantees
  // references to existing elements stay valid across later inserts, so reading through
  // the pointer afterward (the actual deserialization work) needs no lock.
  const Layout * layoutFor(const std::string & type)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto cached = layouts_.find(type);
    if (cached != layouts_.end()) {
      return cached->second ? &cached->second.value() : nullptr;
    }
    auto & slot = layouts_[type];
    slot = buildLayout(type);
    return slot ? &slot.value() : nullptr;
  }

  static std::optional<Layout> buildLayout(const std::string & type)
  {
    Layout layout{};
    try {
      layout.introspection_library = rosbag2_cpp::get_typesupport_library(
        type, "rosidl_typesupport_introspection_cpp");
      const auto * introspection_support = rosbag2_cpp::get_typesupport_handle(
        type, "rosidl_typesupport_introspection_cpp", layout.introspection_library);
      layout.members =
        static_cast<const introspection::MessageMembers *>(introspection_support->data);

      layout.type_support_library =
        rosbag2_cpp::get_typesupport_library(type, "rosidl_typesupport_cpp");
      layout.type_support = rosbag2_cpp::get_typesupport_handle(
        type, "rosidl_typesupport_cpp", layout.type_support_library);
    } catch (const std::exception &) {
      return std::nullopt;
    }

    // Scoped exclusively to a real std_msgs/Header wrapping a real builtin_interfaces/Time.
    // A field merely named "header" or "stamp" of some other type does not qualify.
    const auto * header = find_member(layout.members, "header");
    if (!is_message_of_type(header, "std_msgs::msg", "Header")) {
      return std::nullopt;
    }
    const auto * header_members =
      static_cast<const introspection::MessageMembers *>(header->members_->data);
    const auto * stamp = find_member(header_members, "stamp");
    if (!is_message_of_type(stamp, "builtin_interfaces::msg", "Time")) {
      return std::nullopt;
    }
    const auto * stamp_members =
      static_cast<const introspection::MessageMembers *>(stamp->members_->data);
    const auto * sec = find_member(stamp_members, "sec");
    const auto * nanosec = find_member(stamp_members, "nanosec");
    if (sec == nullptr || nanosec == nullptr) {
      return std::nullopt;
    }

    layout.stamp_offset = header->offset_ + stamp->offset_;
    layout.sec_offset = sec->offset_;
    layout.nanosec_offset = nanosec->offset_;
    return layout;
  }

  std::mutex mutex_;
  std::unordered_map<std::string, std::optional<Layout>> layouts_;
};

HeaderStampReader & header_stamp_reader()
{
  static HeaderStampReader reader;
  return reader;
}

bool topic_uses_interval_single_msg_narrowing(
  const TopicDetails & details,
  const std::unordered_set<std::string> & interval_single_msg_types)
{
  // Built-in, unconditional: these narrowed before this package took a config-driven
  // approach to eligibility, so keeping them hardcoded avoids silently dropping that
  // behavior for any deployment that upgrades without also listing them in
  // interval_single_msg_types. Both come from sensor_msgs/visualization_msgs, which this
  // package already depends on regardless of which robot it runs on, so hardcoding them
  // doesn't break the "no robot-specific dependencies" goal the way a robot's own message
  // type would.
  if (details.type == "sensor_msgs/msg/CameraInfo") {
    return true;
  }
  if (details.type == "visualization_msgs/msg/ImageMarker") {
    return true;
  }
  // Compressed Image topics (h264 or jpg/png) always narrow to a single message. This is
  // unconditional too, unlike everything else below which is opt-in via config.
  if (details.type == "sensor_msgs/msg/Image") {
    return details.img_compression_opts_.use_compression;
  }
  // Everything else is explicit opt-in: this package works on any robot, so it can't know
  // a robot's own message types up front. Each deployment lists the ones it wants here.
  return interval_single_msg_types.count(details.type) > 0;
}

MessageQueue::range_t narrow_range_for_interval_single_msg(
  MessageQueue::range_t range,
  const TopicDetails & topic_details,
  const builtin_interfaces::msg::Time & goal_stamp_builtin,
  rclcpp::Logger logger)
{
  if (range.first == range.second) {
    return range;
  }

  const rclcpp::Time goal_rt(goal_stamp_builtin);

  if (!header_stamp_reader().hasHeader(topic_details.type)) {
    RCLCPP_WARN(
      logger,
      "interval_mode_single_msg: type %s on topic %s has no std_msgs/Header with a "
      "builtin_interfaces/Time stamp; cannot narrow to a single message",
      topic_details.type.c_str(), topic_details.name.c_str());
    return range;
  }

  MessageQueue::range_t::first_type exact_it = range.second;
  MessageQueue::range_t::first_type closest_it = range.second;
  int64_t best_abs_ns = INT64_MAX;

  for (auto it = range.first; it != range.second; ++it) {
    builtin_interfaces::msg::Time sem_builtin{};
    rclcpp::Time candidate_rt;
    try {
      auto stamp = header_stamp_reader().read(topic_details.type, *it->msg);
      if (!stamp) {
        continue;
      }
      sem_builtin = *stamp;
      candidate_rt = semantic_time_or_receive(sem_builtin, it->time);
    } catch (const std::exception & e) {
      RCLCPP_WARN(
        logger, "interval_mode_single_msg: skipped buffered message on %s (%s)",
        topic_details.name.c_str(), e.what());
      continue;
    }

    if (exact_it == range.second && builtin_time_nonzero(sem_builtin) &&
      builtin_time_equal(sem_builtin, goal_stamp_builtin))
    {
      exact_it = it;
    }

    const int64_t d = abs_duration_ns(candidate_rt, goal_rt);
    if (d < best_abs_ns) {
      best_abs_ns = d;
      closest_it = it;
    }
  }

  MessageQueue::range_t::first_type chosen = range.second;
  if (exact_it != range.second) {
    chosen = exact_it;
    RCLCPP_INFO(
      logger, "[INTERVAL_MODE]: single_msg exact stamp match on topic %s",
      topic_details.name.c_str());
  } else if (closest_it != range.second) {
    chosen = closest_it;
    RCLCPP_WARN(
      logger,
      "[INTERVAL_MODE]: single_msg no exact stamp on topic %s; using closest (abs dt = %.3f ms). "
      "Cross-topic dataset alignment is not guaranteed.",
      topic_details.name.c_str(), static_cast<double>(best_abs_ns) / 1e6);
  }

  if (chosen == range.second) {
    RCLCPP_WARN(
      logger,
      "interval_mode_single_msg: no message could be matched on topic %s for goal stamp",
      topic_details.name.c_str());
    return MessageQueue::range_t(range.second, range.second);
  }

  auto next = chosen;
  ++next;
  return MessageQueue::range_t(chosen, next);
}
}  // namespace

const rclcpp::Duration SnapshotterTopicOptions::NO_DURATION_LIMIT = rclcpp::Duration(-1s);
const int32_t SnapshotterTopicOptions::NO_MEMORY_LIMIT = -1;
const rclcpp::Duration SnapshotterTopicOptions::INHERIT_DURATION_LIMIT = rclcpp::Duration(0s);
const int32_t SnapshotterTopicOptions::INHERIT_MEMORY_LIMIT = 0;
static constexpr uint32_t MB_TO_B = 1e6;

SnapshotterTopicOptions::SnapshotterTopicOptions(
  rclcpp::Duration duration_limit,
  int32_t memory_limit)
: duration_limit_(duration_limit), memory_limit_(memory_limit)
{
}

SnapshotterOptions::SnapshotterOptions(
  rclcpp::Duration default_duration_limit,
  int32_t default_memory_limit)
: default_duration_limit_(default_duration_limit),
  default_memory_limit_(default_memory_limit),
  topics_()
{
}

bool SnapshotterOptions::addTopic(
  const TopicDetails & topic_details,
  rclcpp::Duration duration,
  int32_t memory)
{
  SnapshotterTopicOptions ops(duration, memory);
  std::pair<topics_t::iterator, bool> ret;
  ret = topics_.emplace(topic_details, ops);
  return ret.second;
}

SnapshotMessage::SnapshotMessage(
  std::shared_ptr<const rclcpp::SerializedMessage> _msg, Time _time)
: msg(_msg), time(_time)
{
}

MessageQueue::MessageQueue(const SnapshotterTopicOptions & options, const rclcpp::Logger & logger)
: options_(options), logger_(logger), size_(0)
{
}

void MessageQueue::setSubscriber(shared_ptr<rclcpp::GenericSubscription> sub)
{
  sub_ = sub;
}

std::shared_ptr<MessageQueue> MessageQueue::clone()
{
  std::lock_guard<std::mutex> l(lock);
  auto cloned = std::make_shared<MessageQueue>(this->options_, this->logger_);
  
  // Validate current state before cloning to prevent corruption
  if (size_ < 0) {
    RCLCPP_ERROR(logger_, "Invalid negative size detected during clone, resetting");
    size_ = 0;
    return std::make_shared<MessageQueue>(this->options_, this->logger_);
  }
  
  // Safely copy the queue with validation
  try {
    cloned->queue_ = this->queue_; // Copy the queue
    cloned->size_ = this->size_;   // Copy the size
    
    // Validate cloned state
    if (cloned->size_ != this->size_ || cloned->queue_.size() != this->queue_.size()) {
      RCLCPP_ERROR(logger_, "Clone validation failed, creating empty clone. Original: size=%ld, queue=%zu, Cloned: size=%ld, queue=%zu", 
                   this->size_, this->queue_.size(), cloned->size_, cloned->queue_.size());
      cloned->queue_.clear();
      cloned->size_ = 0;
    }
  } catch (const std::exception& e) {
    RCLCPP_ERROR(logger_, "Exception during cloning: %s, creating empty clone", e.what());
    cloned->queue_.clear();
    cloned->size_ = 0;
  }
  
  return cloned;
}

void MessageQueue::clear()
{
  std::lock_guard<std::mutex> l(lock);
  _clear();
}

void MessageQueue::_clear()
{
  if(options_.duration_limit_.seconds() > 0.0)
  {
    // Safely clear the queue
    try {
      queue_.clear();
      size_ = 0;
    } catch (const std::exception& e) {
      RCLCPP_ERROR(logger_, "Exception during queue clear: %s", e.what());
      size_ = 0;  // Reset size even if clear failed
    }
  }
  else
  {
    RCLCPP_INFO(logger_, "Not clearing queue for topic %s because duration is set to %f", sub_->get_topic_name(), options_.duration_limit_.seconds());
  }
}

rclcpp::Duration MessageQueue::duration() const
{
  // No duration if 0 or 1 messages
  if (queue_.size() <= 1) {
    return rclcpp::Duration(0s);
  }
  return queue_.back().time - queue_.front().time;
}

bool MessageQueue::preparePush(int32_t size, rclcpp::Time const & time)
{
  // If new message is older than back of queue, time has gone backwards and buffer must be cleared
  if (!queue_.empty() && time < queue_.back().time) {
    RCLCPP_WARN(logger_, "Time has gone backwards. Clearing buffer for this topic.");
    _clear();
  }

  // The only case where message cannot be addded is if size is greater than limit
  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT &&
    size > options_.memory_limit_)
  {
    static rclcpp::Clock clock;
    RCLCPP_WARN_THROTTLE(logger_, clock, 5000,
                         "Message size (%d bytes) from topic %s exceeds memory limit (%d bytes), dropping", 
                         size, sub_->get_topic_name(), options_.memory_limit_);
    return false;
  }

  // If memory limit is enforced, remove elements from front of queue until limit
  // would be met once message is added
  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT) {
    while (queue_.size() != 0 && size_ + size > options_.memory_limit_) {
      _pop();
    }
  }

  // Periodic aggressive cleanup when memory usage is high (>90% of limit)
  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT && 
      size_ > (options_.memory_limit_ * 0.90)) {
    size_t removed = 0;
    // Aggressively clean down to 50% to prevent memory pressure crashes
    while (!queue_.empty() && size_ > (options_.memory_limit_ * 0.5)) {
      _pop();
      removed++;
    }
    if (removed > 0) {
      static rclcpp::Clock clock;
      RCLCPP_DEBUG_THROTTLE(logger_, clock, 10000,
                          "Aggressive cleanup: removed %zu messages to reduce memory pressure", removed);
    }
  }

  // If duration limit is encforced, remove elements from front of queue until duration limit
  // would be met once message is added
  if (options_.duration_limit_ > SnapshotterTopicOptions::NO_DURATION_LIMIT &&
    queue_.size() != 0)
  {
    rclcpp::Duration dt = time - queue_.front().time;
    while (dt > options_.duration_limit_) {
      _pop();
      if (queue_.empty()) {
        break;
      }
      dt = time - queue_.front().time;
    }
  }
  return true;
}
bool MessageQueue::refreshBuffer(rclcpp::Time const& time)
{
  if (options_.duration_limit_ > SnapshotterTopicOptions::NO_DURATION_LIMIT && queue_.size() != 0)
  {
    rclcpp::Duration dt = time - queue_.front().time;
    while (dt > options_.duration_limit_)
    {
      _pop();
      if (queue_.empty())
      {
          break;
      }
      dt = time - queue_.front().time;
    }
  }
  return true;
}
void MessageQueue::push(SnapshotMessage const& _out)
{
  std::unique_lock<std::mutex> l(lock);
  if (!l.owns_lock()) {
    static rclcpp::Clock clock;
    RCLCPP_WARN_THROTTLE(logger_, clock, 1000, 
                         "Failed to acquire lock for topic %s, dropping message", 
                         sub_ ? sub_->get_topic_name() : "unknown");
    return;
  }
  _push(_out);
}

SnapshotMessage MessageQueue::pop()
{
  std::lock_guard<std::mutex> l(lock);
  return _pop();
}

int64_t MessageQueue::getMessageSize(SnapshotMessage const & snapshot_msg) const
{
  // Account for message data + metadata + deque overhead + shared_ptr overhead
  size_t message_size = snapshot_msg.msg->size();
  size_t metadata_size = sizeof(SnapshotMessage);
  size_t deque_overhead = sizeof(std::deque<SnapshotMessage>::value_type) + 32; // Approximate node overhead
  size_t shared_ptr_overhead = sizeof(std::shared_ptr<const rclcpp::SerializedMessage>) + 
                               sizeof(rclcpp::SerializedMessage);
  
  return message_size + metadata_size + deque_overhead + shared_ptr_overhead;
}

void MessageQueue::_push(SnapshotMessage const & _out)
{
  int32_t size = _out.msg->size();
  // If message cannot be added without violating limits, it must be dropped
  if (!preparePush(size, _out.time)) {
    return;
  }
  queue_.push_back(_out);
  // Add size of new message to running count to maintain correctness
  size_ += getMessageSize(_out);
}

SnapshotMessage MessageQueue::_pop()
{
  SnapshotMessage tmp = queue_.front();
  queue_.pop_front();
  //  Remove size of popped message to maintain correctness of size_
  size_ -= getMessageSize(tmp);
  return tmp;
}

MessageQueue::range_t MessageQueue::rangeFromTimes(Time const & start, Time const & stop, int old_messages_to_keep)
{
  range_t::first_type begin = queue_.begin();
  range_t::second_type end = queue_.end();

  
  if(options_.duration_limit_ != options_.NO_DURATION_LIMIT)
  {
    // Increment / Decrement iterators until time contraints are met
    range_t::first_type time_begin = begin;
    if (start.seconds() != 0.0 || start.nanoseconds() != 0) {
      while (time_begin != end && (*time_begin).time < start) {
        ++time_begin;
      }
    }
    if (stop.seconds() != 0.0 || stop.nanoseconds() != 0) {
      while (end != time_begin && (*(end - 1)).time > stop) {
        --end;
      }
    }

    // If old_messages_to_keep is positive, include that many messages before the time window
    if (old_messages_to_keep > 0 && time_begin != begin) {
      begin = (time_begin - old_messages_to_keep > begin) ? time_begin - old_messages_to_keep : begin;
    } else {
      begin = time_begin;
    }
  }
  return range_t(begin, end);
}

MessageQueue::range_t MessageQueue::intervalFromTimesMsg(Time const & msg_timestamp, const double & tolerance)
{
  range_t::first_type begin = queue_.begin();
  range_t::second_type end = queue_.end();

  // determine start and stop times
  Time start = msg_timestamp - rclcpp::Duration::from_seconds(tolerance);
  Time stop = msg_timestamp + rclcpp::Duration::from_seconds(tolerance);

  // Check that msg_timestamp is within the range of the queue
  if(options_.duration_limit_ != options_.NO_DURATION_LIMIT)
  {
    // Increment / Decrement iterators until time contraints are met
    if (start.seconds() != 0.0 || start.nanoseconds() != 0) {
      while (begin != end && (*begin).time < start) {
        ++begin;
      }
    }
    if (stop.seconds() != 0.0 || stop.nanoseconds() != 0) {
      while (end != begin && (*(end - 1)).time > stop) {
        --end;
      }
    }
  }
  return range_t(begin, end);
}

const int Snapshotter::QUEUE_SIZE = 10;

Snapshotter::Snapshotter(const rclcpp::NodeOptions & options)
: rclcpp::Node("snapshotter", options),
  recording_(true),
  writing_(false),
  topic_resolver_(this)
{
  parseOptionsFromParams();

  // Create the queue for each topic and set up the subscriber to add to it on new messages
  for (auto & pair : options_.topics_) {
    string topic{pair.first.name}, type{pair.first.type};
    fixTopicOptions(pair.second);
    shared_ptr<MessageQueue> queue;
    queue.reset(new MessageQueue(pair.second, get_logger()));

    TopicDetails details{};
    details.name = topic;
    details.type = type;
    details.qos = pair.first.qos;
    details.override_old_timestamps = pair.first.override_old_timestamps;
    details.old_messages_to_keep = pair.first.old_messages_to_keep;
    details.queue_depth = pair.first.queue_depth;
    details.default_bag_duration = pair.first.default_bag_duration;
    details.img_compression_opts_ = pair.first.img_compression_opts_;
    details.throttle_period = pair.first.throttle_period;
    details.h264_throttle_skip = pair.first.h264_throttle_skip;
    std::pair<buffers_t::iterator, bool> res =
      buffers_.emplace(details, queue);
    assert(res.second);

    subscribe(details, queue);
  }

  // Union of every capture profile's topics: subscribe whatever resolves now,
  // queue the rest for the retry timer below. No-op if capture_profiles_dir_
  // was never set (profiles_ is empty).
  subscribeProfileTopics();

  // Now that subscriptions are setup, setup service servers for writing and pausing
  trigger_snapshot_action_server_ = rclcpp_action::create_server<TriggerSnapAction>(
      this,
      "trigger_snapshot",
      std::bind(&Snapshotter::handle_goal, this, _1, _2),
      std::bind(&Snapshotter::handle_cancel, this, _1),
      std::bind(&Snapshotter::handle_accepted, this, _1));
  enable_server_ = create_service<SetBool>(
    "enable_snapshot", std::bind(&Snapshotter::enableCb, this, _1, _2, _3));

  // Start timer to poll for topics (all_topics_ discovery) and/or retry
  // resolving pending capture-profile topics.
  if (options_.all_topics_ || !pending_profile_topics_.empty()) {
    poll_topic_timer_ =
      create_wall_timer(
      std::chrono::duration(1s),
      std::bind(&Snapshotter::pollAndResolveTopics, this));
  }
}

Snapshotter::~Snapshotter()
{
  for (auto & buffer : buffers_) {
    buffer.second->sub_.reset();
  }
}

ImageCompressionOptions Snapshotter::getCompressionOptions(std::string topic)
{
  std::string prefix = "topic_details." + topic;
  ImageCompressionOptions img_compression_opts;

  // use compression?
  try {
    bool use_compression = declare_parameter<bool>(prefix + ".compression.enabled");
    img_compression_opts.use_compression = use_compression;
  } catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex) {
    if (std::string{ex.what()}.find("not set") == std::string::npos) {
      RCLCPP_INFO(get_logger(), "Not using image compression for topic %s", topic.c_str());
      img_compression_opts.use_compression = false;
      return img_compression_opts;
    } else { throw ex; }
  }

  if(img_compression_opts.use_compression)
  {
    // get compression format
    try {
      std::string compression_format = declare_parameter<std::string>(prefix + ".compression.format");
      img_compression_opts.format = compression_format;
    } catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex) {
      if (std::string{ex.what()}.find("not set") == std::string::npos) {
        RCLCPP_INFO(get_logger(), "Compression enabled for topic %s but compression format not specified, using jpg with default quality", topic.c_str());
        img_compression_opts.format = "jpg";
        img_compression_opts.imwrite_flag_value = 95;
        img_compression_opts.imwrite_flag = cv::IMWRITE_JPEG_QUALITY;
        return img_compression_opts;
      } else { throw ex; }
    }

    // get jpg compression flags
    if(img_compression_opts.format == "jpg" || img_compression_opts.format == "jpeg")
    {
      img_compression_opts.format = "jpg";
      img_compression_opts.imwrite_flag = cv::IMWRITE_JPEG_QUALITY;
      try{
        int jpg_quality = declare_parameter<int>(prefix + ".compression.jpg_quality");
        img_compression_opts.imwrite_flag_value = jpg_quality;
      } catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex) {
        if (std::string{ex.what()}.find("not set") == std::string::npos) {
          RCLCPP_INFO(get_logger(), "jpg compression enabled for topic %s but quality not specified, using jpg with default quality", topic.c_str());
          img_compression_opts.imwrite_flag_value = 95;
        } else { throw ex; }
      }
    }
    // get png compression flags
    else if(img_compression_opts.format == "png")
    {
      img_compression_opts.imwrite_flag = cv::IMWRITE_PNG_COMPRESSION;
      try{
        int png_compression_level = declare_parameter<int>(prefix + ".compression.png_compression");
        img_compression_opts.imwrite_flag_value = png_compression_level;
      } catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex) {
        if (std::string{ex.what()}.find("not set") == std::string::npos) {
          RCLCPP_INFO(get_logger(), "png compression enabled for topic %s but compression not specified, using png with default compression", topic.c_str());
          img_compression_opts.imwrite_flag_value = 3;
        } else { throw ex; }
      }
    }
    // no use compression if is different than jpeg or png
    else
    {
      RCLCPP_ERROR(get_logger(), "An invalid compression format was passed for topic %s: %s. Compression will be disabled for this topic", topic.c_str(), img_compression_opts.format.c_str());
      img_compression_opts.use_compression = false;
    }
  }

  // Init encoder for h264 in case any event triggers the use of h264
  img_compression_opts.encoder = std::make_shared<FFMPEGEncoder>();
  img_compression_opts.encoder->setParameters(this, "h264.");

  return img_compression_opts;
}

void Snapshotter::parseOptionsFromParams()
{
  std::vector<std::string> topics{};

  try {
    options_.default_duration_limit_ = rclcpp::Duration::from_seconds(
      declare_parameter<double>("default_duration_limit", -1.0));
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "default_duration_limit is of incorrect type.");
    throw ex;
  }

  try {
    options_.default_memory_limit_ =
      declare_parameter<double>("default_memory_limit", 300.0); // Safe limit for concurrent operations
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "default_memory_limit is of incorrect type.");
    throw ex;
  }

  // Convert memory limit in MB to B
  if (options_.default_memory_limit_ != -1.0) {
    options_.default_memory_limit_ *= MB_TO_B;
  }

  try {
    options_.rosbag_preset_profile_ =
      declare_parameter<std::string>("rosbag_preset_profile", "zstd_small");
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_WARN(get_logger(), "param rosbag_preset_profile must be a string");
    throw ex;
  }

  RCLCPP_DEBUG(get_logger(), "using %s preset for rosbag recording", options_.rosbag_preset_profile_.c_str());

  try {
    auto interval_single_msg_types = declare_parameter<std::vector<std::string>>(
      "interval_single_msg_types", std::vector<std::string>{});
    options_.interval_single_msg_types_ = std::unordered_set<std::string>(
      interval_single_msg_types.begin(), interval_single_msg_types.end());
  } catch (const rclcpp::ParameterTypeException & ex) {
    if (std::string{ex.what()}.find("not set") == std::string::npos) {
      RCLCPP_ERROR(get_logger(), "interval_single_msg_types must be an array of strings.");
      throw ex;
    }
  }

  try {
    options_.capture_profiles_dir_ =
      declare_parameter<std::string>("capture_profiles_dir", "");
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "capture_profiles_dir must be a string.");
    throw ex;
  }

  if (!options_.capture_profiles_dir_.empty()) {
    auto parsed = loadProfilesDir(options_.capture_profiles_dir_);
    for (const auto & warning : parsed.warnings) {
      RCLCPP_WARN(get_logger(), "capture_profiles_dir: %s", warning.c_str());
    }
    profiles_.profiles = parsed.profiles.profiles;
    RCLCPP_INFO(
      get_logger(), "Loaded %zu capture profile(s) from %s",
      profiles_.profiles.size(), options_.capture_profiles_dir_.c_str());
  }

  try {
    topics = declare_parameter<std::vector<std::string>>(
      "topics", std::vector<std::string>{});
  } catch (const rclcpp::ParameterTypeException & ex) {
    if (std::string{ex.what()}.find("not set") == std::string::npos) {
      RCLCPP_ERROR(get_logger(), "topics must be an array of strings.");
      throw ex;
    }
  }

  if (topics.size() > 0) {
    options_.all_topics_ = false;

    for (const auto & topic : topics) {
      std::string prefix = "topic_details." + topic;
      std::string topic_type{};
      SnapshotterTopicOptions opts{};
      ImageCompressionOptions img_compression_opts;
      std::string topic_qos{};
      bool override_old_timestamps;
      int queue_depth = -1;
      int old_messages_to_keep = -1;
      double throttle_period = -1.0;
      bool h264_throttle_skip = false;

      try {
        topic_type = declare_parameter<std::string>(prefix + ".type");
      } catch (const rclcpp::ParameterTypeException & ex) {
        if (std::string{ex.what()}.find("not set") == std::string::npos) {
          RCLCPP_ERROR(get_logger(), "Topic type must be a string.");
        } else {
          RCLCPP_ERROR(get_logger(), "Topic %s is missing a type.", topic.c_str());
        }
        throw ex;
      }

      if(topic_type == "sensor_msgs/msg/Image")
      {
        img_compression_opts = getCompressionOptions(topic);
      }

      try
      {
        topic_qos = declare_parameter<std::string>(prefix + ".qos");
      }
        catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex)
      {
        if (std::string{ex.what()}.find("not set") == std::string::npos)
        {
          RCLCPP_DEBUG(get_logger(), "Qos not defined for topic %s, using defaul qos", topic.c_str());
        }
        topic_qos = "DEFAULT";
      } catch (const rclcpp::ParameterTypeException& ex)
      {
        if (std::string{ex.what()}.find("not set") == std::string::npos)
        {
          RCLCPP_WARN(get_logger(), "Qos not defined for topic %s, using defaul qos", topic.c_str());
        }
        topic_qos = "DEFAULT";
      }

      try
      {
        override_old_timestamps = declare_parameter<bool>(prefix + ".override_old_timestamps");
      }
        catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex)
      {
        override_old_timestamps = false;
      } catch (const rclcpp::ParameterTypeException& ex)
      {
        override_old_timestamps = false;
      }

      try
      {
        queue_depth = declare_parameter<int>(prefix + ".queue_depth");
      }
        catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex)
      {
        queue_depth = -1;
      } catch (const rclcpp::ParameterTypeException& ex)
      {
        queue_depth = -1;
      }

      try
      {
        old_messages_to_keep = declare_parameter<int>(prefix + ".old_messages_to_keep");
      }
        catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex)
      {
        old_messages_to_keep = -1;
      } catch (const rclcpp::ParameterTypeException& ex)
      {
        old_messages_to_keep = -1;
      }

      try
      {
        throttle_period = declare_parameter<double>(prefix + ".throttle_period");
      }
        catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex)
      {
        throttle_period = -1.0;
      } catch (const rclcpp::ParameterTypeException& ex)
      {
        throttle_period = -1.0;
      }

      try
      {
        h264_throttle_skip = declare_parameter<bool>(prefix + ".h264_throttle_skip");
      }
        catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex)
      {
        h264_throttle_skip = false;
      } catch (const rclcpp::ParameterTypeException& ex)
      {
        h264_throttle_skip = false;
      }
  
      try {
        opts.duration_limit_ = rclcpp::Duration::from_seconds(
          declare_parameter<double>(prefix + ".duration")
        );
      }   
      catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex)
      {
        opts.duration_limit_ = options_.default_duration_limit_;
      }
      catch (const rclcpp::ParameterTypeException & ex) {
        if (std::string{ex.what()}.find("not set") == std::string::npos) {
          RCLCPP_ERROR(
            get_logger(), "Duration limit for topic %s must be a double.", topic.c_str());
          throw ex;
        }
      }

      try {
        opts.memory_limit_ = declare_parameter<double>(prefix + ".memory");
      }    
      catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex)
      {
        opts.memory_limit_ = options_.default_memory_limit_;
      }
      catch (const rclcpp::ParameterTypeException & ex) {
        if (std::string{ex.what()}.find("not set") == std::string::npos) {
          RCLCPP_ERROR(
            get_logger(), "Memory limit for topic %s is of the wrong type.", topic.c_str());
          throw ex;
        }
      }

      TopicDetails dets{};
      dets.name = topic;
      dets.type = topic_type;
      dets.qos = qos_string_to_qos(topic_qos);
      dets.override_old_timestamps = override_old_timestamps;
      dets.queue_depth = queue_depth;
      dets.old_messages_to_keep = old_messages_to_keep;
      dets.img_compression_opts_ = img_compression_opts;
      dets.default_bag_duration = options_.default_duration_limit_;
      dets.throttle_period = throttle_period;
      dets.h264_throttle_skip = h264_throttle_skip;

      if(dets.override_old_timestamps)
      {
        RCLCPP_DEBUG(get_logger(), "Old timestamps will be overriden for topic %s", topic.c_str());
      }

      if(dets.queue_depth > 0)
      {
        RCLCPP_DEBUG(get_logger(), "Queue depth is set to %i for topic %s. Only the most %i recent messages will be saved on each bag for it", dets.queue_depth, topic.c_str(), dets.queue_depth);
      }

      if(dets.old_messages_to_keep > 0)
      {
        RCLCPP_DEBUG(get_logger(), "Old messages to keep is set to %i for topic %s. %i old messages will be kept in the bag, even if they are older than the duration limit", dets.old_messages_to_keep, topic.c_str(), dets.old_messages_to_keep);
      }

      if(dets.img_compression_opts_.use_compression)
      {
        RCLCPP_DEBUG(get_logger(), "compression: %i for topic %s using format %s and compression flag %i", dets.img_compression_opts_.use_compression, topic.c_str(), dets.img_compression_opts_.format.c_str(), dets.img_compression_opts_.imwrite_flag_value);
      }

      if(dets.throttle_period > 0.0)
      {
        RCLCPP_DEBUG(get_logger(), "Throttle period: %f for topic %s messages subsampled", dets.throttle_period, topic.c_str());
      }

      options_.topics_.insert(
        SnapshotterOptions::topics_t::value_type(dets, opts));
    }
  } else {
    options_.all_topics_ = true;
    RCLCPP_INFO(get_logger(), "No topics list provided. Logging all topics.");
    RCLCPP_WARN(get_logger(), "Logging all topics is very memory-intensive.");
  }
}

void Snapshotter::fixTopicOptions(SnapshotterTopicOptions & options)
{
  if (options.duration_limit_ == SnapshotterTopicOptions::INHERIT_DURATION_LIMIT) {
    options.duration_limit_ = options_.default_duration_limit_;
  }
  if (options.memory_limit_ == SnapshotterTopicOptions::INHERIT_MEMORY_LIMIT) {
    options.memory_limit_ = options_.default_memory_limit_;
  }
}

bool Snapshotter::postfixFilename(string & file)
{
  size_t ind = file.rfind(".bag");
  // If requested ends in .bag, this is literal name do not append date
  if (ind != string::npos && ind == file.size() - 4) {
    return true;
  }
  // Otherwise treat as prefix and append datetime and extension
  file += timeAsStr() + ".bag";
  return true;
}

string Snapshotter::timeAsStr()
{
  std::stringstream msg;
  const auto now = std::chrono::system_clock::now();
  const auto now_in_t = std::chrono::system_clock::to_time_t(now);
  msg << std::put_time(std::localtime(&now_in_t), "%Y-%m-%d-%H-%M-%S");
  return msg.str();
}

void Snapshotter::topicCb(
  std::shared_ptr<const rclcpp::SerializedMessage> msg,
  std::shared_ptr<MessageQueue> queue)
{
  // Pack message and metadata into SnapshotMessage holder
  SnapshotMessage out(msg, this->now());
  queue->push(out);
}

void Snapshotter::subscribe(
  const TopicDetails & topic_details,
  std::shared_ptr<MessageQueue> queue)
{
  RCLCPP_DEBUG(get_logger(), "Subscribing to %s", topic_details.name.c_str());

  auto opts = rclcpp::SubscriptionOptions{};
  opts.topic_stats_options.state = rclcpp::TopicStatisticsState::Enable;
  opts.topic_stats_options.publish_topic = topic_details.name + "/statistics";

  auto sub = create_generic_subscription(
    topic_details.name,
    topic_details.type,
    topic_details.qos,
    std::bind(&Snapshotter::topicCb, this, _1, queue),
    opts
  );

  queue->setSubscriber(sub);
}

bool Snapshotter::writeTopic(
  rosbag2_cpp::Writer & bag_writer,
  MessageQueue & message_queue,
  const TopicDetails & topic_details,
  const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle,
  rclcpp::Time& request_time,
  bool force_throttle)
{
  auto req = goal_handle->get_goal();
  MessageQueue::range_t range;
  if (!req->use_interval_mode)
    range = message_queue.rangeFromTimes(req->start_time, req->stop_time, topic_details.old_messages_to_keep);
  else
    range = message_queue.intervalFromTimesMsg(req->msg_timestamp, req->interval_mode_tolerance);

  rosbag2_storage::TopicMetadata tm;
  tm.name = topic_details.name;
  tm.type = topic_details.type;
  tm.serialization_format = "cdr";

  rclcpp::Serialization<sensor_msgs::msg::Image> img_serializer;
  cv_bridge::CvImagePtr cv_bridge_img;
  std::vector<int> compression_params; 
  if(topic_details.img_compression_opts_.use_compression)
  {
    if (req->use_h264)
    {
      RCLCPP_INFO(get_logger(), "H264 enabled for topic %s. applying h264 compression", topic_details.name.c_str());
      tm.type = "foxglove_msgs/msg/CompressedVideo";
    }
    else
    {
      RCLCPP_INFO(get_logger(), "topic %s is an image. applying %s compression", topic_details.name.c_str(), topic_details.img_compression_opts_.format.c_str() );
      compression_params.push_back(topic_details.img_compression_opts_.imwrite_flag);
      compression_params.push_back(topic_details.img_compression_opts_.imwrite_flag_value); // Set JPEG quality (0-100) or png compression (0-9)
      tm.type = "sensor_msgs/msg/CompressedImage";
    }
    img_serializer = rclcpp::Serialization<sensor_msgs::msg::Image>();
  }

  bag_writer.create_topic(tm);

  double prev_msg_time = 0.0;
  auto start = std::chrono::high_resolution_clock::now();
  bool h264_throttle_skip = req->use_h264 && topic_details.h264_throttle_skip;
  if(topic_details.queue_depth > 0 && !req->use_interval_mode)
  {
    range.first = std::max(range.first, range.second - topic_details.queue_depth);
    RCLCPP_INFO(get_logger(), "Only %li messages will be saved on topic %s. its queue size set in the params is %i", range.second - range.first, topic_details.name.c_str(), topic_details.queue_depth);
    if(topic_details.throttle_period > 0.0 && !h264_throttle_skip)
    {
      RCLCPP_ERROR(get_logger(), "Topic %s has a queue size of %i but has a throttle period of %f. This may have unexpected consequences",topic_details.name.c_str(), topic_details.queue_depth, topic_details.throttle_period);
    }
  }
  if (req->use_interval_mode && req->interval_mode_single_msg &&
    topic_uses_interval_single_msg_narrowing(topic_details, options_.interval_single_msg_types_))
  {
    range = narrow_range_for_interval_single_msg(
      range, topic_details, req->msg_timestamp, get_logger());
  }
  for (auto msg_it = range.first; msg_it != range.second; ++msg_it) {
    // Create BAG message
    auto bag_message = std::make_shared<rosbag2_storage::SerializedBagMessage>();
    auto ret = rcutils_system_time_now(&bag_message->time_stamp);
    if (ret != RCL_RET_OK) {
      RCLCPP_ERROR(get_logger(), "Failed to assign time to rosbag message.");
      return false;
    }
    
    if (!req->use_interval_mode && (req->throttle_msgs || force_throttle) && !h264_throttle_skip &&
        topic_details.throttle_period > 0.0 &&
        msg_it->time.nanoseconds() - prev_msg_time <= topic_details.throttle_period * 1e9)
    {
      RCLCPP_DEBUG(get_logger(),
          "topic %s is being throttled. message time: %ld, previous message time: %f, throttle_period: %f",
          topic_details.name.c_str(), msg_it->time.nanoseconds(), prev_msg_time, topic_details.throttle_period
      );
      continue;
    }

    prev_msg_time = msg_it->time.nanoseconds();

    bag_message->topic_name = tm.name;
    rclcpp::Duration bag_duration = rclcpp::Time(req->stop_time) - rclcpp::Time(req->start_time);
    if((topic_details.override_old_timestamps || topic_details.old_messages_to_keep > 0) && (request_time - msg_it->time) > bag_duration)
    {
      // Put old messages at the beginning of the bag
      RCLCPP_WARN_THROTTLE(get_logger(), *get_clock(), 10000, "Overriding old timestamps for topic %s", tm.name.c_str());
      bag_message->time_stamp = req->start_time.sec*1e9 + req->start_time.nanosec;
    }
    else
    {
      bag_message->time_stamp = msg_it->time.nanoseconds();
    }

    if(topic_details.img_compression_opts_.use_compression)
    {
      cv::Mat cv_img;
      sensor_msgs::msg::Image raw_img;
      img_serializer.deserialize_message(msg_it->msg.get(), &raw_img);
      // imencode expects rgb images in `bgr` encoding, so we need to change incoming images that
      // use `rbg8` encoding to `bgr8` encoding by hand.
      if(raw_img.encoding == "rgb8")
      {
        cv_img = cv::Mat(raw_img.height, raw_img.width, CV_8UC3, raw_img.data.data());
        cv::cvtColor(cv_img, cv_img, cv::COLOR_RGB2BGR);
      }
      else
      {
        cv_bridge_img = cv_bridge::toCvCopy(raw_img, raw_img.encoding);
        cv_img = cv_bridge_img->image;
      }

      if (req->use_h264)
      {
        auto encoder = topic_details.img_compression_opts_.encoder;
        if (!encoder->isInitialized() && !encoder->initialize((int)raw_img.width, (int)raw_img.height))
        {
          RCLCPP_ERROR(get_logger(), "Couldn't initialize H264 encoder!");
          return false;
        }
        
        foxglove_msgs::msg::CompressedVideo compressed_img;
        encoder->encodeImage(cv_img, raw_img.header, now());
        compressed_img = encoder->getCompressedImage();
        compressed_img.timestamp = raw_img.header.stamp;
        bag_writer.write(compressed_img, tm.name, rclcpp::Time(bag_message->time_stamp));
      }
      else
      {
        sensor_msgs::msg::CompressedImage compressed_img;
        cv::imencode("." + topic_details.img_compression_opts_.format, cv_img, compressed_img.data, compression_params);
        compressed_img.format = topic_details.img_compression_opts_.format;
        compressed_img.header = raw_img.header;
        bag_writer.write(compressed_img, tm.name, rclcpp::Time(bag_message->time_stamp));
      }
    }
    else
    {
      bag_message->serialized_data = std::make_shared<rcutils_uint8_array_t>(
        msg_it->msg->get_rcl_serialized_message()
      );
      bag_writer.write(bag_message);
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  RCLCPP_DEBUG(get_logger(), "Encoding time: %ld ms", duration.count());

  return true;
}

rclcpp_action::GoalResponse Snapshotter::handle_goal(
  const rclcpp_action::GoalUUID &,
  std::shared_ptr<const TriggerSnapAction::Goal> goal)
{
  // Check if requested ends in .bag
  size_t ind = goal->filename.rfind(".bag");
  if (goal->filename.empty() || ind == string::npos || ind != goal->filename.size() - 4) 
  {
    RCLCPP_WARN(this->get_logger(), "Rejecting request to snapshot. Empty filename or not ending in .bag");
    return rclcpp_action::GoalResponse::REJECT;
  }

  if (!goal->profile.empty() && profiles_.find(goal->profile) == nullptr) {
    RCLCPP_WARN(
      this->get_logger(), "Rejecting request to snapshot. Unknown capture profile '%s'.",
      goal->profile.c_str());
    return rclcpp_action::GoalResponse::REJECT;
  }

  return rclcpp_action::GoalResponse::ACCEPT_AND_EXECUTE;
}

rclcpp_action::CancelResponse Snapshotter::handle_cancel(
  const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle)
{
  RCLCPP_INFO(this->get_logger(), "Received request to cancel snapshotting.");
  (void)goal_handle;
  return rclcpp_action::CancelResponse::ACCEPT;
}

void Snapshotter::handle_accepted(const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle)
{
  auto req = goal_handle->get_goal();
  auto res = std::make_shared<TriggerSnapAction::Result>();

  std::shared_ptr<rosbag2_cpp::Writer> bag_writer_ptr;
  bag_writer_ptr = std::make_shared<rosbag2_cpp::Writer>();

  
  RCLCPP_INFO(get_logger(), "opening %s", req->filename.c_str());

  try {
    rosbag2_storage::StorageOptions storage_opts;
    storage_opts.storage_id = "mcap";
    storage_opts.uri = req->filename;
    storage_opts.storage_preset_profile = req->rosbag_preset_profile != "" ? req->rosbag_preset_profile : options_.rosbag_preset_profile_;
    rosbag2_cpp::ConverterOptions converter_opts{};
    bag_writer_ptr->open(storage_opts, converter_opts);
  } catch (const std::exception & ex) {
    RCLCPP_WARN(
          get_logger(), "Failed to open %s file, reason: %s", req->filename.c_str(), ex.what());
    res->success = false;
    res->message = "Unable to open file for writing, " + std::string(ex.what());
    return goal_handle->abort(res);
  }

  std::vector<std::pair<TopicDetails, std::shared_ptr<MessageQueue>>> cloned_buffers;
  {
    std::shared_lock<std::shared_mutex> read_lock(state_lock_);
    for (const auto& buffer : buffers_) {
      cloned_buffers.emplace_back(buffer.first, buffer.second->clone());
    }
  }

  // Use RAII to ensure cloned buffers are cleaned up properly  
  auto cleanup_buffers = rcpputils::make_scope_exit([&cloned_buffers, this]() {
    // Calculate total memory before cleanup for logging  
    size_t total_memory_freed = 0;
    for (const auto& buffer_pair : cloned_buffers) {
      if (buffer_pair.second) {
        total_memory_freed += buffer_pair.second->size_;
      }
    }
    RCLCPP_INFO(get_logger(), "Cleaned up rosbag cloned buffers, freed ~%.1f MB", 
                total_memory_freed / 1e6);
    // Let destructors handle cleanup automatically - don't manually clear/reset
  });

  // Detach thread to prevent blocking the main thread
  std::thread{std::bind(&Snapshotter::createBag, this, _1, _2, _3), goal_handle, cloned_buffers, bag_writer_ptr}.detach();
}

void Snapshotter::createBag(
  const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle,
  std::vector<std::pair<TopicDetails, std::shared_ptr<MessageQueue>>> cloned_buffers,
  std::shared_ptr<rosbag2_cpp::Writer> bag_writer_ptr)
{
  auto result = std::make_shared<TriggerSnapAction::Result>();
  auto feedback = std::make_shared<TriggerSnapAction::Feedback>();
  auto req = goal_handle->get_goal();

  rclcpp::Time request_time = this->now();
  bool success = true;
  std::string message = req->filename;
  float count_topics = 0.0;

  // A named profile picks the topic list and each topic's max_rate_hz
  // (as throttle_period, via the same override mechanism a request's own
  // topics[].throttle_period already uses below). Empty profile: unchanged
  // behavior, using req->topics as-is.
  bool use_profile = !req->profile.empty();
  std::vector<DetailsMsg> profile_topics;
  if (use_profile) {
    const CaptureProfile * profile = profiles_.find(req->profile);
    // handle_goal() already rejected an unknown profile; guard again in case
    // profiles_ ever changes between goal acceptance and execution.
    if (profile != nullptr) {
      for (const auto & spec : profile->topics) {
        DetailsMsg msg{};
        msg.name = spec.name;
        msg.throttle_period = spec.max_rate_hz > 0.0 ? (1.0 / spec.max_rate_hz) : -1.0;
        profile_topics.push_back(msg);
      }
    }
  }
  const std::vector<DetailsMsg> & topics_to_write = use_profile ? profile_topics : req->topics;

  if (topics_to_write.size() && topics_to_write.at(0).name.size()) {
    if (req->use_interval_mode) RCLCPP_WARN(get_logger(), "[INTERVAL_MODE]: enabled for snapshotting");
    for (auto & topic : topics_to_write) {
      if (goal_handle->is_canceling()) {
        result->success = false;
        result->message = "Rosbag creation canceled";
        goal_handle->canceled(result);
        return;
      }
      count_topics++;
      auto it = std::find_if(cloned_buffers.begin(), cloned_buffers.end(),
        [&topic](const auto &saved_topic) {
          return saved_topic.first.name == topic.name;
        });

      if (it == cloned_buffers.end()) {
        RCLCPP_WARN(get_logger(), "Requested topic %s is not subscribed, skipping.", topic.name.c_str());
        continue;
      }

      TopicDetails& details = it->first;
      overrideTopicDetails(topic, details);

      std::shared_ptr<MessageQueue> message_queue = it->second;

      if (message_queue->size_ == 0) RCLCPP_DEBUG(get_logger(), "Queue size for topic %s is zero", topic.name.c_str());

      // Only force this topic's throttle when the profile itself set a max_rate_hz for
      // it (topic.throttle_period != -1.0): a profile topic with no max_rate_hz must not
      // pick up an unrelated static throttle_period configured on the same topic name via
      // topics_/topic_details, which force_throttle would otherwise apply unconditionally.
      bool force_topic_throttle = use_profile && topic.throttle_period != -1.0;
      if (!writeTopic(*bag_writer_ptr, *message_queue, details, goal_handle, request_time, force_topic_throttle)) {
        success = false;
        message = "Failed to write topic " + topic.type + " to bag file.";
        break;
      }
      feedback->duration = (this->now() - request_time).seconds();
      feedback->progress = 100 * (count_topics / topics_to_write.size());
      feedback->message = "Writing topic " + topic.name + " to bag file.";
      goal_handle->publish_feedback(feedback);
    }
  } else {  // If topic list empty, record all buffered topics
    for (const auto & pair : cloned_buffers) {
      if (goal_handle->is_canceling()) {
        result->success = false;
        result->message = "Rosbag creation canceled";
        goal_handle->canceled(result);
        return;
      }
      count_topics++;
      std::shared_ptr<MessageQueue> message_queue = pair.second;
      message_queue->refreshBuffer(request_time);
      if (!writeTopic(*bag_writer_ptr, *message_queue, pair.first, goal_handle, request_time)) {
        success = false;
        message = "Failed to write topic " + pair.first.name + " to bag file.";
        break;
      }
      feedback->duration = (this->now() - request_time).seconds();
      feedback->progress = 100 * (count_topics / cloned_buffers.size());
      feedback->message = "Writing topic " + pair.first.name + " to bag file.";
      goal_handle->publish_feedback(feedback);
    }
  }
  
  // Create a file lock before closing
  std::string lock_file_path = req->filename + ".lock";
  std::ofstream lock_file(lock_file_path, std::ios::out | std::ios::trunc);
  if (lock_file.is_open()) {
    lock_file << std::this_thread::get_id() << std::endl;
    lock_file.close();
    
    // Close the bag writer
    bag_writer_ptr->close();
    RCLCPP_DEBUG(get_logger(), "Bag writer closed successfully");
    
    // Remove the lock file
    std::filesystem::remove(lock_file_path);
  } else {
    RCLCPP_WARN(get_logger(), "Failed to create lock file, but proceeding with close");
    bag_writer_ptr->close();
    RCLCPP_INFO(get_logger(), "Bag writer closed successfully");
  }

  result->success = success;
  result->message = message;
  goal_handle->succeed(result);
}

void Snapshotter::overrideTopicDetails(const DetailsMsg& req_msg, TopicDetails& details)
{
  // Only change if override is not default
  if (req_msg.throttle_period != -1.0) details.throttle_period = req_msg.throttle_period;
  if (req_msg.h264_throttle_skip != -1) details.h264_throttle_skip = req_msg.h264_throttle_skip;
  if (req_msg.override_old_timestamps != -1) details.override_old_timestamps = req_msg.override_old_timestamps;
  if (req_msg.queue_depth != -1) details.queue_depth = req_msg.queue_depth;
  if (req_msg.old_messages_to_keep != -1) details.old_messages_to_keep = req_msg.old_messages_to_keep;

  // Image compression
  if (req_msg.use_compression != -1) details.img_compression_opts_.use_compression = req_msg.use_compression;
  if (req_msg.format != "")
  {
    details.img_compression_opts_.format = req_msg.format;
    if (req_msg.format == "jpg" || req_msg.format == "jpeg") 
    {
      details.img_compression_opts_.imwrite_flag = cv::IMWRITE_JPEG_QUALITY;
      if (req_msg.jpg_quality != -1) details.img_compression_opts_.imwrite_flag_value = req_msg.jpg_quality;
    } 
    else if (req_msg.format == "png") 
    {
      details.img_compression_opts_.imwrite_flag = cv::IMWRITE_PNG_COMPRESSION;
      if (req_msg.png_compression != -1) details.img_compression_opts_.imwrite_flag_value = req_msg.png_compression;
    }
    else 
    {
      RCLCPP_WARN(get_logger(), "Invalid format to override compression: %s", req_msg.format.c_str());
      details.img_compression_opts_.use_compression = false;
    }
  }

}

void Snapshotter::clear()
{
  for (const buffers_t::value_type & pair : buffers_) {
    // if oldest message is older than default_bag_duration, clear the queue
    // Kiwi Added this condition to avoid clearing the buffer constantly
    // but still clear it if the duration exceeds the limit
    if (pair.second->duration() > pair.first.default_bag_duration) {
      RCLCPP_WARN(get_logger(), 
        "Clearing buffer for topic %s current duration: %f, default_bag_duration: %f", 
        pair.first.name.c_str(), pair.second->duration().seconds(), pair.first.default_bag_duration.seconds()
      );
      pair.second->clear();
    }
  }
}

void Snapshotter::pause()
{
  recording_ = false;
}

void Snapshotter::resume()
{
  clear();
  recording_ = true;
  RCLCPP_INFO(get_logger(), "Buffering resumed");
}

void Snapshotter::enableCb(
  const std::shared_ptr<rmw_request_id_t> request_header,
  const SetBool::Request::SharedPtr req,
  SetBool::Response::SharedPtr res)
{
  (void)request_header;

  {
    std::shared_lock<std::shared_mutex> read_lock(state_lock_);
    // Cannot enable while writing
    if (req->data && writing_) {
      res->success = false;
      res->message = "cannot enable recording while writing.";
      return;
    }
  }

  // Obtain write lock and update state if requested state is different from current
  if (req->data && !recording_) {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    resume();
  } else if (!req->data && recording_) {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    // pause();
  }

  res->success = true;
}

bool Snapshotter::isBuffered(const std::string & name) const
{
  for (const auto & buf : buffers_) {
    if (buf.first.name == name) {
      return true;
    }
  }
  return false;
}

void Snapshotter::pollTopics()
{
  const auto topic_names_and_types = get_topic_names_and_types();

  for (const auto & name_type : topic_names_and_types) {
    if (name_type.second.size() < 1) {
      RCLCPP_ERROR(get_logger(), "Subscribed topic has no associated type.");
      return;
    }

    if (name_type.second.size() > 1) {
      RCLCPP_ERROR(get_logger(), "Subscribed topic has more than one associated type.");
      return;
    }

    if (isBuffered(name_type.first)) {
      // Already buffered, e.g. via a capture profile -- don't double-subscribe
      // under all_topics_'s own (potentially different) QoS.
      continue;
    }

    TopicDetails details{};
    details.name = name_type.first;
    details.type = name_type.second[0];

    if (options_.addTopic(details)) {
      SnapshotterTopicOptions topic_options;
      fixTopicOptions(topic_options);
      auto queue = std::make_shared<MessageQueue>(topic_options, get_logger());

      std::pair<buffers_t::iterator,
        bool> res = buffers_.emplace(details, queue);
      assert(res.second);
      subscribe(details, queue);
    }
  }
}

void Snapshotter::pollAndResolveTopics()
{
  // Profile topics resolve first, so a topic a profile wants (with its
  // adapted QoS) is never grabbed first by all_topics_'s generic, non-adaptive
  // QoS(5) default -- isBuffered() only prevents a double subscription, it
  // doesn't pick which side wins the race, so the order here decides that.
  resolvePendingProfileTopics();
  if (options_.all_topics_) {
    pollTopics();
  }
}

std::map<std::string, ProfileTopicSpec> Snapshotter::uniqueProfileTopics() const
{
  std::map<std::string, ProfileTopicSpec> unique_topics;
  for (const auto & profile_pair : profiles_.profiles) {
    for (const auto & topic_spec : profile_pair.second.topics) {
      unique_topics.emplace(topic_spec.name, topic_spec);
    }
  }
  return unique_topics;
}

bool Snapshotter::subscribeResolvedTopic(
  const std::string & name, const std::string & type, const rclcpp::QoS & qos)
{
  if (isBuffered(name)) {
    return true;  // already buffered via the static topics_ list or another profile
  }

  TopicDetails details{};
  details.name = name;
  details.type = type;
  details.qos = qos;

  SnapshotterTopicOptions topic_options;
  fixTopicOptions(topic_options);
  auto queue = std::make_shared<MessageQueue>(topic_options, get_logger());
  buffers_.emplace(details, queue);
  subscribe(details, queue);
  RCLCPP_INFO(get_logger(), "Buffering profile topic %s (%s)", name.c_str(), type.c_str());
  return true;
}

bool Snapshotter::resolveAndSubscribeProfileTopic(const ProfileTopicSpec & spec)
{
  if (isBuffered(spec.name)) {
    return true;  // already buffered
  }

  std::string type = spec.type;
  if (type.empty() && !topic_resolver_.resolveType(spec.name, type)) {
    return false;
  }

  rclcpp::QoS qos(5);
  if (!spec.qos.empty()) {
    qos = qos_string_to_qos(spec.qos);
  } else if (!topic_resolver_.resolveQos(spec.name, qos)) {
    return false;
  }

  return subscribeResolvedTopic(spec.name, type, qos);
}

void Snapshotter::subscribeProfileTopics()
{
  for (const auto & entry : uniqueProfileTopics()) {
    if (!resolveAndSubscribeProfileTopic(entry.second)) {
      RCLCPP_WARN(
        get_logger(), "Profile topic %s has no publisher yet, will retry.", entry.first.c_str());
      pending_profile_topics_.push_back(entry.first);
    }
  }
}

void Snapshotter::resolvePendingProfileTopics()
{
  if (pending_profile_topics_.empty()) {
    return;
  }

  auto unique_topics = uniqueProfileTopics();
  std::vector<std::string> still_pending;
  for (const auto & name : pending_profile_topics_) {
    auto it = unique_topics.find(name);
    if (it != unique_topics.end() && !resolveAndSubscribeProfileTopic(it->second)) {
      still_pending.push_back(name);
    }
  }
  pending_profile_topics_ = still_pending;
}

}  // namespace rosbag2_snapshot

#include <rclcpp_components/register_node_macro.hpp>  // NOLINT
RCLCPP_COMPONENTS_REGISTER_NODE(rosbag2_snapshot::Snapshotter)
