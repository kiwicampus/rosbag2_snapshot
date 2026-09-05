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
#include <rclcpp/rclcpp.hpp>
#include <rmw/rmw.h>
#include <rosbag2_cpp/typesupport_helpers.hpp>
#include <rosbag2_snapshot/snapshotter.hpp>
#include <rosbag2_snapshot/timestamp_override.hpp>
#include <rosbag2_transport/qos.hpp>
#include <rosidl_typesupport_introspection_cpp/field_types.hpp>
#include <rosidl_typesupport_introspection_cpp/message_introspection.hpp>
#include <yaml-cpp/yaml.h>

#include <filesystem>
#include <fstream>

#include <algorithm>
#include <climits>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <future>
#include <iomanip>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <thread>

namespace rosbag2_snapshot
{

using namespace std::chrono_literals;  // NOLINT

using rclcpp::Time;
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

// rosbag2 stores offered_qos_profiles as a YAML sequence of QoS profiles.
std::string encodeQos(const rclcpp::QoS & qos)
{
  YAML::Node sequence;
  sequence.push_back(
    YAML::convert<rosbag2_transport::Rosbag2QoS>::encode(rosbag2_transport::Rosbag2QoS(qos)));
  std::ostringstream out;
  out << sequence;
  return out.str();
}

// Writer::open() always produces a directory (<uri>/<uri>_0.mcap plus a
// metadata.yaml); only the .mcap is useful standalone.
std::filesystem::path findMcapFile(const std::filesystem::path & directory)
{
  std::error_code ec;
  for (std::filesystem::recursive_directory_iterator it(directory, ec), end; it != end; ++it) {
    if (it->is_regular_file(ec) && it->path().extension() == ".mcap") {
      return it->path();
    }
  }
  return {};
}

// The mcap plugin names the data file after bag_dir's basename *at the time
// it was opened* (<that_name>_0.mcap). Since bag_dir is opened under its
// staging name and later renamed into place, the embedded file and
// metadata.yaml's relative_file_paths/files[].path both still carry the
// old name -- this renames the file and patches both to match bag_dir's
// current (final) basename, which is what a consumer that reconstructs the
// data file path itself (rather than reading metadata.yaml) needs.
void renameBagFileToMatchDirectory(const std::filesystem::path & bag_dir)
{
  const std::filesystem::path mcap = findMcapFile(bag_dir);
  if (mcap.empty()) {
    return;
  }
  const std::string old_name = mcap.filename().string();
  const std::string new_name = bag_dir.filename().string() + "_0.mcap";
  if (old_name == new_name) {
    return;
  }

  std::error_code ec;
  std::filesystem::rename(mcap, mcap.parent_path() / new_name, ec);
  if (ec) {
    return;
  }

  const std::filesystem::path metadata_path = bag_dir / "metadata.yaml";
  std::error_code exists_ec;
  if (!std::filesystem::exists(metadata_path, exists_ec)) {
    return;
  }
  try {
    YAML::Node metadata = YAML::LoadFile(metadata_path.string());
    YAML::Node info = metadata["rosbag2_bagfile_information"];
    if (info["relative_file_paths"]) {
      YAML::Node paths = info["relative_file_paths"];
      for (std::size_t i = 0; i < paths.size(); ++i) {
        if (paths[i].as<std::string>() == old_name) {
          paths[i] = new_name;
        }
      }
    }
    if (info["files"]) {
      for (auto file : info["files"]) {
        if (file["path"] && file["path"].as<std::string>() == old_name) {
          file["path"] = new_name;
        }
      }
    }
    std::ofstream(metadata_path) << metadata;
  } catch (const std::exception &) {
    // metadata.yaml is left stale; the file itself is still correctly
    // named and readable by anything that finds it by name rather than
    // through metadata.yaml.
  }
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
 * Reads header.stamp out of a serialized message given only its type name, with no
 * compile-time knowledge of the type. Verifies the field is really a std_msgs/Header with
 * a builtin_interfaces/Time stamp (not just named "header"/"stamp") before reading it, and
 * cleanly rejects anything else.
 *
 * This package targets any robot, so it can't depend on a robot's own message packages to
 * read a timestamp the way deserializing into a hardcoded C++ type would require. Resolving
 * the layout at runtime avoids that and works regardless of field order. Each type's layout
 * is resolved once and cached, since the lookup loads a shared library.
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
  // Hardcoded and unconditional, so an upgrade doesn't silently drop this narrowing for a
  // deployment that hasn't listed them in interval_single_msg_types. Safe to hardcode: both
  // types come from sensor_msgs/visualization_msgs, which this package already depends on
  // regardless of which robot it runs on.
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
const int64_t SnapshotterTopicOptions::NO_MEMORY_LIMIT = -1;
const rclcpp::Duration SnapshotterTopicOptions::INHERIT_DURATION_LIMIT = rclcpp::Duration(0s);
const int64_t SnapshotterTopicOptions::INHERIT_MEMORY_LIMIT = 0;
// uint32_t is enough to hold 1e6 itself; the multiply it's used in
// (options_.default_memory_limit_ *= MB_TO_B, an int64_t) promotes this to
// int64_t, so it doesn't reintroduce the overflow int64_t was widened to fix.
static constexpr uint32_t MB_TO_B = 1e6;

SnapshotterTopicOptions::SnapshotterTopicOptions(
  rclcpp::Duration duration_limit,
  int64_t memory_limit)
: duration_limit_(duration_limit), memory_limit_(memory_limit)
{
}

SnapshotterOptions::SnapshotterOptions(
  rclcpp::Duration default_duration_limit,
  int64_t default_memory_limit)
: default_duration_limit_(default_duration_limit),
  default_memory_limit_(default_memory_limit),
  topics_()
{
}

bool SnapshotterOptions::addTopic(
  const TopicDetails & topic_details,
  rclcpp::Duration duration,
  int64_t memory)
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

MessageQueue::MessageQueue(
  const SnapshotterTopicOptions & options, const rclcpp::Logger & logger,
  SharedMemoryBudget * shared_budget)
: options_(options), logger_(logger), size_(0), shared_budget_(shared_budget)
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
  cloned->queue_ = this->queue_;
  cloned->size_ = this->size_;
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
    try {
      queue_.clear();
      size_ = 0;
    } catch (const std::exception& e) {
      RCLCPP_ERROR(logger_, "Exception during queue clear: %s", e.what());
      size_ = 0;
    }
  }
  else
  {
    RCLCPP_INFO(logger_, "Not clearing queue for topic %s because duration is set to %f", sub_->get_topic_name(), options_.duration_limit_.seconds());
  }
}

rclcpp::Duration MessageQueue::duration() const
{
  if (queue_.size() <= 1) {
    return rclcpp::Duration(0s);
  }
  return queue_.back().time - queue_.front().time;
}

MessageQueuePushResult MessageQueue::preparePush(int32_t size, rclcpp::Time const & time)
{
  // A message older than the current back means the clock jumped backwards; the buffer's
  // ordering assumption no longer holds, so start over.
  if (!queue_.empty() && time < queue_.back().time) {
    RCLCPP_WARN(logger_, "Time has gone backwards. Clearing buffer for this topic.");
    _clear();
  }

  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT &&
    size > options_.memory_limit_)
  {
    RCLCPP_WARN(logger_,
                "Message size (%d bytes) from topic %s exceeds memory limit (%ld bytes), dropping",
                size, sub_->get_topic_name(), static_cast<long>(options_.memory_limit_));
    return MessageQueuePushResult::DROPPED_TOO_LARGE;
  }

  // Evict oldest-first until the new message would fit under each enforced limit.
  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT) {
    while (queue_.size() != 0 && size_ + size > options_.memory_limit_) {
      _pop();
    }
  }

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

  // Checked last, once this queue has trimmed itself as far as its own
  // limits allow. If it still doesn't fit, the caller evicts elsewhere.
  if (shared_budget_ != nullptr && !shared_budget_->fits(size)) {
    return MessageQueuePushResult::BUDGET_FULL;
  }

  return MessageQueuePushResult::STORED;
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
MessageQueuePushResult MessageQueue::push(SnapshotMessage const& _out)
{
  std::lock_guard<std::mutex> l(lock);
  return _push(_out);
}

SnapshotMessage MessageQueue::pop()
{
  std::lock_guard<std::mutex> l(lock);
  return _pop();
}

int64_t MessageQueue::getMessageSize(SnapshotMessage const & snapshot_msg) const
{
  // Message payload plus an estimate of its SnapshotMessage/deque-node/shared_ptr overhead,
  // so size_ and the memory limits it's checked against are in the same units.
  size_t message_size = snapshot_msg.msg->size();
  size_t metadata_size = sizeof(SnapshotMessage);
  size_t deque_overhead = sizeof(std::deque<SnapshotMessage>::value_type) + 32;
  size_t shared_ptr_overhead = sizeof(std::shared_ptr<const rclcpp::SerializedMessage>) +
                               sizeof(rclcpp::SerializedMessage);

  return message_size + metadata_size + deque_overhead + shared_ptr_overhead;
}

MessageQueuePushResult MessageQueue::_push(SnapshotMessage const & _out)
{
  int32_t size = _out.msg->size();
  MessageQueuePushResult result = preparePush(size, _out.time);
  if (result != MessageQueuePushResult::STORED) {
    return result;
  }
  queue_.push_back(_out);
  const int64_t added = getMessageSize(_out);
  size_ += added;
  if (shared_budget_ != nullptr) {
    shared_budget_->add(added);
  }
  return MessageQueuePushResult::STORED;
}

SnapshotMessage MessageQueue::_pop()
{
  SnapshotMessage tmp = queue_.front();
  queue_.pop_front();
  const int64_t freed = getMessageSize(tmp);
  size_ -= freed;
  if (shared_budget_ != nullptr) {
    shared_budget_->release(freed);
  }
  return tmp;
}

bool MessageQueue::popOldest()
{
  std::lock_guard<std::mutex> l(lock);
  if (queue_.empty()) {
    return false;
  }
  _pop();
  return true;
}

MessageQueue::range_t MessageQueue::rangeFromTimes(Time const & start, Time const & stop, int old_messages_to_keep)
{
  range_t::first_type begin = queue_.begin();
  range_t::second_type end = queue_.end();

  if(options_.duration_limit_ != options_.NO_DURATION_LIMIT)
  {
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

    // old_messages_to_keep extends the window backwards by that many messages.
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

  Time start = msg_timestamp - rclcpp::Duration::from_seconds(tolerance);
  Time stop = msg_timestamp + rclcpp::Duration::from_seconds(tolerance);

  if(options_.duration_limit_ != options_.NO_DURATION_LIMIT)
  {
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

// Poll period for createBag()'s forward-capture wait loop: balances cancel
// responsiveness/deadline precision against feedback-publish volume for a
// wait that can run up to max_post_duration_s.
static constexpr std::chrono::milliseconds kForwardPollPeriod{500};

Snapshotter::Snapshotter(const rclcpp::NodeOptions & options)
: rclcpp::Node("snapshotter", options),
  recording_(true),
  topic_resolver_(this)
{
  // Created first (before subscribeProfileTopics(), which can itself
  // trigger a publishState() call via subscribeResolvedTopic()) so state_pub_
  // is never null when publishState() runs. Plain volatile QoS, not
  // transient_local: this node is always run with intra-process communication
  // enabled (see main.cpp), which only supports volatile durability -- a
  // transient_local publisher fails to even construct in that mode. A late
  // subscriber gets the current state on the next change instead of
  // immediately.
  state_pub_ = create_publisher<rosbag2_snapshot_msgs::msg::SnapshotState>(
    "snapshot_state", rclcpp::QoS(1));
  capture_event_pub_ = create_publisher<rosbag2_snapshot_msgs::msg::SnapshotCaptureEvent>(
    "snapshot_capture_event", rclcpp::QoS(10));

  parseOptionsFromParams();
  total_memory_budget_.setLimit(options_.total_memory_limit_);

  for (auto & pair : options_.topics_) {
    string topic{pair.first.name}, type{pair.first.type};
    fixTopicOptions(pair.second);
    shared_ptr<MessageQueue> queue;
    queue.reset(new MessageQueue(pair.second, get_logger(), &total_memory_budget_));

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
    {
      std::unique_lock<std::shared_mutex> write_lock(buffers_lock_);
      std::pair<buffers_t::iterator, bool> res =
        buffers_.emplace(details, queue);
      assert(res.second);
    }

    subscribe(details, queue);
  }

  // Union of every capture profile's topics: subscribe whatever resolves now,
  // queue the rest for the retry timer below. No-op if capture_profiles_dir_
  // was never set (profiles_ is empty).
  subscribeProfileTopics();

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

  publishState();
}

Snapshotter::~Snapshotter()
{
  // Explicitly wait for every in-flight capture before touching buffers_ or
  // any MessageQueue below. capture_futures_ being the last-declared member
  // of Snapshotter (each std::future in it, from std::async(launch::async,
  // ...), blocks in its own destructor until that capture finishes) only
  // guarantees no capture thread is still running once this destructor BODY
  // returns -- member destruction happens after the body, in reverse
  // declaration order -- it says nothing about the body itself, which is
  // exactly where the loop below runs. Without this explicit wait, a
  // forward capture still mid-wait on its own thread could be iterating
  // buffers_, or reading a queue's sub_, at the same time this loop resets
  // it.
  for (auto & f : capture_futures_) {
    if (f.valid()) {
      f.wait();
    }
  }
  std::shared_lock<std::shared_mutex> read_lock(buffers_lock_);
  for (auto & buffer : buffers_) {
    buffer.second->sub_.reset();
  }
}

ImageCompressionOptions Snapshotter::getCompressionOptions(std::string topic)
{
  std::string prefix = "topic_details." + topic;
  ImageCompressionOptions img_compression_opts;

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
    else
    {
      RCLCPP_ERROR(get_logger(), "An invalid compression format was passed for topic %s: %s. Compression will be disabled for this topic", topic.c_str(), img_compression_opts.format.c_str());
      img_compression_opts.use_compression = false;
    }
  }

#ifdef ROSBAG2_SNAPSHOT_HAVE_H264
  // Created unconditionally so it's ready if h264 encoding is later selected for this topic.
  img_compression_opts.encoder = std::make_shared<FFMPEGEncoder>();
  img_compression_opts.encoder->setParameters(this, "h264.");
#endif

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
      declare_parameter<double>("default_memory_limit", 300.0);
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "default_memory_limit is of incorrect type.");
    throw ex;
  }

  try {
    options_.max_post_duration_s_ =
      declare_parameter<double>("max_post_duration_s", 300.0);
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "max_post_duration_s is of incorrect type.");
    throw ex;
  }

  if (options_.default_memory_limit_ != -1.0) {
    options_.default_memory_limit_ *= MB_TO_B;
  }

  try {
    // 0 = no shared cap across topics.
    options_.total_memory_limit_ =
      static_cast<int64_t>(declare_parameter<double>("total_memory_limit", 0.0));
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "total_memory_limit is of incorrect type.");
    throw ex;
  }
  if (options_.total_memory_limit_ > 0) {
    options_.total_memory_limit_ *= MB_TO_B;
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
  if (ind != string::npos && ind == file.size() - 4) {
    return true;
  }
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
  {
    std::shared_lock<std::shared_mutex> read_lock(state_lock_);
    if (!recording_) {
      return;
    }
  }
  SnapshotMessage out(msg, this->now());
  if (queue->push(out) == MessageQueuePushResult::BUDGET_FULL) {
    evictFromLargestBuffer(static_cast<int64_t>(msg->size()));
    queue->push(out);
  }
}

void Snapshotter::evictFromLargestBuffer(int64_t bytes)
{
  std::vector<std::shared_ptr<MessageQueue>> queues;
  {
    std::shared_lock<std::shared_mutex> read_lock(buffers_lock_);
    queues.reserve(buffers_.size());
    for (const auto & entry : buffers_) {
      queues.push_back(entry.second);
    }
  }
  while (!total_memory_budget_.fits(bytes)) {
    std::shared_ptr<MessageQueue> largest;
    int64_t largest_bytes = 0;
    for (const auto & candidate : queues) {
      const int64_t used = candidate->usedBytes();
      if (used > largest_bytes) {
        largest_bytes = used;
        largest = candidate;
      }
    }
    if (largest == nullptr || !largest->popOldest()) {
      return;
    }
  }
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
  if (!req->use_interval_mode) {
    // A forward capture normally writes everything buffered up to now,
    // including whatever arrived during the wait (req->stop_time left at
    // 0). A topic with include_post_trigger=false only gets its pre-trigger
    // buffer: trim the upper bound at request_time (the moment the capture
    // began) instead.
    rclcpp::Time stop_time(req->stop_time);
    if (isForwardCaptureRequest(req->post_duration_s) && !topic_details.include_post_trigger) {
      stop_time = request_time;
    }
    range = message_queue.rangeFromTimes(req->start_time, stop_time, topic_details.old_messages_to_keep);
  } else {
    range = message_queue.intervalFromTimesMsg(req->msg_timestamp, req->interval_mode_tolerance);
  }

  rosbag2_storage::TopicMetadata tm;
  tm.name = topic_details.name;
  tm.type = topic_details.type;
  tm.serialization_format = "cdr";
  // Replay needs the QoS the messages were actually recorded under.
  tm.offered_qos_profiles = encodeQos(topic_details.qos);

  rclcpp::Serialization<sensor_msgs::msg::Image> img_serializer;
  cv_bridge::CvImagePtr cv_bridge_img;
  std::vector<int> compression_params; 
  if(topic_details.img_compression_opts_.use_compression)
  {
#ifdef ROSBAG2_SNAPSHOT_HAVE_H264
    if (req->use_h264)
    {
      RCLCPP_INFO(get_logger(), "H264 enabled for topic %s. applying h264 compression", topic_details.name.c_str());
      tm.type = "foxglove_msgs/msg/CompressedVideo";
    }
    else
#else
    if (req->use_h264)
    {
      RCLCPP_ERROR(
        get_logger(),
        "H264 requested for topic %s but this build has no foxglove_msgs/FFmpeg support; "
        "falling back to %s compression",
        topic_details.name.c_str(), topic_details.img_compression_opts_.format.c_str());
    }
#endif
    {
      RCLCPP_INFO(get_logger(), "topic %s is an image. applying %s compression", topic_details.name.c_str(), topic_details.img_compression_opts_.format.c_str() );
      compression_params.push_back(topic_details.img_compression_opts_.imwrite_flag);
      compression_params.push_back(topic_details.img_compression_opts_.imwrite_flag_value); // Set JPEG quality (0-100) or png compression (0-9)
      tm.type = "sensor_msgs/msg/CompressedImage";
    }
    img_serializer = rclcpp::Serialization<sensor_msgs::msg::Image>();
  }

  // The two-arg form embeds the schema, so a bare .mcap is readable on its
  // own; fall back to undeclared-schema rather than fail the whole capture.
  try {
    bag_writer.create_topic(tm, definitions_.get_full_text(tm.type));
  } catch (const std::exception & e) {
    RCLCPP_WARN(
      get_logger(), "no message definition for %s (%s): %s -- the bag will carry no schema for it",
      tm.name.c_str(), tm.type.c_str(), e.what());
    bag_writer.create_topic(tm);
  }

  double prev_msg_time = 0.0;
  auto start = std::chrono::high_resolution_clock::now();
#ifdef ROSBAG2_SNAPSHOT_HAVE_H264
  bool h264_throttle_skip = req->use_h264 && topic_details.h264_throttle_skip;
#else
  // H264 never actually happens without support for it (see the fallback
  // above), so this is never true regardless of what the request asked for.
  bool h264_throttle_skip = false;
#endif
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
  // Loop-invariant: req->start_time/stop_time don't change per message, and
  // shouldOverrideOldTimestamp() only depends on whether each was actually
  // set, not on the current message.
  const bool start_time_specified = builtin_time_nonzero(req->start_time);
  const bool stop_time_specified = builtin_time_nonzero(req->stop_time);
  const rclcpp::Duration bag_duration = rclcpp::Time(req->stop_time) - rclcpp::Time(req->start_time);
  bool logged_timestamp_override = false;
  for (auto msg_it = range.first; msg_it != range.second; ++msg_it) {
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
    if (shouldOverrideOldTimestamp(
        topic_details.override_old_timestamps, topic_details.old_messages_to_keep,
        start_time_specified, stop_time_specified,
        (request_time - msg_it->time).nanoseconds(), bag_duration.nanoseconds()))
    {
      if (!logged_timestamp_override) {
        RCLCPP_WARN(get_logger(), "Overriding old timestamps for topic %s", tm.name.c_str());
        logged_timestamp_override = true;
      }
      bag_message->time_stamp = rclcpp::Time(req->start_time).nanoseconds();
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
      // imencode expects bgr ordering; rgb8 images need converting by hand first.
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

#ifdef ROSBAG2_SNAPSHOT_HAVE_H264
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
#endif
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
  // filename becomes the literal on-disk final path (see finalizeCapture()),
  // so it must end in .bag (the usual rosbag2 directory-mode naming
  // convention) or .mcap (a use_flat_output=true caller's real destination).
  if (!hasAcceptedGoalFilename(goal->filename)) {
    RCLCPP_WARN(
      this->get_logger(),
      "Rejecting request to snapshot. Empty filename or not ending in .bag/.mcap");
    return rclcpp_action::GoalResponse::REJECT;
  }

  if (!goal->profile.empty() && profiles_.find(goal->profile) == nullptr) {
    RCLCPP_WARN(
      this->get_logger(), "Rejecting request to snapshot. Unknown capture profile '%s'.",
      goal->profile.c_str());
    return rclcpp_action::GoalResponse::REJECT;
  }

  if (isForwardCaptureRequest(goal->post_duration_s) &&
    !forwardCaptureWithinLimit(goal->post_duration_s, options_.max_post_duration_s_))
  {
    RCLCPP_WARN(
      this->get_logger(),
      "Rejecting request to snapshot: post_duration_s=%.1f exceeds this node's "
      "max_post_duration_s (%.1f), or forward captures are disabled.",
      goal->post_duration_s, options_.max_post_duration_s_);
    return rclcpp_action::GoalResponse::REJECT;
  }

  // Reject only a second goal for the exact same filename -- two captures
  // opening the same staging path concurrently would corrupt each other's
  // output. This is deliberately narrow: it does not limit concurrency
  // across distinct filenames at all, since concurrent captures for
  // different events are a real, relied-upon usage pattern (a client may
  // track multiple simultaneous goals itself).
  {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    if (active_filenames_.count(goal->filename)) {
      RCLCPP_WARN(
        this->get_logger(),
        "Rejecting request to snapshot: '%s' is already being written by "
        "another in-flight capture.", goal->filename.c_str());
      return rclcpp_action::GoalResponse::REJECT;
    }
    active_filenames_.insert(goal->filename);
    ++active_capture_count_;
  }
  // Called after the lock above is released -- state_lock_ is a
  // std::shared_mutex, not reentrant; publishState() takes its own lock.
  publishState();

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

  // Reap any capture futures that have already finished. Just bookkeeping
  // so this vector doesn't grow unbounded -- the corresponding captures have
  // already run to completion; this call never blocks.
  capture_futures_.erase(
    std::remove_if(
      capture_futures_.begin(), capture_futures_.end(),
      [](std::future<void> & f) {
        return f.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
      }),
    capture_futures_.end());

  std::filesystem::path final_path(req->filename);
  std::filesystem::path staging_path = stagingPathFor(final_path);

  if (std::filesystem::exists(staging_path)) {
    // There's no "storage root" concept in this package, so there's no
    // well-defined, bounded set of directories to sweep for orphaned
    // staging files at startup. This lazily reclaims one left behind by a
    // previous crash/kill -9 the moment that exact final_path is requested
    // again.
    RCLCPP_WARN(
      get_logger(),
      "Staging file %s already exists (likely left behind by a previous "
      "crash or incomplete capture); it will be overwritten.",
      staging_path.string().c_str());
  }

  std::shared_ptr<rosbag2_cpp::Writer> bag_writer_ptr;
  bag_writer_ptr = std::make_shared<rosbag2_cpp::Writer>();

  RCLCPP_INFO(
    get_logger(), "opening %s (staging at %s)",
    req->filename.c_str(), staging_path.string().c_str());

  try {
    rosbag2_storage::StorageOptions storage_opts;
    storage_opts.storage_id = "mcap";
    storage_opts.uri = staging_path.string();
    storage_opts.storage_preset_profile = req->rosbag_preset_profile != "" ? req->rosbag_preset_profile : options_.rosbag_preset_profile_;
    rosbag2_cpp::ConverterOptions converter_opts{};
    bag_writer_ptr->open(storage_opts, converter_opts);
  } catch (const std::exception & ex) {
    RCLCPP_WARN(
          get_logger(), "Failed to open %s file, reason: %s", staging_path.string().c_str(), ex.what());
    res->success = false;
    res->message = "Unable to open file for writing, " + std::string(ex.what());
    {
      std::unique_lock<std::shared_mutex> write_lock(state_lock_);
      --active_capture_count_;
      active_filenames_.erase(req->filename);
      has_last_capture_ = true;
      last_capture_success_ = false;
      last_capture_message_ = res->message;
      last_capture_stamp_ = this->now();
    }
    publishState();
    return goal_handle->abort(res);
  }

  std::vector<std::pair<TopicDetails, std::shared_ptr<MessageQueue>>> cloned_buffers;
  if (!isForwardCaptureRequest(req->post_duration_s)) {
    std::shared_lock<std::shared_mutex> read_lock(buffers_lock_);
    for (const auto& buffer : buffers_) {
      cloned_buffers.emplace_back(buffer.first, buffer.second->clone());
    }
  }
  // else: left empty here -- createBag() takes the (deferred) clone itself,
  // once the forward window elapses or the goal is canceled, so it captures
  // everything buffered up to that later point instead of this one.

  PendingCapture capture;
  capture.goal_handle = goal_handle;
  capture.cloned_buffers = std::move(cloned_buffers);
  capture.bag_writer_ptr = bag_writer_ptr;
  capture.staging_path = staging_path;
  capture.final_path = final_path;
  capture.profile = req->profile;
  capture.flat_output = req->use_flat_output;

  // std::async(launch::async, ...) instead of a detached thread: the
  // returned future is kept in capture_futures_ so ~Snapshotter() can wait
  // for it (see the comment on capture_futures_ in snapshotter.hpp).
  capture_futures_.push_back(
    std::async(std::launch::async, &Snapshotter::createBag, this, std::move(capture)));
}

void Snapshotter::createBag(PendingCapture capture)
{
  auto goal_handle = capture.goal_handle;
  auto & cloned_buffers = capture.cloned_buffers;
  auto & bag_writer_ptr = capture.bag_writer_ptr;

  auto result = std::make_shared<TriggerSnapAction::Result>();
  auto feedback = std::make_shared<TriggerSnapAction::Feedback>();
  auto req = goal_handle->get_goal();

  rclcpp::Time request_time = this->now();
  bool success = true;
  std::string message = req->filename;
  float count_topics = 0.0;

  if (isForwardCaptureRequest(req->post_duration_s)) {
    // request_time is kept anchored at goal-acceptance (not recomputed
    // after the wait below): it's also the reference instant for
    // writeTopic()'s old-timestamp-override math and for
    // refreshBuffer(request_time) further down, both of which mean "when
    // was this capture requested," same as in the immediate-capture case.
    const rclcpp::Time deadline =
      request_time + rclcpp::Duration::from_seconds(req->post_duration_s);
    while (this->now() < deadline) {
      if (goal_handle->is_canceling()) {
        finalizeCapture(
          capture, false, "Canceled while waiting for forward capture window",
          result, 0, request_time);
        goal_handle->canceled(result);
        return;
      }
      feedback->duration = (this->now() - request_time).seconds();
      feedback->progress = 100.0f * std::min(
        1.0f, static_cast<float>(feedback->duration / req->post_duration_s));
      feedback->message = "Waiting for forward capture window to elapse...";
      goal_handle->publish_feedback(feedback);
      std::this_thread::sleep_for(kForwardPollPeriod);
    }

    // Deferred clone: every topic has kept being buffered by topicCb() the
    // whole time (recording_ permitting), exactly as when idle -- this is
    // the same clone handle_accepted takes for an immediate capture, just
    // taken later so it includes what arrived during the wait. Callers
    // using this mode leave req->stop_time at its default (0), and
    // rangeFromTimes() already treats stop_time==0 as "no upper trim"
    // (unchanged), so writeTopic()'s unmodified call below naturally
    // picks up everything through this point.
    std::shared_lock<std::shared_mutex> read_lock(buffers_lock_);
    for (const auto & buffer : buffers_) {
      cloned_buffers.emplace_back(buffer.first, buffer.second->clone());
    }
  }

  // A named profile picks the topic list and each topic's max_rate_hz (as
  // throttle_period, via the same override mechanism topics[].throttle_period
  // uses below). An empty profile falls back to req->topics as-is.
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
        msg.include_post_trigger = spec.include_post_trigger ? 1 : 0;
        profile_topics.push_back(msg);
      }
    }
  }
  const std::vector<DetailsMsg> & topics_to_write = use_profile ? profile_topics : req->topics;

  if (topics_to_write.size() && topics_to_write.at(0).name.size()) {
    if (req->use_interval_mode) RCLCPP_WARN(get_logger(), "[INTERVAL_MODE]: enabled for snapshotting");
    for (auto & topic : topics_to_write) {
      if (goal_handle->is_canceling()) {
        finalizeCapture(
          capture, false, "Rosbag creation canceled", result,
          static_cast<size_t>(count_topics), request_time);
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
  } else {  // Empty topic list: record every buffered topic.
    for (const auto & pair : cloned_buffers) {
      if (goal_handle->is_canceling()) {
        finalizeCapture(
          capture, false, "Rosbag creation canceled", result,
          static_cast<size_t>(count_topics), request_time);
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
  
  finalizeCapture(capture, success, message, result, static_cast<size_t>(count_topics), request_time);
  // Action status is always "succeeded" here; a failed capture is reported
  // through result->success/message instead, not through the action outcome.
  goal_handle->succeed(result);
}

void Snapshotter::finalizeCapture(
  PendingCapture & capture, bool success, std::string message,
  const std::shared_ptr<TriggerSnapAction::Result> & result,
  size_t topics_written, const rclcpp::Time & request_time)
{
  // Always attempt to close, even on a failed/canceled capture, so the
  // staging file left on disk is a well-formed, readable bag rather than
  // whatever state the Writer's destructor happens to leave it in.
  bool file_is_valid = true;
  try {
    capture.bag_writer_ptr->close();
    RCLCPP_DEBUG(get_logger(), "Bag writer closed successfully");
  } catch (const std::exception & ex) {
    file_is_valid = false;
    success = false;
    message = "Failed to close bag file: " + std::string(ex.what());
    RCLCPP_WARN(get_logger(), "%s", message.c_str());
  }

  // Where the file actually ends up. Recorded data is never deleted just
  // because a capture didn't fully complete -- a robot shutting down
  // mid-recording is exactly this case -- so a canceled capture or a topic
  // that failed to write is still saved, at a clearly distinct path from a
  // full success (see partialPathFor()'s own comment for why). Only an
  // actual close() failure leaves the file at its staging path, since its
  // integrity can't be vouched for.
  std::filesystem::path saved_path = capture.staging_path;
  if (file_is_valid && !capture.flat_output) {
    // Default: rosbag2's usual bag-directory layout, for a caller that
    // doesn't ask for flat_output and instead reads a capture back at the
    // fixed <filename>/<basename>_0.mcap path rosbag2 always writes to.
    saved_path = success ? capture.final_path : partialPathFor(capture.final_path);
    std::error_code ec;
    std::filesystem::rename(capture.staging_path, saved_path, ec);
    if (ec) {
      success = false;
      saved_path = capture.staging_path;
      message = "Failed to move staged bag " + capture.staging_path.string() +
        " to " + saved_path.string() + ": " + ec.message();
      RCLCPP_ERROR(get_logger(), "%s", message.c_str());
    } else {
      renameBagFileToMatchDirectory(saved_path);
      if (success) {
        message = saved_path.string();
      } else {
        RCLCPP_WARN(
          get_logger(), "Capture ended early (%s); partial bag saved to %s",
          message.c_str(), saved_path.string().c_str());
      }
    }
  } else if (file_is_valid) {
    const std::filesystem::path target =
      success ? capture.final_path : partialPathFor(capture.final_path);
    const std::filesystem::path mcap = findMcapFile(capture.staging_path);
    if (mcap.empty()) {
      success = false;
      message = "the writer produced no .mcap under " + capture.staging_path.string();
      RCLCPP_ERROR(get_logger(), "%s", message.c_str());
    } else {
      std::error_code ec;
      std::filesystem::rename(mcap, target, ec);
      if (ec) {
        success = false;
        message = "Failed to move staged bag " + mcap.string() +
          " to " + target.string() + ": " + ec.message();
        RCLCPP_ERROR(get_logger(), "%s", message.c_str());
      } else {
        saved_path = target;
        // The rest of the staging directory (metadata.yaml) is no longer
        // needed once the .mcap has been extracted from it.
        std::error_code cleanup_ec;
        std::filesystem::remove_all(capture.staging_path, cleanup_ec);
        if (success) {
          message = saved_path.string();
        } else {
          RCLCPP_WARN(
            get_logger(), "Capture ended early (%s); partial bag saved to %s",
            message.c_str(), saved_path.string().c_str());
        }
      }
    }
  } else {
    RCLCPP_WARN(
      get_logger(), "Bag writer failed to close cleanly; leaving file at staging path %s",
      capture.staging_path.string().c_str());
  }

  result->success = success;
  result->message = message;

  rclcpp::Time stamp = this->now();
  {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    --active_capture_count_;
    active_filenames_.erase(capture.final_path.string());
    has_last_capture_ = true;
    last_capture_success_ = success;
    last_capture_message_ = message;
    last_capture_stamp_ = stamp;
  }

  auto event = std::make_shared<rosbag2_snapshot_msgs::msg::SnapshotCaptureEvent>();
  event->filename = saved_path.string();
  event->profile = capture.profile;
  event->success = success;
  event->message = message;
  event->topics_written = static_cast<uint32_t>(topics_written);
  event->duration = static_cast<float>((stamp - request_time).seconds());
  event->stamp = stamp;
  capture_event_pub_->publish(*event);

  publishState();
}

void Snapshotter::publishState()
{
  auto msg = std::make_shared<rosbag2_snapshot_msgs::msg::SnapshotState>();
  {
    std::shared_lock<std::shared_mutex> read_lock(state_lock_);
    msg->recording = recording_;
    msg->active_capture_count = active_capture_count_;
    msg->has_last_capture = has_last_capture_;
    msg->last_capture_success = last_capture_success_;
    msg->last_capture_message = last_capture_message_;
    msg->last_capture_stamp = last_capture_stamp_;
  }
  {
    std::shared_lock<std::shared_mutex> read_lock(buffers_lock_);
    msg->buffered_topic_count = static_cast<uint32_t>(buffers_.size());
    msg->buffered_topics.reserve(buffers_.size());
    double window_s = 0.0;
    for (const auto & buffer : buffers_) {
      msg->buffered_topics.push_back(buffer.first.name);
      window_s = std::max(window_s, buffer.second->duration().seconds());
    }
    msg->buffered_window_s = static_cast<float>(window_s);
  }
  state_pub_->publish(*msg);
}

void Snapshotter::overrideTopicDetails(const DetailsMsg& req_msg, TopicDetails& details)
{
  if (req_msg.throttle_period != -1.0) details.throttle_period = req_msg.throttle_period;
  if (req_msg.h264_throttle_skip != -1) details.h264_throttle_skip = req_msg.h264_throttle_skip;
  if (req_msg.override_old_timestamps != -1) details.override_old_timestamps = req_msg.override_old_timestamps;
  if (req_msg.queue_depth != -1) details.queue_depth = req_msg.queue_depth;
  if (req_msg.old_messages_to_keep != -1) details.old_messages_to_keep = req_msg.old_messages_to_keep;
  if (req_msg.include_post_trigger != -1) details.include_post_trigger = req_msg.include_post_trigger;

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
  std::shared_lock<std::shared_mutex> read_lock(buffers_lock_);
  for (const buffers_t::value_type & pair : buffers_) {
    // Only clear a topic once its buffered duration exceeds its own
    // default_bag_duration, rather than on every resume.
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
    // Cannot enable while any capture is in flight (active_capture_count_
    // covers the whole reserved-through-finalized window, see its
    // doc-comment in snapshotter.hpp).
    if (req->data && active_capture_count_ > 0) {
      res->success = false;
      res->message = "cannot enable recording while writing.";
      return;
    }
  }

  bool changed = false;
  if (req->data && !recording_) {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    resume();
    changed = true;
  } else if (!req->data && recording_) {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    pause();
    changed = true;
  }
  if (changed) {
    publishState();
  }

  res->success = true;
}

bool Snapshotter::isBuffered(const std::string & name) const
{
  std::shared_lock<std::shared_mutex> read_lock(buffers_lock_);
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
      auto queue = std::make_shared<MessageQueue>(topic_options, get_logger(), &total_memory_budget_);

      {
        std::unique_lock<std::shared_mutex> write_lock(buffers_lock_);
        std::pair<buffers_t::iterator,
          bool> res = buffers_.emplace(details, queue);
        assert(res.second);
      }
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
  auto queue = std::make_shared<MessageQueue>(topic_options, get_logger(), &total_memory_budget_);
  {
    std::unique_lock<std::shared_mutex> write_lock(buffers_lock_);
    buffers_.emplace(details, queue);
  }
  subscribe(details, queue);
  RCLCPP_INFO(get_logger(), "Buffering profile topic %s (%s)", name.c_str(), type.c_str());
  if (state_pub_) {
    // Defensive: publishers are created before subscribeProfileTopics() runs
    // in the constructor, but guard anyway since this method can also be
    // reached from that same constructor call chain.
    publishState();
  }
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
