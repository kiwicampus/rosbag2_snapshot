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

#ifndef ROSBAG2_SNAPSHOT__SNAPSHOTTER_HPP_
#define ROSBAG2_SNAPSHOT__SNAPSHOTTER_HPP_

#include <rclcpp/rclcpp.hpp>
#include <rclcpp_action/rclcpp_action.hpp>
#include <rclcpp/time.hpp>
#include <rosbag2_snapshot_msgs/msg/topic_details.hpp>
#include <rosbag2_snapshot_msgs/msg/snapshot_state.hpp>
#include <rosbag2_snapshot_msgs/msg/snapshot_capture_event.hpp>
#include <rosbag2_snapshot_msgs/action/trigger_snapshot.hpp>
#include <std_srvs/srv/set_bool.hpp>
#include <rosbag2_cpp/message_definitions/local_message_definition_source.hpp>
#include <rosbag2_cpp/writer.hpp>
#include <rosbag2_compression/sequential_compression_writer.hpp>
#include <sensor_msgs/msg/image.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/camera_info.hpp>
#include <visualization_msgs/msg/image_marker.hpp>
#include <cv_bridge/cv_bridge.hpp>
#include <opencv2/opencv.hpp>
#include <opencv2/imgcodecs.hpp>
#include <chrono>
#include <deque>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include <thread>

#ifdef ROSBAG2_SNAPSHOT_HAVE_H264
#include "rosbag2_snapshot/ffmpeg_encoding/ffmpeg_encoder.hpp"
#endif
#include "rosbag2_snapshot/accepted_filename.hpp"
#include "rosbag2_snapshot/capture_profiles.hpp"
#include "rosbag2_snapshot/forward_capture.hpp"
#include "rosbag2_snapshot/shared_memory_budget.hpp"
#include "rosbag2_snapshot/staging_path.hpp"
#include "rosbag2_snapshot/topic_resolver.hpp"

namespace rosbag2_snapshot
{
using namespace std::chrono_literals;  // NOLINT
using DetailsMsg = rosbag2_snapshot_msgs::msg::TopicDetails;
using TriggerSnapAction = rosbag2_snapshot_msgs::action::TriggerSnapshot;
#ifdef ROSBAG2_SNAPSHOT_HAVE_H264
using namespace ffmpeg_image_transport;
#endif

// Compression settings for a single image topic.
struct ImageCompressionOptions
{
  bool use_compression = false;
  std::string format;  // "jpg" or "png"
  cv::ImwriteFlags imwrite_flag;  // opencv imencode() flag
  int imwrite_flag_value;  // jpg quality (0-100) or png compression level (0-9)
#ifdef ROSBAG2_SNAPSHOT_HAVE_H264
  std::shared_ptr<FFMPEGEncoder> encoder;  // video compression, if used
#endif
};

struct TopicDetails
{
  std::string name;
  std::string type;
  rclcpp::QoS qos = rclcpp::QoS(5);
  bool override_old_timestamps = false;
  int queue_depth = -1;
  int old_messages_to_keep = -1;
  rclcpp::Duration default_bag_duration = rclcpp::Duration(0, 0);
  ImageCompressionOptions img_compression_opts_;
  double throttle_period = -1.0;  // min seconds between saved messages
  // If true (and H264 enabled), throttle_period is ignored and every message is saved.
  bool h264_throttle_skip = false;
  // In a forward capture, whether arrivals after the trigger are included.
  // Named apart from "forward" (the capture-level post_duration_s mode)
  // since this is a per-topic participation switch, not a mode switch.
  bool include_post_trigger = true;

  TopicDetails() {}

  TopicDetails(std::string name, std::string type)
  : name(name), type(type) {}

  bool operator==(const TopicDetails & t) const
  {
    return name == t.name && type == t.type;
  }

  bool operator<(const TopicDetails & t) const
  {
    return t.name < name || (t.name == name && t.type < type);
  }

  bool operator>(const TopicDetails & t) const
  {
    return t.name > name || (t.name == name && t.type > type);
  }

  DetailsMsg asMessage() const
  {
    DetailsMsg msg{};
    msg.name = name;
    msg.type = type;
    return msg;
  }
};

// Falls back to DEFAULT QoS(5) on an unrecognized string (logging an error) rather than
// throwing: this is reached from a node constructor and from timer callbacks (capture
// profile QoS strings), where an uncaught exception would crash the whole process instead
// of just misconfiguring one topic.
const rclcpp::QoS qos_string_to_qos(std::string str)
{
    if (str == "DEFAULT") return rclcpp::QoS(5);
    if (str == "SENSOR_DATA") return rclcpp::QoS(5).best_effort();
    if (str == "TRANSIENT_LOCAL") return rclcpp::QoS(5).durability(rclcpp::DurabilityPolicy::TransientLocal);
    RCLCPP_ERROR(
      rclcpp::get_logger("rosbag2_snapshot"),
      "Unknown QoS string '%s', falling back to DEFAULT QoS(5)", str.c_str());
    return rclcpp::QoS(5);
}

class Snapshotter;

// Per-topic buffer limits: how much time (newest vs. oldest message) and
// memory a single topic's queue may hold before older messages are dropped.
struct SnapshotterTopicOptions
{
  // duration_limit_ value meaning "never truncate by age".
  static const rclcpp::Duration NO_DURATION_LIMIT;
  // memory_limit_ value meaning "never truncate by size" (dangerous: unbounded growth).
  static const int64_t NO_MEMORY_LIMIT;
  // duration_limit_ value meaning "use the node's configured default".
  static const rclcpp::Duration INHERIT_DURATION_LIMIT;
  // memory_limit_ value meaning "use the node's configured default".
  static const int64_t INHERIT_MEMORY_LIMIT;

  rclcpp::Duration duration_limit_;
  // int64_t (not int32_t): this is a byte count, and default_memory_limit is
  // configured in MB then multiplied by 1e6 -- a config value of a couple
  // GB would overflow a 32-bit count.
  int64_t memory_limit_;

  SnapshotterTopicOptions(
    rclcpp::Duration duration_limit = INHERIT_DURATION_LIMIT,
    int64_t memory_limit = INHERIT_MEMORY_LIMIT);
};

// Node-wide configuration: default per-topic limits, plus the explicit
// topics_ map of overrides.
struct SnapshotterOptions
{
  // Default duration_limit_ for a topic that doesn't specify its own.
  rclcpp::Duration default_duration_limit_;
  // Default memory_limit_ (bytes) for a topic that doesn't specify its own.
  // int64_t: see the comment on SnapshotterTopicOptions::memory_limit_.
  int64_t default_memory_limit_;
  // Upper bound on a goal's post_duration_s (forward/live capture window).
  // <= 0 disables forward captures entirely, not "unlimited" -- see
  // forward_capture.hpp's forwardCaptureWithinLimit().
  double max_post_duration_s_ = 300.0;
  bool all_topics_;  // record every topic on the graph, not just topics_
  std::string rosbag_preset_profile_;  // rosbag2 storage compression preset
  // Message types narrowed to one message in interval mode (interval_single_msg_types
  // param). Deployment-configured rather than hardcoded, since this package targets
  // any robot. Only applies to types with a real header.stamp -- see HeaderStampReader.
  std::unordered_set<std::string> interval_single_msg_types_;
  // Directory of "<name>.yaml" capture profile files (see capture_profiles.hpp).
  // "" means none configured.
  std::string capture_profiles_dir_;
  // Combined byte cap across every topic's queue, on top of each topic's own
  // memory_limit_. <= 0 (default) means no shared cap. Set via the
  // total_memory_limit param (MB), converted to bytes like default_memory_limit_.
  int64_t total_memory_limit_{0};

  typedef std::map<TopicDetails, SnapshotterTopicOptions> topics_t;
  topics_t topics_;

  SnapshotterOptions(
    rclcpp::Duration default_duration_limit = rclcpp::Duration(30s),
    int64_t default_memory_limit = -1);

  // Adds a topic to the configuration; false if it was already present.
  bool addTopic(
    const TopicDetails & topic_details,
    rclcpp::Duration duration_limit = SnapshotterTopicOptions::INHERIT_DURATION_LIMIT,
    int64_t memory_limit = SnapshotterTopicOptions::INHERIT_MEMORY_LIMIT);
};

// A buffered message plus its arrival time, held until written to disk.
struct SnapshotMessage
{
  SnapshotMessage(
    std::shared_ptr<const rclcpp::SerializedMessage> _msg,
    rclcpp::Time _time);
  std::shared_ptr<const rclcpp::SerializedMessage> msg;
  rclcpp::Time time;  // receipt time, not the message's own header stamp
};

// Outcome of trying to add a message to a MessageQueue.
enum class MessageQueuePushResult
{
  // Stored (any eviction needed to make room already happened).
  STORED,
  // The message alone is bigger than the topic's own memory_limit_; refused
  // rather than emptying the whole queue for it. Unchanged from the
  // pre-existing "cannot be added" behavior.
  DROPPED_TOO_LARGE,
  // This queue's own limits are satisfied, but the shared budget still
  // doesn't fit even after trimming. Caller should evict from the largest
  // queue and retry -- see evictFromLargestBuffer(). Never returned when
  // shared_budget_ is null.
  BUDGET_FULL,
};

// A single topic's buffered messages, truncated on push() as needed to
// respect its duration and memory limits.
class MessageQueue
{
  friend Snapshotter;

private:
  rclcpp::Logger logger_;
  mutable std::mutex lock;  // guards size_ and queue_
  SnapshotterTopicOptions options_;
  int64_t size_;  // current total size of queue_, in bytes
  typedef std::deque<SnapshotMessage> queue_t;
  queue_t queue_;
  std::shared_ptr<rclcpp::GenericSubscription> sub_;
  // Not owned; null for a clone() (never pushed to).
  SharedMemoryBudget * shared_budget_{nullptr};

public:
  explicit MessageQueue(
    const SnapshotterTopicOptions & options, const rclcpp::Logger & logger,
    SharedMemoryBudget * shared_budget = nullptr);
  MessageQueuePushResult push(const SnapshotMessage & msg);
  // Removes and returns the oldest message.
  SnapshotMessage pop();
  // Time difference between the newest and oldest buffered message, or 0 if size <= 1.
  rclcpp::Duration duration() const;
  void clear();
  // Keeps the subscription alive for as long as this queue exists.
  void setSubscriber(std::shared_ptr<rclcpp::GenericSubscription> sub);
  typedef std::pair<queue_t::const_iterator, queue_t::const_iterator> range_t;
  // [start, end] window into the buffer.
  range_t rangeFromTimes(const rclcpp::Time & start, const rclcpp::Time & end, int old_messages_to_keep = -1);
  // Window into the buffer around msg_timestamp +/- tolerance.
  range_t intervalFromTimesMsg(const rclcpp::Time & msg_timestamp, const double & tolerance);

  // Total message size, including metadata overhead.
  int64_t getMessageSize(SnapshotMessage const & msg) const;

  bool refreshBuffer(rclcpp::Time const& time);
  // Deep-copies the queue and its state (used to snapshot a buffer for writing
  // without blocking new messages from arriving on the live queue).
  std::shared_ptr<MessageQueue> clone();
  // Bytes currently held, for cross-queue eviction comparisons.
  int64_t usedBytes() const
  {
    std::lock_guard<std::mutex> l(lock);
    return size_;
  }
  // Drops the single oldest message. False if already empty.
  bool popOldest();

private:
  // Lock-free counterparts of push()/pop()/clear(), for callers already holding `lock`.
  MessageQueuePushResult _push(SnapshotMessage const & msg);
  SnapshotMessage _pop();
  void _clear();
  // Truncates the front of the queue to fit a new message of the given size/time.
  MessageQueuePushResult preparePush(int32_t size, rclcpp::Time const & time);
};

// Buffers the most recent messages from configured topics, enforcing each
// topic's memory/duration limits, and writes some or all of a buffer to a
// bag on a TriggerSnapshot action goal. See the package README.
class Snapshotter : public rclcpp::Node
{
public:
  explicit Snapshotter(const rclcpp::NodeOptions & options);
  ~Snapshotter();

private:
  static const int QUEUE_SIZE;  // subscription queue size for every topic
  SnapshotterOptions options_;
  typedef std::map<TopicDetails, std::shared_ptr<MessageQueue>> buffers_t;
  buffers_t buffers_;
  // Guards buffers_ itself (iteration, emplace, size()), separate from
  // state_lock_ so the two can't be confused into a lock-ordering mistake.
  // A capture's worker task (std::async, off the executor thread) can read
  // buffers_ while a forward capture waits out its post_duration_s, at the
  // same time poll_topic_timer_ inserts newly-discovered topics on the
  // executor thread. Each MessageQueue has its own separate lock once
  // reached through buffers_, so a critical section here must cover only the
  // direct map access, never a nested call into a method that also takes
  // this lock -- std::shared_mutex is not reentrant.
  mutable std::shared_mutex buffers_lock_;
  // Shared across every MessageQueue in buffers_.
  SharedMemoryBudget total_memory_budget_;
  // Locks recording_, active_capture_count_, active_filenames_ and
  // last_capture_* below.
  std::shared_mutex state_lock_;
  // True if new messages are being written to the internal buffer
  bool recording_;
  // Captures currently in flight, from goal acceptance (handle_goal) through
  // full finalization (finalizeCapture) -- covers bag-open, buffer clone,
  // write, close and rename, not just active writing. A count rather than a
  // single-slot flag: concurrent captures of different filenames are a real,
  // relied-upon usage pattern (a client may track several simultaneous
  // TriggerSnapshot goals itself).
  uint32_t active_capture_count_ = 0;
  // Filenames currently being written. The only concurrency-related
  // admission check this class makes: a second goal for a filename already
  // in this set is rejected in handle_goal, since two captures opening the
  // same staging path concurrently would corrupt each other's output. Goals
  // for distinct filenames are never limited by this.
  std::set<std::string> active_filenames_;
  // Outcome of the most recently *finished* capture (of any filename) --
  // a non-authoritative rollup for status reporting; the authoritative
  // per-capture record is the SnapshotCaptureEvent published by
  // finalizeCapture().
  bool has_last_capture_ = false;
  bool last_capture_success_ = false;
  std::string last_capture_message_;
  builtin_interfaces::msg::Time last_capture_stamp_;
  rclcpp_action::Server<TriggerSnapAction>::SharedPtr
    trigger_snapshot_action_server_;
  rclcpp::Service<std_srvs::srv::SetBool>::SharedPtr enable_server_;
  rclcpp::TimerBase::SharedPtr poll_topic_timer_;
  rclcpp::Publisher<rosbag2_snapshot_msgs::msg::SnapshotState>::SharedPtr state_pub_;
  rclcpp::Publisher<rosbag2_snapshot_msgs::msg::SnapshotCaptureEvent>::SharedPtr capture_event_pub_;

  // Capture profiles loaded from options_.capture_profiles_dir_ at startup.
  // Populated once in the constructor and read-only afterward, so reading it
  // from another thread (e.g. the detached createBag thread) needs no lock,
  // same as options_.interval_single_msg_types_ today.
  ProfileSet profiles_;
  TopicResolver topic_resolver_;
  // Embeds each topic's schema at create_topic() time, so a bare .mcap opens
  // in Foxglove without the rest of the bag directory.
  rosbag2_cpp::LocalMessageDefinitionSource definitions_;
  // Profile topics whose publisher hasn't appeared yet. Retried on
  // poll_topic_timer_ alongside pollTopics() (all_topics_ discovery).
  std::vector<std::string> pending_profile_topics_;

  void parseOptionsFromParams();
  // Replaces a topic's INHERIT_* limits with the node's configured defaults.
  void fixTopicOptions(SnapshotterTopicOptions & options);
  // In "prefix" mode (filename doesn't already end in .bag), appends the
  // current datetime and .bag.
  bool postfixFilename(std::string & file);
  // Current local datetime as e.g. "2018-05-22-14-28-51", for bag filenames.
  std::string timeAsStr();
  // Clears every topic's buffer, so resuming after a pause doesn't leave a
  // time gap spanned by stale messages.
  void clear();
  void subscribe(
    const TopicDetails & topic_details,
    std::shared_ptr<MessageQueue> queue);
  void topicCb(
    std::shared_ptr<const rclcpp::SerializedMessage> msg,
    std::shared_ptr<MessageQueue> queue);
  // Called from topicCb() when push() reports BUDGET_FULL: pops from
  // whichever buffer holds the most until `bytes` fits or nothing is left.
  void evictFromLargestBuffer(int64_t bytes);
  // TriggerSnapshot action server callbacks.
  rclcpp_action::GoalResponse handle_goal(
    const rclcpp_action::GoalUUID & uuid,
    std::shared_ptr<const TriggerSnapAction::Goal> goal);
  rclcpp_action::CancelResponse handle_cancel(
    const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle);
  void handle_accepted(const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle);
  // enable_snapshot service callback: pauses/resumes buffering.
  void enableCb(
    const std::shared_ptr<rmw_request_id_t> request_header,
    const std_srvs::srv::SetBool::Request::SharedPtr req,
    std_srvs::srv::SetBool_Response::SharedPtr res
  );
  // pause()/resume() toggle recording_. CALLER MUST HOLD state_lock_.
  void pause();
  void resume();
  // Polls the ROS graph for new topics (all_topics_ mode).
  void pollTopics();
  // True if a topic of this name is already in buffers_, regardless of its
  // resolved type/QoS. Used to keep all_topics_ discovery and capture-profile
  // discovery from double-subscribing the same topic.
  bool isBuffered(const std::string & name) const;
  // Timer callback: runs pollTopics() (if all_topics_) and
  // resolvePendingProfileTopics(), so a single poll_topic_timer_ covers both.
  void pollAndResolveTopics();
  // Union of every profile's topics, keyed by name (first profile to mention
  // a topic wins its type/qos/max_rate_hz if more than one does).
  std::map<std::string, ProfileTopicSpec> uniqueProfileTopics() const;
  // Resolves (if needed) and subscribes one profile topic. Returns true if it
  // is now buffered (including if it already was); false if still pending.
  bool resolveAndSubscribeProfileTopic(const ProfileTopicSpec & spec);
  // Startup pass over every profile topic; unresolved ones go into
  // pending_profile_topics_ for the retry timer.
  void subscribeProfileTopics();
  // Retries pending_profile_topics_ against the current ROS graph.
  void resolvePendingProfileTopics();
  // Creates the buffer and subscribes a topic whose type/QoS are already
  // known. No-op (returns true) if a topic of this name is already buffered.
  bool subscribeResolvedTopic(
    const std::string & name, const std::string & type, const rclcpp::QoS & qos);
  // Writes message_queue's messages within req's time window to bag_writer.
  // False (with res.message set) on a bag open/write error. force_throttle:
  // apply each topic's throttle_period regardless of req->throttle_msgs --
  // set when topic_details came from a named capture profile, whose
  // max_rate_hz always applies.
  bool writeTopic(
    rosbag2_cpp::Writer & bag_writer, MessageQueue & message_queue,
    const TopicDetails & topic_details,
    const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle,
    rclcpp::Time& request_time,
    bool force_throttle = false);

  // Applies the goal's own per-topic overrides on top of the buffered topic's details.
  void overrideTopicDetails(const DetailsMsg& topic, TopicDetails& details);

  ImageCompressionOptions getCompressionOptions(std::string topic);

  // Everything a capture's worker task needs, so it can be threaded through
  // std::async without an ever-growing argument list.
  struct PendingCapture
  {
    std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle;
    std::vector<std::pair<TopicDetails, std::shared_ptr<MessageQueue>>> cloned_buffers;
    std::shared_ptr<rosbag2_cpp::Writer> bag_writer_ptr;
    // Same directory as final_path (guarantees an atomic same-filesystem
    // rename), e.g. "<final_path>.tmp".
    std::filesystem::path staging_path;
    // req->filename, verbatim -- never opened for writing directly.
    std::filesystem::path final_path;
    // req->profile, copied for use off the executor thread (event message).
    std::string profile;
    // req->use_flat_output, copied for use off the executor thread.
    bool flat_output{false};
  };

  void createBag(PendingCapture capture);

  // Closes the bag writer (best-effort, even on failure/cancel so the
  // staging file is left well-formed), and -- only if still successful --
  // atomically renames staging_path to final_path. Updates
  // result->success/message, active_capture_count_/active_filenames_/
  // last_capture_* state, and publishes the capture-completed event plus a
  // refreshed state.
  void finalizeCapture(
    PendingCapture & capture, bool success, std::string message,
    const std::shared_ptr<TriggerSnapAction::Result> & result,
    size_t topics_written, const rclcpp::Time & request_time);

  // Publishes the current recording/active-capture-count/buffered-topic
  // state plus the most recent capture outcome. Safe to call from either
  // the executor thread or a capture's worker task.
  void publishState();

  // Not protected by state_lock_: touched only from the executor thread
  // (handle_accepted, and implicitly by this class's own destructor), never
  // from a capture's worker task. Must stay the LAST member declared: members
  // are destroyed in reverse declaration order, so this is the FIRST thing
  // torn down in ~Snapshotter(), before state_lock_/buffers_/the publishers
  // above. Each entry is a std::async(std::launch::async, ...) future, which
  // blocks in its destructor until that task finishes -- so destroying this
  // vector joins every outstanding capture before the rest of the node tears
  // down.
  std::vector<std::future<void>> capture_futures_;
};

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__SNAPSHOTTER_HPP_
