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

/* Configuration for a the compression settings of an image topic

 */
struct ImageCompressionOptions
{
  bool use_compression = false; // whether to use compression
  std::string format; // can be jpg or png
  cv::ImwriteFlags imwrite_flag; // The flag to set in opencv imencode function;
  int imwrite_flag_value; // quality for the jpg compression (0-100) or compression level for png compression (0-9)
#ifdef ROSBAG2_SNAPSHOT_HAVE_H264
  std::shared_ptr<FFMPEGEncoder> encoder; // The encoder to use for video compression
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
  // compression options for image topics;
  ImageCompressionOptions img_compression_opts_;
  // max time between messages to save (in seconds)
  double throttle_period = -1.0;
  // If true and H264 enabled, throttle_period is ignored and all messages are saved
  bool h264_throttle_skip = false;
  // In a forward capture, whether arrivals after the trigger are included.
  bool forward = true;

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

/* Configuration for a single topic in the Snapshotter node. Holds
 * the buffer limits for a topic by duration (time difference between newest and oldest message)
 * and memory usage, in bytes.
 */
struct SnapshotterTopicOptions
{
  // When the value of duration_limit_, do not truncate the buffer
  // no matter how large the duration is
  static const rclcpp::Duration NO_DURATION_LIMIT;
  // When the value of memory_limit_, do not trunctate the buffer
  // no matter how much memory it consumes (DANGROUS)
  static const int64_t NO_MEMORY_LIMIT;
  // When the value of duration_limit_, inherit the limit from
  // the node's configured default
  static const rclcpp::Duration INHERIT_DURATION_LIMIT;
  // When the value of memory_limit_, inherit the limit from
  // the node's configured default
  static const int64_t INHERIT_MEMORY_LIMIT;

  // Maximum difference in time from newest and oldest message in
  // buffer before older messages are removed
  rclcpp::Duration duration_limit_;
  // Maximum memory usage of the buffer before older messages are removed.
  // int64_t (not int32_t): this is a byte count, and default_memory_limit is
  // configured in MB then multiplied by 1e6 -- a config value of a couple
  // GB would overflow a 32-bit count.
  int64_t memory_limit_;

  SnapshotterTopicOptions(
    rclcpp::Duration duration_limit = INHERIT_DURATION_LIMIT,
    int64_t memory_limit = INHERIT_MEMORY_LIMIT);
};

/* Configuration for the Snapshotter node. Contains default limits for memory and duration
 * and a map of topics to their limits which may override the defaults.
 */
struct SnapshotterOptions
{
  // Duration limit to use for a topic's buffer if one is not specified
  rclcpp::Duration default_duration_limit_;
  // Memory limit to use for a topic's buffer if one is not specified, in
  // bytes. int64_t: see the comment on SnapshotterTopicOptions::memory_limit_.
  int64_t default_memory_limit_;
  // Upper bound on a goal's post_duration_s (forward/live capture window).
  // <= 0 disables forward captures entirely, not "unlimited" -- see
  // forward_capture.hpp's forwardCaptureWithinLimit().
  double max_post_duration_s_ = 300.0;
  // Flag if all topics should be recorded
  bool all_topics_;
  // Flag to tell if compression should be used
  std::string rosbag_preset_profile_;
  // Message types to narrow down to one message in interval mode. This package is meant
  // to work on any robot, so it can't hardcode a robot's own message types; instead each
  // deployment lists its own here (interval_single_msg_types param). Only works for types
  // that really have a header.stamp, see HeaderStampReader.
  std::unordered_set<std::string> interval_single_msg_types_;
  // Directory of "<name>.yaml" capture profile files (see capture_profiles.hpp).
  // Optional; "" means no profiles are configured and only the static topics_
  // list below exists, exactly as before this feature.
  std::string capture_profiles_dir_;
  // Total bytes every topic's queue may hold combined, on top of each
  // topic's own memory_limit_. <= 0 (the default) means no shared cap.
  // Set via the total_memory_limit param (MB), converted to bytes like
  // default_memory_limit_.
  int64_t total_memory_limit_{0};

  typedef std::map<TopicDetails, SnapshotterTopicOptions> topics_t;
  // Provides list of topics to snapshot and their limit configurations
  topics_t topics_;

  SnapshotterOptions(
    rclcpp::Duration default_duration_limit = rclcpp::Duration(30s),
    int64_t default_memory_limit = -1);

  // Add a new topic to the configuration, returns false if the topic was already present
  bool addTopic(
    const TopicDetails & topic_details,
    rclcpp::Duration duration_limit = SnapshotterTopicOptions::INHERIT_DURATION_LIMIT,
    int64_t memory_limit = SnapshotterTopicOptions::INHERIT_MEMORY_LIMIT);
};

/* Stores a buffered message of an ambiguous type and it's associated metadata (time of arrival),
 * for later writing to disk
 */
struct SnapshotMessage
{
  SnapshotMessage(
    std::shared_ptr<const rclcpp::SerializedMessage> _msg,
    rclcpp::Time _time);
  std::shared_ptr<const rclcpp::SerializedMessage> msg;
  // ROS time when messaged arrived (does not use header stamp)
  rclcpp::Time time;
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

/* Stores a queue of buffered messages for a single topic ensuring
 * that the duration and memory limits are respected by truncating
 * as needed on push() operations.
 */
class MessageQueue
{
  friend Snapshotter;

private:
  // Logger for outputting ROS logging messages
  rclcpp::Logger logger_;
  // Locks access to size_ and queue_
  mutable std::mutex lock;
  // Stores limits on buffer size and duration
  SnapshotterTopicOptions options_;
  // Current total size of the queue, in bytes
  int64_t size_;
  typedef std::deque<SnapshotMessage> queue_t;
  queue_t queue_;
  // Subscriber to the callback which uses this queue
  std::shared_ptr<rclcpp::GenericSubscription> sub_;
  // Not owned; null for a clone() (never pushed to).
  SharedMemoryBudget * shared_budget_{nullptr};

public:
  explicit MessageQueue(
    const SnapshotterTopicOptions & options, const rclcpp::Logger & logger,
    SharedMemoryBudget * shared_budget = nullptr);
  // Add a new message to the internal queue if possible, truncating the front
  // of the queue as needed to enforce limits
  MessageQueuePushResult push(const SnapshotMessage & msg);
  // Removes the message at the front of the queue (oldest) and returns it
  SnapshotMessage pop();
  // Returns the time difference between back and front of queue, or 0 if size <= 1
  rclcpp::Duration duration() const;
  // Clear internal buffer
  void clear();
  // Store the subscriber for this topic's queue internaly so it is not deleted
  void setSubscriber(std::shared_ptr<rclcpp::GenericSubscription> sub);
  typedef std::pair<queue_t::const_iterator, queue_t::const_iterator> range_t;
  // Get a begin and end iterator into the buffer respecting the start and
  // end timestamp constraints
  range_t rangeFromTimes(const rclcpp::Time & start, const rclcpp::Time & end, int old_messages_to_keep = -1);
  // Get a begin and end iterator into the buffer around the msg_timestamp and tolerance
  range_t intervalFromTimesMsg(const rclcpp::Time & msg_timestamp, const double & tolerance);

  // Return the total message size including the meta-information
  int64_t getMessageSize(SnapshotMessage const & msg) const;

  bool refreshBuffer(rclcpp::Time const& time);
  // Method to clone the current queue and its state
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
  // Internal push whitch does not obtain lock
  MessageQueuePushResult _push(SnapshotMessage const & msg);
  // Internal pop which does not obtain lock
  SnapshotMessage _pop();
  // Internal clear which does not obtain lock
  void _clear();
  // Truncate front of queue as needed to fit a new message of specified size and time.
  // Returns the same tri-state _push()/push() do.
  MessageQueuePushResult preparePush(int32_t size, rclcpp::Time const & time);
};

// Snapshotter node. Maintains a circular buffer of the most recent messages
// from configured topics while enforcing limits on memory and duration.
// The node can be triggered to write some or all of these buffers to a bag
// file via a service call. Useful in live testing scenerios where interesting
// data may be produced before a user has the oppurtunity to "rosbag record" the data.
class Snapshotter : public rclcpp::Node
{
public:
  explicit Snapshotter(const rclcpp::NodeOptions & options);
  ~Snapshotter();

private:
  // Subscribe queue size for each topic
  static const int QUEUE_SIZE;
  SnapshotterOptions options_;
  typedef std::map<TopicDetails, std::shared_ptr<MessageQueue>> buffers_t;
  buffers_t buffers_;
  // Protects buffers_ (the map itself: iteration, emplace, size()) only.
  // Kept separate from state_lock_ below rather than folded into it, since
  // state_lock_'s own scope is deliberately narrower (see its comment) and
  // conflating the two would risk a lock-ordering mistake. A capture's
  // worker task (std::async, off the executor thread -- see
  // capture_futures_ below) can read buffers_ while a forward capture is
  // waiting out its post_duration_s, at the same time the 1Hz
  // poll_topic_timer_ on the executor thread inserts newly-discovered
  // topics into it; each MessageQueue reached through buffers_ has its own
  // separate internal lock (see MessageQueue::lock) once obtained, so this
  // mutex's critical sections should cover only the direct map access
  // itself, never a nested call into another method that also takes it --
  // std::shared_mutex is not reentrant.
  mutable std::shared_mutex buffers_lock_;
  // Shared across every MessageQueue in buffers_.
  SharedMemoryBudget total_memory_budget_;
  // Locks recording_, active_capture_count_, active_filenames_ and
  // last_capture_* below.
  std::shared_mutex state_lock_;
  // True if new messages are being written to the internal buffer
  bool recording_;
  // Number of captures currently in flight, from the moment a goal is
  // accepted (handle_goal) until it's fully finalized (finalizeCapture) --
  // i.e. covers bag-open, buffer clone, write, close and rename, not just
  // "bytes being written right now". Replaces the old single bool writing_:
  // concurrent captures for different filenames are a real, relied-upon
  // usage pattern (data_server_cpp tracks multiple simultaneous
  // TriggerSnapshot goals itself), so this is a count, not a single-slot
  // gate.
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

  // Convert parameter values into a SnapshotterOptions object
  void parseOptionsFromParams();
  // Replace individual topic limits with node defaults if they are
  // flagged for it (see SnapshotterTopicOptions)
  void fixTopicOptions(SnapshotterTopicOptions & options);
  // If file is "prefix" mode (doesn't end in .bag), append current datetime and .bag to end
  bool postfixFilename(std::string & file);
  /// Return current local datetime as a string such as 2018-05-22-14-28-51.
  // Used to generate bag filenames
  std::string timeAsStr();
  // Clear the internal buffers of all topics. Used when resuming after a pause to avoid time gaps
  void clear();
  // Subscribe to one of the topics, setting up the callback to add to the respective queue
  void subscribe(
    const TopicDetails & topic_details,
    std::shared_ptr<MessageQueue> queue);
  // Called on new message from any configured topic. Adds to queue for that topic
  void topicCb(
    std::shared_ptr<const rclcpp::SerializedMessage> msg,
    std::shared_ptr<MessageQueue> queue);
  // Called from topicCb() when push() reports BUDGET_FULL: pops from
  // whichever buffer holds the most until `bytes` fits or nothing is left.
  void evictFromLargestBuffer(int64_t bytes);
  // Action Server callbacks, write all of part of the internal buffers to a bag file
  // according to request parameters
  // Handle Goal
  rclcpp_action::GoalResponse handle_goal(
    const rclcpp_action::GoalUUID & uuid,
    std::shared_ptr<const TriggerSnapAction::Goal> goal);
  // Handle Cancel
  rclcpp_action::CancelResponse handle_cancel(
    const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle);
  // Handle Accepted
  void handle_accepted(const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle);
  // Service callback, enable or disable recording (storing new messages into queue).
  // Used to pause before writing
  void enableCb(
    const std::shared_ptr<rmw_request_id_t> request_header,
    const std_srvs::srv::SetBool::Request::SharedPtr req,
    std_srvs::srv::SetBool_Response::SharedPtr res
  );
  // Set recording_ to false and do nessesary cleaning, CALLER MUST OBTAIN LOCK
  void pause();
  // Set recording_ to true and do nesessary cleaning, CALLER MUST OBTAIN LOCK
  void resume();
  // Poll master for new topics
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
  // Write the parts of message_queue within the time constraints of req to the queue
  // If returns false, there was an error opening/writing the bag and an error message
  // was written to res.message
  // force_throttle: when true, each topic's throttle_period (if any) is applied
  // regardless of req->throttle_msgs -- used when topics_details came from a
  // named capture profile, whose max_rate_hz always applies.
  bool writeTopic(
    rosbag2_cpp::Writer & bag_writer, MessageQueue & message_queue,
    const TopicDetails & topic_details,
    const std::shared_ptr<rclcpp_action::ServerGoalHandle<TriggerSnapAction>> goal_handle,
    rclcpp::Time& request_time,
    bool force_throttle = false);

  // Override the topic details with the topic details from the goal
  void overrideTopicDetails(const DetailsMsg& topic, TopicDetails& details);

  // Get the configuration of image compression for a given topic
  ImageCompressionOptions getCompressionOptions(std::string topic);

  // Bundles everything a capture's worker task needs. Replaces the old
  // 3-argument std::bind call (goal_handle, cloned_buffers, bag_writer_ptr)
  // so the staging/final path pair (and profile, for the event message) can
  // be threaded through without an ever-growing argument list.
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
  };

  // Iter through the message queue and write the messages to the bag
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
  // (handle_accepted, and implicitly by its own destructor), never from a
  // capture's worker task. Must be the LAST member declared in this class:
  // members are destroyed in reverse declaration order, so declaring this
  // last makes it the FIRST thing torn down in ~Snapshotter(), before
  // state_lock_/buffers_/the publishers above. Each entry comes from
  // std::async(std::launch::async, ...), whose returned std::future blocks
  // in its destructor until that task finishes -- so simply letting this
  // vector be destroyed joins every outstanding capture, fixing the old
  // detached-thread use-after-free on shutdown with no explicit join code.
  std::vector<std::future<void>> capture_futures_;
};

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__SNAPSHOTTER_HPP_
