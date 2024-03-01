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

#include <rcpputils/scope_exit.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rosbag2_snapshot/snapshotter.hpp>

#include <filesystem>

#include <cassert>
#include <chrono>
#include <ctime>
#include <exception>
#include <iomanip>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

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

SnapshotterClientOptions::SnapshotterClientOptions()
: action_(SnapshotterClientOptions::TRIGGER_WRITE)
{
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

void MessageQueue::clear()
{
  std::lock_guard<std::mutex> l(lock);
  _clear();
}

void MessageQueue::_clear()
{
  if(options_.duration_limit_.seconds() > 0.0)
  {
    queue_.clear();
    size_ = 0;
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
    return false;
  }

  // If memory limit is enforced, remove elements from front of queue until limit
  // would be met once message is added
  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT) {
    while (queue_.size() != 0 && size_ + size > options_.memory_limit_) {
      _pop();
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
  auto ret = lock.try_lock();
  if (!ret) {
    RCLCPP_ERROR(logger_, "Failed to lock. Time %f", _out.time.seconds());
    return;
  }
  _push(_out);
  if (ret) {
    lock.unlock();
  }
}

SnapshotMessage MessageQueue::pop()
{
  std::lock_guard<std::mutex> l(lock);
  return _pop();
}

int64_t MessageQueue::getMessageSize(SnapshotMessage const & snapshot_msg) const
{
  return snapshot_msg.msg->size() + sizeof(SnapshotMessage);
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

MessageQueue::range_t MessageQueue::rangeFromTimes(Time const & start, Time const & stop)
{
  range_t::first_type begin = queue_.begin();
  range_t::second_type end = queue_.end();

  
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
  writing_(false)
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
    details.default_bag_duration = pair.first.default_bag_duration;
    details.img_compression_opts_ = pair.first.img_compression_opts_;
    details.throttle_period = pair.first.throttle_period;
    std::pair<buffers_t::iterator, bool> res =
      buffers_.emplace(details, queue);
    assert(res.second);

    subscribe(details, queue);
  }

  // Now that subscriptions are setup, setup service servers for writing and pausing
  trigger_snapshot_server_ = create_service<TriggerSnapshot>(
    "trigger_snapshot", std::bind(&Snapshotter::triggerSnapshotCb, this, _1, _2, _3));
  enable_server_ = create_service<SetBool>(
    "enable_snapshot", std::bind(&Snapshotter::enableCb, this, _1, _2, _3));

  // Start timer to poll for topics
  if (options_.all_topics_) {
    poll_topic_timer_ =
      create_wall_timer(
      std::chrono::duration(1s),
      std::bind(&Snapshotter::pollTopics, this));
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
      declare_parameter<double>("default_memory_limit", -1.0);
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

  RCLCPP_INFO(get_logger(), "using %s preset for rosbag recording", options_.rosbag_preset_profile_.c_str());

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
      double throttle_period = -1.0;

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
          RCLCPP_WARN(get_logger(), "Qos not defined for topic %s, using defaul qos", topic.c_str());
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
        throttle_period = declare_parameter<double>(prefix + ".throttle_period");
      }
        catch (const rclcpp::exceptions::UninitializedStaticallyTypedParameterException& ex)
      {
        throttle_period = -1.0;
      } catch (const rclcpp::ParameterTypeException& ex)
      {
        throttle_period = -1.0;
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
      dets.img_compression_opts_ = img_compression_opts;
      dets.default_bag_duration = options_.default_duration_limit_;
      dets.throttle_period = throttle_period;

      if(dets.override_old_timestamps)
      {
        RCLCPP_WARN(get_logger(), "Old timestamps will be overriden for topic %s", topic.c_str());
      }

      if(dets.img_compression_opts_.use_compression)
      {
        RCLCPP_INFO(get_logger(), "compression: %i for topic %s using format %s and compression flag %i", dets.img_compression_opts_.use_compression, topic.c_str(), dets.img_compression_opts_.format.c_str(), dets.img_compression_opts_.imwrite_flag_value);
      }

      if(dets.throttle_period > 0.0)
      {
        RCLCPP_INFO(get_logger(), "Throttle period: %f for topic %s messages subsampled", dets.throttle_period, topic.c_str());
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
  // If recording is paused (or writing), exit
  {
    std::shared_lock<std::shared_mutex> lock(state_lock_);
    if (!recording_) {
      return;
    }
  }

  // Pack message and metadata into SnapshotMessage holder
  SnapshotMessage out(msg, now());
  queue->push(out);
}

void Snapshotter::subscribe(
  const TopicDetails & topic_details,
  std::shared_ptr<MessageQueue> queue)
{
  RCLCPP_INFO(get_logger(), "Subscribing to %s", topic_details.name.c_str());

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
  const TriggerSnapshot::Request::SharedPtr & req,
  const TriggerSnapshot::Response::SharedPtr & res,
  rclcpp::Time& request_time)
{
  // acquire lock for this queue
  std::lock_guard l(message_queue.lock);

  MessageQueue::range_t range = message_queue.rangeFromTimes(req->start_time, req->stop_time);

  rosbag2_storage::TopicMetadata tm;
  tm.name = topic_details.name;
  tm.type = topic_details.type;
  tm.serialization_format = "cdr";

  rclcpp::Serialization<sensor_msgs::msg::Image> img_serializer;
  cv_bridge::CvImagePtr cv_bridge_img;
  std::vector<int> compression_params; 
  if(topic_details.img_compression_opts_.use_compression)
  {
    RCLCPP_INFO(get_logger(), "topic %s is an image. applying %s compression", topic_details.name.c_str(), topic_details.img_compression_opts_.format.c_str() );
    compression_params.push_back(topic_details.img_compression_opts_.imwrite_flag);
    compression_params.push_back(topic_details.img_compression_opts_.imwrite_flag_value); // Set JPEG quality (0-100) or png compression (0-9)
    img_serializer = rclcpp::Serialization<sensor_msgs::msg::Image>();
    tm.type = "sensor_msgs/msg/CompressedImage";
  }

  bag_writer.create_topic(tm);

  double prev_msg_time = 0.0;
  for (auto msg_it = range.first; msg_it != range.second; ++msg_it) {
    // Create BAG message
    auto bag_message = std::make_shared<rosbag2_storage::SerializedBagMessage>();
    auto ret = rcutils_system_time_now(&bag_message->time_stamp);
    if (ret != RCL_RET_OK) {
      RCLCPP_ERROR(get_logger(), "Failed to assign time to rosbag message.");
      return false;
    }
      
    if (topic_details.throttle_period > 0.0 && msg_it->time.nanoseconds() - prev_msg_time <= topic_details.throttle_period * 1e9)
    {
      RCLCPP_DEBUG(get_logger(), "topic %s is being throttled. message time: %ld, previous message time: %f, throttle_period: %f", topic_details.name.c_str(), msg_it->time.nanoseconds(), prev_msg_time, topic_details.throttle_period);
      continue;
    }

    prev_msg_time = msg_it->time.nanoseconds();

    bag_message->topic_name = tm.name;
    rclcpp::Duration bag_duration = rclcpp::Time(req->stop_time) - rclcpp::Time(req->start_time);
    if(topic_details.override_old_timestamps && (request_time - msg_it->time) > bag_duration)
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
      sensor_msgs::msg::Image raw_img;
      sensor_msgs::msg::CompressedImage compressed_img;
      img_serializer.deserialize_message(msg_it->msg.get(), &raw_img);
      // imencode expects rgb images in `bgr` encoding, so we need to change incoming images that
      // use `rbg8` encoding to `bgr8` encoding by hand.
      if (raw_img.encoding == "rgb8")
      {
        // Create a Mat from the image message (without copying).
        cv::Mat cv_img(raw_img.height, raw_img.width, CV_8UC3, raw_img.data.data());
        cv::cvtColor(cv_img, cv_img, cv::COLOR_RGB2BGR);
        cv::imencode("." + topic_details.img_compression_opts_.format, cv_img, compressed_img.data, compression_params);
      }
      else
      {
        cv_bridge_img = cv_bridge::toCvCopy(raw_img, raw_img.encoding);
        cv::imencode("." + topic_details.img_compression_opts_.format, cv_bridge_img->image, compressed_img.data, compression_params);
      }
      compressed_img.format = topic_details.img_compression_opts_.format;
      compressed_img.header = raw_img.header;
      bag_writer.write(compressed_img, tm.name, rclcpp::Time(bag_message->time_stamp));
    }
    else
    {
      bag_message->serialized_data = std::make_shared<rcutils_uint8_array_t>(
        msg_it->msg->get_rcl_serialized_message()
      );
      bag_writer.write(bag_message);
    }
  }

  return true;
}

void Snapshotter::triggerSnapshotCb(
  const std::shared_ptr<rmw_request_id_t> request_header,
  const TriggerSnapshot::Request::SharedPtr req,
  TriggerSnapshot::Response::SharedPtr res)
{
  (void)request_header;

  if (req->filename.empty() || !postfixFilename(req->filename)) {
    res->success = false;
    res->message = "Invalid filename";
    return;
  }

  // Store if we were recording prior to write to restore this state after write
  bool recording_prior{true};

  {
    std::shared_lock<std::shared_mutex> read_lock(state_lock_);
    recording_prior = recording_;
    if (writing_) {
      res->success = false;
      res->message = "Already writing";
      return;
    }
  }

  {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    if (recording_prior) {
      // pause();
    }
    writing_ = true;
  }

  // Ensure that state is updated when function exits, regardlesss of branch path / exception events
  RCPPUTILS_SCOPE_EXIT(
    // Clear buffers beacuase time gaps (skipped messages) may have occured while paused
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    // Turn off writing flag and return recording to its state before writing
    writing_ = false;
    if (recording_prior) {
      this->resume();
    }
  );
  std::shared_ptr<rosbag2_cpp::Writer> bag_writer_ptr;;
  bag_writer_ptr = std::make_shared<rosbag2_cpp::Writer>();

  
  RCLCPP_INFO(get_logger(), "opening %s", req->filename.c_str());

  try {
    rosbag2_storage::StorageOptions storage_opts;
    storage_opts.storage_id = "mcap";
    storage_opts.uri = req->filename;
    storage_opts.storage_preset_profile = options_.rosbag_preset_profile_;
    rosbag2_cpp::ConverterOptions converter_opts{};
    bag_writer_ptr->open(storage_opts, converter_opts);
  } catch (const std::exception & ex) {
    RCLCPP_WARN(
          get_logger(), "Failed to open %s file, reason: %s", req->filename.c_str(), ex.what());
    res->success = false;
    res->message = "Unable to open file for writing, " + std::string(ex.what());
    return;
  }

  rclcpp::Time request_time = now();

  // Write each selected topic's queue to bag file
  if (req->topics.size() && req->topics.at(0).name.size()) {
    for (auto & topic : req->topics) {

      if (topic.type.empty()) {
        auto it = std::find_if(buffers_.begin(), buffers_.end(),
          [&topic](const auto &saved_topic) {
            return saved_topic.first.name == topic.name;
          });

        if (it != buffers_.end()) {
          topic.type = it->first.type;
          RCLCPP_DEBUG(get_logger(), "Assigned type %s to topic %s", topic.type.c_str(), topic.name.c_str());
        }
      }

      TopicDetails details{topic.name, topic.type};
      // Find the message queue for this topic if it exsists
      auto found = buffers_.find(details);

      if (found == buffers_.end()) {
        RCLCPP_WARN(
          get_logger(), "Requested topic %s is not subscribed, skipping.", topic.name.c_str());
        continue;
      }

      details = found->first;
      MessageQueue & message_queue = *(found->second);

      // print size of the queue if queue size is zero
      if (message_queue.size_ == 0)
      {
        RCLCPP_WARN(get_logger(), "Queue size for topic %s is %ld", topic.name.c_str(), message_queue.size_);
      }

      if (!writeTopic(*bag_writer_ptr, message_queue, details, req, res, request_time)) {
        res->success = false;
        res->message = "Failed to write topic " + topic.type + " to bag file.";
        return;
      }
    }
  } else {  // If topic list empty, record all buffered topics
    for (const buffers_t::value_type & pair : buffers_) {
      MessageQueue & message_queue = *(pair.second);
      message_queue.refreshBuffer(request_time);
      if (!writeTopic(*bag_writer_ptr, message_queue, pair.first, req, res, request_time)) {
        res->success = false;
        res->message = "Failed to write topic " + pair.first.name + " to bag file.";
        return;
      }
    }
  }
  /*
  // If no topics were subscribed/valid/contained data, this is considered a non-success
  if (!bag.isOpen()) {
    res->success = false;
    res->message = res->NO_DATA_MESSAGE;
    return;
  }
  */

  res->success = true;
  res->message = req->filename;
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
  RCLCPP_INFO(get_logger(), "Buffering paused");
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

SnapshotterClient::SnapshotterClient(const rclcpp::NodeOptions & options)
: rclcpp::Node("snapshotter_client", options)
{
  std::string action_str{};

  SnapshotterClientOptions opts{};

  try {
    action_str = declare_parameter<std::string>("action_type");
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "action_type parameter is missing or of incorrect type.");
    throw ex;
  }

  if (action_str == "trigger_write") {
    opts.action_ = SnapshotterClientOptions::TRIGGER_WRITE;
  } else if (action_str == "resume") {
    opts.action_ = SnapshotterClientOptions::RESUME;
  } else if (action_str == "pause") {
    opts.action_ = SnapshotterClientOptions::PAUSE;
  } else {
    RCLCPP_ERROR(get_logger(), "action_type must be one of: trigger_write, resume, or pause");
    throw std::invalid_argument{"Invalid value for action_type parameter."};
  }

  std::vector<std::string> topic_names{};

  try {
    topic_names = declare_parameter<std::vector<std::string>>("topics");
  } catch (const rclcpp::ParameterTypeException & ex) {
    if (std::string{ex.what()}.find("not set") == std::string::npos) {
      RCLCPP_ERROR(get_logger(), "topics must be an array of strings.");
      throw ex;
    }
  }

  if (topic_names.size() > 0) {
    for (const auto & topic : topic_names) {
      std::string prefix = "topic_details." + topic;
      std::string topic_type{};

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

      TopicDetails details{};
      details.name = topic;
      details.type = topic_type;
      opts.topics_.push_back(details);
    }
  }

  try {
    opts.filename_ = declare_parameter<std::string>("filename");
  } catch (const rclcpp::ParameterTypeException & ex) {
    if (opts.action_ == SnapshotterClientOptions::TRIGGER_WRITE &&
      std::string{ex.what()}.find("not set") == std::string::npos)
    {
      RCLCPP_ERROR(get_logger(), "filename must be a string.");
      throw ex;
    }
  }

  try {
    opts.prefix_ = declare_parameter<std::string>("prefix");
  } catch (const rclcpp::ParameterTypeException & ex) {
    if (opts.action_ == SnapshotterClientOptions::TRIGGER_WRITE &&
      std::string{ex.what()}.find("not set") == std::string::npos)
    {
      RCLCPP_ERROR(get_logger(), "prefix must be a string.");
      throw ex;
    }
  }

  if (opts.action_ == SnapshotterClientOptions::TRIGGER_WRITE && opts.topics_.size() == 0) {
    RCLCPP_INFO(get_logger(), "No topics provided - logging all topics.");
    RCLCPP_WARN(get_logger(), "Logging all topics is very memory-intensive.");
  }

  setSnapshotterClientOptions(opts);
}

void SnapshotterClient::setSnapshotterClientOptions(const SnapshotterClientOptions & opts)
{
  if (opts.action_ == SnapshotterClientOptions::TRIGGER_WRITE) {
    auto client = create_client<TriggerSnapshot>("trigger_snapshot");
    if (!client->service_is_ready()) {
      throw std::runtime_error{
              "Service trigger_snapshot is not ready. "
              "Is snapshot running in this namespace?"
      };
    }

    TriggerSnapshot::Request::SharedPtr req;

    for (const auto & topic : opts.topics_) {
      req->topics.push_back(topic.asMessage());
    }

    // Prefix mode
    if (opts.filename_.empty()) {
      req->filename = opts.prefix_;
      size_t ind = req->filename.rfind(".bag");
      if (ind != string::npos && ind == req->filename.size() - 4) {
        req->filename.erase(ind);
      }
    } else {
      req->filename = opts.filename_;
      size_t ind = req->filename.rfind(".bag");
      if (ind == string::npos || ind != req->filename.size() - 4) {
        req->filename += ".bag";
      }
    }

    // Resolve filename relative to clients working directory to avoid confusion
    // Special case of no specified file, ensure still in working directory of client
    if (req->filename.empty()) {
      req->filename = "./";
    }
    std::filesystem::path p(std::filesystem::absolute(req->filename));
    req->filename = p.string();

    auto result_future = client->async_send_request(req);
    auto future_result =
      rclcpp::spin_until_future_complete(this->get_node_base_interface(), result_future);

    if (future_result == rclcpp::FutureReturnCode::SUCCESS) {
      RCLCPP_ERROR(get_logger(), "Calling the service failed.");
    } else {
      auto result = result_future.get();
      RCLCPP_INFO(
        get_logger(),
        "Service returned: [%s] %s",
        (result->success ? "SUCCESS" : "FAILURE"),
        result->message.c_str()
      );
    }

    return;
  } else if (  // NOLINT
    opts.action_ == SnapshotterClientOptions::PAUSE ||
    opts.action_ == SnapshotterClientOptions::RESUME)
  {
    auto client = create_client<SetBool>("enable_snapshot");
    if (!client->service_is_ready()) {
      throw std::runtime_error{
              "Service enable_snapshot does not exist. "
              "Is snapshot running in this namespace?"
      };
    }

    SetBool::Request::SharedPtr req;
    req->data = (opts.action_ == SnapshotterClientOptions::RESUME);

    auto result_future = client->async_send_request(req);
    auto future_result =
      rclcpp::spin_until_future_complete(this->get_node_base_interface(), result_future);

    if (future_result == rclcpp::FutureReturnCode::SUCCESS) {
      RCLCPP_ERROR(get_logger(), "Calling the service failed.");
    } else {
      auto result = result_future.get();
      RCLCPP_INFO(
        get_logger(),
        "Service returned: [%s] %s",
        (result->success ? "SUCCESS" : "FAILURE"),
        result->message.c_str()
      );
    }

    return;
  } else {
    throw std::runtime_error{"Invalid options received."};
  }
}

}  // namespace rosbag2_snapshot

#include <rclcpp_components/register_node_macro.hpp>  // NOLINT
RCLCPP_COMPONENTS_REGISTER_NODE(rosbag2_snapshot::Snapshotter)
RCLCPP_COMPONENTS_REGISTER_NODE(rosbag2_snapshot::SnapshotterClient)
