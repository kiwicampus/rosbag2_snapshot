cmake_minimum_required(VERSION 3.5)
project(rosbag2_snapshot)

# Default to C99
if(NOT CMAKE_C_STANDARD)
  set(CMAKE_C_STANDARD 99)
endif()

# Default to C++17
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(rosbag2_cpp REQUIRED)
find_package(rosbag2_compression REQUIRED)
find_package(rosbag2_compression_zstd REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_action REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(rmw REQUIRED)
find_package(std_srvs REQUIRED)
find_package(rosbag2_snapshot_msgs REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(OpenCV REQUIRED)
find_package(cv_bridge REQUIRED)
find_package(rosidl_typesupport_introspection_cpp REQUIRED)
find_package(visualization_msgs REQUIRED)
find_package(rosbag2_transport REQUIRED)
find_package(yaml-cpp REQUIRED)

set(ENV{PKG_CONFIG_PATH} ${FFMPEG_PKGCONFIG})
find_package(PkgConfig REQUIRED)

# H264 image compression needs foxglove_msgs (FFMPEGEncoder's output type) on
# top of OpenCV/cv_bridge/FFmpeg -- not published for every ROS distro/OS
# combination this package builds on. JPG/PNG compression (OpenCV/cv_bridge
# alone) stays available either way; only the H264 path is compiled out when
# it's missing, same pattern this package's own consumers use elsewhere for
# an optional heavy dependency (e.g. object_detection's ONNX Runtime stub).
find_package(foxglove_msgs QUIET)
pkg_check_modules(LIBAV QUIET IMPORTED_TARGET
    libavcodec
    libswresample
    libswscale
    libavutil
  )
if(foxglove_msgs_FOUND AND LIBAV_FOUND)
  set(ROSBAG2_SNAPSHOT_HAVE_H264 TRUE)
else()
  set(ROSBAG2_SNAPSHOT_HAVE_H264 FALSE)
  message(WARNING "rosbag2_snapshot: foxglove_msgs and/or FFmpeg not found -- "
                   "building without H264 image compression (JPG/PNG still available)")
endif()

include_directories(
  include
)

set(DEPENDENCIES
  rosbag2_cpp
  rosbag2_compression
  rosbag2_compression_zstd
  rclcpp
  rclcpp_action
  rclcpp_components
  rmw
  std_srvs
  rosbag2_snapshot_msgs
  sensor_msgs
  OpenCV
  cv_bridge
  rosidl_typesupport_introspection_cpp
  visualization_msgs
  rosbag2_transport
)
if(ROSBAG2_SNAPSHOT_HAVE_H264)
  list(APPEND DEPENDENCIES foxglove_msgs)
endif()

set(executable_name snapshotter)
set(library_name ${executable_name}_core)

add_library(${library_name} SHARED
  include/rosbag2_snapshot/snapshotter.hpp
  src/snapshotter.cpp
  src/capture_profiles.cpp
  src/topic_resolver.cpp
)

add_executable(${executable_name}
  src/main.cpp
)

target_link_libraries(${library_name} yaml-cpp)
target_link_libraries(${executable_name} ${library_name})

if(ROSBAG2_SNAPSHOT_HAVE_H264)
  add_library(encoder SHARED
    src/ffmpeg_encoding/ffmpeg_encoder.cpp
    src/ffmpeg_encoding/tdiff.cpp
    src/ffmpeg_encoding/utils.cpp
  )
  ament_target_dependencies(encoder ${DEPENDENCIES})
  target_link_libraries(${library_name} PkgConfig::LIBAV encoder)
  target_link_libraries(${executable_name} PkgConfig::LIBAV encoder)
  install(TARGETS encoder
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
  )
endif()

target_compile_definitions(${library_name} PUBLIC
  $<$<BOOL:${ROSBAG2_SNAPSHOT_HAVE_H264}>:ROSBAG2_SNAPSHOT_HAVE_H264>
)

ament_target_dependencies(${library_name}
  ${DEPENDENCIES}
)
ament_target_dependencies(${executable_name}
  ${DEPENDENCIES}
)

# Declare rosbag2_snapshot node component
rclcpp_components_register_nodes(${library_name} "rosbag2_snapshot::Snapshotter")

install(TARGETS ${library_name}
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

install(TARGETS ${executable_name}
  DESTINATION lib/${PROJECT_NAME}
)

if(BUILD_TESTING)
  find_package(ament_cmake_gtest REQUIRED)
  ament_add_gtest(test_capture_profiles
    test/test_capture_profiles.cpp
    src/capture_profiles.cpp
  )
  target_include_directories(test_capture_profiles PRIVATE include)
  target_link_libraries(test_capture_profiles yaml-cpp)

  # Pure/ROS-free, exercises only stagingPathFor() from snapshotter.hpp.
  ament_add_gtest(test_staging_path
    test/test_staging_path.cpp
  )
  target_include_directories(test_staging_path PRIVATE include)

  # Pure/ROS-free, exercises only forward_capture.hpp.
  ament_add_gtest(test_forward_capture
    test/test_forward_capture.cpp
  )
  target_include_directories(test_forward_capture PRIVATE include)

  # Pure/ROS-free, exercises only timestamp_override.hpp.
  ament_add_gtest(test_timestamp_override
    test/test_timestamp_override.cpp
  )
  target_include_directories(test_timestamp_override PRIVATE include)

  # Pure/ROS-free, exercises only shared_memory_budget.hpp.
  ament_add_gtest(test_shared_memory_budget
    test/test_shared_memory_budget.cpp
  )
  target_include_directories(test_shared_memory_budget PRIVATE include)
endif()

ament_package()
