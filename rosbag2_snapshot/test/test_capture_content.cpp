#include <gtest/gtest.h>

#include "rosbag2_snapshot/capture_content.hpp"

using rosbag2_snapshot::CaptureContent;

TEST(CaptureContent, EmptyCaptureHasNoSpan)
{
  CaptureContent content;
  EXPECT_TRUE(content.topics().empty());
  EXPECT_EQ(content.totalMessages(), 0u);
  EXPECT_EQ(content.firstReceiptNs(), 0);
  EXPECT_EQ(content.lastReceiptNs(), 0);
}

TEST(CaptureContent, DeclaredTopicWithoutMessagesCountsZero)
{
  CaptureContent content;
  content.addTopic("/a");
  content.addTopic("/a");
  ASSERT_EQ(content.topics().size(), 1u);
  EXPECT_EQ(content.counts().at(0), 0u);
  EXPECT_EQ(content.firstReceiptNs(), 0);
}

TEST(CaptureContent, CountsPerTopicInDeclarationOrder)
{
  CaptureContent content;
  content.addTopic("/b");
  content.recordMessage("/a", 30);
  content.recordMessage("/b", 20);
  content.recordMessage("/a", 10);
  ASSERT_EQ(content.topics().size(), 2u);
  EXPECT_EQ(content.topics().at(0), "/b");
  EXPECT_EQ(content.topics().at(1), "/a");
  EXPECT_EQ(content.counts().at(0), 1u);
  EXPECT_EQ(content.counts().at(1), 2u);
  EXPECT_EQ(content.totalMessages(), 3u);
}

TEST(CaptureContent, SpanIsTheMinAndMaxReceiptAcrossTopics)
{
  CaptureContent content;
  content.recordMessage("/a", 500);
  content.recordMessage("/b", 100);
  content.recordMessage("/a", 900);
  EXPECT_EQ(content.firstReceiptNs(), 100);
  EXPECT_EQ(content.lastReceiptNs(), 900);
}
