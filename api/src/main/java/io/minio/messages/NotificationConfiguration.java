/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2017 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.minio.messages;

import io.minio.Utils;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Path;
import org.simpleframework.xml.Root;

/**
 * Object representation of request XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotificationConfiguration.html">PutBucketNotificationConfiguration
 * API</a> and response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotificationConfiguration.html">GetBucketNotificationConfiguration
 * API</a>.
 */
@Root(name = "NotificationConfiguration", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class NotificationConfiguration {
  @ElementList(name = "CloudFunctionConfiguration", inline = true, required = false)
  private List<CloudFunctionConfiguration> cloudFunctionConfigurations;

  @ElementList(name = "QueueConfiguration", inline = true, required = false)
  private List<QueueConfiguration> queueConfigurations;

  @ElementList(name = "TopicConfiguration", inline = true, required = false)
  private List<TopicConfiguration> topicConfigurations;

  @Element(name = "EventBridgeConfiguration", required = false)
  private EventBridgeConfiguration eventBridgeConfiguration;

  private NotificationConfiguration() {}

  public NotificationConfiguration(
      @ElementList(name = "CloudFunctionConfiguration", inline = true, required = false)
          List<CloudFunctionConfiguration> cloudFunctionConfigurations,
      @ElementList(name = "QueueConfiguration", inline = true, required = false)
          List<QueueConfiguration> queueConfigurations,
      @ElementList(name = "TopicConfiguration", inline = true, required = false)
          List<TopicConfiguration> topicConfigurations,
      @Element(name = "EventBridgeConfiguration", required = false)
          EventBridgeConfiguration eventBridgeConfiguration) {
    this.cloudFunctionConfigurations = cloudFunctionConfigurations;
    this.queueConfigurations = queueConfigurations;
    this.topicConfigurations = topicConfigurations;
    this.eventBridgeConfiguration = eventBridgeConfiguration;
  }

  /** Returns cloud function configuration. */
  public List<CloudFunctionConfiguration> cloudFunctionConfigurations() {
    return Utils.unmodifiableList(cloudFunctionConfigurations);
  }

  /** Returns queue configuration list. */
  public List<QueueConfiguration> queueConfigurations() {
    return Utils.unmodifiableList(queueConfigurations);
  }

  /** Returns topic configuration list. */
  public List<TopicConfiguration> topicConfigurations() {
    return Utils.unmodifiableList(topicConfigurations);
  }

  public EventBridgeConfiguration eventBridgeConfiguration() {
    return this.eventBridgeConfiguration;
  }

  @Override
  public String toString() {
    return String.format(
        "NotificationConfiguration{cloudFunctionConfigurations=%s, queueConfigurations=%s,"
            + " topicConfigurations=%s, eventBridgeConfiguration=%s}",
        Utils.stringify(cloudFunctionConfigurations),
        Utils.stringify(queueConfigurations),
        Utils.stringify(topicConfigurations),
        Utils.stringify(eventBridgeConfiguration));
  }

  @Root(name = "EventBridgeConfiguration")
  public static class EventBridgeConfiguration {
    @Override
    public String toString() {
      return String.format("EventBridgeConfiguration{}");
    }
  }

  public abstract static class BaseConfiguration {
    @Element(name = "Id", required = false)
    private String id;

    @ElementList(entry = "Event", inline = true)
    private List<String> events;

    @Element(name = "Filter", required = false)
    private Filter filter;

    public BaseConfiguration(String id, @Nonnull List<String> events, Filter filter) {
      this.id = id;
      this.events =
          Utils.unmodifiableList(Objects.requireNonNull(events, "Events must not be null"));
      this.filter = filter;
    }

    public String id() {
      return id;
    }

    public List<String> events() {
      return Utils.unmodifiableList(events);
    }

    public Filter filter() {
      return filter;
    }

    @Override
    public String toString() {
      return String.format(
          "id=%s, events=%s, filter=%s",
          Utils.stringify(id), Utils.stringify(events), Utils.stringify(filter));
    }
  }

  @Root(name = "Filter")
  public static class Filter {
    @Path(value = "S3Key")
    @ElementList(name = "FilterRule", inline = true)
    private List<FilterRule> rules;

    public Filter(
        @Nonnull @Path(value = "S3Key") @ElementList(name = "FilterRule", inline = true)
            List<FilterRule> rules) {
      Objects.requireNonNull(rules, "Filter rules must not be null");
      if (rules.size() < 1) {
        throw new IllegalArgumentException("At least one rule must be provided");
      }
      if (rules.size() > 2) {
        throw new IllegalArgumentException("Maximum two rules must be provided");
      }
      if (rules.size() == 2 && rules.get(0).name().equals(rules.get(1).name())) {
        throw new IllegalArgumentException(
            "Two rules '" + rules.get(0).name() + "' must not be same");
      }
      this.rules = Utils.unmodifiableList(rules);
    }

    public Filter(@Nonnull String prefix, @Nonnull String suffix) {
      if (prefix == null && suffix == null) {
        throw new IllegalArgumentException("Either prefix or suffix must be provided");
      }
      List<FilterRule> rules = new LinkedList<>();
      if (prefix != null) rules.add(FilterRule.newPrefixFilterRule(prefix));
      if (suffix != null) rules.add(FilterRule.newSuffixFilterRule(suffix));
      this.rules = Utils.unmodifiableList(rules);
    }

    public List<FilterRule> rules() {
      return rules;
    }

    @Override
    public String toString() {
      return String.format("Filter{rules=%s}", Utils.stringify(rules));
    }
  }

  @Root(name = "FilterRule")
  public static class FilterRule {
    @Element(name = "Name")
    private String name;

    @Element(name = "Value")
    private String value;

    public FilterRule(
        @Nonnull @Element(name = "Name") String name,
        @Nonnull @Element(name = "Value") String value) {
      Objects.requireNonNull(name, "Name must not be null");
      if (!"prefix".equals(name) && !"suffix".equals(name)) {
        throw new IllegalArgumentException("Name must be 'prefix' or 'suffix'");
      }
      Objects.requireNonNull(value, "Value must not be null");
      this.name = name;
      this.value = value;
    }

    public static FilterRule newPrefixFilterRule(@Nonnull String value) {
      return new FilterRule("prefix", value);
    }

    public static FilterRule newSuffixFilterRule(@Nonnull String value) {
      return new FilterRule("suffix", value);
    }

    public String name() {
      return name;
    }

    public String value() {
      return value;
    }

    @Override
    public String toString() {
      return String.format(
          "FilterRule{name=%s, value=%s}", Utils.stringify(name), Utils.stringify(value));
    }
  }

  @Root(name = "CloudFunctionConfiguration", strict = false)
  public static class CloudFunctionConfiguration extends BaseConfiguration {
    @Element(name = "CloudFunction")
    private String cloudFunction;

    public CloudFunctionConfiguration(
        @Nonnull @Element(name = "CloudFunction") String cloudFunction,
        @Element(name = "Id", required = false) String id,
        @Nonnull @ElementList(entry = "Event", inline = true) List<String> events,
        @Element(name = "Filter", required = false) Filter filter) {
      super(id, events, filter);
      this.cloudFunction = cloudFunction;
    }

    /** Returns cloudFunction. */
    public String cloudFunction() {
      return cloudFunction;
    }

    @Override
    public String toString() {
      return String.format(
          "CloudFunctionConfiguration{cloudFunction=%s, %s}",
          Utils.stringify(cloudFunction), super.toString());
    }
  }

  @Root(name = "QueueConfiguration", strict = false)
  public static class QueueConfiguration extends BaseConfiguration {
    @Element(name = "Queue")
    private String queue;

    public QueueConfiguration(
        @Nonnull @Element(name = "Queue") String queue,
        @Element(name = "Id", required = false) String id,
        @Nonnull @ElementList(entry = "Event", inline = true) List<String> events,
        @Element(name = "Filter", required = false) Filter filter) {
      super(id, events, filter);
      this.queue = queue;
    }

    /** Returns queue. */
    public String queue() {
      return queue;
    }

    @Override
    public String toString() {
      return String.format(
          "QueueConfiguration{queue=%s, %s}", Utils.stringify(queue), super.toString());
    }
  }

  @Root(name = "TopicConfiguration", strict = false)
  public static class TopicConfiguration extends BaseConfiguration {
    @Element(name = "Topic")
    private String topic;

    public TopicConfiguration(
        @Nonnull @Element(name = "Topic") String topic,
        @Element(name = "Id", required = false) String id,
        @Nonnull @ElementList(entry = "Event", inline = true) List<String> events,
        @Element(name = "Filter", required = false) Filter filter) {
      super(id, events, filter);
      this.topic = topic;
    }

    /** Returns topic. */
    public String topic() {
      return topic;
    }

    @Override
    public String toString() {
      return String.format(
          "TopicConfiguration{topic=%s, %s}", Utils.stringify(topic), super.toString());
    }
  }
}
