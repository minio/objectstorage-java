/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2025 MinIO, Inc.
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
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementMap;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.convert.Convert;

/**
 * Helper class to denote filter information for {@link ReplicationConfiguration.Rule} and {@link
 * LifecycleConfiguration.Rule}.
 */
@Root(name = "Filter")
public class Filter {
  @Element(name = "And", required = false)
  private And andOperator;

  @Element(name = "Prefix", required = false)
  @Convert(PrefixConverter.class)
  private String prefix;

  @Element(name = "Tag", required = false)
  private Tag tag;

  @Element(name = "ObjectSizeLessThan", required = false)
  private Long objectSizeLessThan;

  @Element(name = "ObjectSizeGreaterThan", required = false)
  private Long objectSizeGreaterThan;

  public Filter(
      @Nullable @Element(name = "And", required = false) And andOperator,
      @Nullable @Element(name = "Prefix", required = false) String prefix,
      @Nullable @Element(name = "Tag", required = false) Tag tag) {
    if (andOperator != null ^ prefix != null ^ tag != null) {
      this.andOperator = andOperator;
      this.prefix = prefix;
      this.tag = tag;
    } else {
      throw new IllegalArgumentException("Only one of And, Prefix or Tag must be set");
    }
  }

  public Filter(
      @Nullable @Element(name = "And", required = false) And andOperator,
      @Nullable @Element(name = "Prefix", required = false) String prefix,
      @Nullable @Element(name = "Tag", required = false) Tag tag,
      @Nullable @Element(name = "ObjectSizeLessThan", required = false) Long objectSizeLessThan,
      @Nullable @Element(name = "ObjectSizeGreaterThan", required = false)
          Long objectSizeGreaterThan) {
    this(andOperator, prefix, tag);
    this.objectSizeLessThan = objectSizeLessThan;
    this.objectSizeGreaterThan = objectSizeGreaterThan;
  }

  public Filter(@Nonnull And andOperator) {
    this.andOperator = Objects.requireNonNull(andOperator, "And operator must not be null");
  }

  public Filter(@Nonnull String prefix) {
    this.prefix = Objects.requireNonNull(prefix, "Prefix must not be null");
  }

  public Filter(@Nonnull Tag tag) {
    this.tag = Objects.requireNonNull(tag, "Tag must not be null");
  }

  public And andOperator() {
    return this.andOperator;
  }

  public String prefix() {
    return this.prefix;
  }

  public Tag tag() {
    return this.tag;
  }

  public Long objectSizeLessThan() {
    return this.objectSizeLessThan;
  }

  public Long objectSizeGreaterThan() {
    return this.objectSizeGreaterThan;
  }

  @Override
  public String toString() {
    return String.format(
        "Filter{andOperator=%s, prefix=%s, tag=%s, objectSizeLessThan=%s,"
            + " objectSizeGreaterThan=%s}",
        Utils.stringify(andOperator),
        Utils.stringify(prefix),
        Utils.stringify(tag),
        Utils.stringify(objectSizeLessThan),
        Utils.stringify(objectSizeGreaterThan));
  }

  @Root(name = "And")
  public static class And {
    @Element(name = "Prefix", required = false)
    @Convert(PrefixConverter.class)
    private String prefix;

    @Element(name = "ObjectSizeLessThan", required = false)
    private Long objectSizeLessThan;

    @Element(name = "ObjectSizeGreaterThan", required = false)
    private Long objectSizeGreaterThan;

    @ElementMap(
        attribute = false,
        entry = "Tag",
        inline = true,
        key = "Key",
        value = "Value",
        required = false)
    private Map<String, String> tags;

    public And(
        @Nullable @Element(name = "Prefix", required = false) String prefix,
        @Nullable
            @ElementMap(
                attribute = false,
                entry = "Tag",
                inline = true,
                key = "Key",
                value = "Value",
                required = false)
            Map<String, String> tags) {
      if (prefix == null && tags == null) {
        throw new IllegalArgumentException("At least Prefix or Tags must be set");
      }

      if (tags != null) {
        for (String key : tags.keySet()) {
          if (key.isEmpty()) {
            throw new IllegalArgumentException("Tags must not contain empty key");
          }
        }
      }

      this.prefix = prefix;
      this.tags = Utils.unmodifiableMap(tags);
    }

    public And(
        @Nullable @Element(name = "Prefix", required = false) String prefix,
        @Nullable
            @ElementMap(
                attribute = false,
                entry = "Tag",
                inline = true,
                key = "Key",
                value = "Value",
                required = false)
            Map<String, String> tags,
        @Nullable @Element(name = "ObjectSizeLessThan", required = false) Long objectSizeLessThan,
        @Nullable @Element(name = "ObjectSizeGreaterThan", required = false)
            Long objectSizeGreaterThan) {
      this(prefix, tags);
      this.objectSizeLessThan = objectSizeLessThan;
      this.objectSizeGreaterThan = objectSizeGreaterThan;
    }

    public String prefix() {
      return this.prefix;
    }

    public Long objectSizeLessThan() {
      return this.objectSizeLessThan;
    }

    public Long objectSizeGreaterThan() {
      return this.objectSizeGreaterThan;
    }

    public Map<String, String> tags() {
      return this.tags;
    }

    @Override
    public String toString() {
      return String.format(
          "And{prefix=%s, objectSizeLessThan=%s, objectSizeGreaterThan=%s, tags=%s}",
          Utils.stringify(prefix),
          Utils.stringify(objectSizeLessThan),
          Utils.stringify(objectSizeGreaterThan),
          Utils.stringify(tags));
    }
  }

  @Root(name = "Tag")
  public static class Tag {
    @Element(name = "Key")
    private String key;

    @Element(name = "Value")
    private String value;

    public Tag(
        @Nonnull @Element(name = "Key") String key,
        @Nonnull @Element(name = "Value") String value) {
      Objects.requireNonNull(key, "Key must not be null");
      if (key.isEmpty()) {
        throw new IllegalArgumentException("Key must not be empty");
      }

      this.key = key;
      this.value = Objects.requireNonNull(value, "Value must not be null");
    }

    public String key() {
      return this.key;
    }

    public String value() {
      return this.value;
    }

    @Override
    public String toString() {
      return String.format("Tag{key=%s, value=%s}", Utils.stringify(key), Utils.stringify(value));
    }
  }
}
