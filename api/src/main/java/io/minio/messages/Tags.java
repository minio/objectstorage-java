/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2020 MinIO, Inc.
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
import org.simpleframework.xml.ElementMap;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Path;
import org.simpleframework.xml.Root;

/**
 * Object representation of request and response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html">PutBucketTagging
 * API</a>, <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html">PutObjectTagging
 * API</a>, <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html">GetBucketTagging
 * API</a> and <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html">GetObjectTagging
 * API</a>.
 */
@Root(name = "Tagging", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class Tags {
  /*
   * Limits are specified in https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html and
   * https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions
   */
  private static final int MAX_KEY_LENGTH = 128;
  private static final int MAX_VALUE_LENGTH = 256;
  private static final int MAX_OBJECT_TAG_COUNT = 10;
  private static final int MAX_TAG_COUNT = 50;

  @Path(value = "TagSet")
  @ElementMap(
      attribute = false,
      entry = "Tag",
      inline = true,
      key = "Key",
      value = "Value",
      required = false)
  Map<String, String> tags;

  public Tags() {}

  private Tags(Map<String, String> tags, boolean isObject) throws IllegalArgumentException {
    if (tags == null) {
      return;
    }

    int limit = isObject ? MAX_OBJECT_TAG_COUNT : MAX_TAG_COUNT;
    if (tags.size() > limit) {
      throw new IllegalArgumentException(
          "too many "
              + (isObject ? "object" : "bucket")
              + " tags; allowed = "
              + limit
              + ", found = "
              + tags.size());
    }

    for (Map.Entry<String, String> entry : tags.entrySet()) {
      String key = entry.getKey();
      if (key.length() == 0 || key.length() > MAX_KEY_LENGTH || key.contains("&")) {
        throw new IllegalArgumentException("invalid tag key '" + key + "'");
      }

      String value = entry.getValue();
      if (value.length() > MAX_VALUE_LENGTH || value.contains("&")) {
        throw new IllegalArgumentException("invalid tag value '" + value + "'");
      }
    }

    this.tags = Utils.unmodifiableMap(tags);
  }

  /** Creates new bucket tags. */
  public static Tags newBucketTags(Map<String, String> tags) throws IllegalArgumentException {
    return new Tags(tags, false);
  }

  /** Creates new object tags. */
  public static Tags newObjectTags(Map<String, String> tags) throws IllegalArgumentException {
    return new Tags(tags, true);
  }

  /** Returns tags. */
  public Map<String, String> get() {
    return Utils.unmodifiableMap(tags);
  }

  @Override
  public String toString() {
    return String.format("Tags{%s}", Utils.stringify(tags));
  }
}
