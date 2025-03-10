/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2015, 2016, 2017 MinIO, Inc.
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
import java.util.List;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">ListObjectsV2
 * API</a>.
 */
@Root(name = "ListBucketResult", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class ListBucketResultV2 extends ListObjectsResult {
  @Element(name = "KeyCount", required = false)
  private int keyCount;

  @Element(name = "StartAfter", required = false)
  private String startAfter;

  @Element(name = "ContinuationToken", required = false)
  private String continuationToken;

  @Element(name = "NextContinuationToken", required = false)
  private String nextContinuationToken;

  @ElementList(name = "Contents", inline = true, required = false)
  private List<Contents> contents;

  /** Returns key count. */
  public int keyCount() {
    return keyCount;
  }

  /** Returns start after. */
  public String startAfter() {
    return Utils.urlDecode(startAfter, encodingType());
  }

  /** Returns continuation token. */
  public String continuationToken() {
    return continuationToken;
  }

  /** Returns next continuation token. */
  public String nextContinuationToken() {
    return nextContinuationToken;
  }

  /** Returns List of Items. */
  @Override
  public List<Contents> contents() {
    return Utils.unmodifiableList(contents);
  }

  @Override
  public String toString() {
    return String.format(
        "ListBucketResultV2{%s, keyCount=%s, startAfter=%s, continuationToken=%s,"
            + " nextContinuationToken=%s, contents=%s}",
        super.toString(),
        Utils.stringify(keyCount),
        Utils.stringify(startAfter),
        Utils.stringify(continuationToken),
        Utils.stringify(nextContinuationToken),
        Utils.stringify(contents));
  }
}
