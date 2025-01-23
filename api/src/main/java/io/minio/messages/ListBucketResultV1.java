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
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">ListObjects API</a>.
 */
@Root(name = "ListBucketResult", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class ListBucketResultV1 extends ListObjectsResult {
  @Element(name = "Marker", required = false)
  private String marker;

  @Element(name = "NextMarker", required = false)
  private String nextMarker;

  @ElementList(name = "Contents", inline = true, required = false)
  private List<Contents> contents;

  public String marker() {
    return Utils.urlDecode(marker, encodingType());
  }

  public String nextMarker() {
    return Utils.urlDecode(nextMarker, encodingType());
  }

  @Override
  public List<Contents> contents() {
    return Utils.unmodifiableList(contents);
  }

  @Override
  public String toString() {
    return String.format(
        "ListBucketResultV1{%s, marker=%s, nextMarker=%s, contents=%s}",
        super.toString(),
        Utils.stringify(marker),
        Utils.stringify(nextMarker),
        Utils.stringify(contents));
  }
}
