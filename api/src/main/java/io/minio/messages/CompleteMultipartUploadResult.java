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
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html">CompleteMultipartUpload
 * API</a>.
 */
@Root(name = "CompleteMultipartUploadResult")
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class CompleteMultipartUploadResult extends Checksum {
  @Element(name = "Location")
  private String location;

  @Element(name = "Bucket")
  private String bucket;

  @Element(name = "Key")
  private String object;

  @Element(name = "ETag")
  private String etag;

  public CompleteMultipartUploadResult() {}

  public String location() {
    return location;
  }

  public String bucket() {
    return bucket;
  }

  public String object() {
    return object;
  }

  public String etag() {
    return etag;
  }

  @Override
  public String toString() {
    return String.format(
        "CompleteMultipartUploadResult{location=%s, bucket=%s, object=%s, etag=%s, %s}",
        Utils.stringify(location),
        Utils.stringify(bucket),
        Utils.stringify(object),
        Utils.stringify(etag),
        super.stringify());
  }
}
