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
import java.util.List;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html">ListObjectVersions
 * API</a>.
 */
@Root(name = "ListVersionsResult", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class ListVersionsResult extends ListObjectsResult {
  @Element(name = "KeyMarker", required = false)
  private String keyMarker;

  @Element(name = "NextKeyMarker", required = false)
  private String nextKeyMarker;

  @Element(name = "VersionIdMarker", required = false)
  private String versionIdMarker;

  @Element(name = "NextVersionIdMarker", required = false)
  private String nextVersionIdMarker;

  @ElementList(name = "Version", inline = true, required = false)
  private List<Version> contents;

  @ElementList(name = "DeleteMarker", inline = true, required = false)
  private List<DeleteMarker> deleteMarkers;

  public String keyMarker() {
    return Utils.urlDecode(keyMarker, encodingType());
  }

  public String nextKeyMarker() {
    return Utils.urlDecode(nextKeyMarker, encodingType());
  }

  public String versionIdMarker() {
    return versionIdMarker;
  }

  public String nextVersionIdMarker() {
    return nextVersionIdMarker;
  }

  @Override
  public List<Version> contents() {
    return Utils.unmodifiableList(contents);
  }

  @Override
  public List<DeleteMarker> deleteMarkers() {
    return Utils.unmodifiableList(deleteMarkers);
  }

  @Override
  public String toString() {
    return String.format(
        "ListVersionsResult{%s, keyMarker=%s, nextKeyMarker=%s, versionIdMarker=%s,"
            + " nextVersionIdMarker=%s, contents=%s, deleteMarkers=%s}",
        super.toString(),
        Utils.stringify(keyMarker),
        Utils.stringify(nextKeyMarker),
        Utils.stringify(versionIdMarker),
        Utils.stringify(nextVersionIdMarker),
        Utils.stringify(contents),
        Utils.stringify(deleteMarkers));
  }

  @Root(name = "Version", strict = false)
  public static class Version extends Item {
    public Version() {
      super();
    }

    public Version(String prefix) {
      super(prefix);
    }

    @Override
    public String toString() {
      return String.format("Version{%s}", super.toString());
    }
  }

  @Root(name = "DeleteMarker", strict = false)
  public static class DeleteMarker extends Item {
    public DeleteMarker() {
      super();
    }

    public DeleteMarker(String prefix) {
      super(prefix);
    }

    @Override
    public String toString() {
      return String.format("DeleteMarker{%s}", super.toString());
    }
  }
}
