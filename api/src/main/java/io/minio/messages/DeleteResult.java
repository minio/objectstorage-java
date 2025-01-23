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
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html">DeleteObjects
 * API</a>.
 */
@Root(name = "DeleteResult", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class DeleteResult {
  @ElementList(name = "Deleted", inline = true, required = false)
  private List<Deleted> objects;

  @ElementList(name = "Error", inline = true, required = false)
  private List<Error> errors;

  public DeleteResult() {}

  /** Constructs new delete result with an error. */
  public DeleteResult(Error error) {
    this.errors = new LinkedList<Error>();
    this.errors.add(error);
  }

  /** Returns deleted object list. */
  public List<Deleted> objects() {
    return Utils.unmodifiableList(objects);
  }

  /** Returns delete error list. */
  public List<Error> errors() {
    return Utils.unmodifiableList(errors);
  }

  @Override
  public String toString() {
    return String.format(
        "DeleteResult{objects=%s, errors=%s}", Utils.stringify(objects), Utils.stringify(errors));
  }

  @Root(name = "Deleted", strict = false)
  public static class Deleted {
    @Element(name = "Key")
    private String name;

    @Element(name = "VersionId", required = false)
    private String versionId;

    @Element(name = "DeleteMarker", required = false)
    private boolean deleteMarker;

    @Element(name = "DeleteMarkerVersionId", required = false)
    private String deleteMarkerVersionId;

    public Deleted() {}

    public String name() {
      return name;
    }

    public String versionId() {
      return versionId;
    }

    public boolean deleteMarker() {
      return deleteMarker;
    }

    public String deleteMarkerVersionId() {
      return deleteMarkerVersionId;
    }

    @Override
    public String toString() {
      return String.format(
          "Deleted{name=%s, versionId=%s, deleteMarker=%s, deleteMarkerVersionId=%s}",
          Utils.stringify(name),
          Utils.stringify(versionId),
          Utils.stringify(deleteMarker),
          Utils.stringify(deleteMarkerVersionId));
    }
  }

  @Root(name = "Error", strict = false)
  @Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
  public static class Error extends ErrorResponse {
    private static final long serialVersionUID = 1905162041950251407L; // fix SE_BAD_FIELD

    @Override
    public String toString() {
      return String.format("Error{%s}", super.stringify());
    }
  }
}
