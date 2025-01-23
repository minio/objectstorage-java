/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2015 MinIO, Inc.
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
import org.simpleframework.xml.Root;

@Root(name = "Initiator", strict = false)
public class Initiator {
  @Element(name = "ID", required = false)
  private String id;

  @Element(name = "DisplayName", required = false)
  private String displayName;

  public Initiator() {}

  /** Returns initiator ID. */
  public String id() {
    return id;
  }

  /** Returns initiator display name. */
  public String displayName() {
    return displayName;
  }

  @Override
  public String toString() {
    return String.format(
        "Initiator{id=%s, displayName=%s}", Utils.stringify(id), Utils.stringify(displayName));
  }
}
