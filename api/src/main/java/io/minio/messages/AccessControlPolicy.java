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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.minio.Utils;
import java.util.List;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

/**
 * Object representation of response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html">GetObjectAcl
 * API</a>.
 */
@Root(name = "AccessControlPolicy", strict = false)
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
public class AccessControlPolicy {
  @Element(name = "Owner", required = false)
  private Owner owner;

  @Element(name = "AccessControlList", required = false)
  private AccessControlList accessControlList;

  public AccessControlPolicy(
      @Element(name = "Owner", required = false) Owner owner,
      @Element(name = "AccessControlList", required = false) AccessControlList accessControlList) {
    this.owner = owner;
    this.accessControlList = accessControlList;
  }

  public Owner owner() {
    return owner;
  }

  public AccessControlList accessControlList() {
    return accessControlList;
  }

  public String cannedAcl() {
    if (accessControlList == null) return "";

    List<AccessControlList.Grant> grants = accessControlList.grants();
    int size = grants.size();

    if (size < 1 || size > 3) return "";

    for (AccessControlList.Grant grant : grants) {
      if (grant == null) continue;

      String uri = grant.granteeUri();
      if (grant.permission() == AccessControlList.Permission.FULL_CONTROL
          && size == 1
          && "".equals(uri)) {
        return "private";
      } else if (grant.permission() == AccessControlList.Permission.READ && size == 2) {
        if ("http://acs.amazonaws.com/groups/global/AuthenticatedUsers".equals(uri)) {
          return "authenticated-read";
        }
        if ("http://acs.amazonaws.com/groups/global/AllUsers".equals(uri)) return "public-read";
        if (owner.id() != null
            && grant.granteeId() != null
            && owner.id().equals(grant.granteeId())) {
          return "bucket-owner-read";
        }
      } else if (grant.permission() == AccessControlList.Permission.WRITE
          && size == 3
          && "http://acs.amazonaws.com/groups/global/AllUsers".equals(uri)) {
        return "public-read-write";
      }
    }

    return "";
  }

  public Multimap<String, String> grantAcl() {
    Multimap<String, String> map = null;

    if (accessControlList != null) {
      map = HashMultimap.create();
      for (AccessControlList.Grant grant : accessControlList.grants()) {
        if (grant == null) continue;

        String value = "id=" + grant.granteeId();
        if (grant.permission() == AccessControlList.Permission.READ) {
          map.put("X-Amz-Grant-Read", value);
        } else if (grant.permission() == AccessControlList.Permission.WRITE) {
          map.put("X-Amz-Grant-Write", value);
        } else if (grant.permission() == AccessControlList.Permission.READ_ACP) {
          map.put("X-Amz-Grant-Read-Acp", value);
        } else if (grant.permission() == AccessControlList.Permission.WRITE_ACP) {
          map.put("X-Amz-Grant-Write-Acp", value);
        } else if (grant.permission() == AccessControlList.Permission.FULL_CONTROL) {
          map.put("X-Amz-Grant-Full-Control", value);
        }
      }
    }

    return map;
  }

  @Override
  public String toString() {
    return String.format(
        "AccessControlPolicy(owner=%s, accessControlList=%s)",
        Utils.stringify(owner), Utils.stringify(accessControlList));
  }
}
