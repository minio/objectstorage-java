/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2021 MinIO, Inc.
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
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.convert.Convert;
import org.simpleframework.xml.convert.Converter;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * Helper class to denote access control list of {@link RestoreRequest.S3} and {@link
 * AccessControlPolicy}.
 */
@Root(name = "AccessControlList")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
public class AccessControlList {
  @ElementList(name = "Grant", inline = true)
  private List<Grant> grants;

  public AccessControlList(
      @Nonnull @ElementList(name = "Grant", inline = true) List<Grant> grants) {
    Objects.requireNonNull(grants, "Grants must not be null");
    if (grants.size() == 0) {
      throw new IllegalArgumentException("Grants must not be empty");
    }
    this.grants = Utils.unmodifiableList(grants);
  }

  public List<Grant> grants() {
    return Utils.unmodifiableList(grants);
  }

  @Override
  public String toString() {
    return String.format("AccessControlList{grants=%s}", Utils.stringify(grants));
  }

  @Root(name = "Grant")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class Grant {
    @Element(name = "Grantee", required = false)
    private Grantee grantee;

    @Element(name = "Permission", required = false)
    private Permission permission;

    public Grant(
        @Nullable @Element(name = "Grantee", required = false) Grantee grantee,
        @Nullable @Element(name = "Permission", required = false) Permission permission) {
      if (grantee == null && permission == null) {
        throw new IllegalArgumentException("Either Grantee or Permission must be provided");
      }
      this.grantee = grantee;
      this.permission = permission;
    }

    public Grantee grantee() {
      return grantee;
    }

    public Permission permission() {
      return permission;
    }

    public String granteeUri() {
      return grantee == null ? null : grantee.uri();
    }

    public String granteeId() {
      return grantee == null ? null : grantee.id();
    }

    @Override
    public String toString() {
      return String.format(
          "Grant{grantee=%s, permission=%s}",
          Utils.stringify(grantee), Utils.stringify(permission));
    }
  }

  @Root(name = "Grantee")
  @Namespace(prefix = "xsi", reference = "http://www.w3.org/2001/XMLSchema-instance")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class Grantee {
    @Attribute(name = "type")
    private String xsiType;

    @Element(name = "DisplayName", required = false)
    private String displayName;

    @Element(name = "EmailAddress", required = false)
    private String emailAddress;

    @Element(name = "ID", required = false)
    private String id;

    @Element(name = "Type")
    private Type type;

    @Element(name = "URI", required = false)
    private String uri;

    public Grantee(
        @Nonnull Type type,
        @Nullable String displayName,
        @Nullable String emailAddress,
        @Nullable String id,
        @Nullable String uri) {
      this.type = Objects.requireNonNull(type, "Type must not be null");
      this.displayName = displayName;
      this.emailAddress = emailAddress;
      this.id = id;
      this.uri = uri;
    }

    public Grantee(
        @Nonnull @Attribute(name = "type") String xsiType,
        @Nonnull @Element(name = "Type") Type type,
        @Nullable @Element(name = "DisplayName", required = false) String displayName,
        @Nullable @Element(name = "EmailAddress", required = false) String emailAddress,
        @Nullable @Element(name = "ID", required = false) String id,
        @Nullable @Element(name = "URI", required = false) String uri) {
      this(type, displayName, emailAddress, id, uri);
      this.xsiType = xsiType;
    }

    public String displayName() {
      return displayName;
    }

    public String emailAddress() {
      return emailAddress;
    }

    public String id() {
      return id;
    }

    public Type type() {
      return type;
    }

    public String uri() {
      return uri;
    }

    @Override
    public String toString() {
      return String.format(
          "Grantee{xsiType=%s, displayName=%s, emailAddress=%s, id=%s, type=%s, uri=%s}",
          xsiType,
          Utils.stringify(displayName),
          Utils.stringify(emailAddress),
          Utils.stringify(id),
          Utils.stringify(type),
          Utils.stringify(uri));
    }
  }

  @Root(name = "Type")
  @Convert(Type.TypeConverter.class)
  public static enum Type {
    CANONICAL_USER("CanonicalUser"),
    AMAZON_CUSTOMER_BY_EMAIL("AmazonCustomerByEmail"),
    GROUP("Group");

    private final String value;

    private Type(String value) {
      this.value = value;
    }

    public String toString() {
      return this.value;
    }

    /** Returns Type of given string. */
    public static Type fromString(String granteeTypeString) {
      for (Type granteeType : Type.values()) {
        if (granteeTypeString.equals(granteeType.value)) {
          return granteeType;
        }
      }

      throw new IllegalArgumentException("Unknown grantee type '" + granteeTypeString + "'");
    }

    /** XML converter class. */
    public static class TypeConverter implements Converter<Type> {
      @Override
      public Type read(InputNode node) throws Exception {
        return Type.fromString(node.getValue());
      }

      @Override
      public void write(OutputNode node, Type granteeType) throws Exception {
        node.setValue(granteeType.toString());
      }
    }
  }

  @Root(name = "Permission")
  public static enum Permission {
    FULL_CONTROL,
    WRITE,
    WRITE_ACP,
    READ,
    READ_ACP;
  }
}
