/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2018 MinIO, Inc.
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

package io.minio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;

/** Server-side encryption support. */
public abstract class ServerSideEncryption {
  private static final Map<String, String> emptyHeaders = Utils.unmodifiableMap(null);

  public abstract Map<String, String> headers();

  public boolean tlsRequired() {
    return true;
  }

  public Map<String, String> copySourceHeaders() {
    return emptyHeaders;
  }

  /** S3 type of Server-side encryption. */
  public static class S3 extends ServerSideEncryption {
    private static final Map<String, String> headers;

    static {
      Map<String, String> map = new HashMap<>();
      map.put("X-Amz-Server-Side-Encryption", "AES256");
      headers = Utils.unmodifiableMap(map);
    }

    @Override
    public final Map<String, String> headers() {
      return headers;
    }

    @Override
    public final boolean tlsRequired() {
      return false;
    }

    @Override
    public String toString() {
      return "SSE-S3";
    }
  }

  /** KMS type of Server-side encryption. */
  public static class KMS extends ServerSideEncryption {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, String> headers;

    public KMS(String keyId, Map<String, String> context) throws JsonProcessingException {
      if (keyId == null) {
        throw new IllegalArgumentException("Key ID cannot be null");
      }

      Map<String, String> headers = new HashMap<>();
      headers.put("X-Amz-Server-Side-Encryption", "aws:kms");
      headers.put("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", keyId);
      if (context != null) {
        headers.put(
            "X-Amz-Server-Side-Encryption-Context",
            Base64.getEncoder()
                .encodeToString(
                    objectMapper.writeValueAsString(context).getBytes(StandardCharsets.UTF_8)));
      }

      this.headers = Utils.unmodifiableMap(headers);
    }

    @Override
    public final Map<String, String> headers() {
      return headers;
    }

    @Override
    public String toString() {
      return "SSE-KMS";
    }
  }

  /** Customer-key type of Server-side encryption. */
  public static class CustomerKey extends ServerSideEncryption {
    private boolean isDestroyed = false;
    private final SecretKey secretKey;
    private final Map<String, String> headers;
    private final Map<String, String> copySourceHeaders;

    public CustomerKey(SecretKey key) throws InvalidKeyException, NoSuchAlgorithmException {
      if (key == null || !key.getAlgorithm().equals("AES") || key.getEncoded().length != 32) {
        throw new IllegalArgumentException("Secret key must be 256 bit AES key");
      }

      if (key.isDestroyed()) {
        throw new IllegalArgumentException("Secret key already destroyed");
      }

      this.secretKey = key;

      byte[] keyBytes = key.getEncoded();
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      md5.update(keyBytes);
      String customerKey = BaseEncoding.base64().encode(keyBytes);
      String customerKeyMd5 = BaseEncoding.base64().encode(md5.digest());

      Map<String, String> map = new HashMap<>();
      map.put("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256");
      map.put("X-Amz-Server-Side-Encryption-Customer-Key", customerKey);
      map.put("X-Amz-Server-Side-Encryption-Customer-Key-Md5", customerKeyMd5);
      this.headers = Utils.unmodifiableMap(map);

      map = new HashMap<>();
      map.put("X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm", "AES256");
      map.put("X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key", customerKey);
      map.put("X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5", customerKeyMd5);
      this.copySourceHeaders = Utils.unmodifiableMap(map);
    }

    @Override
    public final Map<String, String> headers() {
      if (isDestroyed) {
        throw new IllegalStateException("Secret key was destroyed");
      }

      return headers;
    }

    @Override
    public final Map<String, String> copySourceHeaders() {
      if (isDestroyed) {
        throw new IllegalStateException("Secret key was destroyed");
      }

      return copySourceHeaders;
    }

    public final void destroy() throws DestroyFailedException {
      secretKey.destroy();
      isDestroyed = true;
    }

    @Override
    public String toString() {
      return "SSE-C";
    }
  }
}
