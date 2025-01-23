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

package io.minio;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Locale;

/** Collection of checksum algorithms. */
public class Checksum {
  // MD5 hash of zero length byte array.
  public static final String ZERO_MD5_HASH = "1B2M2Y8AsgTpgAmY7PhCfg==";
  // SHA-256 hash of zero length byte array.
  public static final String ZERO_SHA256_HASH =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  public static String base64String(byte[] sum) {
    return Base64.getEncoder().encodeToString(sum);
  }

  public static String hexString(byte[] sum) {
    StringBuilder builder = new StringBuilder();
    for (byte b : sum) builder.append(String.format("%02x", b));
    return builder.toString();
  }

  // /** Returns MD5 hash of byte array. */
  // public static String md5Hash(byte[] data, int length) throws NoSuchAlgorithmException {
  //   MessageDigest md5Digest = MessageDigest.getInstance("MD5");
  //   md5Digest.update(data, 0, length);
  //   return Base64.getEncoder().encodeToString(md5Digest.digest());
  // }
  //
  // /** Returns SHA-256 hash of byte array. */
  // public static String sha256Hash(byte[] data, int length) throws NoSuchAlgorithmException {
  //   MessageDigest sha256Digest = MessageDigest.getInstance("SHA-256");
  //   sha256Digest.update((byte[]) data, 0, length);
  //   return BaseEncoding.base16().encode(sha256Digest.digest()).toLowerCase(Locale.US);
  // }
  //
  // /** Returns SHA-256 hash of given string. */
  // public static String sha256Hash(String string) throws NoSuchAlgorithmException {
  //   byte[] data = string.getBytes(StandardCharsets.UTF_8);
  //   return sha256Hash(data, data.length);
  // }

  /** Algorithm type. */
  public static enum Type {
    COMPOSITE,
    FULL_OBJECT;
  }

  /** Checksum algorithm. */
  public static enum Algorithm {
    CRC32,
    CRC32C,
    CRC64NVME,
    SHA1,
    SHA256,
    MD5;

    public String header() {
      if (this == MD5) return "Content-MD5";
      return ("x-amz-checksum-" + this).toLowerCase(Locale.US);
    }

    public boolean compositeSupport() {
      return this != CRC64NVME && this != MD5;
    }

    public Hasher hasher() throws NoSuchAlgorithmException {
      if (this == CRC32) return new CRC32();
      if (this == CRC32C) return new CRC32C();
      if (this == CRC64NVME) return new CRC64NVME();
      if (this == SHA1) return new SHA1();
      if (this == SHA256) return new SHA256();
      if (this == MD5) return new MD5();
      return null;
    }
  }

  public abstract static class Hasher extends OutputStream {
    public abstract void update(byte[] b, int off, int len);

    public abstract byte[] sum();

    public void update(byte[] b) {
      update(b, 0, b.length);
    }

    @Override
    public void write(int b) {
      update(new byte[] {(byte) b});
    }

    @Override
    public void write(byte[] b) {
      update(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      update(b, off, len);
    }
  }

  /** CRC32 checksum is java.util.zip.CRC32 compatible to Hasher. */
  public static class CRC32 extends Hasher {
    private java.util.zip.CRC32 hasher;

    public CRC32() {
      hasher = new java.util.zip.CRC32();
    }

    @Override
    public void update(byte[] b, int off, int len) {
      hasher.update(b, off, len);
    }

    @Override
    public void update(byte[] b) {
      hasher.update(b);
    }

    @Override
    public byte[] sum() {
      int value = (int) hasher.getValue();
      return new byte[] {
        (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
      };
    }

    @Override
    public String toString() {
      return "CRC32{" + hexString(sum()) + "}";
    }
  }

  /** CRC32C checksum. */
  public static class CRC32C extends Hasher implements java.util.zip.Checksum {
    private static final int[] CRC32C_TABLE = new int[256];
    private int crc = 0xFFFFFFFF;

    static {
      for (int i = 0; i < 256; i++) {
        int crc = i;
        for (int j = 0; j < 8; j++) {
          crc = (crc >>> 1) ^ ((crc & 1) != 0 ? 0x82F63B78 : 0);
        }
        CRC32C_TABLE[i] = crc;
      }
    }

    @Override
    public void update(int b) {
      crc = CRC32C_TABLE[(crc ^ b) & 0xFF] ^ (crc >>> 8);
    }

    @Override
    public void update(byte[] b, int off, int len) {
      for (int i = off; i < off + len; i++) {
        update(b[i]);
      }
    }

    @Override
    public long getValue() {
      return (crc ^ 0xFFFFFFFFL) & 0xFFFFFFFFL;
    }

    @Override
    public void reset() {
      crc = 0xFFFFFFFF;
    }

    @Override
    public byte[] sum() {
      int value = (int) this.getValue();
      return new byte[] {
        (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
      };
    }

    @Override
    public String toString() {
      return "CRC32C{" + hexString(sum()) + "}";
    }
  }

  /** CRC64NVME checksum logic copied from https://github.com/minio/crc64nvme. */
  public static class CRC64NVME extends Hasher implements java.util.zip.Checksum {
    private static final long[] CRC64_TABLE = new long[256];
    private static final long[][] SLICING8_TABLE_NVME = new long[8][256];

    static {
      long polynomial = 0x9A6C9329AC4BC9B5L;
      for (int i = 0; i < 256; i++) {
        long crc = i;
        for (int j = 0; j < 8; j++) {
          if ((crc & 1) == 1) {
            crc = (crc >>> 1) ^ polynomial;
          } else {
            crc >>>= 1;
          }
        }
        CRC64_TABLE[i] = crc;
      }

      SLICING8_TABLE_NVME[0] = CRC64_TABLE;
      for (int i = 0; i < 256; i++) {
        long crc = CRC64_TABLE[i];
        for (int j = 1; j < 8; j++) {
          crc = CRC64_TABLE[(int) crc & 0xFF] ^ (crc >>> 8);
          SLICING8_TABLE_NVME[j][i] = crc;
        }
      }
    }

    private long crc = 0;

    public CRC64NVME() {}

    @Override
    public void update(byte[] p, int off, int len) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(p, off, len);
      byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
      int offset = byteBuffer.position();

      crc = ~crc;
      while (p.length >= 64 && (p.length - offset) > 8) {
        long value = byteBuffer.getLong();
        crc ^= value;
        crc =
            SLICING8_TABLE_NVME[7][(int) (crc & 0xFF)]
                ^ SLICING8_TABLE_NVME[6][(int) ((crc >>> 8) & 0xFF)]
                ^ SLICING8_TABLE_NVME[5][(int) ((crc >>> 16) & 0xFF)]
                ^ SLICING8_TABLE_NVME[4][(int) ((crc >>> 24) & 0xFF)]
                ^ SLICING8_TABLE_NVME[3][(int) ((crc >>> 32) & 0xFF)]
                ^ SLICING8_TABLE_NVME[2][(int) ((crc >>> 40) & 0xFF)]
                ^ SLICING8_TABLE_NVME[1][(int) ((crc >>> 48) & 0xFF)]
                ^ SLICING8_TABLE_NVME[0][(int) (crc >>> 56)];
        offset = byteBuffer.position();
      }

      for (; offset < len; offset++) {
        crc = CRC64_TABLE[(int) ((crc ^ (long) p[offset]) & 0xFF)] ^ (crc >>> 8);
      }

      crc = ~crc;
    }

    @Override
    public void update(int b) {
      update(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public long getValue() {
      return crc;
    }

    @Override
    public void reset() {
      crc = 0;
    }

    @Override
    public byte[] sum() {
      long value = this.getValue();
      return new byte[] {
        (byte) (value >>> 56),
        (byte) (value >>> 48),
        (byte) (value >>> 40),
        (byte) (value >>> 32),
        (byte) (value >>> 24),
        (byte) (value >>> 16),
        (byte) (value >>> 8),
        (byte) value
      };
    }

    @Override
    public String toString() {
      return "CRC64NVME{" + hexString(sum()) + "}";
    }
  }

  public static class SHA1 extends Hasher {
    MessageDigest hasher;

    public SHA1() throws NoSuchAlgorithmException {
      this.hasher = MessageDigest.getInstance("SHA-1");
    }

    public void update(byte[] b, int off, int len) {
      hasher.update(b, off, len);
    }

    public void update(byte[] b) {
      hasher.update(b);
    }

    public byte[] sum() {
      return hasher.digest();
    }

    @Override
    public String toString() {
      return "SHA1{" + hexString(sum()) + "}";
    }
  }

  public static class SHA256 extends Hasher {
    MessageDigest hasher;

    public SHA256() throws NoSuchAlgorithmException {
      this.hasher = MessageDigest.getInstance("SHA-256");
    }

    public void update(byte[] b, int off, int len) {
      hasher.update(b, off, len);
    }

    public void update(byte[] b) {
      hasher.update(b);
    }

    public byte[] sum() {
      return hasher.digest();
    }

    @Override
    public String toString() {
      return "SHA256{" + hexString(sum()) + "}";
    }

    public static byte[] sum(byte[] b, int off, int len) throws NoSuchAlgorithmException {
      SHA256 sha256 = new SHA256();
      sha256.update(b, off, len);
      return sha256.sum();
    }

    public static byte[] sum(byte[] b) throws NoSuchAlgorithmException {
      return sum(b, 0, b.length);
    }

    public static byte[] sum(String value) throws NoSuchAlgorithmException {
      return sum(value.getBytes(StandardCharsets.UTF_8));
    }
  }

  public static class MD5 extends Hasher {
    MessageDigest hasher;

    public MD5() throws NoSuchAlgorithmException {
      this.hasher = MessageDigest.getInstance("MD5");
    }

    public void update(byte[] b, int off, int len) {
      hasher.update(b, off, len);
    }

    public void update(byte[] b) {
      hasher.update(b);
    }

    public byte[] sum() {
      return hasher.digest();
    }

    @Override
    public String toString() {
      return "MD5{" + base64String(sum()) + "}";
    }

    public static byte[] sum(byte[] b, int off, int len) throws NoSuchAlgorithmException {
      MD5 md5 = new MD5();
      md5.update(b, off, len);
      return md5.sum();
    }

    public static byte[] sum(byte[] b) throws NoSuchAlgorithmException {
      return sum(b, 0, b.length);
    }
  }
}
