/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2015-2021 MinIO, Inc.
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

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import com.google.common.io.BaseEncoding;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.minio.BucketExistsArgs;
import io.minio.CloseableIterator;
import io.minio.ComposeObjectArgs;
import io.minio.ComposeSource;
import io.minio.CopyObjectArgs;
import io.minio.CopySource;
import io.minio.DeleteBucketCorsArgs;
import io.minio.DeleteBucketEncryptionArgs;
import io.minio.DeleteBucketLifecycleArgs;
import io.minio.DeleteBucketNotificationArgs;
import io.minio.DeleteBucketPolicyArgs;
import io.minio.DeleteBucketReplicationArgs;
import io.minio.DeleteBucketTagsArgs;
import io.minio.DeleteObjectLockConfigurationArgs;
import io.minio.DeleteObjectTagsArgs;
import io.minio.Directive;
import io.minio.DisableObjectLegalHoldArgs;
import io.minio.DownloadObjectArgs;
import io.minio.EnableObjectLegalHoldArgs;
import io.minio.GetBucketCorsArgs;
import io.minio.GetBucketEncryptionArgs;
import io.minio.GetBucketLifecycleArgs;
import io.minio.GetBucketNotificationArgs;
import io.minio.GetBucketPolicyArgs;
import io.minio.GetBucketReplicationArgs;
import io.minio.GetBucketTagsArgs;
import io.minio.GetBucketVersioningArgs;
import io.minio.GetObjectAclArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectAttributesArgs;
import io.minio.GetObjectAttributesResponse;
import io.minio.GetObjectLockConfigurationArgs;
import io.minio.GetObjectRetentionArgs;
import io.minio.GetObjectTagsArgs;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.Http;
import io.minio.IsObjectLegalHoldEnabledArgs;
import io.minio.ListBucketsArgs;
import io.minio.ListObjectsArgs;
import io.minio.ListenBucketNotificationArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PostPolicy;
import io.minio.PutObjectArgs;
import io.minio.PutObjectFanOutArgs;
import io.minio.PutObjectFanOutEntry;
import io.minio.RemoveBucketArgs;
import io.minio.RemoveObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.SelectObjectContentArgs;
import io.minio.SelectResponseStream;
import io.minio.ServerSideEncryption;
import io.minio.SetBucketCorsArgs;
import io.minio.SetBucketEncryptionArgs;
import io.minio.SetBucketLifecycleArgs;
import io.minio.SetBucketNotificationArgs;
import io.minio.SetBucketPolicyArgs;
import io.minio.SetBucketReplicationArgs;
import io.minio.SetBucketTagsArgs;
import io.minio.SetBucketVersioningArgs;
import io.minio.SetObjectLockConfigurationArgs;
import io.minio.SetObjectRetentionArgs;
import io.minio.SetObjectTagsArgs;
import io.minio.SnowballObject;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.Time;
import io.minio.UploadObjectArgs;
import io.minio.UploadSnowballObjectsArgs;
import io.minio.Xml;
import io.minio.admin.MinioAdminClient;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.AccessControlList;
import io.minio.messages.AccessControlPolicy;
import io.minio.messages.CORSConfiguration;
import io.minio.messages.DeleteRequest;
import io.minio.messages.EventType;
import io.minio.messages.Filter;
import io.minio.messages.InputSerialization;
import io.minio.messages.LifecycleConfiguration;
import io.minio.messages.ListAllMyBucketsResult;
import io.minio.messages.NotificationConfiguration;
import io.minio.messages.NotificationRecords;
import io.minio.messages.ObjectLockConfiguration;
import io.minio.messages.OutputSerialization;
import io.minio.messages.ReplicationConfiguration;
import io.minio.messages.Retention;
import io.minio.messages.RetentionMode;
import io.minio.messages.SseAlgorithm;
import io.minio.messages.SseConfiguration;
import io.minio.messages.Stats;
import io.minio.messages.Status;
import io.minio.messages.Tags;
import io.minio.messages.VersioningConfiguration;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.crypto.KeyGenerator;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import okio.Okio;
import org.junit.Assert;

@SuppressFBWarnings(
    value = "REC",
    justification = "Allow catching super class Exception since it's tests")
public class FunctionalTest {
  private static final String OS = System.getProperty("os.name").toLowerCase(Locale.US);
  private static final String MINIO_BINARY;
  private static final String PASS = "PASS";
  private static final String FAILED = "FAIL";
  private static final String IGNORED = "NA";
  private static final int KB = 1024;
  private static final int MB = 1024 * 1024;
  private static final Random random = new Random(new SecureRandom().nextLong());
  private static final String customContentType = "application/javascript";
  private static String bucketName = getRandomName();
  private static String bucketNameWithLock = getRandomName();
  private static boolean mintEnv = false;
  private static boolean isQuickTest = false;
  private static boolean isRunOnFail = false;
  private static Path dataFile1Kb;
  private static Path dataFile6Mb;
  private static String endpoint;
  private static String endpointTLS;
  private static String accessKey;
  private static String secretKey;
  private static String region;
  private static boolean isSecureEndpoint = false;
  private static String sqsArn = null;
  private static String replicationSrcBucket = null;
  private static String replicationRole = null;
  private static String replicationBucketArn = null;
  private static MinioClient client = null;
  private static TestMinioAdminClient adminClientTests;

  private static ServerSideEncryption.CustomerKey ssec = null;
  private static ServerSideEncryption sseS3 = new ServerSideEncryption.S3();
  private static ServerSideEncryption sseKms = null;

  static {
    String binaryName = "minio";
    if (OS.contains("windows")) {
      binaryName = "minio.exe";
    }

    MINIO_BINARY = binaryName;

    try {
      KeyGenerator keyGen = KeyGenerator.getInstance("AES");
      keyGen.init(256);
      ssec = new ServerSideEncryption.CustomerKey(keyGen.generateKey());
    } catch (InvalidKeyException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static OkHttpClient newHttpClient() {
    try {
      return Http.disableCertCheck(Http.newDefaultClient());
    } catch (KeyManagementException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /** Do no-op. */
  public static void ignore(Object... args) {}

  /** Create given sized file and returns its name. */
  public static String createFile(int size) throws IOException {
    String filename = getRandomName();

    try (OutputStream os = Files.newOutputStream(Paths.get(filename), CREATE, APPEND)) {
      int totalBytesWritten = 0;
      int bytesToWrite = 0;
      byte[] buf = new byte[1 * MB];
      while (totalBytesWritten < size) {
        random.nextBytes(buf);
        bytesToWrite = size - totalBytesWritten;
        if (bytesToWrite > buf.length) {
          bytesToWrite = buf.length;
        }

        os.write(buf, 0, bytesToWrite);
        totalBytesWritten += bytesToWrite;
      }
    }

    return filename;
  }

  /** Create 1 KB temporary file. */
  public static String createFile1Kb() throws IOException {
    if (mintEnv) {
      String filename = getRandomName();
      Files.createSymbolicLink(Paths.get(filename).toAbsolutePath(), dataFile1Kb);
      return filename;
    }

    return createFile(1 * KB);
  }

  /** Create 6 MB temporary file. */
  public static String createFile6Mb() throws IOException {
    if (mintEnv) {
      String filename = getRandomName();
      Files.createSymbolicLink(Paths.get(filename).toAbsolutePath(), dataFile6Mb);
      return filename;
    }

    return createFile(6 * MB);
  }

  /** Generate random name. */
  public static String getRandomName() {
    return "minio-java-test-" + new BigInteger(32, random).toString(32);
  }

  /** Returns byte array contains all data in given InputStream. */
  public static byte[] readAllBytes(InputStream is) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    int nRead;
    byte[] data = new byte[16384];
    while ((nRead = is.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }
    return buffer.toByteArray();
  }

  /** Prints a success log entry in JSON format. */
  public static void mintSuccessLog(String function, String args, long startTime) {
    if (mintEnv) {
      System.out.println(
          new MintLogger(
              function, args, System.currentTimeMillis() - startTime, PASS, null, null, null));
    }
  }

  /** Prints a failure log entry in JSON format. */
  public static void mintFailedLog(
      String function, String args, long startTime, String message, String error) {
    if (mintEnv) {
      System.out.println(
          new MintLogger(
              function,
              args,
              System.currentTimeMillis() - startTime,
              FAILED,
              null,
              message,
              error));
    }
  }

  /** Prints a ignore log entry in JSON format. */
  public static void mintIgnoredLog(String function, String args, long startTime) {
    if (mintEnv) {
      System.out.println(
          new MintLogger(
              function, args, System.currentTimeMillis() - startTime, IGNORED, null, null, null));
    }
  }

  /** Read object content of the given url. */
  public static byte[] readObject(String urlString) throws Exception {
    Request.Builder requestBuilder = new Request.Builder();
    Request request = requestBuilder.url(HttpUrl.parse(urlString)).method("GET", null).build();
    OkHttpClient transport = newHttpClient();
    Response response = transport.newCall(request).execute();

    try {
      if (response.isSuccessful()) {
        return response.body().bytes();
      }

      String errorXml = response.body().string();
      throw new Exception(
          "failed to create object. Response: " + response + ", Response body: " + errorXml);
    } finally {
      response.close();
    }
  }

  /** Write data to given object url. */
  public static void writeObject(String urlString, byte[] dataBytes) throws Exception {
    Request.Builder requestBuilder = new Request.Builder();
    // Set header 'x-amz-acl' to 'bucket-owner-full-control', so objects created
    // anonymously, can be downloaded by bucket owner in AWS S3.
    Request request =
        requestBuilder
            .url(HttpUrl.parse(urlString))
            .method("PUT", RequestBody.create(dataBytes, null))
            .addHeader("x-amz-acl", "bucket-owner-full-control")
            .build();
    OkHttpClient transport = newHttpClient();
    Response response = transport.newCall(request).execute();

    try {
      if (!response.isSuccessful()) {
        String errorXml = response.body().string();
        throw new Exception(
            "failed to create object. Response: " + response + ", Response body: " + errorXml);
      }
    } finally {
      response.close();
    }
  }

  public static String getSha256Sum(InputStream stream, int len) throws Exception {
    MessageDigest sha256Digest = MessageDigest.getInstance("SHA-256");

    // 16KiB buffer for optimization
    byte[] buf = new byte[16384];
    int bytesToRead = buf.length;
    int bytesRead = 0;
    int totalBytesRead = 0;
    while (totalBytesRead < len) {
      if ((len - totalBytesRead) < bytesToRead) {
        bytesToRead = len - totalBytesRead;
      }

      bytesRead = stream.read(buf, 0, bytesToRead);
      Assert.assertFalse(
          "data length mismatch. expected: " + len + ", got: " + totalBytesRead, bytesRead < 0);

      if (bytesRead > 0) {
        sha256Digest.update(buf, 0, bytesRead);
        totalBytesRead += bytesRead;
      }
    }

    return BaseEncoding.base16().encode(sha256Digest.digest()).toLowerCase(Locale.US);
  }

  public static void skipStream(InputStream stream, int len) throws Exception {
    // 16KiB buffer for optimization
    byte[] buf = new byte[16384];
    int bytesToRead = buf.length;
    int bytesRead = 0;
    int totalBytesRead = 0;
    while (totalBytesRead < len) {
      if ((len - totalBytesRead) < bytesToRead) {
        bytesToRead = len - totalBytesRead;
      }

      bytesRead = stream.read(buf, 0, bytesToRead);
      Assert.assertFalse(
          "insufficient data. expected: " + len + ", got: " + totalBytesRead, bytesRead < 0);
      if (bytesRead > 0) {
        totalBytesRead += bytesRead;
      }
    }
  }

  public static void handleException(String methodName, String args, long startTime, Exception e)
      throws Exception {
    if (e instanceof ErrorResponseException) {
      if (((ErrorResponseException) e).errorResponse().code().equals("NotImplemented")) {
        mintIgnoredLog(methodName, args, startTime);
        return;
      }
    }

    if (mintEnv) {
      mintFailedLog(
          methodName,
          args,
          startTime,
          null,
          e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
      if (isRunOnFail) {
        return;
      }
    } else {
      System.out.println("<FAILED> " + methodName + " " + ((args == null) ? "" : args));
    }

    throw e;
  }

  public static void testBucketApi(
      String methodName,
      String testTags,
      MakeBucketArgs args,
      boolean existCheck,
      boolean removeCheck)
      throws Exception {
    long startTime = System.currentTimeMillis();
    try {
      client.makeBucket(args);
      try {
        Assert.assertFalse(
            methodName + " failed after bucket creation",
            existCheck
                && !client.bucketExists(
                    BucketExistsArgs.builder()
                        .bucket(args.bucket())
                        .region(args.region())
                        .build()));
        if (removeCheck) {
          client.removeBucket(
              RemoveBucketArgs.builder().bucket(args.bucket()).region(args.region()).build());
        }
        mintSuccessLog(methodName, null, startTime);
      } finally {
        if (!removeCheck) {
          client.removeBucket(
              RemoveBucketArgs.builder().bucket(args.bucket()).region(args.region()).build());
        }
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void testBucketApiCases(String methodName, boolean existCheck, boolean removeCheck)
      throws Exception {
    testBucketApi(
        methodName,
        "[basic check]",
        MakeBucketArgs.builder().bucket(getRandomName()).build(),
        existCheck,
        removeCheck);

    if (isQuickTest) {
      return;
    }

    testBucketApi(
        methodName,
        "[object lock]",
        MakeBucketArgs.builder().bucket(getRandomName()).objectLock(true).build(),
        existCheck,
        removeCheck);
    testBucketApi(
        methodName,
        "[name contains period]",
        MakeBucketArgs.builder().bucket(getRandomName() + ".withperiod").build(),
        existCheck,
        removeCheck);
  }

  public static void makeBucket() throws Exception {
    String methodName = "makeBucket()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    testBucketApiCases(methodName, false, false);

    if (isQuickTest) {
      return;
    }

    if (!endpoint.contains(".amazonaws.com")) {
      mintIgnoredLog(methodName, "[region]", System.currentTimeMillis());
      mintIgnoredLog(methodName, "[region, object lock]", System.currentTimeMillis());
      return;
    }

    testBucketApi(
        methodName,
        "[region]",
        MakeBucketArgs.builder().bucket(getRandomName()).region("eu-west-1").build(),
        false,
        false);
    testBucketApi(
        methodName,
        "[region, object lock]",
        MakeBucketArgs.builder()
            .bucket(getRandomName())
            .region("eu-central-1")
            .objectLock(true)
            .build(),
        false,
        false);
  }

  public static void listBuckets() throws Exception {
    String methodName = "listBuckets()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    List<String> expectedBucketNames = new LinkedList<>();
    try {
      try {
        String bucketName = getRandomName();
        client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        expectedBucketNames.add(bucketName);

        bucketName = getRandomName();
        client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).objectLock(true).build());
        expectedBucketNames.add(bucketName);

        bucketName = getRandomName() + ".withperiod";
        client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        expectedBucketNames.add(bucketName);

        List<String> bucketNames = new LinkedList<>();
        for (Result<ListAllMyBucketsResult.Bucket> result :
            client.listBuckets(ListBucketsArgs.builder().maxBuckets(1).build())) {
          ListAllMyBucketsResult.Bucket bucket = result.get();
          if (expectedBucketNames.contains(bucket.name())) {
            bucketNames.add(bucket.name());
          }
        }

        Assert.assertTrue(
            "bucket names differ; expected = " + expectedBucketNames + ", got = " + bucketNames,
            expectedBucketNames.containsAll(bucketNames));

        mintSuccessLog(methodName, null, startTime);
      } finally {
        for (String bucketName : expectedBucketNames) {
          client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
        }
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void bucketExists() throws Exception {
    String methodName = "bucketExists()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    testBucketApiCases(methodName, true, false);
  }

  public static void removeBucket() throws Exception {
    String methodName = "removeBucket()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    testBucketApiCases(methodName, false, true);
  }

  public static void setBucketVersioning() throws Exception {
    String methodName = "setBucketVersioning()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String name = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(name).build());
      try {
        client.setBucketVersioning(
            SetBucketVersioningArgs.builder()
                .bucket(name)
                .config(new VersioningConfiguration(VersioningConfiguration.Status.ENABLED, null))
                .build());
        client.setBucketVersioning(
            SetBucketVersioningArgs.builder()
                .bucket(name)
                .config(new VersioningConfiguration(VersioningConfiguration.Status.SUSPENDED, null))
                .build());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(name).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getBucketVersioning() throws Exception {
    String methodName = "getBucketVersioning()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String name = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(name).build());
      try {
        VersioningConfiguration config =
            client.getBucketVersioning(GetBucketVersioningArgs.builder().bucket(name).build());
        Assert.assertEquals(
            "getBucketVersioning(); expected = \"\", got = " + config.status(),
            config.status(),
            VersioningConfiguration.Status.OFF);
        client.setBucketVersioning(
            SetBucketVersioningArgs.builder()
                .bucket(name)
                .config(new VersioningConfiguration(VersioningConfiguration.Status.ENABLED, null))
                .build());
        config = client.getBucketVersioning(GetBucketVersioningArgs.builder().bucket(name).build());
        Assert.assertEquals(
            "getBucketVersioning(); expected = "
                + VersioningConfiguration.Status.ENABLED
                + ", got = "
                + config.status(),
            config.status(),
            VersioningConfiguration.Status.ENABLED);

        client.setBucketVersioning(
            SetBucketVersioningArgs.builder()
                .bucket(name)
                .config(new VersioningConfiguration(VersioningConfiguration.Status.SUSPENDED, null))
                .build());
        config = client.getBucketVersioning(GetBucketVersioningArgs.builder().bucket(name).build());
        Assert.assertEquals(
            "getBucketVersioning(); expected = "
                + VersioningConfiguration.Status.SUSPENDED
                + ", got = "
                + config.status(),
            config.status(),
            VersioningConfiguration.Status.SUSPENDED);
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(name).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void setup() throws Exception {
    long startTime = System.currentTimeMillis();

    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
    } catch (Exception e) {
      handleException("makeBucket()", null, startTime, e);
    }

    try {
      client.makeBucket(
          MakeBucketArgs.builder().bucket(bucketNameWithLock).objectLock(true).build());
    } catch (Exception e) {
      if (e instanceof ErrorResponseException) {
        if (((ErrorResponseException) e).errorResponse().code().equals("NotImplemented")) {
          bucketNameWithLock = null;
          return;
        }
      }

      handleException("makeBucket()", "[object lock]", startTime, e);
    }
  }

  public static void teardown() throws Exception {
    long startTime = System.currentTimeMillis();
    try {
      if (bucketName != null) {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }

      if (bucketNameWithLock != null) {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketNameWithLock).build());
      }
    } catch (Exception e) {
      handleException("removeBucket()", null, startTime, e);
    }
  }

  public static void testUploadObject(String testTags, String filename, String contentType)
      throws Exception {
    String methodName = "uploadObject()";
    long startTime = System.currentTimeMillis();
    try {
      try {
        UploadObjectArgs.Builder builder =
            UploadObjectArgs.builder().bucket(bucketName).object(filename).filename(filename);
        if (contentType != null) {
          builder.contentType(contentType);
        }
        client.uploadObject(builder.build());
        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        Files.delete(Paths.get(filename));
        client.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(filename).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void uploadObject() throws Exception {
    String methodName = "uploadObject()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    testUploadObject("[single upload]", createFile1Kb(), null);

    if (isQuickTest) {
      return;
    }

    testUploadObject("[multi-part upload]", createFile6Mb(), null);
    testUploadObject("[custom content-type]", createFile1Kb(), customContentType);
  }

  public static void testPutObject(String testTags, PutObjectArgs args, String errorCode)
      throws Exception {
    String methodName = "putObject()";
    long startTime = System.currentTimeMillis();
    try {
      ObjectWriteResponse objectInfo = null;
      try {
        objectInfo = client.putObject(args);
      } catch (ErrorResponseException e) {
        if (errorCode == null || !e.errorResponse().code().equals(errorCode)) {
          throw e;
        }
      }
      if (args.retention() != null) {
        client.setObjectRetention(
            SetObjectRetentionArgs.builder()
                .bucket(args.bucket())
                .object(args.object())
                .config(new Retention())
                .bypassGovernanceMode(true)
                .build());
      }
      client.removeObject(
          RemoveObjectArgs.builder()
              .bucket(args.bucket())
              .object(args.object())
              .versionId(objectInfo != null ? objectInfo.versionId() : null)
              .build());

      mintSuccessLog(methodName, testTags, startTime);
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void testThreadedPutObject() throws Exception {
    String methodName = "putObject()";
    String testTags = "[threaded]";
    long startTime = System.currentTimeMillis();
    try {
      int count = 7;
      Thread[] threads = new Thread[count];

      for (int i = 0; i < count; i++) {
        threads[i] = new Thread(new PutObjectRunnable(client, bucketName, createFile6Mb()));
      }

      for (int i = 0; i < count; i++) {
        threads[i].start();
      }

      // Waiting for threads to complete.
      for (int i = 0; i < count; i++) {
        threads[i].join();
      }

      // All threads are completed.
      mintSuccessLog(methodName, testTags, startTime);
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void putObject() throws Exception {
    String methodName = "putObject()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    testPutObject(
        "[single upload]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(1 * KB), 1 * KB, -1)
            .contentType(customContentType)
            .build(),
        null);

    if (isQuickTest) {
      return;
    }

    testPutObject(
        "[multi-part upload]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(11 * MB), 11 * MB, -1)
            .contentType(customContentType)
            .build(),
        null);

    testPutObject(
        "[object name with path segments]",
        PutObjectArgs.builder().bucket(bucketName).object("path/to/" + getRandomName()).stream(
                new ContentInputStream(1 * KB), 1 * KB, -1)
            .contentType(customContentType)
            .build(),
        null);

    testPutObject(
        "[zero sized object]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(0), 0, -1)
            .build(),
        null);

    testPutObject(
        "[object name ends with '/']",
        PutObjectArgs.builder().bucket(bucketName).object("path/to/" + getRandomName() + "/")
            .stream(new ContentInputStream(0), 0, -1)
            .contentType(customContentType)
            .build(),
        null);

    testPutObject(
        "[unknown stream size, single upload]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(1 * KB), -1, PutObjectArgs.MIN_MULTIPART_SIZE)
            .contentType(customContentType)
            .build(),
        null);

    testPutObject(
        "[unknown stream size, multi-part upload]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(11 * MB), -1, PutObjectArgs.MIN_MULTIPART_SIZE)
            .contentType(customContentType)
            .build(),
        null);

    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put("My-Project", "Project One");
    userMetadata.put("My-header1", "    a   b   c  ");
    userMetadata.put("My-Header2", "\"a   b   c\"");
    userMetadata.put("My-Unicode-Tag", "商品");

    testPutObject(
        "[user metadata]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(1 * KB), 1 * KB, -1)
            .userMetadata(userMetadata)
            .build(),
        null);

    Map<String, String> headers = new HashMap<>();

    headers.put("X-Amz-Storage-Class", "REDUCED_REDUNDANCY");
    testPutObject(
        "[storage-class=REDUCED_REDUNDANCY]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(1 * KB), 1 * KB, -1)
            .headers(headers)
            .build(),
        null);

    headers.put("X-Amz-Storage-Class", "STANDARD");
    testPutObject(
        "[storage-class=STANDARD]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(1 * KB), 1 * KB, -1)
            .headers(headers)
            .build(),
        null);

    headers.put("X-Amz-Storage-Class", "INVALID");
    testPutObject(
        "[storage-class=INVALID negative case]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(1 * KB), 1 * KB, -1)
            .headers(headers)
            .build(),
        "InvalidStorageClass");

    testPutObject(
        "[SSE-S3]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(1 * KB), 1 * KB, -1)
            .contentType(customContentType)
            .sse(sseS3)
            .build(),
        null);

    if (bucketNameWithLock != null) {
      testPutObject(
          "[with retention]",
          PutObjectArgs.builder().bucket(bucketNameWithLock).object(getRandomName()).stream(
                  new ContentInputStream(1 * KB), 1 * KB, -1)
              .retention(
                  new Retention(RetentionMode.GOVERNANCE, ZonedDateTime.now(Time.UTC).plusDays(1)))
              .build(),
          null);
    }

    testThreadedPutObject();

    if (!isSecureEndpoint) {
      return;
    }

    testPutObject(
        "[SSE-C single upload]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(1 * KB), 1 * KB, -1)
            .contentType(customContentType)
            .sse(ssec)
            .build(),
        null);

    testPutObject(
        "[SSE-C multi-part upload]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(11 * MB), 11 * MB, -1)
            .contentType(customContentType)
            .sse(ssec)
            .build(),
        null);

    if (sseKms == null) {
      mintIgnoredLog(methodName, null, System.currentTimeMillis());
      return;
    }

    testPutObject(
        "[SSE-KMS]",
        PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                new ContentInputStream(1 * KB), 1 * KB, -1)
            .contentType(customContentType)
            .sse(sseKms)
            .build(),
        null);
  }

  public static void testStatObject(
      String testTags, PutObjectArgs args, StatObjectResponse expectedStat) throws Exception {
    String methodName = "statObject()";
    long startTime = System.currentTimeMillis();
    try {
      client.putObject(args);
      try {
        ServerSideEncryption.CustomerKey ssec = null;
        if (args.sse() instanceof ServerSideEncryption.CustomerKey) {
          ssec = (ServerSideEncryption.CustomerKey) args.sse();
        }
        StatObjectResponse stat =
            client.statObject(
                StatObjectArgs.builder()
                    .bucket(args.bucket())
                    .object(args.object())
                    .ssec(ssec)
                    .build());

        Assert.assertEquals(
            "bucket name: expected = " + expectedStat.bucket() + ", got = " + stat.bucket(),
            expectedStat.bucket(),
            stat.bucket());

        Assert.assertEquals(
            "object name: expected = " + expectedStat.object() + ", got = " + stat.object(),
            expectedStat.object(),
            stat.object());

        Assert.assertEquals(
            "length: expected = " + expectedStat.size() + ", got = " + stat.size(),
            expectedStat.size(),
            stat.size());

        Assert.assertEquals(
            "content-type: expected = "
                + expectedStat.contentType()
                + ", got = "
                + stat.contentType(),
            expectedStat.contentType(),
            stat.contentType());

        for (String key : expectedStat.userMetadata().keySet()) {
          Assert.assertTrue("metadata " + key + " not found", stat.userMetadata().containsKey(key));
          Assert.assertEquals(
              "metadata "
                  + key
                  + " value: expected: "
                  + expectedStat.userMetadata().get(key)
                  + ", got: "
                  + stat.userMetadata().get(key),
              expectedStat.userMetadata().get(key),
              stat.userMetadata().get(key));
        }

        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(args.bucket()).object(args.object()).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void statObject() throws Exception {
    String methodName = "statObject()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    String objectName = getRandomName();

    PutObjectArgs.Builder builder =
        PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
            new ContentInputStream(1024), 1024, -1);
    Headers.Builder headersBuilder =
        new Headers.Builder()
            .add("Content-Type: application/octet-stream")
            .add("Content-Length: 1024")
            .add("Last-Modified", ZonedDateTime.now().format(Time.HTTP_HEADER_DATE_FORMAT));

    testStatObject(
        "[basic check]",
        builder.build(),
        new StatObjectResponse(headersBuilder.build(), bucketName, null, objectName));

    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", customContentType);
    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put("My-Project", "Project One");
    builder = builder.headers(headers).userMetadata(userMetadata);
    builder = builder.stream(new ContentInputStream(1024), 1024, -1);

    StatObjectResponse stat =
        new StatObjectResponse(
            headersBuilder
                .set("Content-Type", customContentType)
                .add("X-Amz-Meta-My-Project: Project One")
                .build(),
            bucketName,
            null,
            objectName);

    testStatObject("[user metadata]", builder.build(), stat);

    if (isQuickTest) {
      return;
    }

    builder = builder.stream(new ContentInputStream(1024), 1024, -1);
    testStatObject("[SSE-S3]", builder.sse(sseS3).build(), stat);

    if (!isSecureEndpoint) {
      mintIgnoredLog(methodName, "[SSE-C]", System.currentTimeMillis());
      return;
    }

    builder = builder.stream(new ContentInputStream(1024), 1024, -1);
    testStatObject("[SSE-C]", builder.sse(ssec).build(), stat);

    if (sseKms == null) {
      mintIgnoredLog(methodName, "[SSE-KMS]", System.currentTimeMillis());
      return;
    }

    builder = builder.stream(new ContentInputStream(1024), 1024, -1);
    testStatObject("[SSE-KMS]", builder.sse(sseKms).build(), stat);
  }

  public static void testGetObject(
      String testTags,
      long objectSize,
      ServerSideEncryption sse,
      GetObjectArgs args,
      int length,
      String sha256sum)
      throws Exception {
    String methodName = "getObject()";
    long startTime = System.currentTimeMillis();
    try {
      PutObjectArgs.Builder builder =
          PutObjectArgs.builder().bucket(args.bucket()).object(args.object()).stream(
              new ContentInputStream(objectSize), objectSize, -1);
      if (sse != null) {
        builder.sse(sse);
      }
      client.putObject(builder.build());

      try (InputStream is = client.getObject(args)) {
        String checksum = getSha256Sum(is, length);
        Assert.assertEquals(
            "checksum mismatch. expected: " + sha256sum + ", got: " + checksum,
            checksum,
            sha256sum);
      }
      mintSuccessLog(methodName, testTags, startTime);
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    } finally {
      client.removeObject(
          RemoveObjectArgs.builder().bucket(args.bucket()).object(args.object()).build());
    }
  }

  public static void getObject() throws Exception {
    String methodName = "getObject()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    testGetObject(
        "[single upload]",
        1 * KB,
        null,
        GetObjectArgs.builder().bucket(bucketName).object(getRandomName()).build(),
        1 * KB,
        getSha256Sum(new ContentInputStream(1 * KB), 1 * KB));

    if (isQuickTest) {
      return;
    }

    InputStream cis = new ContentInputStream(1 * KB);
    skipStream(cis, 1000);
    testGetObject(
        "[single upload, offset]",
        1 * KB,
        null,
        GetObjectArgs.builder().bucket(bucketName).object(getRandomName()).offset(1000L).build(),
        1 * KB - 1000,
        getSha256Sum(cis, 1 * KB - 1000));

    testGetObject(
        "[single upload, length]",
        1 * KB,
        null,
        GetObjectArgs.builder().bucket(bucketName).object(getRandomName()).length(256L).build(),
        256,
        getSha256Sum(new ContentInputStream(1 * KB), 256));

    cis = new ContentInputStream(1 * KB);
    skipStream(cis, 1000);
    testGetObject(
        "[single upload, offset, length]",
        1 * KB,
        null,
        GetObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .offset(1000L)
            .length(24L)
            .build(),
        24,
        getSha256Sum(cis, 24));

    cis = new ContentInputStream(1 * KB);
    skipStream(cis, 1000);
    testGetObject(
        "[single upload, offset, length beyond available]",
        1 * KB,
        null,
        GetObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .offset(1000L)
            .length(30L)
            .build(),
        24,
        getSha256Sum(cis, 24));

    testGetObject(
        "[multi-part upload]",
        6 * MB,
        null,
        GetObjectArgs.builder().bucket(bucketName).object(getRandomName()).build(),
        6 * MB,
        getSha256Sum(new ContentInputStream(6 * MB), 6 * MB));

    cis = new ContentInputStream(6 * MB);
    skipStream(cis, 1000);
    testGetObject(
        "[multi-part upload, offset, length]",
        6 * MB,
        null,
        GetObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .offset(1000L)
            .length(64 * 1024L)
            .build(),
        64 * KB,
        getSha256Sum(cis, 64 * 1024));

    cis = new ContentInputStream(0);
    testGetObject(
        "[zero sized object]",
        0,
        null,
        GetObjectArgs.builder().bucket(bucketName).object(getRandomName()).build(),
        0,
        getSha256Sum(cis, 0));

    if (!isSecureEndpoint) {
      return;
    }

    testGetObject(
        "[single upload, SSE-C]",
        1 * KB,
        ssec,
        GetObjectArgs.builder().bucket(bucketName).object(getRandomName()).ssec(ssec).build(),
        1 * KB,
        getSha256Sum(new ContentInputStream(1 * KB), 1 * KB));
  }

  public static void testDownloadObject(
      String testTags, int objectSize, ServerSideEncryption sse, DownloadObjectArgs args)
      throws Exception {
    String methodName = "downloadObject()";
    long startTime = System.currentTimeMillis();
    try {
      PutObjectArgs.Builder builder =
          PutObjectArgs.builder().bucket(args.bucket()).object(args.object()).stream(
              new ContentInputStream(objectSize), objectSize, -1);
      if (sse != null) {
        builder.sse(sse);
      }
      client.putObject(builder.build());
      client.downloadObject(args);
      Files.delete(Paths.get(args.filename()));
      mintSuccessLog(methodName, testTags, startTime);
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    } finally {
      client.removeObject(
          RemoveObjectArgs.builder().bucket(args.bucket()).object(args.object()).build());
    }
  }

  public static void downloadObject() throws Exception {
    String methodName = "downloadObject()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    String objectName = getRandomName();
    testDownloadObject(
        "[single upload]",
        1 * KB,
        null,
        DownloadObjectArgs.builder()
            .bucket(bucketName)
            .object(objectName)
            .filename(objectName + ".downloaded")
            .build());

    if (isQuickTest) {
      return;
    }

    String baseName = getRandomName();
    objectName = "path/to/" + baseName;
    testDownloadObject(
        "[single upload with multiple path segments]",
        1 * KB,
        null,
        DownloadObjectArgs.builder()
            .bucket(bucketName)
            .object(objectName)
            .filename(baseName + ".downloaded")
            .build());

    if (!isSecureEndpoint) {
      return;
    }

    objectName = getRandomName();
    testDownloadObject(
        "[single upload, SSE-C]",
        1 * KB,
        ssec,
        DownloadObjectArgs.builder()
            .bucket(bucketName)
            .object(objectName)
            .ssec(ssec)
            .filename(objectName + ".downloaded")
            .build());
  }

  public static List<ObjectWriteResponse> createObjects(String bucketName, int count, int versions)
      throws Exception {
    List<ObjectWriteResponse> results = new LinkedList<>();
    for (int i = 0; i < count; i++) {
      String objectName = getRandomName();
      results.add(
          client.putObject(
              PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
                      new ContentInputStream(1), 1, -1)
                  .build()));
      if (versions > 1) {
        for (int j = 0; j < versions - 1; j++) {
          results.add(
              client.putObject(
                  PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
                          new ContentInputStream(1), 1, -1)
                      .build()));
        }
      }
    }

    return results;
  }

  public static void removeObjects(String bucketName, List<ObjectWriteResponse> results)
      throws Exception {
    List<DeleteRequest.Object> objects =
        results.stream()
            .map(
                result -> {
                  return new DeleteRequest.Object(result.object(), result.versionId());
                })
            .collect(Collectors.toList());
    for (Result<?> r :
        client.removeObjects(
            RemoveObjectsArgs.builder().bucket(bucketName).objects(objects).build())) {
      ignore(r.get());
    }
  }

  public static void testListObjects(
      String testTags, ListObjectsArgs args, int objCount, int versions) throws Exception {
    String methodName = "listObjects()";
    long startTime = System.currentTimeMillis();
    String bucketName = args.bucket();
    List<ObjectWriteResponse> results = null;
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        if (versions > 0) {
          client.setBucketVersioning(
              SetBucketVersioningArgs.builder()
                  .bucket(bucketName)
                  .config(new VersioningConfiguration(VersioningConfiguration.Status.ENABLED, null))
                  .build());
        }

        results = createObjects(bucketName, objCount, versions);

        int i = 0;
        for (Result<?> r : client.listObjects(args)) {
          r.get();
          i++;
        }

        if (versions > 0) {
          objCount *= versions;
        }

        Assert.assertEquals("object count; expected=" + objCount + ", got=" + i, i, objCount);
        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        if (results != null) {
          removeObjects(bucketName, results);
        }
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void listObjects() throws Exception {
    if (!mintEnv) {
      System.out.println("listObjects()");
    }

    testListObjects("[bucket]", ListObjectsArgs.builder().bucket(getRandomName()).build(), 3, 0);

    testListObjects(
        "[bucket, prefix]",
        ListObjectsArgs.builder().bucket(getRandomName()).prefix("minio").build(),
        3,
        0);

    testListObjects(
        "[bucket, prefix, recursive]",
        ListObjectsArgs.builder().bucket(getRandomName()).prefix("minio").recursive(true).build(),
        3,
        0);

    testListObjects(
        "[bucket, versions]",
        ListObjectsArgs.builder().bucket(getRandomName()).includeVersions(true).build(),
        3,
        2);

    if (isQuickTest) {
      return;
    }

    testListObjects(
        "[empty bucket]", ListObjectsArgs.builder().bucket(getRandomName()).build(), 0, 0);

    testListObjects(
        "[bucket, prefix, recursive, 1050 objects]",
        ListObjectsArgs.builder().bucket(getRandomName()).prefix("minio").recursive(true).build(),
        1050,
        0);

    testListObjects(
        "[bucket, apiVersion1]",
        ListObjectsArgs.builder().bucket(getRandomName()).useApiVersion1(true).build(),
        3,
        0);
  }

  public static void testRemoveObject(
      String testTags, ServerSideEncryption sse, RemoveObjectArgs args) throws Exception {
    String methodName = "removeObject()";
    long startTime = System.currentTimeMillis();
    try {
      PutObjectArgs.Builder builder =
          PutObjectArgs.builder().bucket(args.bucket()).object(args.object()).stream(
              new ContentInputStream(1), 1, -1);
      if (sse != null) {
        builder.sse(sse);
      }
      client.putObject(builder.build());
      client.removeObject(args);
      mintSuccessLog(methodName, testTags, startTime);
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void removeObject() throws Exception {
    String methodName = "removeObject()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    testRemoveObject(
        "[base check]",
        null,
        RemoveObjectArgs.builder().bucket(bucketName).object(getRandomName()).build());
    testRemoveObject(
        "[multiple path segments]",
        null,
        RemoveObjectArgs.builder().bucket(bucketName).object("path/to/" + getRandomName()).build());

    if (isQuickTest) {
      return;
    }

    testRemoveObject(
        "[SSE-S3]",
        sseS3,
        RemoveObjectArgs.builder().bucket(bucketName).object(getRandomName()).build());

    if (!isSecureEndpoint) {
      mintIgnoredLog(methodName, "[SSE-C]", System.currentTimeMillis());
      mintIgnoredLog(methodName, "[SSE-KMS]", System.currentTimeMillis());
      return;
    }

    testRemoveObject(
        "[SSE-C]",
        ssec,
        RemoveObjectArgs.builder().bucket(bucketName).object(getRandomName()).build());

    if (sseKms == null) {
      mintIgnoredLog(methodName, "[SSE-KMS]", System.currentTimeMillis());
      return;
    }

    testRemoveObject(
        "[SSE-KMS]",
        sseKms,
        RemoveObjectArgs.builder().bucket(bucketName).object(getRandomName()).build());
  }

  public static void testRemoveObjects(String testTags, List<ObjectWriteResponse> results)
      throws Exception {
    String methodName = "removeObjects()";
    long startTime = System.currentTimeMillis();
    try {
      removeObjects(bucketName, results);
      mintSuccessLog(methodName, testTags, startTime);
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    } finally {
      removeObjects(bucketName, results);
    }
  }

  public static void removeObjects() throws Exception {
    String methodName = "removeObjects()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    testRemoveObjects("[basic]", createObjects(bucketName, 3, 0));

    String testTags = "[3005 objects]";
    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    List<ObjectWriteResponse> results = new LinkedList<>();
    for (int i = 0; i < 3004; i++) {
      results.add(
          new ObjectWriteResponse(null, bucketName, null, objectName + "-" + i, null, null));
    }
    List<ObjectWriteResponse> existingObject = createObjects(bucketName, 1, 0);
    results.addAll(existingObject);
    testRemoveObjects(testTags, results);
    try {
      client.statObject(
          StatObjectArgs.builder()
              .bucket(bucketName)
              .object(existingObject.get(0).object())
              .build());
      handleException(
          methodName,
          testTags,
          startTime,
          new Exception("object " + existingObject.get(0).object() + " still exist"));
    } catch (ErrorResponseException e) {
      if (!e.errorResponse().code().equals("NoSuchKey")) {
        throw e;
      }
    }
  }

  public static void testGetPresignedUrl(GetPresignedObjectUrlArgs args, String expectedChecksum)
      throws Exception {
    String urlString = client.getPresignedObjectUrl(args);
    byte[] data = readObject(urlString);
    String checksum = getSha256Sum(new ByteArrayInputStream(data), data.length);
    Assert.assertEquals(
        "content checksum differs; expected = " + expectedChecksum + ", got = " + checksum,
        expectedChecksum,
        checksum);
  }

  public static void testGetPresignedObjectUrlForGet() throws Exception {
    String methodName = "getPresignedObjectUrl()";
    String testTags = null;
    long startTime = System.currentTimeMillis();
    try {
      String expectedChecksum = getSha256Sum(new ContentInputStream(1 * KB), 1 * KB);
      String objectName = getRandomName();
      client.putObject(
          PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
                  new ContentInputStream(1 * KB), 1 * KB, -1)
              .build());

      try {
        testTags = "[GET]";
        testGetPresignedUrl(
            GetPresignedObjectUrlArgs.builder()
                .method(Http.Method.GET)
                .bucket(bucketName)
                .object(objectName)
                .build(),
            expectedChecksum);

        testTags = "[GET, expiry]";
        testGetPresignedUrl(
            GetPresignedObjectUrlArgs.builder()
                .method(Http.Method.GET)
                .bucket(bucketName)
                .object(objectName)
                .expiry(1, TimeUnit.DAYS)
                .build(),
            expectedChecksum);

        testTags = "[GET, expiry, query params]";
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("response-content-type", "application/json");
        testGetPresignedUrl(
            GetPresignedObjectUrlArgs.builder()
                .method(Http.Method.GET)
                .bucket(bucketName)
                .object(objectName)
                .expiry(1, TimeUnit.DAYS)
                .extraQueryParams(queryParams)
                .build(),
            expectedChecksum);

        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void testPutPresignedUrl(
      String testTags, byte[] data, String expectedChecksum, GetPresignedObjectUrlArgs args)
      throws Exception {
    String methodName = "getPresignedObjectUrl()";
    long startTime = System.currentTimeMillis();
    try {
      String urlString = client.getPresignedObjectUrl(args);
      try {
        writeObject(urlString, data);
        InputStream is =
            client.getObject(
                GetObjectArgs.builder().bucket(args.bucket()).object(args.object()).build());
        data = readAllBytes(is);
        String checksum = getSha256Sum(new ByteArrayInputStream(data), data.length);
        Assert.assertEquals(
            "content checksum differs; expected = " + expectedChecksum + ", got = " + checksum,
            expectedChecksum,
            checksum);
        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(args.bucket()).object(args.object()).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void testGetPresignedObjectUrlForPut() throws Exception {
    byte[] data = "hello, world".getBytes(StandardCharsets.UTF_8);
    String expectedChecksum = getSha256Sum(new ByteArrayInputStream(data), data.length);
    String objectName = getRandomName();

    testPutPresignedUrl(
        "[PUT]",
        data,
        expectedChecksum,
        GetPresignedObjectUrlArgs.builder()
            .method(Http.Method.PUT)
            .bucket(bucketName)
            .object(objectName)
            .build());

    testPutPresignedUrl(
        "[PUT, expiry]",
        data,
        expectedChecksum,
        GetPresignedObjectUrlArgs.builder()
            .method(Http.Method.PUT)
            .bucket(bucketName)
            .object(objectName)
            .expiry(1, TimeUnit.DAYS)
            .build());
  }

  public static void getPresignedObjectUrl() throws Exception {
    if (!mintEnv) {
      System.out.println("getPresignedObjectUrl()");
    }

    testGetPresignedObjectUrlForGet();
    testGetPresignedObjectUrlForPut();
  }

  public static void getPresignedPostFormData() throws Exception {
    String methodName = "getPresignedPostFormData()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    try {
      String objectName = getRandomName();

      PostPolicy policy = new PostPolicy(bucketName, ZonedDateTime.now().plusDays(7));
      policy.addEqualsCondition("key", objectName);
      policy.addEqualsCondition("content-type", "image/png");
      policy.addContentLengthRangeCondition(1 * MB, 4 * MB);
      Map<String, String> formData = client.getPresignedPostFormData(policy);

      MultipartBody.Builder multipartBuilder = new MultipartBody.Builder();
      multipartBuilder.setType(MultipartBody.FORM);
      for (Map.Entry<String, String> entry : formData.entrySet()) {
        multipartBuilder.addFormDataPart(entry.getKey(), entry.getValue());
      }
      multipartBuilder.addFormDataPart("key", objectName);
      multipartBuilder.addFormDataPart("Content-Type", "image/png");
      multipartBuilder.addFormDataPart(
          "file",
          objectName,
          RequestBody.create(readAllBytes(new ContentInputStream(1 * MB)), null));

      Request.Builder requestBuilder = new Request.Builder();
      String urlString =
          client.getPresignedObjectUrl(
              GetPresignedObjectUrlArgs.builder()
                  .method(Http.Method.GET)
                  .bucket(bucketName)
                  .object("x")
                  .build());
      urlString = urlString.split("\\?")[0]; // Remove query parameters.
      // remove last two characters to get clean url string of bucket.
      urlString = urlString.substring(0, urlString.length() - 2);
      Request request = requestBuilder.url(urlString).post(multipartBuilder.build()).build();
      OkHttpClient transport = newHttpClient();
      Response response = transport.newCall(request).execute();
      Assert.assertNotNull("no response from server", response);

      try {
        if (!response.isSuccessful()) {
          String errorXml = response.body().string();
          throw new Exception(
              "failed to upload object. Response: " + response + ", Error: " + errorXml);
        }
      } finally {
        response.close();
      }
      client.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void testCopyObject(
      String testTags, ServerSideEncryption sse, CopyObjectArgs args, boolean negativeCase)
      throws Exception {
    String methodName = "copyObject()";
    long startTime = System.currentTimeMillis();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(args.source().bucket()).build());
      try {
        PutObjectArgs.Builder builder =
            PutObjectArgs.builder().bucket(args.source().bucket()).object(args.source().object())
                .stream(new ContentInputStream(1 * KB), 1 * KB, -1);
        if (sse != null) {
          builder.sse(sse);
        }
        client.putObject(builder.build());

        if (negativeCase) {
          try {
            client.copyObject(args);
          } catch (ErrorResponseException e) {
            if (!e.errorResponse().code().equals("PreconditionFailed")) {
              throw e;
            }
          }
        } else {
          client.copyObject(args);

          ServerSideEncryption.CustomerKey ssec = null;
          if (sse instanceof ServerSideEncryption.CustomerKey) {
            ssec = (ServerSideEncryption.CustomerKey) sse;
          }
          client.statObject(
              StatObjectArgs.builder()
                  .bucket(args.bucket())
                  .object(args.object())
                  .ssec(ssec)
                  .build());
        }
        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder()
                .bucket(args.source().bucket())
                .object(args.source().object())
                .build());
        client.removeObject(
            RemoveObjectArgs.builder().bucket(args.bucket()).object(args.object()).build());
        client.removeBucket(RemoveBucketArgs.builder().bucket(args.source().bucket()).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void testCopyObjectMatchETag() throws Exception {
    String methodName = "copyObject()";
    String testTags = "[match etag]";
    long startTime = System.currentTimeMillis();
    String srcBucketName = getRandomName();
    String srcObjectName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(srcBucketName).build());
      try {
        ObjectWriteResponse result =
            client.putObject(
                PutObjectArgs.builder().bucket(srcBucketName).object(srcObjectName).stream(
                        new ContentInputStream(1 * KB), 1 * KB, -1)
                    .build());

        client.copyObject(
            CopyObjectArgs.builder()
                .bucket(bucketName)
                .object(srcObjectName + "-copy")
                .source(
                    CopySource.builder()
                        .bucket(srcBucketName)
                        .object(srcObjectName)
                        .matchETag(result.etag())
                        .build())
                .build());

        client.statObject(
            StatObjectArgs.builder().bucket(bucketName).object(srcObjectName + "-copy").build());

        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(srcBucketName).object(srcObjectName).build());
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(srcObjectName + "-copy").build());
        client.removeBucket(RemoveBucketArgs.builder().bucket(srcBucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void testCopyObjectMetadataReplace() throws Exception {
    String methodName = "copyObject()";
    String testTags = "[metadata replace]";
    long startTime = System.currentTimeMillis();
    String srcBucketName = getRandomName();
    String srcObjectName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(srcBucketName).build());
      try {
        client.putObject(
            PutObjectArgs.builder().bucket(srcBucketName).object(srcObjectName).stream(
                    new ContentInputStream(1 * KB), 1 * KB, -1)
                .build());

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", customContentType);
        headers.put("X-Amz-Meta-My-Project", "Project One");
        client.copyObject(
            CopyObjectArgs.builder()
                .bucket(bucketName)
                .object(srcObjectName + "-copy")
                .source(CopySource.builder().bucket(srcBucketName).object(srcObjectName).build())
                .headers(headers)
                .metadataDirective(Directive.REPLACE)
                .build());

        StatObjectResponse stat =
            client.statObject(
                StatObjectArgs.builder()
                    .bucket(bucketName)
                    .object(srcObjectName + "-copy")
                    .build());
        Assert.assertEquals(
            "content type differs. expected: " + customContentType + ", got: " + stat.contentType(),
            customContentType,
            stat.contentType());
        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(srcBucketName).object(srcObjectName).build());
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(srcObjectName + "-copy").build());
        client.removeBucket(RemoveBucketArgs.builder().bucket(srcBucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void testCopyObjectEmptyMetadataReplace() throws Exception {
    String methodName = "copyObject()";
    String testTags = "[empty metadata replace]";
    long startTime = System.currentTimeMillis();
    String srcBucketName = getRandomName();
    String srcObjectName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(srcBucketName).build());
      try {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", customContentType);
        headers.put("X-Amz-Meta-My-Project", "Project One");
        client.putObject(
            PutObjectArgs.builder().bucket(srcBucketName).object(srcObjectName).stream(
                    new ContentInputStream(1 * KB), 1 * KB, -1)
                .headers(headers)
                .build());

        client.copyObject(
            CopyObjectArgs.builder()
                .bucket(bucketName)
                .object(srcObjectName + "-copy")
                .source(CopySource.builder().bucket(srcBucketName).object(srcObjectName).build())
                .metadataDirective(Directive.REPLACE)
                .build());

        StatObjectResponse stat =
            client.statObject(
                StatObjectArgs.builder()
                    .bucket(bucketName)
                    .object(srcObjectName + "-copy")
                    .build());
        Assert.assertFalse(
            "expected user metadata to be removed in new object",
            stat.userMetadata().containsKey("My-Project"));
        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(srcBucketName).object(srcObjectName).build());
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(srcObjectName + "-copy").build());
        client.removeBucket(RemoveBucketArgs.builder().bucket(srcBucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void copyObject() throws Exception {
    String methodName = "copyObject()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    String objectName = getRandomName();
    testCopyObject(
        "[basic check]",
        null,
        CopyObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .source(CopySource.builder().bucket(getRandomName()).object(objectName).build())
            .build(),
        false);

    if (isQuickTest) {
      return;
    }

    testCopyObject(
        "[negative match etag]",
        null,
        CopyObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .source(
                CopySource.builder()
                    .bucket(getRandomName())
                    .object(getRandomName())
                    .matchETag("invalid-etag")
                    .build())
            .build(),
        true);

    testCopyObjectMatchETag();

    testCopyObject(
        "[not match etag]",
        null,
        CopyObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .source(
                CopySource.builder()
                    .bucket(getRandomName())
                    .object(getRandomName())
                    .notMatchETag("not-etag-of-source-object")
                    .build())
            .build(),
        false);

    testCopyObject(
        "[modified since]",
        null,
        CopyObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .source(
                CopySource.builder()
                    .bucket(getRandomName())
                    .object(getRandomName())
                    .modifiedSince(ZonedDateTime.of(2015, 05, 3, 3, 10, 10, 0, Time.UTC))
                    .build())
            .build(),
        false);

    testCopyObject(
        "[negative unmodified since]",
        null,
        CopyObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .source(
                CopySource.builder()
                    .bucket(getRandomName())
                    .object(getRandomName())
                    .unmodifiedSince(ZonedDateTime.of(2015, 05, 3, 3, 10, 10, 0, Time.UTC))
                    .build())
            .build(),
        true);

    testCopyObjectMetadataReplace();
    testCopyObjectEmptyMetadataReplace();

    testCopyObject(
        "[SSE-S3]",
        sseS3,
        CopyObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sse(sseS3)
            .source(CopySource.builder().bucket(getRandomName()).object(getRandomName()).build())
            .build(),
        false);

    if (!isSecureEndpoint) {
      mintIgnoredLog(methodName, "[SSE-C]", System.currentTimeMillis());
      mintIgnoredLog(methodName, "[SSE-KMS]", System.currentTimeMillis());
      return;
    }

    testCopyObject(
        "[SSE-C]",
        ssec,
        CopyObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sse(ssec)
            .source(
                CopySource.builder()
                    .bucket(getRandomName())
                    .object(getRandomName())
                    .ssec(ssec)
                    .build())
            .build(),
        false);

    if (sseKms == null) {
      mintIgnoredLog(methodName, "[SSE-KMS]", System.currentTimeMillis());
      return;
    }

    testCopyObject(
        "[SSE-KMS]",
        sseKms,
        CopyObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sse(sseKms)
            .source(CopySource.builder().bucket(getRandomName()).object(getRandomName()).build())
            .build(),
        false);
  }

  public static void testComposeObject(String testTags, ComposeObjectArgs args) throws Exception {
    String methodName = "composeObject()";
    long startTime = System.currentTimeMillis();
    try {
      client.composeObject(args);
      client.removeObject(
          RemoveObjectArgs.builder().bucket(args.bucket()).object(args.object()).build());
      mintSuccessLog(methodName, testTags, startTime);
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static List<ComposeSource> createComposeSourceList(ComposeSource... sources) {
    return Arrays.asList(sources);
  }

  public static void composeObjectTests(String object1Mb, String object6Mb, String object6MbSsec)
      throws Exception {
    testComposeObject(
        "[single source]",
        ComposeObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sources(
                createComposeSourceList(
                    ComposeSource.builder().bucket(bucketName).object(object1Mb).build()))
            .build());

    testComposeObject(
        "[single source with offset]",
        ComposeObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sources(
                createComposeSourceList(
                    ComposeSource.builder()
                        .bucket(bucketName)
                        .object(object1Mb)
                        .offset(2L * KB)
                        .build()))
            .build());

    testComposeObject(
        "[single source with offset and length]",
        ComposeObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sources(
                createComposeSourceList(
                    ComposeSource.builder()
                        .bucket(bucketName)
                        .object(object1Mb)
                        .offset(2L * KB)
                        .length(5L * KB)
                        .build()))
            .build());

    testComposeObject(
        "[single multipart source]",
        ComposeObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sources(
                createComposeSourceList(
                    ComposeSource.builder().bucket(bucketName).object(object6Mb).build()))
            .build());

    testComposeObject(
        "[two multipart source]",
        ComposeObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sources(
                createComposeSourceList(
                    ComposeSource.builder().bucket(bucketName).object(object6Mb).build(),
                    ComposeSource.builder().bucket(bucketName).object(object6Mb).build()))
            .build());

    testComposeObject(
        "[two multipart sources with offset and length]",
        ComposeObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sources(
                createComposeSourceList(
                    ComposeSource.builder()
                        .bucket(bucketName)
                        .object(object6Mb)
                        .offset(10L)
                        .length(6291436L)
                        .build(),
                    ComposeSource.builder().bucket(bucketName).object(object6Mb).build()))
            .build());

    if (isQuickTest) {
      return;
    }

    if (!isSecureEndpoint) {
      return;
    }

    testComposeObject(
        "[two SSE-C multipart sources]",
        ComposeObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sse(ssec)
            .sources(
                createComposeSourceList(
                    ComposeSource.builder()
                        .bucket(bucketName)
                        .object(object6MbSsec)
                        .ssec(ssec)
                        .build(),
                    ComposeSource.builder()
                        .bucket(bucketName)
                        .object(object6MbSsec)
                        .ssec(ssec)
                        .build()))
            .build());

    testComposeObject(
        "[two multipart sources with one SSE-C]",
        ComposeObjectArgs.builder()
            .bucket(bucketName)
            .object(getRandomName())
            .sources(
                createComposeSourceList(
                    ComposeSource.builder()
                        .bucket(bucketName)
                        .object(object6MbSsec)
                        .ssec(ssec)
                        .build(),
                    ComposeSource.builder().bucket(bucketName).object(object6Mb).build()))
            .build());
  }

  public static void composeObject() throws Exception {
    String methodName = "composeObject()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    List<ObjectWriteResponse> createdObjects = new LinkedList<>();

    try {
      String object1Mb = null;
      String object6Mb = null;
      String object6MbSsec = null;
      try {
        ObjectWriteResponse response;
        response =
            client.putObject(
                PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                        new ContentInputStream(1 * MB), 1 * MB, -1)
                    .build());
        createdObjects.add(response);
        object1Mb = response.object();

        response =
            client.putObject(
                PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                        new ContentInputStream(6 * MB), 6 * MB, -1)
                    .build());
        createdObjects.add(response);
        object6Mb = response.object();

        if (isSecureEndpoint) {
          response =
              client.putObject(
                  PutObjectArgs.builder().bucket(bucketName).object(getRandomName()).stream(
                          new ContentInputStream(6 * MB), 6 * MB, -1)
                      .sse(ssec)
                      .build());
          createdObjects.add(response);
          object6MbSsec = response.object();
        }
      } catch (Exception e) {
        handleException(methodName, null, startTime, e);
      }

      composeObjectTests(object1Mb, object6Mb, object6MbSsec);
    } finally {
      removeObjects(bucketName, createdObjects);
    }
  }

  public static void checkObjectLegalHold(String bucketName, String objectName, boolean enableCheck)
      throws Exception {
    if (enableCheck) {
      client.enableObjectLegalHold(
          EnableObjectLegalHoldArgs.builder().bucket(bucketName).object(objectName).build());
    } else {
      client.disableObjectLegalHold(
          DisableObjectLegalHoldArgs.builder().bucket(bucketName).object(objectName).build());
    }

    boolean result =
        client.isObjectLegalHoldEnabled(
            IsObjectLegalHoldEnabledArgs.builder().bucket(bucketName).object(objectName).build());
    Assert.assertEquals(
        "object legal hold: expected: " + enableCheck + ", got: " + result, result, enableCheck);
  }

  public static void enableObjectLegalHold() throws Exception {
    if (bucketNameWithLock == null) return;

    String methodName = "enableObjectLegalHold()";
    if (!mintEnv) {
      System.out.println(methodName);
    }
    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    ObjectWriteResponse objectInfo = null;
    try {
      try {
        objectInfo =
            client.putObject(
                PutObjectArgs.builder().bucket(bucketNameWithLock).object(objectName).stream(
                        new ContentInputStream(1 * KB), 1 * KB, -1)
                    .build());

        checkObjectLegalHold(bucketNameWithLock, objectName, true);
        client.disableObjectLegalHold(
            DisableObjectLegalHoldArgs.builder()
                .bucket(bucketNameWithLock)
                .object(objectName)
                .build());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        if (objectInfo != null) {
          client.removeObject(
              RemoveObjectArgs.builder()
                  .bucket(bucketNameWithLock)
                  .object(objectName)
                  .versionId(objectInfo.versionId())
                  .build());
        }
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void disableObjectLegalHold() throws Exception {
    if (bucketNameWithLock == null) return;

    String methodName = "disableObjectLegalHold()";
    if (!mintEnv) {
      System.out.println(methodName);
    }
    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    ObjectWriteResponse objectInfo = null;
    try {
      try {
        objectInfo =
            client.putObject(
                PutObjectArgs.builder().bucket(bucketNameWithLock).object(objectName).stream(
                        new ContentInputStream(1 * KB), 1 * KB, -1)
                    .build());

        checkObjectLegalHold(bucketNameWithLock, objectName, false);
        client.enableObjectLegalHold(
            EnableObjectLegalHoldArgs.builder()
                .bucket(bucketNameWithLock)
                .object(objectName)
                .build());
        checkObjectLegalHold(bucketNameWithLock, objectName, false);
        mintSuccessLog(methodName, null, startTime);
      } finally {
        if (objectInfo != null) {
          client.removeObject(
              RemoveObjectArgs.builder()
                  .bucket(bucketNameWithLock)
                  .object(objectName)
                  .versionId(objectInfo.versionId())
                  .build());
        }
      }
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void isObjectLegalHoldEnabled() throws Exception {
    if (bucketNameWithLock == null) return;

    String methodName = "isObjectLegalHoldEnabled()";
    if (!mintEnv) {
      System.out.println(methodName);
    }
    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    ObjectWriteResponse objectInfo = null;
    try {
      try {
        objectInfo =
            client.putObject(
                PutObjectArgs.builder().bucket(bucketNameWithLock).object(objectName).stream(
                        new ContentInputStream(1 * KB), 1 * KB, -1)
                    .build());

        boolean result =
            client.isObjectLegalHoldEnabled(
                IsObjectLegalHoldEnabledArgs.builder()
                    .bucket(bucketNameWithLock)
                    .object(objectName)
                    .build());
        Assert.assertFalse("object legal hold: expected: false, got: " + result, result);
        checkObjectLegalHold(bucketNameWithLock, objectName, true);
        checkObjectLegalHold(bucketNameWithLock, objectName, false);
        mintSuccessLog(methodName, null, startTime);
      } finally {
        if (objectInfo != null) {
          client.removeObject(
              RemoveObjectArgs.builder()
                  .bucket(bucketNameWithLock)
                  .object(objectName)
                  .versionId(objectInfo.versionId())
                  .build());
        }
      }
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void setObjectLockConfiguration() throws Exception {
    String methodName = "setObjectLockConfiguration()";
    String testTags = "[COMPLIANCE, 10 days]";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).objectLock(true).build());
      try {
        ObjectLockConfiguration config =
            new ObjectLockConfiguration(
                RetentionMode.COMPLIANCE, new ObjectLockConfiguration.RetentionDurationDays(10));
        client.setObjectLockConfiguration(
            SetObjectLockConfigurationArgs.builder().bucket(bucketName).config(config).build());
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
      mintSuccessLog(methodName, testTags, startTime);
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void testGetObjectLockConfiguration(
      String bucketName, RetentionMode mode, ObjectLockConfiguration.RetentionDuration duration)
      throws Exception {
    ObjectLockConfiguration expectedConfig = new ObjectLockConfiguration(mode, duration);
    client.setObjectLockConfiguration(
        SetObjectLockConfigurationArgs.builder().bucket(bucketName).config(expectedConfig).build());
    ObjectLockConfiguration config =
        client.getObjectLockConfiguration(
            GetObjectLockConfigurationArgs.builder().bucket(bucketName).build());
    Assert.assertEquals(
        "retention mode: expected: " + expectedConfig.mode() + ", got: " + config.mode(),
        config.mode(),
        expectedConfig.mode());
    Assert.assertFalse(
        "retention duration: " + expectedConfig.duration() + ", got: " + config.duration(),
        config.duration().unit() != expectedConfig.duration().unit()
            || config.duration().duration() != expectedConfig.duration().duration());
  }

  public static void getObjectLockConfiguration() throws Exception {
    String methodName = "getObjectLockConfiguration()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).objectLock(true).build());
      try {
        testGetObjectLockConfiguration(
            bucketName,
            RetentionMode.COMPLIANCE,
            new ObjectLockConfiguration.RetentionDurationDays(10));
        testGetObjectLockConfiguration(
            bucketName,
            RetentionMode.GOVERNANCE,
            new ObjectLockConfiguration.RetentionDurationYears(1));
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }

      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void deleteObjectLockConfiguration() throws Exception {
    String methodName = "deleteObjectLockConfiguration()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).objectLock(true).build());
      try {
        client.deleteObjectLockConfiguration(
            DeleteObjectLockConfigurationArgs.builder().bucket(bucketName).build());
        ObjectLockConfiguration config =
            new ObjectLockConfiguration(
                RetentionMode.COMPLIANCE, new ObjectLockConfiguration.RetentionDurationDays(10));
        client.setObjectLockConfiguration(
            SetObjectLockConfigurationArgs.builder().bucket(bucketName).config(config).build());
        client.deleteObjectLockConfiguration(
            DeleteObjectLockConfigurationArgs.builder().bucket(bucketName).build());
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void setObjectRetention() throws Exception {
    if (bucketNameWithLock == null) return;

    String methodName = "setObjectRetention()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    ObjectWriteResponse objectInfo = null;
    try {
      try {
        objectInfo =
            client.putObject(
                PutObjectArgs.builder().bucket(bucketNameWithLock).object(objectName).stream(
                        new ContentInputStream(1 * KB), 1 * KB, -1)
                    .build());

        client.setObjectRetention(
            SetObjectRetentionArgs.builder()
                .bucket(bucketNameWithLock)
                .object(objectName)
                .config(
                    new Retention(
                        RetentionMode.GOVERNANCE, ZonedDateTime.now(Time.UTC).plusDays(1)))
                .build());

        client.setObjectRetention(
            SetObjectRetentionArgs.builder()
                .bucket(bucketNameWithLock)
                .object(objectName)
                .config(new Retention())
                .bypassGovernanceMode(true)
                .build());
      } finally {
        if (objectInfo != null) {
          client.removeObject(
              RemoveObjectArgs.builder()
                  .bucket(bucketNameWithLock)
                  .object(objectName)
                  .versionId(objectInfo.versionId())
                  .build());
        }
      }
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void testGetObjectRetention(SetObjectRetentionArgs args) throws Exception {
    client.setObjectRetention(args);
    Retention config =
        client.getObjectRetention(
            GetObjectRetentionArgs.builder().bucket(args.bucket()).object(args.object()).build());

    if (args.config().mode() == null) {
      Assert.assertFalse(
          "retention mode: expected: <null>, got: " + config.mode(),
          config != null && config.mode() != null);
    } else {
      Assert.assertEquals(
          "retention mode: expected: " + args.config().mode() + ", got: " + config.mode(),
          args.config().mode(),
          config.mode());
    }

    ZonedDateTime expectedDate = args.config().retainUntilDate();
    ZonedDateTime date = (config == null) ? null : config.retainUntilDate();

    if (expectedDate == null) {
      Assert.assertNull("retention retain-until-date: expected: <null>, got: " + date, date);
    } else {
      Assert.assertEquals(
          "retention retain-until-date: expected: "
              + expectedDate.withNano(0)
              + ", got: "
              + date.withNano(0),
          date.withNano(0),
          expectedDate.withNano(0));
    }
  }

  public static void getObjectRetention() throws Exception {
    if (bucketNameWithLock == null) return;

    String methodName = "getObjectRetention()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    ObjectWriteResponse objectInfo = null;
    try {
      try {
        objectInfo =
            client.putObject(
                PutObjectArgs.builder().bucket(bucketNameWithLock).object(objectName).stream(
                        new ContentInputStream(1 * KB), 1 * KB, -1)
                    .build());

        testGetObjectRetention(
            SetObjectRetentionArgs.builder()
                .bucket(bucketNameWithLock)
                .object(objectName)
                .config(
                    new Retention(
                        RetentionMode.GOVERNANCE, ZonedDateTime.now(Time.UTC).plusDays(3)))
                .build());

        // Check shortening retention until period
        testGetObjectRetention(
            SetObjectRetentionArgs.builder()
                .bucket(bucketNameWithLock)
                .object(objectName)
                .config(
                    new Retention(
                        RetentionMode.GOVERNANCE, ZonedDateTime.now(Time.UTC).plusDays(1)))
                .bypassGovernanceMode(true)
                .build());

        // Check empty retention.
        // Enable below test when minio server release has a fix.
        // testGetObjectRetention(
        //     SetObjectRetentionArgs.builder()
        //         .bucket(bucketNameWithLock)
        //         .object(objectName)
        //         .config(new Retention())
        //         .bypassGovernanceMode(true)
        //         .build());
      } finally {
        if (objectInfo != null) {
          client.removeObject(
              RemoveObjectArgs.builder()
                  .bucket(bucketNameWithLock)
                  .object(objectName)
                  .versionId(objectInfo.versionId())
                  .bypassGovernanceMode(true)
                  .build());
        }
      }
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getBucketPolicy() throws Exception {
    String methodName = "getBucketPolicy()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        String config =
            client.getBucketPolicy(GetBucketPolicyArgs.builder().bucket(bucketName).build());
        Assert.assertTrue("policy: expected: \"\", got: " + config, config.isEmpty());
        String policy =
            "{'Version':'2012-10-17','Statement':[{'Action':['s3:GetObject'],'Effect':'Allow',"
                + "'Principal':{'AWS':['*']},'Resource':['arn:aws:s3:::"
                + bucketName
                + "/myobject*'],'Sid':''}]}";
        policy = policy.replaceAll("'", "\"");
        client.setBucketPolicy(
            SetBucketPolicyArgs.builder().bucket(bucketName).config(policy).build());
        client.getBucketPolicy(GetBucketPolicyArgs.builder().bucket(bucketName).build());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void setBucketPolicy() throws Exception {
    String methodName = "setBucketPolicy()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        String policy =
            "{'Version':'2012-10-17','Statement':[{'Action':['s3:GetObject'],'Effect':'Allow',"
                + "'Principal':{'AWS':['*']},'Resource':['arn:aws:s3:::"
                + bucketName
                + "/myobject*'],'Sid':''}]}";
        policy = policy.replaceAll("'", "\"");
        client.setBucketPolicy(
            SetBucketPolicyArgs.builder().bucket(bucketName).config(policy).build());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void deleteBucketPolicy() throws Exception {
    String methodName = "deleteBucketPolicy()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        client.deleteBucketPolicy(DeleteBucketPolicyArgs.builder().bucket(bucketName).build());

        String policy =
            "{'Version':'2012-10-17','Statement':[{'Action':['s3:GetObject'],'Effect':'Allow',"
                + "'Principal':{'AWS':['*']},'Resource':['arn:aws:s3:::"
                + bucketName
                + "/myobject*'],'Sid':''}]}";
        policy = policy.replaceAll("'", "\"");
        client.setBucketPolicy(
            SetBucketPolicyArgs.builder().bucket(bucketName).config(policy).build());
        client.deleteBucketPolicy(DeleteBucketPolicyArgs.builder().bucket(bucketName).build());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void testSetBucketLifecycle(String bucketName, LifecycleConfiguration.Rule... rules)
      throws Exception {
    LifecycleConfiguration config = new LifecycleConfiguration(Arrays.asList(rules));
    client.setBucketLifecycle(
        SetBucketLifecycleArgs.builder().bucket(bucketName).config(config).build());
  }

  public static void setBucketLifecycle() throws Exception {
    String methodName = "setBucketLifecycle()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        testSetBucketLifecycle(
            bucketName,
            new LifecycleConfiguration.Rule(
                Status.ENABLED,
                null,
                new LifecycleConfiguration.Expiration((ZonedDateTime) null, 365, null),
                new Filter("logs/"),
                "rule2",
                null,
                null,
                null));
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void deleteBucketLifecycle() throws Exception {
    String methodName = "deleteBucketLifecycle()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        client.deleteBucketLifecycle(
            DeleteBucketLifecycleArgs.builder().bucket(bucketName).build());
        testSetBucketLifecycle(
            bucketName,
            new LifecycleConfiguration.Rule(
                Status.ENABLED,
                null,
                new LifecycleConfiguration.Expiration((ZonedDateTime) null, 365, null),
                new Filter("logs/"),
                "rule2",
                null,
                null,
                null));
        client.deleteBucketLifecycle(
            DeleteBucketLifecycleArgs.builder().bucket(bucketName).build());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getBucketLifecycle() throws Exception {
    String methodName = "getBucketLifecycle()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        LifecycleConfiguration config =
            client.getBucketLifecycle(GetBucketLifecycleArgs.builder().bucket(bucketName).build());
        Assert.assertNull("config: expected: <null>, got: <non-null>", config);
        testSetBucketLifecycle(
            bucketName,
            new LifecycleConfiguration.Rule(
                Status.ENABLED,
                null,
                new LifecycleConfiguration.Expiration((ZonedDateTime) null, 365, null),
                new Filter("logs/"),
                "rule2",
                null,
                null,
                null));
        config =
            client.getBucketLifecycle(GetBucketLifecycleArgs.builder().bucket(bucketName).build());
        Assert.assertNotNull("config: expected: <non-null>, got: <null>", config);
        List<LifecycleConfiguration.Rule> rules = config.rules();
        Assert.assertEquals(
            "config.rules().size(): expected: 1, got: " + config.rules().size(),
            1,
            config.rules().size());
        LifecycleConfiguration.Rule rule = rules.get(0);
        Assert.assertEquals(
            "rule.status(): expected: " + Status.ENABLED + ", got: " + rule.status(),
            rule.status(),
            Status.ENABLED);
        Assert.assertNotNull(
            "rule.expiration(): expected: <non-null>, got: <null>", rule.expiration());
        Assert.assertEquals(
            "rule.expiration().days(): expected: 365, got: " + rule.expiration().days(),
            rule.expiration().days(),
            Integer.valueOf(365));
        Assert.assertNotNull("rule.filter(): expected: <non-null>, got: <null>", rule.filter());
        Assert.assertEquals(
            "rule.filter().prefix(): expected: logs/, got: " + rule.filter().prefix(),
            "logs/",
            rule.filter().prefix());
        Assert.assertEquals("rule.id(): expected: rule2, got: " + rule.id(), "rule2", rule.id());

        testSetBucketLifecycle(
            bucketName,
            new LifecycleConfiguration.Rule(
                Status.ENABLED,
                null,
                new LifecycleConfiguration.Expiration((ZonedDateTime) null, 365, null),
                new Filter(""),
                null,
                null,
                null,
                null));
        config =
            client.getBucketLifecycle(GetBucketLifecycleArgs.builder().bucket(bucketName).build());
        Assert.assertNotNull("config: expected: <non-null>, got: <null>", config);
        Assert.assertEquals(
            "config.rules().size(): expected: 1, got: " + config.rules().size(),
            config.rules().size(),
            1);
        Assert.assertNotNull(
            "rule.filter(): expected: <non-null>, got: <null>", config.rules().get(0).filter());
        Assert.assertEquals(
            "rule.filter().prefix(): expected: <empty>, got: "
                + config.rules().get(0).filter().prefix(),
            "",
            config.rules().get(0).filter().prefix());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void setBucketNotification() throws Exception {
    String methodName = "setBucketNotification()";
    long startTime = System.currentTimeMillis();
    if (sqsArn == null) {
      mintIgnoredLog(methodName, null, startTime);
      return;
    }

    if (!mintEnv) {
      System.out.println(methodName);
    }

    try {
      String bucketName = getRandomName();
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).region(region).build());
      try {
        NotificationConfiguration config =
            new NotificationConfiguration(
                null,
                Arrays.asList(
                    new NotificationConfiguration.QueueConfiguration[] {
                      new NotificationConfiguration.QueueConfiguration(
                          sqsArn,
                          null,
                          Arrays.asList(
                              new String[] {
                                EventType.OBJECT_CREATED_PUT.toString(),
                                EventType.OBJECT_CREATED_COPY.toString()
                              }),
                          new NotificationConfiguration.Filter("images", "pg"))
                    }),
                null,
                null);
        client.setBucketNotification(
            SetBucketNotificationArgs.builder().bucket(bucketName).config(config).build());
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getBucketNotification() throws Exception {
    String methodName = "getBucketNotification()";
    long startTime = System.currentTimeMillis();
    if (sqsArn == null) {
      mintIgnoredLog(methodName, null, startTime);
      return;
    }

    if (!mintEnv) {
      System.out.println(methodName);
    }

    try {
      String bucketName = getRandomName();
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).region(region).build());
      try {
        NotificationConfiguration expectedConfig =
            new NotificationConfiguration(
                null,
                Arrays.asList(
                    new NotificationConfiguration.QueueConfiguration[] {
                      new NotificationConfiguration.QueueConfiguration(
                          sqsArn,
                          null,
                          Arrays.asList(new String[] {EventType.OBJECT_CREATED_PUT.toString()}),
                          new NotificationConfiguration.Filter("images", "pg"))
                    }),
                null,
                null);
        client.setBucketNotification(
            SetBucketNotificationArgs.builder().bucket(bucketName).config(expectedConfig).build());

        NotificationConfiguration config =
            client.getBucketNotification(
                GetBucketNotificationArgs.builder().bucket(bucketName).build());

        if (config.queueConfigurations().size() != 1
            || !sqsArn.equals(config.queueConfigurations().get(0).queue())
            || config.queueConfigurations().get(0).events().size() != 1
            || !EventType.OBJECT_CREATED_PUT
                .toString()
                .equals(config.queueConfigurations().get(0).events().get(0))) {
          System.out.println(
              "config: expected: " + Xml.marshal(expectedConfig) + ", got: " + Xml.marshal(config));
        }
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void deleteBucketNotification() throws Exception {
    String methodName = "deleteBucketNotification()";
    long startTime = System.currentTimeMillis();
    if (sqsArn == null) {
      mintIgnoredLog(methodName, null, startTime);
      return;
    }

    if (!mintEnv) {
      System.out.println(methodName);
    }

    try {
      String bucketName = getRandomName();
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).region(region).build());
      try {
        NotificationConfiguration config =
            new NotificationConfiguration(
                null,
                Arrays.asList(
                    new NotificationConfiguration.QueueConfiguration[] {
                      new NotificationConfiguration.QueueConfiguration(
                          sqsArn,
                          null,
                          Arrays.asList(
                              new String[] {
                                EventType.OBJECT_CREATED_PUT.toString(),
                                EventType.OBJECT_CREATED_COPY.toString()
                              }),
                          new NotificationConfiguration.Filter("images", "pg"))
                    }),
                null,
                null);
        client.setBucketNotification(
            SetBucketNotificationArgs.builder().bucket(bucketName).config(config).build());

        client.deleteBucketNotification(
            DeleteBucketNotificationArgs.builder().bucket(bucketName).build());

        config =
            client.getBucketNotification(
                GetBucketNotificationArgs.builder().bucket(bucketName).build());
        if (config.queueConfigurations().size() != 0) {
          System.out.println("config: expected: <empty>, got: " + Xml.marshal(config));
        }
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void listenBucketNotification() throws Exception {
    String methodName = "listenBucketNotification()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String file = createFile1Kb();
    String bucketName = getRandomName();
    CloseableIterator<Result<NotificationRecords>> ci = null;
    String mintArgs =
        "prefix=prefix, suffix=suffix, events={\"s3:ObjectCreated:*\", \"s3:ObjectAccessed:*\"}";
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).region(region).build());

      String[] events = {"s3:ObjectCreated:*", "s3:ObjectAccessed:*"};
      ci =
          client.listenBucketNotification(
              ListenBucketNotificationArgs.builder()
                  .bucket(bucketName)
                  .prefix("prefix")
                  .suffix("suffix")
                  .events(events)
                  .build());

      client.putObject(
          PutObjectArgs.builder().bucket(bucketName).object("prefix-random-suffix").stream(
                  new ContentInputStream(1 * KB), 1 * KB, -1)
              .build());

      while (ci.hasNext()) {
        NotificationRecords records = ci.next().get();
        if (records.events().size() == 0) {
          continue;
        }

        boolean found = false;
        for (NotificationRecords.Event event : records.events()) {
          if ("prefix-random-suffix".equals(event.object().key())) {
            found = true;
            break;
          }
        }

        if (found) {
          break;
        }
      }

      mintSuccessLog(methodName, mintArgs, startTime);
    } catch (Exception e) {
      handleException(methodName, mintArgs, startTime, e);
    } finally {
      if (ci != null) {
        ci.close();
      }

      Files.delete(Paths.get(file));
      client.removeObject(
          RemoveObjectArgs.builder().bucket(bucketName).object("prefix-random-suffix").build());
      client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
    }
  }

  public static void selectObjectContent() throws Exception {
    String methodName = "selectObjectContent()";
    String sqlExpression = "select * from S3Object";
    String testArgs = "[sqlExpression: " + sqlExpression + ", requestProgress: true]";

    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    SelectResponseStream responseStream = null;
    try {
      String expectedResult =
          "1997,Ford,E350,\"ac, abs, moon\",3000.00\n"
              + "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00\n"
              + "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n"
              + "1996,Jeep,Grand Cherokee,\"MUST SELL!\n"
              + "air, moon roof, loaded\",4799.00\n";
      byte[] data =
          ("Year,Make,Model,Description,Price\n" + expectedResult).getBytes(StandardCharsets.UTF_8);
      ByteArrayInputStream bais = new ByteArrayInputStream(data);
      client.putObject(
          PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
                  bais, data.length, -1)
              .build());

      InputSerialization is =
          InputSerialization.newCSV(
              null, false, null, null, InputSerialization.FileHeaderInfo.USE, null, null, null);
      OutputSerialization os =
          OutputSerialization.newCSV(
              null, null, null, OutputSerialization.QuoteFields.ASNEEDED, null);

      responseStream =
          client.selectObjectContent(
              SelectObjectContentArgs.builder()
                  .bucket(bucketName)
                  .object(objectName)
                  .sqlExpression(sqlExpression)
                  .inputSerialization(is)
                  .outputSerialization(os)
                  .requestProgress(true)
                  .build());

      String result = new String(readAllBytes(responseStream), StandardCharsets.UTF_8);
      Assert.assertEquals(
          "result mismatch; expected: " + expectedResult + ", got: " + result,
          result,
          expectedResult);

      Stats stats = responseStream.stats();
      Assert.assertNotNull("stats is null", stats);
      Assert.assertTrue(
          "stats.bytesScanned mismatch; expected: 258, got: " + stats.bytesScanned(),
          stats.bytesScanned() == 256);
      Assert.assertTrue(
          "stats.bytesProcessed mismatch; expected: 258, got: " + stats.bytesProcessed(),
          stats.bytesProcessed() == 256);
      Assert.assertTrue(
          "stats.bytesReturned mismatch; expected: 222, got: " + stats.bytesReturned(),
          stats.bytesReturned() == 222);
      mintSuccessLog(methodName, testArgs, startTime);
    } catch (Exception e) {
      handleException(methodName, testArgs, startTime, e);
    } finally {
      if (responseStream != null) {
        responseStream.close();
      }
      client.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
    }
  }

  public static void setBucketEncryption() throws Exception {
    String methodName = "setBucketEncryption()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        client.setBucketEncryption(
            SetBucketEncryptionArgs.builder()
                .bucket(bucketName)
                .config(SseConfiguration.newConfigWithSseS3Rule())
                .build());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getBucketEncryption() throws Exception {
    String methodName = "getBucketEncryption()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        SseConfiguration config =
            client.getBucketEncryption(
                GetBucketEncryptionArgs.builder().bucket(bucketName).build());
        Assert.assertNull("rule: expected: <null>, got: <non-null>", config.rule());
        client.setBucketEncryption(
            SetBucketEncryptionArgs.builder()
                .bucket(bucketName)
                .config(SseConfiguration.newConfigWithSseS3Rule())
                .build());
        config =
            client.getBucketEncryption(
                GetBucketEncryptionArgs.builder().bucket(bucketName).build());
        Assert.assertNotNull("rule: expected: <non-null>, got: <null>", config.rule());
        Assert.assertEquals(
            "sse algorithm: expected: "
                + SseAlgorithm.AES256
                + ", got: "
                + config.rule().sseAlgorithm(),
            config.rule().sseAlgorithm(),
            SseAlgorithm.AES256);
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void deleteBucketEncryption() throws Exception {
    String methodName = "deleteBucketEncryption()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        client.deleteBucketEncryption(
            DeleteBucketEncryptionArgs.builder().bucket(bucketName).build());

        client.setBucketEncryption(
            SetBucketEncryptionArgs.builder()
                .bucket(bucketName)
                .config(SseConfiguration.newConfigWithSseS3Rule())
                .build());
        client.deleteBucketEncryption(
            DeleteBucketEncryptionArgs.builder().bucket(bucketName).build());
        SseConfiguration config =
            client.getBucketEncryption(
                GetBucketEncryptionArgs.builder().bucket(bucketName).build());
        Assert.assertNull("rule: expected: <null>, got: <non-null>", config.rule());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void testBucketCors(String methodName, boolean getTest, boolean deleteTest)
      throws Exception {
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        CORSConfiguration expectedConfig =
            new CORSConfiguration(
                Arrays.asList(
                    new CORSConfiguration.CORSRule[] {
                      // Rule 1
                      new CORSConfiguration.CORSRule(
                          Arrays.asList(new String[] {"*"}), // Allowed headers
                          Arrays.asList(new String[] {"PUT", "POST", "DELETE"}), // Allowed methods
                          Arrays.asList(new String[] {"http://www.example.com"}), // Allowed origins
                          Arrays.asList(
                              new String[] {"x-amz-server-side-encryption"}), // Expose headers
                          null, // ID
                          3000), // Maximum age seconds
                      // Rule 2
                      new CORSConfiguration.CORSRule(
                          null, // Allowed headers
                          Arrays.asList(new String[] {"GET"}), // Allowed methods
                          Arrays.asList(new String[] {"*"}), // Allowed origins
                          null, // Expose headers
                          null, // ID
                          null // Maximum age seconds
                          )
                    }));
        client.setBucketCors(
            SetBucketCorsArgs.builder().bucket(bucketName).config(expectedConfig).build());
        if (getTest) {
          CORSConfiguration config =
              client.getBucketCors(GetBucketCorsArgs.builder().bucket(bucketName).build());
          Assert.assertEquals(
              "cors: expected: " + expectedConfig + ", got: " + config, expectedConfig, config);
        }
        if (deleteTest) {
          client.deleteBucketCors(DeleteBucketCorsArgs.builder().bucket(bucketName).build());
        }
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void setBucketCors() throws Exception {
    testBucketCors("setBucketCors()", false, false);
  }

  public static void getBucketCors() throws Exception {
    testBucketCors("getBucketCors()", true, false);
  }

  public static void deleteBucketCors() throws Exception {
    testBucketCors("deleteBucketCors()", false, true);
  }

  public static void setBucketTags() throws Exception {
    String methodName = "setBucketTags()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        Map<String, String> map = new HashMap<>();
        map.put("Project", "Project One");
        map.put("User", "jsmith");
        client.setBucketTags(SetBucketTagsArgs.builder().bucket(bucketName).tags(map).build());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getBucketTags() throws Exception {
    String methodName = "getBucketTags()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        Map<String, String> map = new HashMap<>();
        Tags tags = client.getBucketTags(GetBucketTagsArgs.builder().bucket(bucketName).build());
        Assert.assertEquals("tags: expected: " + map + ", got: " + tags.get(), map, tags.get());

        map.put("Project", "Project One");
        map.put("User", "jsmith");
        client.setBucketTags(SetBucketTagsArgs.builder().bucket(bucketName).tags(map).build());
        tags = client.getBucketTags(GetBucketTagsArgs.builder().bucket(bucketName).build());
        Assert.assertEquals("tags: expected: " + map + ", got: " + tags.get(), map, tags.get());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void deleteBucketTags() throws Exception {
    String methodName = "deleteBucketTags()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String bucketName = getRandomName();
    try {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      try {
        client.deleteBucketTags(DeleteBucketTagsArgs.builder().bucket(bucketName).build());

        Map<String, String> map = new HashMap<>();
        map.put("Project", "Project One");
        map.put("User", "jsmith");
        client.setBucketTags(SetBucketTagsArgs.builder().bucket(bucketName).tags(map).build());
        client.deleteBucketTags(DeleteBucketTagsArgs.builder().bucket(bucketName).build());
        Tags tags = client.getBucketTags(GetBucketTagsArgs.builder().bucket(bucketName).build());
        Assert.assertTrue("tags: expected: <empty>" + ", got: " + tags.get(), tags.get().isEmpty());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void setObjectTags() throws Exception {
    String methodName = "setObjectTags()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    try {
      try {
        client.putObject(
            PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
                    new ContentInputStream(1 * KB), 1 * KB, -1)
                .build());
        Map<String, String> map = new HashMap<>();
        map.put("Project", "Project One");
        map.put("User", "jsmith");
        client.setObjectTags(
            SetObjectTagsArgs.builder().bucket(bucketName).object(objectName).tags(map).build());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getObjectTags() throws Exception {
    String methodName = "getObjectTags()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    try {
      try {
        client.putObject(
            PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
                    new ContentInputStream(1 * KB), 1 * KB, -1)
                .build());
        Map<String, String> map = new HashMap<>();
        Tags tags =
            client.getObjectTags(
                GetObjectTagsArgs.builder().bucket(bucketName).object(objectName).build());
        Assert.assertEquals("tags: expected: " + map + ", got: " + tags.get(), map, tags.get());

        map.put("Project", "Project One");
        map.put("User", "jsmith");
        client.setObjectTags(
            SetObjectTagsArgs.builder().bucket(bucketName).object(objectName).tags(map).build());
        tags =
            client.getObjectTags(
                GetObjectTagsArgs.builder().bucket(bucketName).object(objectName).build());
        Assert.assertEquals("tags: expected: " + map + ", got: " + tags.get(), map, tags.get());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void deleteObjectTags() throws Exception {
    String methodName = "deleteObjectTags()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    try {
      try {
        client.putObject(
            PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
                    new ContentInputStream(1 * KB), 1 * KB, -1)
                .build());
        client.deleteObjectTags(
            DeleteObjectTagsArgs.builder().bucket(bucketName).object(objectName).build());

        Map<String, String> map = new HashMap<>();
        map.put("Project", "Project One");
        map.put("User", "jsmith");
        client.setObjectTags(
            SetObjectTagsArgs.builder().bucket(bucketName).object(objectName).tags(map).build());
        client.deleteObjectTags(
            DeleteObjectTagsArgs.builder().bucket(bucketName).object(objectName).build());
        Tags tags =
            client.getObjectTags(
                GetObjectTagsArgs.builder().bucket(bucketName).object(objectName).build());
        Assert.assertTrue("tags: expected: <empty>, got: " + tags.get(), tags.get().isEmpty());
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getObjectAcl() throws Exception {
    String methodName = "getObjectAcl()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    try {
      try {
        client.putObject(
            PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
                    new ContentInputStream(1 * KB), 1 * KB, -1)
                .build());
        AccessControlPolicy policy =
            client.getObjectAcl(
                GetObjectAclArgs.builder().bucket(bucketName).object(objectName).build());
        Assert.assertEquals(
            "granteeType: expected: "
                + AccessControlList.Type.CANONICAL_USER
                + ", got: "
                + policy.accessControlList().grants().get(0).grantee().type(),
            policy.accessControlList().grants().get(0).grantee().type(),
            AccessControlList.Type.CANONICAL_USER);
        Assert.assertEquals(
            "permission: expected: "
                + AccessControlList.Permission.FULL_CONTROL
                + ", got: "
                + policy.accessControlList().grants().get(0).permission(),
            policy.accessControlList().grants().get(0).permission(),
            AccessControlList.Permission.FULL_CONTROL);
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getObjectAttributes() throws Exception {
    String methodName = "getObjectAttributes()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String objectName = getRandomName();
    try {
      try {
        client.putObject(
            PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(
                    new ContentInputStream(1 * KB), 1 * KB, -1)
                .build());
        GetObjectAttributesResponse response =
            client.getObjectAttributes(
                GetObjectAttributesArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .objectAttributes(
                        new String[] {
                          "ETag", "Checksum", "ObjectParts", "StorageClass", "ObjectSize"
                        })
                    .build());
        Assert.assertTrue(
            "objectSize: expected: " + (1 * KB) + ", got: " + response.result().objectSize(),
            response.result().objectSize() == (1 * KB));
        Assert.assertTrue(
            "partNumber: expected: 1, got: "
                + response.result().objectParts().parts().get(0).partNumber(),
            response.result().objectParts().parts().get(0).partNumber() == 1);
        Assert.assertTrue(
            "partSize: expected: "
                + (1 * KB)
                + ", got: "
                + response.result().objectParts().parts().get(0).partSize(),
            response.result().objectParts().parts().get(0).partSize() == (1 * KB));
        mintSuccessLog(methodName, null, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void setBucketReplication() throws Exception {
    String methodName = "setBucketReplication()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    if (replicationSrcBucket == null || replicationRole == null || replicationBucketArn == null) {
      mintIgnoredLog(methodName, "", System.currentTimeMillis());
      return;
    }

    long startTime = System.currentTimeMillis();
    try {
      Map<String, String> tags = new HashMap<>();
      tags.put("key1", "value1");
      tags.put("key2", "value2");

      ReplicationConfiguration.Rule rule =
          new ReplicationConfiguration.Rule(
              new ReplicationConfiguration.DeleteMarkerReplication(Status.DISABLED),
              new ReplicationConfiguration.Destination(
                  null, null, replicationBucketArn, null, null, null, null),
              null,
              new Filter(new Filter.And("TaxDocs", tags)),
              "rule1",
              null,
              1,
              null,
              Status.ENABLED);

      List<ReplicationConfiguration.Rule> rules = new LinkedList<>();
      rules.add(rule);

      ReplicationConfiguration config = new ReplicationConfiguration(replicationRole, rules);

      client.setBucketReplication(
          SetBucketReplicationArgs.builder().bucket(replicationSrcBucket).config(config).build());
      client.deleteBucketReplication(
          DeleteBucketReplicationArgs.builder().bucket(replicationSrcBucket).build());
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void getBucketReplication() throws Exception {
    String methodName = "getBucketReplication()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    if (replicationSrcBucket == null || replicationRole == null || replicationBucketArn == null) {
      mintIgnoredLog(methodName, "", System.currentTimeMillis());
      return;
    }

    long startTime = System.currentTimeMillis();
    try {
      ReplicationConfiguration config =
          client.getBucketReplication(
              GetBucketReplicationArgs.builder().bucket(replicationSrcBucket).build());
      Assert.assertNull("config: expected: <null>, got: <non-null>", config);

      Map<String, String> tags = new HashMap<>();
      tags.put("key1", "value1");
      tags.put("key2", "value2");

      ReplicationConfiguration.Rule rule =
          new ReplicationConfiguration.Rule(
              new ReplicationConfiguration.DeleteMarkerReplication(Status.DISABLED),
              new ReplicationConfiguration.Destination(
                  null, null, replicationBucketArn, null, null, null, null),
              null,
              new Filter(new Filter.And("TaxDocs", tags)),
              "rule1",
              null,
              1,
              null,
              Status.ENABLED);

      List<ReplicationConfiguration.Rule> rules = new LinkedList<>();
      rules.add(rule);

      config = new ReplicationConfiguration(replicationRole, rules);
      client.setBucketReplication(
          SetBucketReplicationArgs.builder().bucket(replicationSrcBucket).config(config).build());
      config =
          client.getBucketReplication(
              GetBucketReplicationArgs.builder().bucket(replicationSrcBucket).build());
      Assert.assertNotNull("config: expected: <non-null>, got: <null>", config);
      client.deleteBucketReplication(
          DeleteBucketReplicationArgs.builder().bucket(replicationSrcBucket).build());
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void deleteBucketReplication() throws Exception {
    String methodName = "deleteBucketReplication()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    if (replicationSrcBucket == null || replicationRole == null || replicationBucketArn == null) {
      mintIgnoredLog(methodName, "", System.currentTimeMillis());
      return;
    }

    long startTime = System.currentTimeMillis();
    try {
      client.deleteBucketReplication(
          DeleteBucketReplicationArgs.builder().bucket(replicationSrcBucket).build());

      Map<String, String> tags = new HashMap<>();
      tags.put("key1", "value1");
      tags.put("key2", "value2");

      ReplicationConfiguration.Rule rule =
          new ReplicationConfiguration.Rule(
              new ReplicationConfiguration.DeleteMarkerReplication(Status.DISABLED),
              new ReplicationConfiguration.Destination(
                  null, null, replicationBucketArn, null, null, null, null),
              null,
              new Filter(new Filter.And("TaxDocs", tags)),
              "rule1",
              null,
              1,
              null,
              Status.ENABLED);

      List<ReplicationConfiguration.Rule> rules = new LinkedList<>();
      rules.add(rule);

      ReplicationConfiguration config = new ReplicationConfiguration(replicationRole, rules);
      client.setBucketReplication(
          SetBucketReplicationArgs.builder().bucket(replicationSrcBucket).config(config).build());
      client.deleteBucketReplication(
          DeleteBucketReplicationArgs.builder().bucket(replicationSrcBucket).build());
      config =
          client.getBucketReplication(
              GetBucketReplicationArgs.builder().bucket(replicationSrcBucket).build());
      Assert.assertNull("config: expected: <null>, got: <non-null>", config);
      mintSuccessLog(methodName, null, startTime);
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void testUploadSnowballObjects(String testTags, boolean compression)
      throws Exception {
    String methodName = "uploadSnowballObjects()";

    long startTime = System.currentTimeMillis();
    String objectName1 = getRandomName();
    String objectName2 = getRandomName();
    try {
      try {
        List<SnowballObject> objects = new LinkedList<SnowballObject>();
        objects.add(
            new SnowballObject(
                objectName1,
                new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)),
                5,
                null));
        objects.add(new SnowballObject(objectName2, createFile1Kb()));
        client.uploadSnowballObjects(
            UploadSnowballObjectsArgs.builder()
                .bucket(bucketName)
                .objects(objects)
                .compression(compression)
                .build());

        StatObjectResponse stat =
            client.statObject(
                StatObjectArgs.builder().bucket(bucketName).object(objectName1).build());
        Assert.assertEquals("object size: expected: 5, got: " + stat.size(), 5, stat.size());
        stat =
            client.statObject(
                StatObjectArgs.builder().bucket(bucketName).object(objectName2).build());
        Assert.assertEquals(
            "object size: expected: " + KB + ", got: " + stat.size(), 1 * KB, stat.size());
        mintSuccessLog(methodName, testTags, startTime);
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName1).build());
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName2).build());
      }
    } catch (Exception e) {
      handleException(methodName, testTags, startTime, e);
    }
  }

  public static void uploadSnowballObjects() throws Exception {
    String methodName = "uploadSnowballObjects()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    testUploadSnowballObjects("[no compression]", false);
    testUploadSnowballObjects("[compression]", true);
  }

  public static void putObjectFanOut() throws Exception {
    String methodName = "putObjectFanOut()";
    if (!mintEnv) {
      System.out.println(methodName);
    }

    long startTime = System.currentTimeMillis();
    String objectName1 = getRandomName();
    String objectName2 = getRandomName();
    try {
      try {
        Map<String, String> map = new HashMap<>();
        map.put("Project", "Project One");
        map.put("User", "jsmith");
        client.putObjectFanOut(
            PutObjectFanOutArgs.builder().bucket(bucketName).stream(
                    new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)), 5)
                .entries(
                    Arrays.asList(
                        new PutObjectFanOutEntry[] {
                          PutObjectFanOutEntry.builder().key(objectName1).userMetadata(map).build(),
                          PutObjectFanOutEntry.builder().key(objectName2).tags(map).build()
                        }))
                .build());

        StatObjectResponse stat =
            client.statObject(
                StatObjectArgs.builder().bucket(bucketName).object(objectName1).build());
        Assert.assertTrue(
            "userMetadata: expected = " + map + ", got = " + stat.userMetadata(),
            map.size() == stat.userMetadata().size()
                && map.entrySet().stream()
                    .allMatch(
                        e ->
                            e.getValue()
                                .equals(
                                    stat.userMetadata().get(e.getKey().toLowerCase(Locale.US)))));

        Tags tags =
            client.getObjectTags(
                GetObjectTagsArgs.builder().bucket(bucketName).object(objectName2).build());
        Assert.assertTrue(
            "tags: expected = " + map + ", got = " + tags.get(), map.equals(tags.get()));
      } finally {
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName1).build());
        client.removeObject(
            RemoveObjectArgs.builder().bucket(bucketName).object(objectName2).build());
      }
    } catch (Exception e) {
      handleException(methodName, null, startTime, e);
    }
  }

  public static void runBucketTests() throws Exception {
    makeBucket();
    bucketExists();
    removeBucket();
    listBuckets();

    setBucketVersioning();
    getBucketVersioning();

    setObjectLockConfiguration();
    getObjectLockConfiguration();

    setBucketEncryption();
    getBucketEncryption();
    deleteBucketEncryption();

    setBucketCors();
    getBucketCors();
    deleteBucketCors();

    setBucketTags();
    getBucketTags();
    deleteBucketTags();

    setBucketPolicy();
    getBucketPolicy();
    deleteBucketPolicy();

    setBucketLifecycle();
    getBucketLifecycle();
    deleteBucketLifecycle();

    setBucketNotification();
    getBucketNotification();
    deleteBucketNotification();

    setBucketReplication();
    getBucketReplication();
    deleteBucketReplication();

    listenBucketNotification();
  }

  public static void runObjectTests() throws Exception {
    listObjects();

    setup();

    putObject();
    getObject();
    removeObject();
    removeObjects();
    statObject();

    copyObject();
    composeObject();
    uploadObject();
    downloadObject();

    setObjectRetention();
    getObjectRetention();

    getPresignedObjectUrl();
    getPresignedPostFormData();

    enableObjectLegalHold();
    disableObjectLegalHold();
    isObjectLegalHoldEnabled();

    selectObjectContent();

    setObjectTags();
    getObjectTags();
    deleteObjectTags();

    getObjectAcl();
    getObjectAttributes();

    uploadSnowballObjects();
    putObjectFanOut();

    teardown();
  }

  public static void runTests() throws Exception {
    runBucketTests();
    runObjectTests();
    adminClientTests.runAdminTests();
  }

  public static boolean downloadMinio() throws IOException {
    String url = "https://dl.min.io/server/minio/release/";
    if (OS.contains("linux")) {
      url += "linux-amd64/minio";
    } else if (OS.contains("windows")) {
      url += "windows-amd64/minio.exe";
    } else if (OS.contains("mac")) {
      url += "darwin-amd64/minio";
    } else {
      System.out.println("unknown operating system " + OS);
      return false;
    }

    File file = new File(MINIO_BINARY);
    if (file.exists()) {
      return true;
    }

    System.out.println("downloading " + MINIO_BINARY + " binary");

    Request.Builder requestBuilder = new Request.Builder();
    Request request = requestBuilder.url(HttpUrl.parse(url)).method("GET", null).build();
    OkHttpClient transport = newHttpClient();
    Response response = transport.newCall(request).execute();

    try {
      if (!response.isSuccessful()) {
        System.out.println("failed to download binary " + MINIO_BINARY);
        return false;
      }

      BufferedSink bufferedSink = Okio.buffer(Okio.sink(new File(MINIO_BINARY)));
      bufferedSink.writeAll(response.body().source());
      bufferedSink.flush();
      bufferedSink.close();
    } finally {
      response.close();
    }

    if (!OS.contains("windows")) {
      file.setExecutable(true);
    }

    return true;
  }

  public static Process runMinio(boolean tls) throws Exception {
    File binaryPath = new File(new File(System.getProperty("user.dir")), MINIO_BINARY);
    ProcessBuilder pb;
    if (tls) {
      pb =
          new ProcessBuilder(
              binaryPath.getPath(),
              "server",
              "--address",
              ":9001",
              "--certs-dir",
              ".cfg/certs",
              ".d{1...4}");
    } else {
      pb = new ProcessBuilder(binaryPath.getPath(), "server", ".d{1...4}");
    }

    Map<String, String> env = pb.environment();
    env.put("MINIO_ROOT_USER", "minio");
    env.put("MINIO_ROOT_PASSWORD", "minio123");
    env.put("MINIO_CI_CD", "1");
    // echo -n abcdefghijklmnopqrstuvwxyzABCDEF | base64 -
    env.put("MINIO_KMS_SECRET_KEY", "my-minio-key:YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXpBQkNERUY=");
    env.put("MINIO_NOTIFY_WEBHOOK_ENABLE_miniojavatest", "on");
    env.put("MINIO_NOTIFY_WEBHOOK_ENDPOINT_miniojavatest", "http://example.org/");
    sqsArn = "arn:minio:sqs::miniojavatest:webhook";

    pb.redirectErrorStream(true);
    pb.redirectOutput(ProcessBuilder.Redirect.to(new File(MINIO_BINARY + ".log")));

    if (tls) {
      System.out.println("starting minio server in TLS");
    } else {
      System.out.println("starting minio server");
    }
    Process p = pb.start();
    Thread.sleep(10 * 1000); // wait for 10 seconds to do real start.
    return p;
  }

  public static void runEndpointTests(boolean automated) throws Exception {
    client = MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build();
    MinioAdminClient adminClient =
        MinioAdminClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build();
    adminClientTests = new TestMinioAdminClient(adminClient, mintEnv);
    // Enable trace for debugging.
    // client.traceOn(System.out);
    if (!mintEnv) System.out.println(">>> Running tests:");
    FunctionalTest.runTests();

    if (automated) {
      // Run tests on TLS endpoint
      client =
          MinioClient.builder().endpoint(endpointTLS).credentials(accessKey, secretKey).build();
      client.ignoreCertCheck();
      adminClient =
          MinioAdminClient.builder()
              .endpoint(endpointTLS)
              .credentials(accessKey, secretKey)
              .build();
      adminClient.ignoreCertCheck();
      adminClientTests = new TestMinioAdminClient(adminClient, mintEnv);
      // Enable trace for debugging.
      // client.traceOn(System.out);
      if (!mintEnv) System.out.println(">>> Running tests on TLS endpoint:");
      isSecureEndpoint = true;
      FunctionalTest.runTests();
    }

    if (!mintEnv) {
      System.out.println();
      System.out.println(">>> Running tests for region:");
      isQuickTest = true;
      isSecureEndpoint = endpoint.toLowerCase(Locale.US).contains("https://");
      // Get new bucket name to avoid minio azure gateway failure.
      bucketName = getRandomName();
      bucketNameWithLock = getRandomName();
      client =
          MinioClient.builder()
              .endpoint(endpoint)
              .credentials(accessKey, secretKey)
              .region(region)
              .build();
      adminClient =
          MinioAdminClient.builder()
              .endpoint(endpoint)
              .credentials(accessKey, secretKey)
              .region(region)
              .build();
      adminClientTests = new TestMinioAdminClient(adminClient, mintEnv);
      FunctionalTest.runTests();
    }
  }

  /** main(). */
  public static void main(String[] args) throws Exception {
    String mintMode = System.getenv("MINT_MODE");
    mintEnv = (mintMode != null);
    if (mintEnv) {
      isQuickTest = !mintMode.equals("full");
      isRunOnFail = "1".equals(System.getenv("RUN_ON_FAIL"));
      String dataDir = System.getenv("MINT_DATA_DIR");
      if (dataDir != null && !dataDir.equals("")) {
        dataFile1Kb = Paths.get(dataDir, "datafile-1-kB");
        dataFile6Mb = Paths.get(dataDir, "datafile-6-MB");
      }
    }
    replicationSrcBucket = System.getenv("MINIO_JAVA_TEST_REPLICATION_SRC_BUCKET");
    replicationRole = System.getenv("MINIO_JAVA_TEST_REPLICATION_ROLE");
    replicationBucketArn = System.getenv("MINIO_JAVA_TEST_REPLICATION_BUCKET_ARN");

    Process minioProcess = null;
    Process minioProcessTLS = null;

    boolean automated = true;
    String kmsKeyName = "my-minio-key";
    if (args.length != 4) {
      endpoint = "http://localhost:9000";
      endpointTLS = "https://localhost:9001";
      accessKey = "minio";
      secretKey = "minio123";
      region = "us-east-1";

      if (!downloadMinio()) {
        System.out.println("usage: FunctionalTest <ENDPOINT> <ACCESSKEY> <SECRETKEY> <REGION>");
        System.exit(-1);
      }

      minioProcess = runMinio(false);
      try {
        int exitValue = minioProcess.exitValue();
        System.out.println("minio server process exited with " + exitValue);
        System.out.println("usage: FunctionalTest <ENDPOINT> <ACCESSKEY> <SECRETKEY> <REGION>");
        System.exit(-1);
      } catch (IllegalThreadStateException e) {
        ignore();
      }

      minioProcessTLS = runMinio(true);
      try {
        int exitValue = minioProcessTLS.exitValue();
        System.out.println("minio server process exited with " + exitValue);
        System.out.println("usage: FunctionalTest <ENDPOINT> <ACCESSKEY> <SECRETKEY> <REGION>");
        System.exit(-1);
      } catch (IllegalThreadStateException e) {
        ignore();
      }
    } else {
      kmsKeyName = System.getenv("MINIO_JAVA_TEST_KMS_KEY_NAME");
      if (kmsKeyName == null) {
        kmsKeyName = System.getenv("MINT_KEY_ID");
      }
      sqsArn = System.getenv("MINIO_JAVA_TEST_SQS_ARN");
      endpoint = args[0];
      accessKey = args[1];
      secretKey = args[2];
      region = args[3];
      automated = false;
    }

    isSecureEndpoint = endpoint.toLowerCase(Locale.US).contains("https://");
    if (kmsKeyName != null) {
      Map<String, String> myContext = new HashMap<>();
      myContext.put("key1", "value1");
      sseKms = new ServerSideEncryption.KMS(kmsKeyName, myContext);
    }

    int exitValue = 0;
    try {
      runEndpointTests(automated);
    } catch (Exception e) {
      if (!mintEnv) {
        e.printStackTrace();
      }
      exitValue = -1;
    } finally {
      if (minioProcess != null) {
        minioProcess.destroy();
      }
      if (minioProcessTLS != null) {
        minioProcessTLS.destroy();
      }
    }

    System.exit(exitValue);
  }
}
