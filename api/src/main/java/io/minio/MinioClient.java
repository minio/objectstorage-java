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

package io.minio;

import io.minio.credentials.Provider;
import io.minio.errors.BucketPolicyTooLargeException;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.AccessControlPolicy;
import io.minio.messages.CORSConfiguration;
import io.minio.messages.DeleteResult;
import io.minio.messages.Item;
import io.minio.messages.LifecycleConfiguration;
import io.minio.messages.ListAllMyBucketsResult;
import io.minio.messages.NotificationConfiguration;
import io.minio.messages.NotificationRecords;
import io.minio.messages.ObjectLockConfiguration;
import io.minio.messages.ReplicationConfiguration;
import io.minio.messages.Retention;
import io.minio.messages.SseConfiguration;
import io.minio.messages.Tags;
import io.minio.messages.VersioningConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

/**
 * Simple Storage Service (aka S3) client to perform bucket and object operations.
 *
 * <h2>Bucket operations</h2>
 *
 * <ul>
 *   <li>Create, list and delete buckets.
 *   <li>Put, get and delete bucket lifecycle configuration.
 *   <li>Put, get and delete bucket policy configuration.
 *   <li>Put, get and delete bucket encryption configuration.
 *   <li>Put and get bucket default retention configuration.
 *   <li>Put and get bucket notification configuration.
 *   <li>Enable and disable bucket versioning.
 * </ul>
 *
 * <h2>Object operations</h2>
 *
 * <ul>
 *   <li>Put, get, delete and list objects.
 *   <li>Create objects by combining existing objects.
 *   <li>Put and get object retention and legal hold.
 *   <li>Filter object content by SQL statement.
 * </ul>
 *
 * <p>If access/secret keys are provided, all S3 operation requests are signed using AWS Signature
 * Version 4; else they are performed anonymously.
 *
 * <p>Examples on using this library are available <a
 * href="https://github.com/minio/minio-java/tree/master/src/test/java/io/minio/examples">here</a>.
 *
 * <p>Use {@code MinioClient.builder()} to create S3 client.
 *
 * <pre>{@code
 * // Create client with anonymous access.
 * MinioClient minioClient = MinioClient.builder().endpoint("https://play.min.io").build();
 *
 * // Create client with credentials.
 * MinioClient minioClient =
 *     MinioClient.builder()
 *         .endpoint("https://play.min.io")
 *         .credentials("Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
 *         .build();
 * }</pre>
 */
public class MinioClient implements AutoCloseable {
  private MinioAsyncClient asyncClient = null;

  private MinioClient(MinioAsyncClient asyncClient) {
    this.asyncClient = asyncClient;
  }

  protected MinioClient(MinioClient client) {
    this.asyncClient = client.asyncClient;
  }

  /**
   * Gets information of an object.
   *
   * <pre>Example:{@code
   * // Get information of an object.
   * StatObjectResponse stat =
   *     minioClient.statObject(
   *         StatObjectArgs.builder().bucket("my-bucketname").object("my-objectname").build());
   *
   * // Get information of SSE-C encrypted object.
   * StatObjectResponse stat =
   *     minioClient.statObject(
   *         StatObjectArgs.builder()
   *             .bucket("my-bucketname")
   *             .object("my-objectname")
   *             .ssec(ssec)
   *             .build());
   *
   * // Get information of a versioned object.
   * StatObjectResponse stat =
   *     minioClient.statObject(
   *         StatObjectArgs.builder()
   *             .bucket("my-bucketname")
   *             .object("my-objectname")
   *             .versionId("version-id")
   *             .build());
   *
   * // Get information of a SSE-C encrypted versioned object.
   * StatObjectResponse stat =
   *     minioClient.statObject(
   *         StatObjectArgs.builder()
   *             .bucket("my-bucketname")
   *             .object("my-objectname")
   *             .versionId("version-id")
   *             .ssec(ssec)
   *             .build());
   * }</pre>
   *
   * @param args {@link StatObjectArgs} object.
   * @return {@link StatObjectResponse} - Populated object information and metadata.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   * @see StatObjectResponse
   */
  public StatObjectResponse statObject(StatObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.statObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Gets data from offset to length of a SSE-C encrypted object. Returned {@link InputStream} must
   * be closed after use to release network resources.
   *
   * <pre>Example:{@code
   * try (InputStream stream =
   *     minioClient.getObject(
   *   GetObjectArgs.builder()
   *     .bucket("my-bucketname")
   *     .object("my-objectname")
   *     .offset(offset)
   *     .length(len)
   *     .ssec(ssec)
   *     .build()
   * ) {
   *   // Read data from stream
   * }
   * }</pre>
   *
   * @param args Object of {@link GetObjectArgs}
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public GetObjectResponse getObject(GetObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Downloads data of a SSE-C encrypted object to file.
   *
   * <pre>Example:{@code
   * minioClient.downloadObject(
   *   DownloadObjectArgs.builder()
   *     .bucket("my-bucketname")
   *     .object("my-objectname")
   *     .ssec(ssec)
   *     .filename("my-filename")
   *     .build());
   * }</pre>
   *
   * @param args Object of {@link DownloadObjectArgs}
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void downloadObject(DownloadObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.downloadObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Creates an object by server-side copying data from another object.
   *
   * <pre>Example:{@code
   * // Create object "my-objectname" in bucket "my-bucketname" by copying from object
   * // "my-objectname" in bucket "my-source-bucketname".
   * minioClient.copyObject(
   *     CopyObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .source(
   *             CopySource.builder()
   *                 .bucket("my-source-bucketname")
   *                 .object("my-objectname")
   *                 .build())
   *         .build());
   *
   * // Create object "my-objectname" in bucket "my-bucketname" by copying from object
   * // "my-source-objectname" in bucket "my-source-bucketname".
   * minioClient.copyObject(
   *     CopyObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .source(
   *             CopySource.builder()
   *                 .bucket("my-source-bucketname")
   *                 .object("my-source-objectname")
   *                 .build())
   *         .build());
   *
   * // Create object "my-objectname" in bucket "my-bucketname" with SSE-KMS server-side
   * // encryption by copying from object "my-objectname" in bucket "my-source-bucketname".
   * minioClient.copyObject(
   *     CopyObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .source(
   *             CopySource.builder()
   *                 .bucket("my-source-bucketname")
   *                 .object("my-objectname")
   *                 .build())
   *         .sse(sseKms) // Replace with actual key.
   *         .build());
   *
   * // Create object "my-objectname" in bucket "my-bucketname" with SSE-S3 server-side
   * // encryption by copying from object "my-objectname" in bucket "my-source-bucketname".
   * minioClient.copyObject(
   *     CopyObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .source(
   *             CopySource.builder()
   *                 .bucket("my-source-bucketname")
   *                 .object("my-objectname")
   *                 .build())
   *         .sse(sseS3) // Replace with actual key.
   *         .build());
   *
   * // Create object "my-objectname" in bucket "my-bucketname" with SSE-C server-side encryption
   * // by copying from object "my-objectname" in bucket "my-source-bucketname".
   * minioClient.copyObject(
   *     CopyObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .source(
   *             CopySource.builder()
   *                 .bucket("my-source-bucketname")
   *                 .object("my-objectname")
   *                 .build())
   *         .sse(ssec) // Replace with actual key.
   *         .build());
   *
   * // Create object "my-objectname" in bucket "my-bucketname" by copying from SSE-C encrypted
   * // object "my-source-objectname" in bucket "my-source-bucketname".
   * minioClient.copyObject(
   *     CopyObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .source(
   *             CopySource.builder()
   *                 .bucket("my-source-bucketname")
   *                 .object("my-source-objectname")
   *                 .ssec(ssec) // Replace with actual key.
   *                 .build())
   *         .build());
   *
   * // Create object "my-objectname" in bucket "my-bucketname" with custom headers conditionally
   * // by copying from object "my-objectname" in bucket "my-source-bucketname".
   * minioClient.copyObject(
   *     CopyObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .source(
   *             CopySource.builder()
   *                 .bucket("my-source-bucketname")
   *                 .object("my-objectname")
   *                 .matchETag(etag) // Replace with actual etag.
   *                 .build())
   *         .headers(headers) // Replace with actual headers.
   *         .build());
   * }</pre>
   *
   * @param args {@link CopyObjectArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public ObjectWriteResponse copyObject(CopyObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.copyObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Creates an object by combining data from different source objects using server-side copy.
   *
   * <pre>Example:{@code
   * List<ComposeSource> sourceObjectList = new ArrayList<ComposeSource>();
   *
   * sourceObjectList.add(
   *    ComposeSource.builder().bucket("my-job-bucket").object("my-objectname-part-one").build());
   * sourceObjectList.add(
   *    ComposeSource.builder().bucket("my-job-bucket").object("my-objectname-part-two").build());
   * sourceObjectList.add(
   *    ComposeSource.builder().bucket("my-job-bucket").object("my-objectname-part-three").build());
   *
   * // Create my-bucketname/my-objectname by combining source object list.
   * minioClient.composeObject(
   *    ComposeObjectArgs.builder()
   *        .bucket("my-bucketname")
   *        .object("my-objectname")
   *        .sources(sourceObjectList)
   *        .build());
   *
   * // Create my-bucketname/my-objectname with user metadata by combining source object
   * // list.
   * Map<String, String> userMetadata = new HashMap<>();
   * userMetadata.put("My-Project", "Project One");
   * minioClient.composeObject(
   *     ComposeObjectArgs.builder()
   *        .bucket("my-bucketname")
   *        .object("my-objectname")
   *        .sources(sourceObjectList)
   *        .userMetadata(userMetadata)
   *        .build());
   *
   * // Create my-bucketname/my-objectname with user metadata and server-side encryption
   * // by combining source object list.
   * minioClient.composeObject(
   *   ComposeObjectArgs.builder()
   *        .bucket("my-bucketname")
   *        .object("my-objectname")
   *        .sources(sourceObjectList)
   *        .userMetadata(userMetadata)
   *        .ssec(sse)
   *        .build());
   * }</pre>
   *
   * @param args {@link ComposeObjectArgs} object.
   * @return {@link ObjectWriteResponse} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public ObjectWriteResponse composeObject(ComposeObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.composeObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Gets presigned URL of an object for HTTP method, expiry time and custom request parameters.
   *
   * <pre>Example:{@code
   * // Get presigned URL string to delete 'my-objectname' in 'my-bucketname' and its life time
   * // is one day.
   * String url =
   *    minioClient.getPresignedObjectUrl(
   *        GetPresignedObjectUrlArgs.builder()
   *            .method(Http.Method.DELETE)
   *            .bucket("my-bucketname")
   *            .object("my-objectname")
   *            .expiry(24 * 60 * 60)
   *            .build());
   * System.out.println(url);
   *
   * // Get presigned URL string to upload 'my-objectname' in 'my-bucketname'
   * // with response-content-type as application/json and life time as one day.
   * Map<String, String> reqParams = new HashMap<String, String>();
   * reqParams.put("response-content-type", "application/json");
   *
   * String url =
   *    minioClient.getPresignedObjectUrl(
   *        GetPresignedObjectUrlArgs.builder()
   *            .method(Http.Method.PUT)
   *            .bucket("my-bucketname")
   *            .object("my-objectname")
   *            .expiry(1, TimeUnit.DAYS)
   *            .extraQueryParams(reqParams)
   *            .build());
   * System.out.println(url);
   *
   * // Get presigned URL string to download 'my-objectname' in 'my-bucketname' and its life time
   * // is 2 hours.
   * String url =
   *    minioClient.getPresignedObjectUrl(
   *        GetPresignedObjectUrlArgs.builder()
   *            .method(Http.Method.GET)
   *            .bucket("my-bucketname")
   *            .object("my-objectname")
   *            .expiry(2, TimeUnit.HOURS)
   *            .build());
   * System.out.println(url);
   * }</pre>
   *
   * @param args {@link GetPresignedObjectUrlArgs} object.
   * @return String - URL string.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   * @throws ServerException
   */
  public String getPresignedObjectUrl(GetPresignedObjectUrlArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          XmlParserException, ServerException {
    return asyncClient.getPresignedObjectUrl(args);
  }

  /**
   * Gets form-data of {@link PostPolicy} of an object to upload its data using POST method.
   *
   * <pre>Example:{@code
   * // Create new post policy for 'my-bucketname' with 7 days expiry from now.
   * PostPolicy policy = new PostPolicy("my-bucketname", ZonedDateTime.now().plusDays(7));
   *
   * // Add condition that 'key' (object name) equals to 'my-objectname'.
   * policy.addEqualsCondition("key", "my-objectname");
   *
   * // Add condition that 'Content-Type' starts with 'image/'.
   * policy.addStartsWithCondition("Content-Type", "image/");
   *
   * // Add condition that 'content-length-range' is between 64kiB to 10MiB.
   * policy.addContentLengthRangeCondition(64 * 1024, 10 * 1024 * 1024);
   *
   * Map<String, String> formData = minioClient.getPresignedPostFormData(policy);
   *
   * // Upload an image using POST object with form-data.
   * MultipartBody.Builder multipartBuilder = new MultipartBody.Builder();
   * multipartBuilder.setType(MultipartBody.FORM);
   * for (Map.Entry<String, String> entry : formData.entrySet()) {
   *   multipartBuilder.addFormDataPart(entry.getKey(), entry.getValue());
   * }
   * multipartBuilder.addFormDataPart("key", "my-objectname");
   * multipartBuilder.addFormDataPart("Content-Type", "image/png");
   *
   * // "file" must be added at last.
   * multipartBuilder.addFormDataPart(
   *     "file", "my-objectname", RequestBody.create(new File("Pictures/avatar.png"), null));
   *
   * Request request =
   *     new Request.Builder()
   *         .url("https://play.min.io/my-bucketname")
   *         .post(multipartBuilder.build())
   *         .build();
   * OkHttpClient httpClient = new OkHttpClient().newBuilder().build();
   * Response response = httpClient.newCall(request).execute();
   * if (response.isSuccessful()) {
   *   System.out.println("Pictures/avatar.png is uploaded successfully using POST object");
   * } else {
   *   System.out.println("Failed to upload Pictures/avatar.png");
   * }
   * }</pre>
   *
   * @param policy Post policy of an object.
   * @return {@code Map<String, String>} - Contains form-data to upload an object using POST method.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   * @see PostPolicy
   */
  public Map<String, String> getPresignedPostFormData(PostPolicy policy)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    return asyncClient.getPresignedPostFormData(policy);
  }

  /**
   * Removes an object.
   *
   * <pre>Example:{@code
   * // Remove object.
   * minioClient.removeObject(
   *     RemoveObjectArgs.builder().bucket("my-bucketname").object("my-objectname").build());
   *
   * // Remove versioned object.
   * minioClient.removeObject(
   *     RemoveObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-versioned-objectname")
   *         .versionId("my-versionid")
   *         .build());
   *
   * // Remove versioned object bypassing Governance mode.
   * minioClient.removeObject(
   *     RemoveObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-versioned-objectname")
   *         .versionId("my-versionid")
   *         .bypassRetentionMode(true)
   *         .build());
   * }</pre>
   *
   * @param args {@link RemoveObjectArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void removeObject(RemoveObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.removeObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Removes multiple objects lazily. Its required to iterate the returned Iterable to perform
   * removal.
   *
   * <pre>Example:{@code
   * List<DeleteObject> objects = new LinkedList<>();
   * objects.add(new DeleteObject("my-objectname1"));
   * objects.add(new DeleteObject("my-objectname2"));
   * objects.add(new DeleteObject("my-objectname3"));
   * Iterable<Result<DeleteResult.Error>> results =
   *     minioClient.removeObjects(
   *         RemoveObjectsArgs.builder().bucket("my-bucketname").objects(objects).build());
   * for (Result<DeleteResult.Error> result : results) {
   *   DeleteResult.Error error = result.get();
   *   System.out.println(
   *       "Error in deleting object " + error.objectName() + "; " + error.message());
   * }
   * }</pre>
   *
   * @param args {@link RemoveObjectsArgs} object.
   * @return {@code Iterable<Result<DeleteResult.Error>>} - Lazy iterator contains object removal
   *     status.
   */
  public Iterable<Result<DeleteResult.Error>> removeObjects(RemoveObjectsArgs args) {
    return asyncClient.removeObjects(args);
  }

  /**
   * Restores an object.
   *
   * <pre>Example:{@code
   * // Restore object.
   * minioClient.restoreObject(
   *     RestoreObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .request(new RestoreRequest(null, null, null, null, null, null))
   *         .build());
   *
   * // Restore versioned object.
   * minioClient.restoreObject(
   *     RestoreObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-versioned-objectname")
   *         .versionId("my-versionid")
   *         .request(new RestoreRequest(null, null, null, null, null, null))
   *         .build());
   * }</pre>
   *
   * @param args {@link RestoreObjectArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void restoreObject(RestoreObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.restoreObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Lists objects information optionally with versions of a bucket. Supports both the versions 1
   * and 2 of the S3 API. By default, the <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">version 2</a> API
   * is used. <br>
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">Version 1</a>
   * can be used by passing the optional argument {@code useVersion1} as {@code true}.
   *
   * <pre>Example:{@code
   * // Lists objects information.
   * Iterable<Result<Item>> results = minioClient.listObjects(
   *     ListObjectsArgs.builder().bucket("my-bucketname").build());
   *
   * // Lists objects information recursively.
   * Iterable<Result<Item>> results = minioClient.listObjects(
   *     ListObjectsArgs.builder().bucket("my-bucketname").recursive(true).build());
   *
   * // Lists maximum 100 objects information whose names starts with 'E' and after
   * // 'ExampleGuide.pdf'.
   * Iterable<Result<Item>> results = minioClient.listObjects(
   *     ListObjectsArgs.builder()
   *         .bucket("my-bucketname")
   *         .startAfter("ExampleGuide.pdf")
   *         .prefix("E")
   *         .maxKeys(100)
   *         .build());
   *
   * // Lists maximum 100 objects information with version whose names starts with 'E' and after
   * // 'ExampleGuide.pdf'.
   * Iterable<Result<Item>> results = minioClient.listObjects(
   *     ListObjectsArgs.builder()
   *         .bucket("my-bucketname")
   *         .startAfter("ExampleGuide.pdf")
   *         .prefix("E")
   *         .maxKeys(100)
   *         .includeVersions(true)
   *         .build());
   * }</pre>
   *
   * @param args Instance of {@link ListObjectsArgs} built using the builder
   * @return {@code Iterable<Result<Item>>} - Lazy iterator contains object information.
   * @throws XmlParserException upon parsing response xml
   */
  public Iterable<Result<Item>> listObjects(ListObjectsArgs args) {
    return asyncClient.listObjects(args);
  }

  /**
   * Lists bucket information of all buckets.
   *
   * <pre>Example:{@code
   * List<ListAllMyBucketsResult.Bucket> bucketList = minioClient.listBuckets();
   * for (Bucket bucket : bucketList) {
   *   System.out.println(bucket.creationDate() + ", " + bucket.name());
   * }
   * }</pre>
   *
   * @return {@code List<ListAllMyBucketsResult.Bucket>} - List of bucket information.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public List<ListAllMyBucketsResult.Bucket> listBuckets()
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.listBuckets().get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Lists bucket information of all buckets.
   *
   * <pre>Example:{@code
   * Iterable<Result<ListAllMyBucketsResult.Bucket>> results = minioClient.listBuckets(ListBucketsArgs.builder().build());
   * for (Result<ListAllMyBucketsResult.Bucket> result : results) {
   *   Bucket bucket = result.get();
   *   System.out.println(String.format("Bucket: %s, Region: %s, CreationDate: %s", bucket.name(), bucket.bucketRegion(), bucket.creationDate()));
   * }
   * }</pre>
   *
   * @return {@link Iterable}&lt;{@link List}&lt;{@link ListAllMyBucketsResult.Bucket}&gt;&gt;
   *     object.
   */
  public Iterable<Result<ListAllMyBucketsResult.Bucket>> listBuckets(ListBucketsArgs args) {
    return asyncClient.listBuckets(args);
  }

  /**
   * Checks if a bucket exists.
   *
   * <pre>Example:{@code
   * boolean found =
   *      minioClient.bucketExists(BucketExistsArgs.builder().bucket("my-bucketname").build());
   * if (found) {
   *   System.out.println("my-bucketname exists");
   * } else {
   *   System.out.println("my-bucketname does not exist");
   * }
   * }</pre>
   *
   * @param args {@link BucketExistsArgs} object.
   * @return boolean - True if the bucket exists.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public boolean bucketExists(BucketExistsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.bucketExists(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return false;
    }
  }

  /**
   * Creates a bucket with region and object lock.
   *
   * <pre>Example:{@code
   * // Create bucket with default region.
   * minioClient.makeBucket(
   *     MakeBucketArgs.builder()
   *         .bucket("my-bucketname")
   *         .build());
   *
   * // Create bucket with specific region.
   * minioClient.makeBucket(
   *     MakeBucketArgs.builder()
   *         .bucket("my-bucketname")
   *         .region("us-west-1")
   *         .build());
   *
   * // Create object-lock enabled bucket with specific region.
   * minioClient.makeBucket(
   *     MakeBucketArgs.builder()
   *         .bucket("my-bucketname")
   *         .region("us-west-1")
   *         .objectLock(true)
   *         .build());
   * }</pre>
   *
   * @param args Object with bucket name, region and lock functionality
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void makeBucket(MakeBucketArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.makeBucket(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Sets versioning configuration of a bucket.
   *
   * <pre>Example:{@code
   * minioClient.setBucketVersioning(
   *     SetBucketVersioningArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketVersioningArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setBucketVersioning(SetBucketVersioningArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setBucketVersioning(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets versioning configuration of a bucket.
   *
   * <pre>Example:{@code
   * VersioningConfiguration config =
   *     minioClient.getBucketVersioning(
   *         GetBucketVersioningArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketVersioningArgs} object.
   * @return {@link VersioningConfiguration} - Versioning configuration.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public VersioningConfiguration getBucketVersioning(GetBucketVersioningArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getBucketVersioning(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Sets default object retention in a bucket.
   *
   * <pre>Example:{@code
   * ObjectLockConfiguration config = new ObjectLockConfiguration(
   *     RetentionMode.COMPLIANCE, new RetentionDurationDays(100));
   * minioClient.setObjectLockConfiguration(
   *     SetObjectLockConfigurationArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetObjectLockConfigurationArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setObjectLockConfiguration(SetObjectLockConfigurationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setObjectLockConfiguration(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Deletes default object retention in a bucket.
   *
   * <pre>Example:{@code
   * minioClient.deleteObjectLockConfiguration(
   *     DeleteObjectLockConfigurationArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteObjectLockConfigurationArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void deleteObjectLockConfiguration(DeleteObjectLockConfigurationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.deleteObjectLockConfiguration(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets default object retention in a bucket.
   *
   * <pre>Example:{@code
   * ObjectLockConfiguration config =
   *     minioClient.getObjectLockConfiguration(
   *         GetObjectLockConfigurationArgs.builder().bucket("my-bucketname").build());
   * System.out.println("Mode: " + config.mode());
   * System.out.println(
   *     "Duration: " + config.duration().duration() + " " + config.duration().unit());
   * }</pre>
   *
   * @param args {@link GetObjectLockConfigurationArgs} object.
   * @return {@link ObjectLockConfiguration} - Default retention configuration.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public ObjectLockConfiguration getObjectLockConfiguration(GetObjectLockConfigurationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getObjectLockConfiguration(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Sets retention configuration to an object.
   *
   * <pre>Example:{@code
   *  Retention retention = new Retention(
   *       RetentionMode.COMPLIANCE, ZonedDateTime.now().plusYears(1));
   *  minioClient.setObjectRetention(
   *      SetObjectRetentionArgs.builder()
   *          .bucket("my-bucketname")
   *          .object("my-objectname")
   *          .config(config)
   *          .bypassGovernanceMode(true)
   *          .build());
   * }</pre>
   *
   * @param args {@link SetObjectRetentionArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setObjectRetention(SetObjectRetentionArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setObjectRetention(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets retention configuration of an object.
   *
   * <pre>Example:{@code
   * Retention retention =
   *     minioClient.getObjectRetention(GetObjectRetentionArgs.builder()
   *        .bucket(bucketName)
   *        .object(objectName)
   *        .versionId(versionId)
   *        .build()););
   * System.out.println(
   *     "mode: " + retention.mode() + "until: " + retention.retainUntilDate());
   * }</pre>
   *
   * @param args {@link GetObjectRetentionArgs} object.
   * @return {@link Retention} - Object retention configuration.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public Retention getObjectRetention(GetObjectRetentionArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getObjectRetention(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Enables legal hold on an object.
   *
   * <pre>Example:{@code
   * minioClient.enableObjectLegalHold(
   *    EnableObjectLegalHoldArgs.builder()
   *        .bucket("my-bucketname")
   *        .object("my-objectname")
   *        .versionId("object-versionId")
   *        .build());
   * }</pre>
   *
   * @param args {@link EnableObjectLegalHoldArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void enableObjectLegalHold(EnableObjectLegalHoldArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.enableObjectLegalHold(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Disables legal hold on an object.
   *
   * <pre>Example:{@code
   * minioClient.disableObjectLegalHold(
   *    DisableObjectLegalHoldArgs.builder()
   *        .bucket("my-bucketname")
   *        .object("my-objectname")
   *        .versionId("object-versionId")
   *        .build());
   * }</pre>
   *
   * @param args {@link DisableObjectLegalHoldArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void disableObjectLegalHold(DisableObjectLegalHoldArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.disableObjectLegalHold(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Returns true if legal hold is enabled on an object.
   *
   * <pre>Example:{@code
   * boolean status =
   *     s3Client.isObjectLegalHoldEnabled(
   *        IsObjectLegalHoldEnabledArgs.builder()
   *             .bucket("my-bucketname")
   *             .object("my-objectname")
   *             .versionId("object-versionId")
   *             .build());
   * if (status) {
   *   System.out.println("Legal hold is on");
   *  } else {
   *   System.out.println("Legal hold is off");
   *  }
   * }</pre>
   *
   * args {@link IsObjectLegalHoldEnabledArgs} object.
   *
   * @return boolean - True if legal hold is enabled.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public boolean isObjectLegalHoldEnabled(IsObjectLegalHoldEnabledArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.isObjectLegalHoldEnabled(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return false;
    }
  }

  /**
   * Removes an empty bucket using arguments
   *
   * <pre>Example:{@code
   * minioClient.removeBucket(RemoveBucketArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link RemoveBucketArgs} bucket.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void removeBucket(RemoveBucketArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.removeBucket(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Uploads data from a stream to an object.
   *
   * <pre>Example:{@code
   * // Upload known sized input stream.
   * minioClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("my-objectname").stream(
   *             inputStream, size, -1)
   *         .contentType("video/mp4")
   *         .build());
   *
   * // Upload unknown sized input stream.
   * minioClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("my-objectname").stream(
   *             inputStream, -1, 10485760)
   *         .contentType("video/mp4")
   *         .build());
   *
   * // Create object ends with '/' (also called as folder or directory).
   * minioClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("path/to/").stream(
   *             new ByteArrayInputStream(new byte[] {}), 0, -1)
   *         .build());
   *
   * // Upload input stream with headers and user metadata.
   * Map<String, String> headers = new HashMap<>();
   * headers.put("X-Amz-Storage-Class", "REDUCED_REDUNDANCY");
   * Map<String, String> userMetadata = new HashMap<>();
   * userMetadata.put("My-Project", "Project One");
   * minioClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("my-objectname").stream(
   *             inputStream, size, -1)
   *         .headers(headers)
   *         .userMetadata(userMetadata)
   *         .build());
   *
   * // Upload input stream with server-side encryption.
   * minioClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("my-objectname").stream(
   *             inputStream, size, -1)
   *         .sse(sse)
   *         .build());
   * }</pre>
   *
   * @param args {@link PutObjectArgs} object.
   * @return {@link ObjectWriteResponse} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public ObjectWriteResponse putObject(PutObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.putObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Uploads data from a file to an object.
   *
   * <pre>Example:{@code
   * // Upload an JSON file.
   * minioClient.uploadObject(
   *     UploadObjectArgs.builder()
   *         .bucket("my-bucketname").object("my-objectname").filename("person.json").build());
   *
   * // Upload a video file.
   * minioClient.uploadObject(
   *     UploadObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .filename("my-video.avi")
   *         .contentType("video/mp4")
   *         .build());
   * }</pre>
   *
   * @param args {@link UploadObjectArgs} object.
   * @return {@link ObjectWriteResponse} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public ObjectWriteResponse uploadObject(UploadObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.uploadObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Gets bucket policy configuration of a bucket.
   *
   * <pre>Example:{@code
   * String config =
   *     minioClient.getBucketPolicy(GetBucketPolicyArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketPolicyArgs} object.
   * @return String - Bucket policy configuration as JSON string.
   * @throws BucketPolicyTooLargeException thrown to indicate returned bucket policy is too large.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public String getBucketPolicy(GetBucketPolicyArgs args)
      throws BucketPolicyTooLargeException, ErrorResponseException, InsufficientDataException,
          InternalException, InvalidKeyException, InvalidResponseException, IOException,
          NoSuchAlgorithmException, ServerException, XmlParserException {
    try {
      return asyncClient.getBucketPolicy(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return "";
    }
  }

  /**
   * Sets bucket policy configuration to a bucket.
   *
   * <pre>Example:{@code
   * // Assume policyJson contains below JSON string;
   * // {
   * //     "Statement": [
   * //         {
   * //             "Action": [
   * //                 "s3:GetBucketLocation",
   * //                 "s3:ListBucket"
   * //             ],
   * //             "Effect": "Allow",
   * //             "Principal": "*",
   * //             "Resource": "arn:aws:s3:::my-bucketname"
   * //         },
   * //         {
   * //             "Action": "s3:GetObject",
   * //             "Effect": "Allow",
   * //             "Principal": "*",
   * //             "Resource": "arn:aws:s3:::my-bucketname/myobject*"
   * //         }
   * //     ],
   * //     "Version": "2012-10-17"
   * // }
   * //
   * minioClient.setBucketPolicy(
   *     SetBucketPolicyArgs.builder().bucket("my-bucketname").config(policyJson).build());
   * }</pre>
   *
   * @param args {@link SetBucketPolicyArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setBucketPolicy(SetBucketPolicyArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setBucketPolicy(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Deletes bucket policy configuration to a bucket.
   *
   * <pre>Example:{@code
   * minioClient.deleteBucketPolicy(DeleteBucketPolicyArgs.builder().bucket("my-bucketname"));
   * }</pre>
   *
   * @param args {@link DeleteBucketPolicyArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void deleteBucketPolicy(DeleteBucketPolicyArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.deleteBucketPolicy(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Sets lifecycle configuration to a bucket.
   *
   * <pre>Example:{@code
   * List<LifecycleRule> rules = new LinkedList<>();
   * rules.add(
   *     new LifecycleRule(
   *         Status.ENABLED,
   *         null,
   *         new Expiration((ZonedDateTime) null, 365, null),
   *         new RuleFilter("logs/"),
   *         "rule2",
   *         null,
   *         null,
   *         null));
   * LifecycleConfiguration config = new LifecycleConfiguration(rules);
   * minioClient.setBucketLifecycle(
   *     SetBucketLifecycleArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketLifecycleArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setBucketLifecycle(SetBucketLifecycleArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setBucketLifecycle(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Deletes lifecycle configuration of a bucket.
   *
   * <pre>Example:{@code
   * deleteBucketLifecycle(DeleteBucketLifecycleArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketLifecycleArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void deleteBucketLifecycle(DeleteBucketLifecycleArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.deleteBucketLifecycle(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets lifecycle configuration of a bucket.
   *
   * <pre>Example:{@code
   * LifecycleConfiguration config =
   *     minioClient.getBucketLifecycle(
   *         GetBucketLifecycleArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketLifecycleArgs} object.
   * @return {@link LifecycleConfiguration} object.
   * @return String - Life cycle configuration as XML string.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public LifecycleConfiguration getBucketLifecycle(GetBucketLifecycleArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getBucketLifecycle(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Gets notification configuration of a bucket.
   *
   * <pre>Example:{@code
   * NotificationConfiguration config =
   *     minioClient.getBucketNotification(
   *         GetBucketNotificationArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketNotificationArgs} object.
   * @return {@link NotificationConfiguration} - Notification configuration.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public NotificationConfiguration getBucketNotification(GetBucketNotificationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getBucketNotification(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Sets notification configuration to a bucket.
   *
   * <pre>Example:{@code
   * List<EventType> eventList = new LinkedList<>();
   * eventList.add(EventType.OBJECT_CREATED_PUT);
   * eventList.add(EventType.OBJECT_CREATED_COPY);
   *
   * QueueConfiguration queueConfiguration = new QueueConfiguration();
   * queueConfiguration.setQueue("arn:minio:sqs::1:webhook");
   * queueConfiguration.setEvents(eventList);
   * queueConfiguration.setPrefixRule("images");
   * queueConfiguration.setSuffixRule("pg");
   *
   * List<QueueConfiguration> queueConfigurationList = new LinkedList<>();
   * queueConfigurationList.add(queueConfiguration);
   *
   * NotificationConfiguration config = new NotificationConfiguration();
   * config.setQueueConfigurationList(queueConfigurationList);
   *
   * minioClient.setBucketNotification(
   *     SetBucketNotificationArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketNotificationArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setBucketNotification(SetBucketNotificationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setBucketNotification(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Deletes notification configuration of a bucket.
   *
   * <pre>Example:{@code
   * minioClient.deleteBucketNotification(
   *     DeleteBucketNotificationArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketNotificationArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void deleteBucketNotification(DeleteBucketNotificationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.deleteBucketNotification(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets bucket replication configuration of a bucket.
   *
   * <pre>Example:{@code
   * ReplicationConfiguration config =
   *     minioClient.getBucketReplication(
   *         GetBucketReplicationArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketReplicationArgs} object.
   * @return {@link ReplicationConfiguration} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public ReplicationConfiguration getBucketReplication(GetBucketReplicationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getBucketReplication(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Sets bucket replication configuration to a bucket.
   *
   * <pre>Example:{@code
   * Map<String, String> tags = new HashMap<>();
   * tags.put("key1", "value1");
   * tags.put("key2", "value2");
   *
   * ReplicationRule rule =
   *     new ReplicationRule(
   *         new DeleteMarkerReplication(Status.DISABLED),
   *         new ReplicationDestination(
   *             null, null, "REPLACE-WITH-ACTUAL-DESTINATION-BUCKET-ARN", null, null, null, null),
   *         null,
   *         new RuleFilter(new AndOperator("TaxDocs", tags)),
   *         "rule1",
   *         null,
   *         1,
   *         null,
   *         Status.ENABLED);
   *
   * List<ReplicationRule> rules = new LinkedList<>();
   * rules.add(rule);
   *
   * ReplicationConfiguration config =
   *     new ReplicationConfiguration("REPLACE-WITH-ACTUAL-ROLE", rules);
   *
   * minioClient.setBucketReplication(
   *     SetBucketReplicationArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketReplicationArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setBucketReplication(SetBucketReplicationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setBucketReplication(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Deletes bucket replication configuration from a bucket.
   *
   * <pre>Example:{@code
   * minioClient.deleteBucketReplication(
   *     DeleteBucketReplicationArgs.builder().bucket("my-bucketname"));
   * }</pre>
   *
   * @param args {@link DeleteBucketReplicationArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void deleteBucketReplication(DeleteBucketReplicationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.deleteBucketReplication(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Listens events of object prefix and suffix of a bucket. The returned closable iterator is
   * lazily evaluated hence its required to iterate to get new records and must be used with
   * try-with-resource to release underneath network resources.
   *
   * <pre>Example:{@code
   * String[] events = {"s3:ObjectCreated:*", "s3:ObjectAccessed:*"};
   * try (CloseableIterator<Result<NotificationRecords>> ci =
   *     minioClient.listenBucketNotification(
   *         ListenBucketNotificationArgs.builder()
   *             .bucket("bucketName")
   *             .prefix("")
   *             .suffix("")
   *             .events(events)
   *             .build())) {
   *   while (ci.hasNext()) {
   *     NotificationRecords records = ci.next().get();
   *     for (Event event : records.events()) {
   *       System.out.println("Event " + event.eventType() + " occurred at "
   *           + event.eventTime() + " for " + event.bucketName() + "/"
   *           + event.objectName());
   *     }
   *   }
   * }
   * }</pre>
   *
   * @param args {@link ListenBucketNotificationArgs} object.
   * @return {@code CloseableIterator<Result<NotificationRecords>>} - Lazy closable iterator
   *     contains event records.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CloseableIterator<Result<NotificationRecords>> listenBucketNotification(
      ListenBucketNotificationArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    return asyncClient.listenBucketNotification(args);
  }

  /**
   * Selects content of an object by SQL expression.
   *
   * <pre>Example:{@code
   * String sqlExpression = "select * from S3Object";
   * InputSerialization is =
   *     new InputSerialization(null, false, null, null, FileHeaderInfo.USE, null, null,
   *         null);
   * OutputSerialization os =
   *     new OutputSerialization(null, null, null, QuoteFields.ASNEEDED, null);
   * SelectResponseStream stream =
   *     minioClient.selectObjectContent(
   *       SelectObjectContentArgs.builder()
   *       .bucket("my-bucketname")
   *       .object("my-objectname")
   *       .sqlExpression(sqlExpression)
   *       .inputSerialization(is)
   *       .outputSerialization(os)
   *       .requestProgress(true)
   *       .build());
   *
   * byte[] buf = new byte[512];
   * int bytesRead = stream.read(buf, 0, buf.length);
   * System.out.println(new String(buf, 0, bytesRead, StandardCharsets.UTF_8));
   *
   * Stats stats = stream.stats();
   * System.out.println("bytes scanned: " + stats.bytesScanned());
   * System.out.println("bytes processed: " + stats.bytesProcessed());
   * System.out.println("bytes returned: " + stats.bytesReturned());
   *
   * stream.close();
   * }</pre>
   *
   * @param args instance of {@link SelectObjectContentArgs}
   * @return {@link SelectResponseStream} - Contains filtered records and progress.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public SelectResponseStream selectObjectContent(SelectObjectContentArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    return asyncClient.selectObjectContent(args);
  }

  /**
   * Sets encryption configuration of a bucket.
   *
   * <pre>Example:{@code
   * minioClient.setBucketEncryption(
   *     SetBucketEncryptionArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketEncryptionArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setBucketEncryption(SetBucketEncryptionArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setBucketEncryption(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets encryption configuration of a bucket.
   *
   * <pre>Example:{@code
   * SseConfiguration config =
   *     minioClient.getBucketEncryption(
   *         GetBucketEncryptionArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketEncryptionArgs} object.
   * @return {@link SseConfiguration} - Server-side encryption configuration.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public SseConfiguration getBucketEncryption(GetBucketEncryptionArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getBucketEncryption(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Deletes encryption configuration of a bucket.
   *
   * <pre>Example:{@code
   * minioClient.deleteBucketEncryption(
   *     DeleteBucketEncryptionArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketEncryptionArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void deleteBucketEncryption(DeleteBucketEncryptionArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.deleteBucketEncryption(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets tags of a bucket.
   *
   * <pre>Example:{@code
   * Tags tags =
   *     minioClient.getBucketTags(GetBucketTagsArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketTagsArgs} object.
   * @return {@link Tags} - Tags.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public Tags getBucketTags(GetBucketTagsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getBucketTags(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Sets tags to a bucket.
   *
   * <pre>Example:{@code
   * Map<String, String> map = new HashMap<>();
   * map.put("Project", "Project One");
   * map.put("User", "jsmith");
   * minioClient.setBucketTags(
   *     SetBucketTagsArgs.builder().bucket("my-bucketname").tags(map).build());
   * }</pre>
   *
   * @param args {@link SetBucketTagsArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setBucketTags(SetBucketTagsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setBucketTags(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Deletes tags of a bucket.
   *
   * <pre>Example:{@code
   * minioClient.deleteBucketTags(DeleteBucketTagsArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketTagsArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void deleteBucketTags(DeleteBucketTagsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.deleteBucketTags(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets tags of an object.
   *
   * <pre>Example:{@code
   * Tags tags =
   *     minioClient.getObjectTags(
   *         GetObjectTagsArgs.builder().bucket("my-bucketname").object("my-objectname").build());
   * }</pre>
   *
   * @param args {@link GetObjectTagsArgs} object.
   * @return {@link Tags} - Tags.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public Tags getObjectTags(GetObjectTagsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getObjectTags(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Sets tags to an object.
   *
   * <pre>Example:{@code
   * Map<String, String> map = new HashMap<>();
   * map.put("Project", "Project One");
   * map.put("User", "jsmith");
   * minioClient.setObjectTags(
   *     SetObjectTagsArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .tags((map)
   *         .build());
   * }</pre>
   *
   * @param args {@link SetObjectTagsArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setObjectTags(SetObjectTagsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setObjectTags(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Deletes tags of an object.
   *
   * <pre>Example:{@code
   * minioClient.deleteObjectTags(
   *     DeleteObjectTags.builder().bucket("my-bucketname").object("my-objectname").build());
   * }</pre>
   *
   * @param args {@link DeleteObjectTagsArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void deleteObjectTags(DeleteObjectTagsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.deleteObjectTags(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets CORS configuration of a bucket.
   *
   * <pre>Example:{@code
   * CORSConfiguration config =
   *     minioClient.getBucketCors(GetBucketCorsArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketCorsArgs} object.
   * @return {@link CORSConfiguration} - CORSConfiguration.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CORSConfiguration getBucketCors(GetBucketCorsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getBucketCors(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Sets CORS configuration to a bucket.
   *
   * <pre>Example:{@code
   * CORSConfiguration config =
   *     new CORSConfiguration(
   *         Arrays.asList(
   *             new CORSConfiguration.CORSRule[] {
   *               // Rule 1
   *               new CORSConfiguration.CORSRule(
   *                   Arrays.asList(new String[] {"*"}), // Allowed headers
   *                   Arrays.asList(new String[] {"PUT", "POST", "DELETE"}), // Allowed methods
   *                   Arrays.asList(new String[] {"http://www.example.com"}), // Allowed origins
   *                   Arrays.asList(
   *                       new String[] {"x-amz-server-side-encryption"}), // Expose headers
   *                   null, // ID
   *                   3000), // Maximum age seconds
   *               // Rule 2
   *               new CORSConfiguration.CORSRule(
   *                   null, // Allowed headers
   *                   Arrays.asList(new String[] {"GET"}), // Allowed methods
   *                   Arrays.asList(new String[] {"*"}), // Allowed origins
   *                   null, // Expose headers
   *                   null, // ID
   *                   null // Maximum age seconds
   *                   )
   *             }));
   * minioClient.setBucketCors(
   *     SetBucketCorsArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketCorsArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void setBucketCors(SetBucketCorsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.setBucketCors(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Deletes CORS configuration of a bucket.
   *
   * <pre>Example:{@code
   * minioClient.deleteBucketCors(DeleteBucketCorsArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketCorsArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public void deleteBucketCors(DeleteBucketCorsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      asyncClient.deleteBucketCors(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
    }
  }

  /**
   * Gets access control policy of an object.
   *
   * <pre>Example:{@code
   * AccessControlPolicy policy =
   *     minioClient.getObjectAcl(
   *         GetObjectAclArgs.builder().bucket("my-bucketname").object("my-objectname").build());
   * }</pre>
   *
   * @param args {@link GetObjectAclArgs} object.
   * @return {@link AccessControlPolicy} - Access control policy object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public AccessControlPolicy getObjectAcl(GetObjectAclArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getObjectAcl(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Gets attributes of an object.
   *
   * <pre>Example:{@code
   * GetObjectAttributesResponse response =
   *     minioClient.getObjectAttributes(
   *         GetObjectAttributesArgs.builder()
   *             .bucket("my-bucketname")
   *             .object("my-objectname")
   *             .objectAttributes(
   *                 new String[] {
   *                   "ETag", "Checksum", "ObjectParts", "StorageClass", "ObjectSize"
   *                 })
   *             .build());
   * }</pre>
   *
   * @param args {@link GetObjectAttributesArgs} object.
   * @return {@link GetObjectAttributesResponse} - Response object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public GetObjectAttributesResponse getObjectAttributes(GetObjectAttributesArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.getObjectAttributes(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Uploads multiple objects in a single put call. It is done by creating intermediate TAR file
   * optionally compressed which is uploaded to S3 service.
   *
   * <pre>Example:{@code
   * // Upload snowball objects.
   * List<SnowballObject> objects = new ArrayList<SnowballObject>();
   * objects.add(
   *     new SnowballObject(
   *         "my-object-one",
   *         new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)),
   *         5,
   *         null));
   * objects.add(
   *     new SnowballObject(
   *         "my-object-two",
   *         new ByteArrayInputStream("java".getBytes(StandardCharsets.UTF_8)),
   *         4,
   *         null));
   * minioClient.uploadSnowballObjects(
   *     UploadSnowballObjectsArgs.builder().bucket("my-bucketname").objects(objects).build());
   * }</pre>
   *
   * @param args {@link UploadSnowballObjectsArgs} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public ObjectWriteResponse uploadSnowballObjects(UploadSnowballObjectsArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.uploadSnowballObjects(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Uploads multiple objects with same content from single stream with optional metadata and tags.
   *
   * <pre>Example:{@code
   * Map<String, String> map = new HashMap<>();
   * map.put("Project", "Project One");
   * map.put("User", "jsmith");
   * PutObjectFanOutResponse future =
   *     minioClient.putObjectFanOut(
   *         PutObjectFanOutArgs.builder().bucket("my-bucketname").stream(
   *                 new ByteArrayInputStream("somedata".getBytes(StandardCharsets.UTF_8)), 8)
   *             .entries(
   *                 Arrays.asList(
   *                     new PutObjectFanOutEntry[] {
   *                       PutObjectFanOutEntry.builder().key("fan-out.0").build(),
   *                       PutObjectFanOutEntry.builder().key("fan-out.1").tags(map).build()
   *                     }))
   *             .build());
   * }</pre>
   *
   * @param args {@link PutObjectFanOutArgs} object.
   * @return {@link PutObjectFanOutResponse} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public PutObjectFanOutResponse putObjectFanOut(PutObjectFanOutArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.putObjectFanOut(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Performs language model inference with the prompt and referenced object as context.
   *
   * @param args {@link PromptObjectArgs} object.
   * @return {@link PromptObjectResponse} object.
   * @throws ErrorResponseException thrown to indicate S3 service returned an error response.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws InvalidResponseException thrown to indicate S3 service returned invalid or no error
   *     response.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public PromptObjectResponse promptObject(PromptObjectArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    try {
      return asyncClient.promptObject(args).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      asyncClient.throwEncapsulatedException(e);
      return null;
    }
  }

  /**
   * Sets HTTP connect, write and read timeouts. A value of 0 means no timeout, otherwise values
   * must be between 1 and Integer.MAX_VALUE when converted to milliseconds.
   *
   * <pre>Example:{@code
   * minioClient.setTimeout(TimeUnit.SECONDS.toMillis(10), TimeUnit.SECONDS.toMillis(10),
   *     TimeUnit.SECONDS.toMillis(30));
   * }</pre>
   *
   * @param connectTimeout HTTP connect timeout in milliseconds.
   * @param writeTimeout HTTP write timeout in milliseconds.
   * @param readTimeout HTTP read timeout in milliseconds.
   */
  public void setTimeout(long connectTimeout, long writeTimeout, long readTimeout) {
    asyncClient.setTimeout(connectTimeout, writeTimeout, readTimeout);
  }

  /**
   * Ignores check on server certificate for HTTPS connection.
   *
   * <pre>Example:{@code
   * minioClient.ignoreCertCheck();
   * }</pre>
   *
   * @throws KeyManagementException thrown to indicate key management error.
   * @throws NoSuchAlgorithmException thrown to indicate missing of SSL library.
   */
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
      value = "SIC",
      justification = "Should not be used in production anyways.")
  public void ignoreCertCheck() throws KeyManagementException, NoSuchAlgorithmException {
    asyncClient.ignoreCertCheck();
  }

  /**
   * Sets application's name/version to user agent. For more information about user agent refer <a
   * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html">#rfc2616</a>.
   *
   * @param name Your application name.
   * @param version Your application version.
   */
  public void setAppInfo(String name, String version) {
    asyncClient.setAppInfo(name, version);
  }

  /**
   * Enables HTTP call tracing and written to traceStream.
   *
   * @param traceStream {@link OutputStream} for writing HTTP call tracing.
   * @see #traceOff
   */
  public void traceOn(OutputStream traceStream) {
    asyncClient.traceOn(traceStream);
  }

  /**
   * Disables HTTP call tracing previously enabled.
   *
   * @see #traceOn
   * @throws IOException upon connection error
   */
  public void traceOff() throws IOException {
    asyncClient.traceOff();
  }

  /** Enables dual-stack endpoint for Amazon S3 endpoint. */
  public void enableDualStackEndpoint() {
    asyncClient.enableDualStackEndpoint();
  }

  /** Disables dual-stack endpoint for Amazon S3 endpoint. */
  public void disableDualStackEndpoint() {
    asyncClient.disableDualStackEndpoint();
  }

  /** Enables virtual-style endpoint. */
  public void enableVirtualStyleEndpoint() {
    asyncClient.enableVirtualStyleEndpoint();
  }

  /** Disables virtual-style endpoint. */
  public void disableVirtualStyleEndpoint() {
    asyncClient.disableVirtualStyleEndpoint();
  }

  /** Sets AWS S3 domain prefix. */
  public void setAwsS3Prefix(String awsS3Prefix) {
    asyncClient.setAwsS3Prefix(awsS3Prefix);
  }

  @Override
  public void close() throws Exception {
    if (asyncClient != null) {
      asyncClient.close();
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link MinioClient}. */
  public static final class Builder {
    private MinioAsyncClient.Builder asyncClientBuilder = null;

    public Builder() {
      asyncClientBuilder = MinioAsyncClient.builder();
    }

    public Builder endpoint(String endpoint) {
      asyncClientBuilder.endpoint(endpoint);
      return this;
    }

    public Builder endpoint(String endpoint, int port, boolean secure) {
      asyncClientBuilder.endpoint(endpoint, port, secure);
      return this;
    }

    public Builder endpoint(URL url) {
      asyncClientBuilder.endpoint(url);
      return this;
    }

    public Builder endpoint(HttpUrl url) {
      asyncClientBuilder.endpoint(url);
      return this;
    }

    public Builder region(String region) {
      asyncClientBuilder.region(region);
      return this;
    }

    public Builder credentials(String accessKey, String secretKey) {
      asyncClientBuilder.credentials(accessKey, secretKey);
      return this;
    }

    public Builder credentialsProvider(Provider provider) {
      asyncClientBuilder.credentialsProvider(provider);
      return this;
    }

    public Builder httpClient(OkHttpClient httpClient) {
      asyncClientBuilder.httpClient(httpClient);
      return this;
    }

    public Builder httpClient(OkHttpClient httpClient, boolean close) {
      asyncClientBuilder.httpClient(httpClient, close);
      return this;
    }

    public MinioClient build() {
      MinioAsyncClient asyncClient = asyncClientBuilder.build();
      return new MinioClient(asyncClient);
    }
  }
}
