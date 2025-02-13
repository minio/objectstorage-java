/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2022 MinIO, Inc.
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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import io.minio.credentials.Credentials;
import io.minio.credentials.Provider;
import io.minio.credentials.StaticProvider;
import io.minio.errors.BucketPolicyTooLargeException;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.http.HttpUtils;
import io.minio.http.Method;
import io.minio.messages.AccessControlPolicy;
import io.minio.messages.Bucket;
import io.minio.messages.CORSConfiguration;
import io.minio.messages.CopyObjectResult;
import io.minio.messages.CreateBucketConfiguration;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.GetObjectAttributesOutput;
import io.minio.messages.Item;
import io.minio.messages.LegalHold;
import io.minio.messages.LifecycleConfiguration;
import io.minio.messages.ListAllMyBucketsResult;
import io.minio.messages.NotificationConfiguration;
import io.minio.messages.NotificationRecords;
import io.minio.messages.ObjectLockConfiguration;
import io.minio.messages.Part;
import io.minio.messages.ReplicationConfiguration;
import io.minio.messages.Retention;
import io.minio.messages.SelectObjectContentRequest;
import io.minio.messages.SseConfiguration;
import io.minio.messages.Tags;
import io.minio.messages.VersioningConfiguration;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import okhttp3.HttpUrl;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

/**
 * Simple Storage Service (aka S3) client to perform bucket and object operations asynchronously.
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
 * <p>Use {@code MinioAsyncClient.builder()} to create S3 client.
 *
 * <pre>{@code
 * // Create client with anonymous access.
 * MinioAsyncClient minioAsyncClient =
 *     MinioAsyncClient.builder().endpoint("https://play.min.io").build();
 *
 * // Create client with credentials.
 * MinioAsyncClient minioAsyncClient =
 *     MinioAsyncClient.builder()
 *         .endpoint("https://play.min.io")
 *         .credentials("Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
 *         .build();
 * }</pre>
 */
public class MinioAsyncClient extends S3Base {
  private MinioAsyncClient(
      HttpUrl baseUrl,
      String awsS3Prefix,
      String awsDomainSuffix,
      boolean awsDualstack,
      boolean useVirtualStyle,
      String region,
      Provider provider,
      OkHttpClient httpClient,
      boolean closeHttpClient,
      ExecutorService executorService) {
    super(
        baseUrl,
        awsS3Prefix,
        awsDomainSuffix,
        awsDualstack,
        useVirtualStyle,
        region,
        provider,
        httpClient,
        closeHttpClient,
        executorService);
  }

  protected MinioAsyncClient(MinioAsyncClient client) {
    super(client);
  }

  /**
   * Gets information of an object asynchronously.
   *
   * <pre>Example:{@code
   * // Get information of an object.
   * CompletableFuture<StatObjectResponse> future =
   *     minioAsyncClient.statObject(
   *         StatObjectArgs.builder().bucket("my-bucketname").object("my-objectname").build());
   *
   * // Get information of SSE-C encrypted object.
   * CompletableFuture<StatObjectResponse> future =
   *     minioAsyncClient.statObject(
   *         StatObjectArgs.builder()
   *             .bucket("my-bucketname")
   *             .object("my-objectname")
   *             .ssec(ssec)
   *             .build());
   *
   * // Get information of a versioned object.
   * CompletableFuture<StatObjectResponse> future =
   *     minioAsyncClient.statObject(
   *         StatObjectArgs.builder()
   *             .bucket("my-bucketname")
   *             .object("my-objectname")
   *             .versionId("version-id")
   *             .build());
   *
   * // Get information of a SSE-C encrypted versioned object.
   * CompletableFuture<StatObjectResponse> future =
   *     minioAsyncClient.statObject(
   *         StatObjectArgs.builder()
   *             .bucket("my-bucketname")
   *             .object("my-objectname")
   *             .versionId("version-id")
   *             .ssec(ssec)
   *             .build());
   * }</pre>
   *
   * @param args {@link StatObjectArgs} object.
   * @return {@link CompletableFuture}&lt;{@link StatObjectResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   * @see StatObjectResponse
   */
  public CompletableFuture<StatObjectResponse> statObject(StatObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return super.statObjectAsync(args);
  }

  /**
   * Gets data from offset to length of a SSE-C encrypted object asynchronously.
   *
   * <pre>Example:{@code
   * CompletableFuture<GetObjectResponse> future = minioAsyncClient.getObject(
   *     GetObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .offset(offset)
   *         .length(len)
   *         .ssec(ssec)
   *         .build()
   * }</pre>
   *
   * @param args Object of {@link GetObjectArgs}
   * @return {@link CompletableFuture}&lt;{@link GetObjectResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   * @see GetObjectResponse
   */
  public CompletableFuture<GetObjectResponse> getObject(GetObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    args.validateSsec(this.baseUrl);
    return executeGetAsync(
            args,
            args.getHeaders(),
            (args.versionId() != null) ? newMultimap("versionId", args.versionId()) : null)
        .thenApply(
            response -> {
              return new GetObjectResponse(
                  response.headers(),
                  args.bucket(),
                  args.region(),
                  args.object(),
                  response.body().byteStream());
            });
  }

  private void downloadObject(
      String filename,
      boolean overwrite,
      StatObjectResponse statObjectResponse,
      GetObjectResponse getObjectResponse)
      throws IOException {
    OutputStream os = null;
    try {
      Path filePath = Paths.get(filename);
      String tempFilename =
          filename + "." + S3Escaper.encode(statObjectResponse.etag()) + ".part.minio";
      Path tempFilePath = Paths.get(tempFilename);
      if (Files.exists(tempFilePath)) Files.delete(tempFilePath);
      os = Files.newOutputStream(tempFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      long bytesWritten = ByteStreams.copy(getObjectResponse, os);
      if (bytesWritten != statObjectResponse.size()) {
        throw new IOException(
            tempFilename
                + ": unexpected data written.  expected = "
                + statObjectResponse.size()
                + ", written = "
                + bytesWritten);
      }

      if (overwrite) {
        Files.move(tempFilePath, filePath, StandardCopyOption.REPLACE_EXISTING);
      } else {
        Files.move(tempFilePath, filePath);
      }
    } finally {
      getObjectResponse.close();
      if (os != null) os.close();
    }
  }

  /**
   * Downloads data of a SSE-C encrypted object to file.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.downloadObject(
   *     DownloadObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .ssec(ssec)
   *         .filename("my-filename")
   *         .build());
   * }</pre>
   *
   * @param args Object of {@link DownloadObjectArgs}
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> downloadObject(DownloadObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    String filename = args.filename();
    Path filePath = Paths.get(filename);
    if (!args.overwrite() && Files.exists(filePath)) {
      throw new IllegalArgumentException("Destination file " + filename + " already exists");
    }

    return statObjectAsync(new StatObjectArgs(args))
        .thenCombine(
            getObject(new GetObjectArgs(args)),
            (statObjectResponse, getObjectResponse) -> {
              try {
                downloadObject(filename, args.overwrite(), statObjectResponse, getObjectResponse);
                return null;
              } catch (IOException e) {
                throw new CompletionException(e);
              }
            })
        .thenAccept(nullValue -> {});
  }

  /**
   * Creates an object by server-side copying data from another object.
   *
   * <pre>Example:{@code
   * // Create object "my-objectname" in bucket "my-bucketname" by copying from object
   * // "my-objectname" in bucket "my-source-bucketname".
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.copyObject(
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
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.copyObject(
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
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.copyObject(
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
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.copyObject(
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
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.copyObject(
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
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.copyObject(
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
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.copyObject(
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
   * @return {@link CompletableFuture}&lt;{@link ObjectWriteResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ObjectWriteResponse> copyObject(CopyObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    args.validateSse(this.baseUrl);

    return CompletableFuture.supplyAsync(
            () -> args.source().offset() != null && args.source().length() != null, executorService)
        .thenCompose(
            condition -> {
              if (condition) {
                try {
                  return statObjectAsync(new StatObjectArgs((ObjectReadArgs) args.source()));
                } catch (InsufficientDataException
                    | InternalException
                    | InvalidKeyException
                    | IOException
                    | NoSuchAlgorithmException
                    | XmlParserException e) {
                  throw new CompletionException(e);
                }
              }
              return CompletableFuture.completedFuture(null);
            })
        .thenApply(stat -> (stat == null) ? (long) -1 : stat.size())
        .thenCompose(
            size -> {
              if (args.source().offset() != null
                  || args.source().length() != null
                  || size > ObjectWriteArgs.MAX_PART_SIZE) {
                if (args.metadataDirective() != null
                    && args.metadataDirective() == Directive.COPY) {
                  throw new IllegalArgumentException(
                      "COPY metadata directive is not applicable to source object size greater than"
                          + " 5 GiB");
                }
                if (args.taggingDirective() != null && args.taggingDirective() == Directive.COPY) {
                  throw new IllegalArgumentException(
                      "COPY tagging directive is not applicable to source object size greater than"
                          + " 5 GiB");
                }

                try {
                  return composeObject(new ComposeObjectArgs(args));
                } catch (InsufficientDataException
                    | InternalException
                    | InvalidKeyException
                    | IOException
                    | NoSuchAlgorithmException
                    | XmlParserException e) {
                  throw new CompletionException(e);
                }
              }
              return CompletableFuture.completedFuture(null);
            })
        .thenCompose(
            objectWriteResponse -> {
              if (objectWriteResponse != null) {
                return CompletableFuture.completedFuture(objectWriteResponse);
              }

              Multimap<String, String> headers = args.genHeaders();

              if (args.metadataDirective() != null) {
                headers.put("x-amz-metadata-directive", args.metadataDirective().name());
              }

              if (args.taggingDirective() != null) {
                headers.put("x-amz-tagging-directive", args.taggingDirective().name());
              }

              headers.putAll(args.source().genCopyHeaders());

              try {
                return executePutAsync(args, headers, null, null, 0)
                    .thenApply(
                        response -> {
                          try {
                            CopyObjectResult result =
                                Xml.unmarshal(CopyObjectResult.class, response.body().charStream());
                            return new ObjectWriteResponse(
                                response.headers(),
                                args.bucket(),
                                args.region(),
                                args.object(),
                                result.etag(),
                                response.header("x-amz-version-id"),
                                result);
                          } catch (XmlParserException e) {
                            throw new CompletionException(e);
                          } finally {
                            response.close();
                          }
                        });
              } catch (InsufficientDataException
                  | InternalException
                  | InvalidKeyException
                  | IOException
                  | NoSuchAlgorithmException
                  | XmlParserException e) {
                throw new CompletionException(e);
              }
            });
  }

  private CompletableFuture<Part[]> uploadPartCopy(
      String bucketName,
      String region,
      String objectName,
      String uploadId,
      int partNumber,
      Multimap<String, String> headers,
      Part[] parts)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return uploadPartCopyAsync(bucketName, region, objectName, uploadId, partNumber, headers, null)
        .thenApply(
            uploadPartCopyResponse -> {
              parts[partNumber - 1] = new Part(partNumber, uploadPartCopyResponse.result().etag());
              return parts;
            });
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
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.composeObject(
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
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.composeObject(
   *     ComposeObjectArgs.builder()
   *        .bucket("my-bucketname")
   *        .object("my-objectname")
   *        .sources(sourceObjectList)
   *        .userMetadata(userMetadata)
   *        .build());
   *
   * // Create my-bucketname/my-objectname with user metadata and server-side encryption
   * // by combining source object list.
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.composeObject(
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
   * @return {@link CompletableFuture}&lt;{@link ObjectWriteResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ObjectWriteResponse> composeObject(ComposeObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    args.validateSse(this.baseUrl);
    List<ComposeSource> sources = args.sources();
    int[] partCount = {0};
    String[] uploadIdCopy = {null};

    return calculatePartCountAsync(sources)
        .thenApply(
            count -> {
              partCount[0] = count;
              return (count == 1
                  && args.sources().get(0).offset() == null
                  && args.sources().get(0).length() == null);
            })
        .thenCompose(
            copyObjectFlag -> {
              if (copyObjectFlag) {
                try {
                  return copyObject(new CopyObjectArgs(args));
                } catch (InsufficientDataException
                    | InternalException
                    | InvalidKeyException
                    | IOException
                    | NoSuchAlgorithmException
                    | XmlParserException e) {
                  throw new CompletionException(e);
                }
              }
              return CompletableFuture.completedFuture(null);
            })
        .thenCompose(
            objectWriteResponse -> {
              if (objectWriteResponse != null) {
                return CompletableFuture.completedFuture(objectWriteResponse);
              }

              CompletableFuture<ObjectWriteResponse> completableFuture =
                  CompletableFuture.supplyAsync(
                          () -> {
                            Multimap<String, String> headers = newMultimap(args.extraHeaders());
                            headers.putAll(args.genHeaders());
                            return headers;
                          }, executorService)
                      .thenCompose(
                          headers -> {
                            try {
                              return createMultipartUploadAsync(
                                  args.bucket(),
                                  args.region(),
                                  args.object(),
                                  headers,
                                  args.extraQueryParams());
                            } catch (InsufficientDataException
                                | InternalException
                                | InvalidKeyException
                                | IOException
                                | NoSuchAlgorithmException
                                | XmlParserException e) {
                              throw new CompletionException(e);
                            }
                          })
                      .thenApply(
                          createMultipartUploadResponse -> {
                            String uploadId = createMultipartUploadResponse.result().uploadId();
                            uploadIdCopy[0] = uploadId;
                            return uploadId;
                          })
                      .thenCompose(
                          uploadId -> {
                            Multimap<String, String> ssecHeaders = HashMultimap.create();
                            if (args.sse() != null
                                && args.sse() instanceof ServerSideEncryptionCustomerKey) {
                              ssecHeaders.putAll(newMultimap(args.sse().headers()));
                            }

                            int partNumber = 0;
                            CompletableFuture<Part[]> future =
                                CompletableFuture.supplyAsync(
                                    () -> {
                                      return new Part[partCount[0]];
                                    }, executorService);
                            for (ComposeSource src : sources) {
                              long size = 0;
                              try {
                                size = src.objectSize();
                              } catch (InternalException e) {
                                throw new CompletionException(e);
                              }
                              if (src.length() != null) {
                                size = src.length();
                              } else if (src.offset() != null) {
                                size -= src.offset();
                              }
                              long offset = 0;
                              if (src.offset() != null) offset = src.offset();

                              final Multimap<String, String> headers;
                              try {
                                headers = newMultimap(src.headers());
                              } catch (InternalException e) {
                                throw new CompletionException(e);
                              }
                              headers.putAll(ssecHeaders);

                              if (size <= ObjectWriteArgs.MAX_PART_SIZE) {
                                partNumber++;
                                if (src.length() != null) {
                                  headers.put(
                                      "x-amz-copy-source-range",
                                      "bytes=" + offset + "-" + (offset + src.length() - 1));
                                } else if (src.offset() != null) {
                                  headers.put(
                                      "x-amz-copy-source-range",
                                      "bytes=" + offset + "-" + (offset + size - 1));
                                }

                                final int partNum = partNumber;
                                future =
                                    future.thenCompose(
                                        parts -> {
                                          try {
                                            return uploadPartCopy(
                                                args.bucket(),
                                                args.region(),
                                                args.object(),
                                                uploadId,
                                                partNum,
                                                headers,
                                                parts);
                                          } catch (InsufficientDataException
                                              | InternalException
                                              | InvalidKeyException
                                              | IOException
                                              | NoSuchAlgorithmException
                                              | XmlParserException e) {
                                            throw new CompletionException(e);
                                          }
                                        });
                                continue;
                              }

                              while (size > 0) {
                                partNumber++;

                                long length = size;
                                if (length > ObjectWriteArgs.MAX_PART_SIZE) {
                                  length = ObjectWriteArgs.MAX_PART_SIZE;
                                }
                                long endBytes = offset + length - 1;

                                Multimap<String, String> headersCopy = newMultimap(headers);
                                headersCopy.put(
                                    "x-amz-copy-source-range", "bytes=" + offset + "-" + endBytes);

                                final int partNum = partNumber;
                                future =
                                    future.thenCompose(
                                        parts -> {
                                          try {
                                            return uploadPartCopy(
                                                args.bucket(),
                                                args.region(),
                                                args.object(),
                                                uploadId,
                                                partNum,
                                                headersCopy,
                                                parts);
                                          } catch (InsufficientDataException
                                              | InternalException
                                              | InvalidKeyException
                                              | IOException
                                              | NoSuchAlgorithmException
                                              | XmlParserException e) {
                                            throw new CompletionException(e);
                                          }
                                        });
                                offset += length;
                                size -= length;
                              }
                            }

                            return future;
                          })
                      .thenCompose(
                          parts -> {
                            try {
                              return completeMultipartUploadAsync(
                                  args.bucket(),
                                  args.region(),
                                  args.object(),
                                  uploadIdCopy[0],
                                  parts,
                                  null,
                                  null);
                            } catch (InsufficientDataException
                                | InternalException
                                | InvalidKeyException
                                | IOException
                                | NoSuchAlgorithmException
                                | XmlParserException e) {
                              throw new CompletionException(e);
                            }
                          });

              completableFuture.exceptionally(
                  e -> {
                    if (uploadIdCopy[0] != null) {
                      try {
                        abortMultipartUploadAsync(
                                args.bucket(),
                                args.region(),
                                args.object(),
                                uploadIdCopy[0],
                                null,
                                null)
                            .get();
                      } catch (InsufficientDataException
                          | InternalException
                          | InvalidKeyException
                          | IOException
                          | NoSuchAlgorithmException
                          | XmlParserException
                          | InterruptedException
                          | ExecutionException ex) {
                        throw new CompletionException(ex);
                      }
                    }
                    throw new CompletionException(e);
                  });
              return completableFuture;
            });
  }

  /**
   * Gets presigned URL of an object for HTTP method, expiry time and custom request parameters.
   *
   * <pre>Example:{@code
   * // Get presigned URL string to delete 'my-objectname' in 'my-bucketname' and its life time
   * // is one day.
   * String url =
   *    minioAsyncClient.getPresignedObjectUrl(
   *        GetPresignedObjectUrlArgs.builder()
   *            .method(Method.DELETE)
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
   *    minioAsyncClient.getPresignedObjectUrl(
   *        GetPresignedObjectUrlArgs.builder()
   *            .method(Method.PUT)
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
   *    minioAsyncClient.getPresignedObjectUrl(
   *        GetPresignedObjectUrlArgs.builder()
   *            .method(Method.GET)
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
    checkArgs(args);

    byte[] body =
        (args.method() == Method.PUT || args.method() == Method.POST) ? HttpUtils.EMPTY_BODY : null;

    Multimap<String, String> queryParams = newMultimap(args.extraQueryParams());
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());

    String region = null;
    try {
      region = getRegionAsync(args.bucket(), args.region()).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throwEncapsulatedException(e);
    }

    if (provider == null) {
      HttpUrl url = buildUrl(args.method(), args.bucket(), args.object(), region, queryParams);
      return url.toString();
    }

    Credentials creds = provider.fetch();
    if (creds.sessionToken() != null) queryParams.put("X-Amz-Security-Token", creds.sessionToken());
    HttpUrl url = buildUrl(args.method(), args.bucket(), args.object(), region, queryParams);
    Request request =
        createRequest(
            url,
            args.method(),
            args.extraHeaders() == null ? null : httpHeaders(args.extraHeaders()),
            body,
            0,
            creds);
    url = Signer.presignV4(request, region, creds.accessKey(), creds.secretKey(), args.expiry());
    return url.toString();
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
   * Map<String, String> formData = minioAsyncClient.getPresignedPostFormData(policy);
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
    if (provider == null) {
      throw new IllegalArgumentException(
          "Anonymous access does not require presigned post form-data");
    }

    String region = null;
    try {
      region = getRegionAsync(policy.bucket(), null).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throwEncapsulatedException(e);
    }
    return policy.formData(provider.fetch(), region);
  }

  /**
   * Removes an object.
   *
   * <pre>Example:{@code
   * // Remove object.
   * CompletableFuture<Void> future = minioAsyncClient.removeObject(
   *     RemoveObjectArgs.builder().bucket("my-bucketname").object("my-objectname").build());
   *
   * // Remove versioned object.
   * CompletableFuture<Void> future = minioAsyncClient.removeObject(
   *     RemoveObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-versioned-objectname")
   *         .versionId("my-versionid")
   *         .build());
   *
   * // Remove versioned object bypassing Governance mode.
   * CompletableFuture<Void> future = minioAsyncClient.removeObject(
   *     RemoveObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-versioned-objectname")
   *         .versionId("my-versionid")
   *         .bypassRetentionMode(true)
   *         .build());
   * }</pre>
   *
   * @param args {@link RemoveObjectArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
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
  public CompletableFuture<Void> removeObject(RemoveObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeDeleteAsync(
            args,
            args.bypassGovernanceMode()
                ? newMultimap("x-amz-bypass-governance-retention", "true")
                : null,
            (args.versionId() != null) ? newMultimap("versionId", args.versionId()) : null)
        .thenAccept(response -> response.close());
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
   * Iterable<Result<DeleteError>> results =
   *     minioAsyncClient.removeObjects(
   *         RemoveObjectsArgs.builder().bucket("my-bucketname").objects(objects).build());
   * for (Result<DeleteError> result : results) {
   *   DeleteError error = result.get();
   *   System.out.println(
   *       "Error in deleting object " + error.objectName() + "; " + error.message());
   * }
   * }</pre>
   *
   * @param args {@link RemoveObjectsArgs} object.
   * @return {@code Iterable<Result<DeleteError>>} - Lazy iterator contains object removal status.
   */
  public Iterable<Result<DeleteError>> removeObjects(RemoveObjectsArgs args) {
    checkArgs(args);

    return new Iterable<Result<DeleteError>>() {
      @Override
      public Iterator<Result<DeleteError>> iterator() {
        return new Iterator<Result<DeleteError>>() {
          private Result<DeleteError> error = null;
          private Iterator<DeleteError> errorIterator = null;
          private boolean completed = false;
          private Iterator<DeleteObject> objectIter = args.objects().iterator();

          private void setError() {
            error = null;
            while (errorIterator.hasNext()) {
              DeleteError deleteError = errorIterator.next();
              if (!"NoSuchVersion".equals(deleteError.code())) {
                error = new Result<>(deleteError);
                break;
              }
            }
          }

          private synchronized void populate() {
            if (completed) {
              return;
            }

            try {
              List<DeleteObject> objectList = new LinkedList<>();
              while (objectIter.hasNext() && objectList.size() < 1000) {
                objectList.add(objectIter.next());
              }

              completed = objectList.isEmpty();
              if (completed) return;
              DeleteObjectsResponse response = null;
              try {
                response =
                    deleteObjectsAsync(
                            args.bucket(),
                            args.region(),
                            objectList,
                            true,
                            args.bypassGovernanceMode(),
                            args.extraHeaders(),
                            args.extraQueryParams())
                        .get();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              } catch (ExecutionException e) {
                throwEncapsulatedException(e);
              }
              if (!response.result().errorList().isEmpty()) {
                errorIterator = response.result().errorList().iterator();
                setError();
                completed = true;
              }
            } catch (ErrorResponseException
                | InsufficientDataException
                | InternalException
                | InvalidKeyException
                | InvalidResponseException
                | IOException
                | NoSuchAlgorithmException
                | ServerException
                | XmlParserException e) {
              error = new Result<>(e);
              completed = true;
            }
          }

          @Override
          public boolean hasNext() {
            while (error == null && errorIterator == null && !completed) {
              populate();
            }

            if (error == null && errorIterator != null) setError();
            if (error != null) return true;
            if (completed) return false;

            errorIterator = null;
            return hasNext();
          }

          @Override
          public Result<DeleteError> next() {
            if (!hasNext()) throw new NoSuchElementException();

            if (this.error != null) {
              Result<DeleteError> error = this.error;
              this.error = null;
              return error;
            }

            // This never happens.
            throw new NoSuchElementException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /**
   * Restores an object asynchronously.
   *
   * <pre>Example:{@code
   * // Restore object.
   * CompletableFuture<Void> future = minioAsyncClient.restoreObject(
   *     RestoreObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .request(new RestoreRequest(null, null, null, null, null, null))
   *         .build());
   *
   * // Restore versioned object.
   * CompletableFuture<Void> future = minioAsyncClient.restoreObject(
   *     RestoreObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-versioned-objectname")
   *         .versionId("my-versionid")
   *         .request(new RestoreRequest(null, null, null, null, null, null))
   *         .build());
   * }</pre>
   *
   * @param args {@link RestoreObjectArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> restoreObject(RestoreObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePostAsync(args, null, newMultimap("restore", ""), args.request())
        .thenAccept(response -> response.close());
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
   * Iterable<Result<Item>> results = minioAsyncClient.listObjects(
   *     ListObjectsArgs.builder().bucket("my-bucketname").build());
   *
   * // Lists objects information recursively.
   * Iterable<Result<Item>> results = minioAsyncClient.listObjects(
   *     ListObjectsArgs.builder().bucket("my-bucketname").recursive(true).build());
   *
   * // Lists maximum 100 objects information whose names starts with 'E' and after
   * // 'ExampleGuide.pdf'.
   * Iterable<Result<Item>> results = minioAsyncClient.listObjects(
   *     ListObjectsArgs.builder()
   *         .bucket("my-bucketname")
   *         .startAfter("ExampleGuide.pdf")
   *         .prefix("E")
   *         .maxKeys(100)
   *         .build());
   *
   * // Lists maximum 100 objects information with version whose names starts with 'E' and after
   * // 'ExampleGuide.pdf'.
   * Iterable<Result<Item>> results = minioAsyncClient.listObjects(
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
    if (args.includeVersions() || args.versionIdMarker() != null) {
      return listObjectVersions(args);
    }

    if (args.useApiVersion1()) {
      return listObjectsV1(args);
    }

    return listObjectsV2(args);
  }

  /**
   * Lists bucket information of all buckets.
   *
   * <pre>Example:{@code
   * CompletableFuture<List<Bucket>> future = minioAsyncClient.listBuckets();
   * }</pre>
   *
   * @return {@link CompletableFuture}&lt;{@link List}&lt;{@link Bucket}&gt;&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<List<Bucket>> listBuckets()
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return listBucketsAsync(null, null, null, null, null, null)
        .thenApply(
            response -> {
              return response.result().buckets();
            });
  }

  /**
   * Lists bucket information of all buckets.
   *
   * <pre>Example:{@code
   * Iterable<Result<Bucket>> results = minioAsyncClient.listBuckets(ListBucketsArgs.builder().build());
   * for (Result<Bucket> result : results) {
   *   Bucket bucket = result.get();
   *   System.out.println(String.format("Bucket: %s, Region: %s, CreationDate: %s", bucket.name(), bucket.bucketRegion(), bucket.creationDate()));
   * }
   * }</pre>
   *
   * @return {@link Iterable}&lt;{@link List}&lt;{@link Bucket}&gt;&gt; object.
   */
  public Iterable<Result<Bucket>> listBuckets(ListBucketsArgs args) {
    return new Iterable<Result<Bucket>>() {
      @Override
      public Iterator<Result<Bucket>> iterator() {
        return new Iterator<Result<Bucket>>() {
          private ListAllMyBucketsResult result = null;
          private Result<Bucket> error = null;
          private Iterator<Bucket> iterator = null;
          private boolean completed = false;

          private synchronized void populate() {
            if (completed) return;

            try {
              this.iterator = new LinkedList<Bucket>().iterator();
              try {
                ListBucketsResponse response =
                    listBucketsAsync(
                            args.bucketRegion(),
                            args.maxBuckets(),
                            args.prefix(),
                            (result == null)
                                ? args.continuationToken()
                                : result.continuationToken(),
                            args.extraHeaders(),
                            args.extraQueryParams())
                        .get();
                this.result = response.result();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              } catch (ExecutionException e) {
                throwEncapsulatedException(e);
              }
              this.iterator = this.result.buckets().iterator();
            } catch (ErrorResponseException
                | InsufficientDataException
                | InternalException
                | InvalidKeyException
                | InvalidResponseException
                | IOException
                | NoSuchAlgorithmException
                | ServerException
                | XmlParserException e) {
              this.error = new Result<>(e);
              completed = true;
            }
          }

          @Override
          public boolean hasNext() {
            if (this.completed) return false;

            if (this.error == null && this.iterator == null) {
              populate();
            }

            if (this.error == null
                && !this.iterator.hasNext()
                && this.result.continuationToken() != null
                && !this.result.continuationToken().isEmpty()) {
              populate();
            }

            if (this.error != null) return true;
            if (this.iterator.hasNext()) return true;

            this.completed = true;
            return false;
          }

          @Override
          public Result<Bucket> next() {
            if (this.completed) throw new NoSuchElementException();
            if (this.error == null && this.iterator == null) {
              populate();
            }

            if (this.error == null
                && !this.iterator.hasNext()
                && this.result.continuationToken() != null
                && !this.result.continuationToken().isEmpty()) {
              populate();
            }

            if (this.error != null) {
              this.completed = true;
              return this.error;
            }

            Bucket item = null;
            if (this.iterator.hasNext()) {
              item = this.iterator.next();
            }

            if (item != null) {
              return new Result<>(item);
            }

            this.completed = true;
            throw new NoSuchElementException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /**
   * Checks if a bucket exists.
   *
   * <pre>Example:{@code
   * CompletableFuture<Boolean> future =
   *      minioAsyncClient.bucketExists(BucketExistsArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link BucketExistsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Boolean}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Boolean> bucketExists(BucketExistsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return executeHeadAsync(args, null, null)
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex).errorResponse().code().equals(NO_SUCH_BUCKET)) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenApply(
            response -> {
              try {
                return response != null;
              } finally {
                if (response != null) response.close();
              }
            });
  }

  /**
   * Creates a bucket with region and object lock.
   *
   * <pre>Example:{@code
   * // Create bucket with default region.
   * CompletableFuture<Void> future = minioAsyncClient.makeBucket(
   *     MakeBucketArgs.builder()
   *         .bucket("my-bucketname")
   *         .build());
   *
   * // Create bucket with specific region.
   * CompletableFuture<Void> future = minioAsyncClient.makeBucket(
   *     MakeBucketArgs.builder()
   *         .bucket("my-bucketname")
   *         .region("us-west-1")
   *         .build());
   *
   * // Create object-lock enabled bucket with specific region.
   * CompletableFuture<Void> future = minioAsyncClient.makeBucket(
   *     MakeBucketArgs.builder()
   *         .bucket("my-bucketname")
   *         .region("us-west-1")
   *         .objectLock(true)
   *         .build());
   * }</pre>
   *
   * @param args Object with bucket name, region and lock functionality
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> makeBucket(MakeBucketArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);

    String region = args.region();
    if (this.region != null && !this.region.isEmpty()) {
      // Error out if region does not match with region passed via constructor.
      if (region != null && !region.equals(this.region)) {
        throw new IllegalArgumentException(
            "region must be " + this.region + ", but passed " + region);
      }

      region = this.region;
    }

    if (region == null) {
      region = US_EAST_1;
    }

    Multimap<String, String> headers =
        args.objectLock() ? newMultimap("x-amz-bucket-object-lock-enabled", "true") : null;
    final String location = region;

    return executeAsync(
            Method.PUT,
            args.bucket(),
            null,
            location,
            httpHeaders(merge(args.extraHeaders(), headers)),
            args.extraQueryParams(),
            location.equals(US_EAST_1) ? null : new CreateBucketConfiguration(location),
            0)
        .thenAccept(
            response -> {
              regionCache.put(args.bucket(), location);
              response.close();
            });
  }

  /**
   * Sets versioning configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.setBucketVersioning(
   *     SetBucketVersioningArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketVersioningArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setBucketVersioning(SetBucketVersioningArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(args, null, newMultimap("versioning", ""), args.config(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Gets versioning configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<VersioningConfiguration> future =
   *     minioAsyncClient.getBucketVersioning(
   *         GetBucketVersioningArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketVersioningArgs} object.
   * @return {@link CompletableFuture}&lt;{@link VersioningConfiguration}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<VersioningConfiguration> getBucketVersioning(
      GetBucketVersioningArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeGetAsync(args, null, newMultimap("versioning", ""))
        .thenApply(
            response -> {
              try {
                return Xml.unmarshal(VersioningConfiguration.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Sets default object retention in a bucket.
   *
   * <pre>Example:{@code
   * ObjectLockConfiguration config = new ObjectLockConfiguration(
   *     RetentionMode.COMPLIANCE, new RetentionDurationDays(100));
   * CompletableFuture<Void> future = minioAsyncClient.setObjectLockConfiguration(
   *     SetObjectLockConfigurationArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetObjectLockConfigurationArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setObjectLockConfiguration(SetObjectLockConfigurationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(args, null, newMultimap("object-lock", ""), args.config(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Deletes default object retention in a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.deleteObjectLockConfiguration(
   *     DeleteObjectLockConfigurationArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteObjectLockConfigurationArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> deleteObjectLockConfiguration(
      DeleteObjectLockConfigurationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(
            args, null, newMultimap("object-lock", ""), new ObjectLockConfiguration(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Gets default object retention in a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<ObjectLockConfiguration> future =
   *     minioAsyncClient.getObjectLockConfiguration(
   *         GetObjectLockConfigurationArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetObjectLockConfigurationArgs} object.
   * @return {@link CompletableFuture}&lt;{@link ObjectLockConfiguration}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ObjectLockConfiguration> getObjectLockConfiguration(
      GetObjectLockConfigurationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeGetAsync(args, null, newMultimap("object-lock", ""))
        .thenApply(
            response -> {
              try {
                return Xml.unmarshal(ObjectLockConfiguration.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Sets retention configuration to an object.
   *
   * <pre>Example:{@code
   *  Retention retention = new Retention(
   *       RetentionMode.COMPLIANCE, ZonedDateTime.now().plusYears(1));
   *  CompletableFuture<Void> future = minioAsyncClient.setObjectRetention(
   *      SetObjectRetentionArgs.builder()
   *          .bucket("my-bucketname")
   *          .object("my-objectname")
   *          .config(config)
   *          .bypassGovernanceMode(true)
   *          .build());
   * }</pre>
   *
   * @param args {@link SetObjectRetentionArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setObjectRetention(SetObjectRetentionArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams = newMultimap("retention", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());
    return executePutAsync(
            args,
            args.bypassGovernanceMode()
                ? newMultimap("x-amz-bypass-governance-retention", "True")
                : null,
            queryParams,
            args.config(),
            0)
        .thenAccept(response -> response.close());
  }

  /**
   * Gets retention configuration of an object.
   *
   * <pre>Example:{@code
   * CompletableFuture<Retention> future =
   *     minioAsyncClient.getObjectRetention(GetObjectRetentionArgs.builder()
   *        .bucket(bucketName)
   *        .object(objectName)
   *        .versionId(versionId)
   *        .build());
   * }</pre>
   *
   * @param args {@link GetObjectRetentionArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Retention}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Retention> getObjectRetention(GetObjectRetentionArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams = newMultimap("retention", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());
    return executeGetAsync(args, null, queryParams)
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex)
                    .errorResponse()
                    .code()
                    .equals(NO_SUCH_OBJECT_LOCK_CONFIGURATION)) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenApply(
            response -> {
              if (response == null) return null;
              try {
                return Xml.unmarshal(Retention.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Enables legal hold on an object.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.enableObjectLegalHold(
   *    EnableObjectLegalHoldArgs.builder()
   *        .bucket("my-bucketname")
   *        .object("my-objectname")
   *        .versionId("object-versionId")
   *        .build());
   * }</pre>
   *
   * @param args {@link EnableObjectLegalHoldArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> enableObjectLegalHold(EnableObjectLegalHoldArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams = newMultimap("legal-hold", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());
    return executePutAsync(args, null, queryParams, new LegalHold(true), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Disables legal hold on an object.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.disableObjectLegalHold(
   *    DisableObjectLegalHoldArgs.builder()
   *        .bucket("my-bucketname")
   *        .object("my-objectname")
   *        .versionId("object-versionId")
   *        .build());
   * }</pre>
   *
   * @param args {@link DisableObjectLegalHoldArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> disableObjectLegalHold(DisableObjectLegalHoldArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams = newMultimap("legal-hold", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());
    return executePutAsync(args, null, queryParams, new LegalHold(false), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Returns true if legal hold is enabled on an object.
   *
   * <pre>Example:{@code
   * CompletableFuture<Boolean> future =
   *     s3Client.isObjectLegalHoldEnabled(
   *        IsObjectLegalHoldEnabledArgs.builder()
   *             .bucket("my-bucketname")
   *             .object("my-objectname")
   *             .versionId("object-versionId")
   *             .build());
   * }</pre>
   *
   * @param args {@link IsObjectLegalHoldEnabledArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Boolean}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Boolean> isObjectLegalHoldEnabled(IsObjectLegalHoldEnabledArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams = newMultimap("legal-hold", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());
    return executeGetAsync(args, null, queryParams)
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex)
                    .errorResponse()
                    .code()
                    .equals(NO_SUCH_OBJECT_LOCK_CONFIGURATION)) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenApply(
            response -> {
              if (response == null) return false;
              try {
                LegalHold result = Xml.unmarshal(LegalHold.class, response.body().charStream());
                return result.status();
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Removes an empty bucket using arguments
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future =
   *     minioAsyncClient.removeBucket(RemoveBucketArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link RemoveBucketArgs} bucket.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> removeBucket(RemoveBucketArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeDeleteAsync(args, null, null)
        .thenAccept(response -> regionCache.remove(args.bucket()));
  }

  /**
   * Uploads data from a stream to an object.
   *
   * <pre>Example:{@code
   * // Upload known sized input stream.
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("my-objectname").stream(
   *             inputStream, size, -1)
   *         .contentType("video/mp4")
   *         .build());
   *
   * // Upload unknown sized input stream.
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("my-objectname").stream(
   *             inputStream, -1, 10485760)
   *         .contentType("video/mp4")
   *         .build());
   *
   * // Create object ends with '/' (also called as folder or directory).
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("path/to/").stream(
   *             new ByteArrayInputStream(new byte[] {}), 0, -1)
   *         .build());
   *
   * // Upload input stream with headers and user metadata.
   * Map<String, String> headers = new HashMap<>();
   * headers.put("X-Amz-Storage-Class", "REDUCED_REDUNDANCY");
   * Map<String, String> userMetadata = new HashMap<>();
   * userMetadata.put("My-Project", "Project One");
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("my-objectname").stream(
   *             inputStream, size, -1)
   *         .headers(headers)
   *         .userMetadata(userMetadata)
   *         .build());
   *
   * // Upload input stream with server-side encryption.
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.putObject(
   *     PutObjectArgs.builder().bucket("my-bucketname").object("my-objectname").stream(
   *             inputStream, size, -1)
   *         .sse(sse)
   *         .build());
   * }</pre>
   *
   * @param args {@link PutObjectArgs} object.
   * @return {@link CompletableFuture}&lt;{@link ObjectWriteResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ObjectWriteResponse> putObject(PutObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    args.validateSse(this.baseUrl);
    return putObjectAsync(
        args,
        args.stream(),
        args.objectSize(),
        args.partSize(),
        args.partCount(),
        args.contentType());
  }

  /**
   * Uploads data from a file to an object.
   *
   * <pre>Example:{@code
   * // Upload an JSON file.
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.uploadObject(
   *     UploadObjectArgs.builder()
   *         .bucket("my-bucketname").object("my-objectname").filename("person.json").build());
   *
   * // Upload a video file.
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.uploadObject(
   *     UploadObjectArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .filename("my-video.avi")
   *         .contentType("video/mp4")
   *         .build());
   * }</pre>
   *
   * @param args {@link UploadObjectArgs} object.
   * @return {@link CompletableFuture}&lt;{@link ObjectWriteResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ObjectWriteResponse> uploadObject(UploadObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    args.validateSse(this.baseUrl);
    final RandomAccessFile file = new RandomAccessFile(args.filename(), "r");
    return putObjectAsync(
            args, file, args.objectSize(), args.partSize(), args.partCount(), args.contentType())
        .exceptionally(
            e -> {
              try {
                file.close();
              } catch (IOException ex) {
                throw new CompletionException(ex);
              }

              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              throw new CompletionException(ex);
            })
        .thenApply(
            objectWriteResponse -> {
              try {
                file.close();
              } catch (IOException e) {
                throw new CompletionException(e);
              }
              return objectWriteResponse;
            });
  }

  /**
   * Gets bucket policy configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<String> future =
   *     minioAsyncClient.getBucketPolicy(
   *         GetBucketPolicyArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketPolicyArgs} object.
   * @return {@link CompletableFuture}&lt;{@link String}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<String> getBucketPolicy(GetBucketPolicyArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeGetAsync(args, null, newMultimap("policy", ""))
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex)
                    .errorResponse()
                    .code()
                    .equals(NO_SUCH_BUCKET_POLICY)) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenApply(
            response -> {
              if (response == null) return "";
              try {
                byte[] buf = new byte[MAX_BUCKET_POLICY_SIZE];
                int bytesRead = 0;
                bytesRead = response.body().byteStream().read(buf, 0, MAX_BUCKET_POLICY_SIZE);
                if (bytesRead < 0) {
                  throw new CompletionException(
                      new IOException("unexpected EOF when reading bucket policy"));
                }

                // Read one byte extra to ensure only MAX_BUCKET_POLICY_SIZE data is sent by the
                // server.
                if (bytesRead == MAX_BUCKET_POLICY_SIZE) {
                  int byteRead = 0;
                  while (byteRead == 0) {
                    byteRead = response.body().byteStream().read();
                    if (byteRead < 0) {
                      break; // reached EOF which is fine.
                    }

                    if (byteRead > 0) {
                      throw new CompletionException(
                          new BucketPolicyTooLargeException(args.bucket()));
                    }
                  }
                }

                return new String(buf, 0, bytesRead, StandardCharsets.UTF_8);
              } catch (IOException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
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
   * CompletableFuture<Void> future = minioAsyncClient.setBucketPolicy(
   *     SetBucketPolicyArgs.builder().bucket("my-bucketname").config(policyJson).build());
   * }</pre>
   *
   * @param args {@link SetBucketPolicyArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setBucketPolicy(SetBucketPolicyArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(
            args,
            newMultimap("Content-Type", "application/json"),
            newMultimap("policy", ""),
            args.config(),
            0)
        .thenAccept(response -> response.close());
  }

  /**
   * Deletes bucket policy configuration to a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future =
   *     minioAsyncClient.deleteBucketPolicy(
   *         DeleteBucketPolicyArgs.builder().bucket("my-bucketname"));
   * }</pre>
   *
   * @param args {@link DeleteBucketPolicyArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> deleteBucketPolicy(DeleteBucketPolicyArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeDeleteAsync(args, null, newMultimap("policy", ""))
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex)
                    .errorResponse()
                    .code()
                    .equals(NO_SUCH_BUCKET_POLICY)) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenAccept(
            response -> {
              if (response != null) response.close();
            });
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
   * CompletableFuture<Void> future = minioAsyncClient.setBucketLifecycle(
   *     SetBucketLifecycleArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketLifecycleArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setBucketLifecycle(SetBucketLifecycleArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(args, null, newMultimap("lifecycle", ""), args.config(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Deletes lifecycle configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = deleteBucketLifecycle(
   *     DeleteBucketLifecycleArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketLifecycleArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> deleteBucketLifecycle(DeleteBucketLifecycleArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeDeleteAsync(args, null, newMultimap("lifecycle", ""))
        .thenAccept(response -> response.close());
  }

  /**
   * Gets lifecycle configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<LifecycleConfiguration> future =
   *     minioAsyncClient.getBucketLifecycle(
   *         GetBucketLifecycleArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketLifecycleArgs} object.
   * @return {@link LifecycleConfiguration} object.
   * @return {@link CompletableFuture}&lt;{@link LifecycleConfiguration}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<LifecycleConfiguration> getBucketLifecycle(GetBucketLifecycleArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeGetAsync(args, null, newMultimap("lifecycle", ""))
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex)
                    .errorResponse()
                    .code()
                    .equals("NoSuchLifecycleConfiguration")) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenApply(
            response -> {
              if (response == null) return null;
              try {
                return Xml.unmarshal(LifecycleConfiguration.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Gets notification configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<NotificationConfiguration> future =
   *     minioAsyncClient.getBucketNotification(
   *         GetBucketNotificationArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketNotificationArgs} object.
   * @return {@link CompletableFuture}&lt;{@link NotificationConfiguration}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<NotificationConfiguration> getBucketNotification(
      GetBucketNotificationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeGetAsync(args, null, newMultimap("notification", ""))
        .thenApply(
            response -> {
              try {
                return Xml.unmarshal(NotificationConfiguration.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
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
   * CompletableFuture<Void> future = minioAsyncClient.setBucketNotification(
   *     SetBucketNotificationArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketNotificationArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setBucketNotification(SetBucketNotificationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(args, null, newMultimap("notification", ""), args.config(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Deletes notification configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.deleteBucketNotification(
   *     DeleteBucketNotificationArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketNotificationArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> deleteBucketNotification(DeleteBucketNotificationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(
            args, null, newMultimap("notification", ""), new NotificationConfiguration(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Gets bucket replication configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<ReplicationConfiguration> future =
   *     minioAsyncClient.getBucketReplication(
   *         GetBucketReplicationArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketReplicationArgs} object.
   * @return {@link CompletableFuture}&lt;{@link ReplicationConfiguration}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ReplicationConfiguration> getBucketReplication(
      GetBucketReplicationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeGetAsync(args, null, newMultimap("replication", ""))
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex)
                    .errorResponse()
                    .code()
                    .equals("ReplicationConfigurationNotFoundError")) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenApply(
            response -> {
              if (response == null) return null;
              try {
                return Xml.unmarshal(ReplicationConfiguration.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
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
   * CompletableFuture<Void> future = minioAsyncClient.setBucketReplication(
   *     SetBucketReplicationArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketReplicationArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setBucketReplication(SetBucketReplicationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(
            args,
            (args.objectLockToken() != null)
                ? newMultimap("x-amz-bucket-object-lock-token", args.objectLockToken())
                : null,
            newMultimap("replication", ""),
            args.config(),
            0)
        .thenAccept(response -> response.close());
  }

  /**
   * Deletes bucket replication configuration from a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.deleteBucketReplication(
   *     DeleteBucketReplicationArgs.builder().bucket("my-bucketname"));
   * }</pre>
   *
   * @param args {@link DeleteBucketReplicationArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> deleteBucketReplication(DeleteBucketReplicationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeDeleteAsync(args, null, newMultimap("replication", ""))
        .thenAccept(response -> response.close());
  }

  /**
   * Listens events of object prefix and suffix of a bucket. The returned closable iterator is
   * lazily evaluated hence its required to iterate to get new records and must be used with
   * try-with-resource to release underneath network resources.
   *
   * <pre>Example:{@code
   * String[] events = {"s3:ObjectCreated:*", "s3:ObjectAccessed:*"};
   * try (CloseableIterator<Result<NotificationRecords>> ci =
   *     minioAsyncClient.listenBucketNotification(
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
    checkArgs(args);

    Multimap<String, String> queryParams =
        newMultimap("prefix", args.prefix(), "suffix", args.suffix());
    for (String event : args.events()) {
      queryParams.put("events", event);
    }

    Response response = null;
    try {
      response = executeGetAsync(args, null, queryParams).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throwEncapsulatedException(e);
    }
    NotificationResultRecords result = new NotificationResultRecords(response);
    return result.closeableIterator();
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
   *     minioAsyncClient.selectObjectContent(
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
    checkArgs(args);
    args.validateSsec(this.baseUrl);
    Response response = null;
    try {
      response =
          executePostAsync(
                  args,
                  (args.ssec() != null) ? newMultimap(args.ssec().headers()) : null,
                  newMultimap("select", "", "select-type", "2"),
                  new SelectObjectContentRequest(
                      args.sqlExpression(),
                      args.requestProgress(),
                      args.inputSerialization(),
                      args.outputSerialization(),
                      args.scanStartRange(),
                      args.scanEndRange()))
              .get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throwEncapsulatedException(e);
    }
    return new SelectResponseStream(response.body().byteStream());
  }

  /**
   * Sets encryption configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.setBucketEncryption(
   *     SetBucketEncryptionArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketEncryptionArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setBucketEncryption(SetBucketEncryptionArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(args, null, newMultimap("encryption", ""), args.config(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Gets encryption configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<SseConfiguration> future =
   *     minioAsyncClient.getBucketEncryption(
   *         GetBucketEncryptionArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketEncryptionArgs} object.
   * @return {@link CompletableFuture}&lt;{@link SseConfiguration}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<SseConfiguration> getBucketEncryption(GetBucketEncryptionArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeGetAsync(args, null, newMultimap("encryption", ""))
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex)
                    .errorResponse()
                    .code()
                    .equals(SERVER_SIDE_ENCRYPTION_CONFIGURATION_NOT_FOUND_ERROR)) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenApply(
            response -> {
              if (response == null) return new SseConfiguration(null);
              try {
                return Xml.unmarshal(SseConfiguration.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Deletes encryption configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.deleteBucketEncryption(
   *     DeleteBucketEncryptionArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketEncryptionArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> deleteBucketEncryption(DeleteBucketEncryptionArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeDeleteAsync(args, null, newMultimap("encryption", ""))
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex)
                    .errorResponse()
                    .code()
                    .equals(SERVER_SIDE_ENCRYPTION_CONFIGURATION_NOT_FOUND_ERROR)) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenAccept(
            response -> {
              if (response != null) response.close();
            });
  }

  /**
   * Gets tags of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Tags> future =
   *     minioAsyncClient.getBucketTags(GetBucketTagsArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketTagsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Tags}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Tags> getBucketTags(GetBucketTagsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeGetAsync(args, null, newMultimap("tagging", ""))
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex).errorResponse().code().equals("NoSuchTagSet")) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenApply(
            response -> {
              if (response == null) return new Tags();
              try {
                return Xml.unmarshal(Tags.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Sets tags to a bucket.
   *
   * <pre>Example:{@code
   * Map<String, String> map = new HashMap<>();
   * map.put("Project", "Project One");
   * map.put("User", "jsmith");
   * CompletableFuture<Void> future = minioAsyncClient.setBucketTags(
   *     SetBucketTagsArgs.builder().bucket("my-bucketname").tags(map).build());
   * }</pre>
   *
   * @param args {@link SetBucketTagsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setBucketTags(SetBucketTagsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(args, null, newMultimap("tagging", ""), args.tags(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Deletes tags of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.deleteBucketTags(
   *     DeleteBucketTagsArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketTagsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> deleteBucketTags(DeleteBucketTagsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeDeleteAsync(args, null, newMultimap("tagging", ""))
        .thenAccept(response -> response.close());
  }

  /**
   * Gets tags of an object.
   *
   * <pre>Example:{@code
   * CompletableFuture<Tags> future =
   *     minioAsyncClient.getObjectTags(
   *         GetObjectTagsArgs.builder().bucket("my-bucketname").object("my-objectname").build());
   * }</pre>
   *
   * @param args {@link GetObjectTagsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Tags}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Tags> getObjectTags(GetObjectTagsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams = newMultimap("tagging", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());
    return executeGetAsync(args, null, queryParams)
        .thenApply(
            response -> {
              try {
                return Xml.unmarshal(Tags.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Sets tags to an object.
   *
   * <pre>Example:{@code
   * Map<String, String> map = new HashMap<>();
   * map.put("Project", "Project One");
   * map.put("User", "jsmith");
   * CompletableFuture<Void> future = minioAsyncClient.setObjectTags(
   *     SetObjectTagsArgs.builder()
   *         .bucket("my-bucketname")
   *         .object("my-objectname")
   *         .tags((map)
   *         .build());
   * }</pre>
   *
   * @param args {@link SetObjectTagsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setObjectTags(SetObjectTagsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams = newMultimap("tagging", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());
    return executePutAsync(args, null, queryParams, args.tags(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Deletes tags of an object.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.deleteObjectTags(
   *     DeleteObjectTags.builder().bucket("my-bucketname").object("my-objectname").build());
   * }</pre>
   *
   * @param args {@link DeleteObjectTagsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> deleteObjectTags(DeleteObjectTagsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams = newMultimap("tagging", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());
    return executeDeleteAsync(args, null, queryParams).thenAccept(response -> response.close());
  }

  /**
   * Gets CORS configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<CORSConfiguration> future =
   *     minioAsyncClient.getBucketCors(GetBucketCorsArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link GetBucketCorsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link CORSConfiguration}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<CORSConfiguration> getBucketCors(GetBucketCorsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeGetAsync(args, null, newMultimap("cors", ""))
        .exceptionally(
            e -> {
              Throwable ex = e.getCause();

              if (ex instanceof CompletionException) {
                ex = ((CompletionException) ex).getCause();
              }

              if (ex instanceof ExecutionException) {
                ex = ((ExecutionException) ex).getCause();
              }

              if (ex instanceof ErrorResponseException) {
                if (((ErrorResponseException) ex).errorResponse().code().equals("NoSuchTagSet")) {
                  return null;
                }
              }
              throw new CompletionException(ex);
            })
        .thenApply(
            response -> {
              if (response == null) return new CORSConfiguration(null);
              try {
                return Xml.unmarshal(CORSConfiguration.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
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
   * CompletableFuture<Void> future = minioAsyncClient.setBucketCors(
   *     SetBucketCorsArgs.builder().bucket("my-bucketname").config(config).build());
   * }</pre>
   *
   * @param args {@link SetBucketCorsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> setBucketCors(SetBucketCorsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(args, null, newMultimap("cors", ""), args.config(), 0)
        .thenAccept(response -> response.close());
  }

  /**
   * Deletes CORS configuration of a bucket.
   *
   * <pre>Example:{@code
   * CompletableFuture<Void> future = minioAsyncClient.deleteBucketCors(
   *     DeleteBucketCorsArgs.builder().bucket("my-bucketname").build());
   * }</pre>
   *
   * @param args {@link DeleteBucketCorsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link Void}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<Void> deleteBucketCors(DeleteBucketCorsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeDeleteAsync(args, null, newMultimap("cors", ""))
        .thenAccept(response -> response.close());
  }

  /**
   * Gets access control policy of an object.
   *
   * <pre>Example:{@code
   * CompletableFuture<AccessControlPolicy> future =
   *     minioAsyncClient.getObjectAcl(
   *         GetObjectAclArgs.builder().bucket("my-bucketname").object("my-objectname").build());
   * }</pre>
   *
   * @param args {@link GetObjectAclArgs} object.
   * @return {@link CompletableFuture}&lt;{@link AccessControlPolicy}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<AccessControlPolicy> getObjectAcl(GetObjectAclArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams = newMultimap("acl", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());
    return executeGetAsync(args, null, queryParams)
        .thenApply(
            response -> {
              try {
                return Xml.unmarshal(AccessControlPolicy.class, response.body().charStream());
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Gets attributes of an object.
   *
   * <pre>Example:{@code
   * CompletableFuture<GetObjectAttributesResponse> future =
   *     minioAsyncClient.getObjectAttributes(
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
   * @return {@link CompletableFuture}&lt;{@link GetObjectAttributesResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<GetObjectAttributesResponse> getObjectAttributes(
      GetObjectAttributesArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);

    Multimap<String, String> queryParams = newMultimap("attributes", "");
    if (args.versionId() != null) queryParams.put("versionId", args.versionId());

    Multimap<String, String> headers = HashMultimap.create();
    if (args.maxParts() != null) headers.put("x-amz-max-parts", args.maxParts().toString());
    if (args.partNumberMarker() != null) {
      headers.put("x-amz-part-number-marker", args.partNumberMarker().toString());
    }
    for (String attribute : args.objectAttributes()) {
      if (attribute != null) headers.put("x-amz-object-attributes", attribute);
    }

    return executeGetAsync(args, headers, queryParams)
        .thenApply(
            response -> {
              try {
                GetObjectAttributesOutput result =
                    Xml.unmarshal(GetObjectAttributesOutput.class, response.body().charStream());

                String value = response.headers().get("x-amz-delete-marker");
                if (value != null) result.setDeleteMarker(Boolean.valueOf(value));
                value = response.headers().get("Last-Modified");
                if (value != null) {
                  result.setLastModified(ZonedDateTime.parse(value, Time.HTTP_HEADER_DATE_FORMAT));
                }
                result.setVersionId(response.headers().get("x-amz-version-id"));

                return new GetObjectAttributesResponse(
                    response.headers(), args.bucket(), args.region(), args.object(), result);
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
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
   * CompletableFuture<ObjectWriteResponse> future = minioAsyncClient.uploadSnowballObjects(
   *     UploadSnowballObjectsArgs.builder().bucket("my-bucketname").objects(objects).build());
   * }</pre>
   *
   * @param args {@link UploadSnowballObjectsArgs} object.
   * @return {@link CompletableFuture}&lt;{@link ObjectWriteResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ObjectWriteResponse> uploadSnowballObjects(
      UploadSnowballObjectsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);

    return CompletableFuture.supplyAsync(
            () -> {
              FileOutputStream fos = null;
              BufferedOutputStream bos = null;
              SnappyFramedOutputStream sos = null;
              ByteArrayOutputStream baos = null;
              TarArchiveOutputStream tarOutputStream = null;

              try {
                OutputStream os = null;
                if (args.stagingFilename() != null) {
                  fos = new FileOutputStream(args.stagingFilename());
                  bos = new BufferedOutputStream(fos);
                  os = bos;
                } else {
                  baos = new ByteArrayOutputStream();
                  os = baos;
                }

                if (args.compression()) {
                  sos = new SnappyFramedOutputStream(os);
                  os = sos;
                }

                tarOutputStream = new TarArchiveOutputStream(os);
                tarOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                for (SnowballObject object : args.objects()) {
                  if (object.filename() != null) {
                    Path filePath = Paths.get(object.filename());
                    TarArchiveEntry entry = new TarArchiveEntry(filePath.toFile(), object.name());
                    tarOutputStream.putArchiveEntry(entry);
                    Files.copy(filePath, tarOutputStream);
                  } else {
                    TarArchiveEntry entry = new TarArchiveEntry(object.name());
                    if (object.modificationTime() != null) {
                      entry.setModTime(Date.from(object.modificationTime().toInstant()));
                    }
                    entry.setSize(object.size());
                    tarOutputStream.putArchiveEntry(entry);
                    ByteStreams.copy(object.stream(), tarOutputStream);
                  }
                  tarOutputStream.closeArchiveEntry();
                }
                tarOutputStream.finish();
              } catch (IOException e) {
                throw new CompletionException(e);
              } finally {
                try {
                  if (tarOutputStream != null) tarOutputStream.flush();
                  if (sos != null) sos.flush();
                  if (bos != null) bos.flush();
                  if (fos != null) fos.flush();
                  if (tarOutputStream != null) tarOutputStream.close();
                  if (sos != null) sos.close();
                  if (bos != null) bos.close();
                  if (fos != null) fos.close();
                } catch (IOException e) {
                  throw new CompletionException(e);
                }
              }
              return baos;
            }, executorService)
        .thenCompose(
            baos -> {
              Multimap<String, String> headers = newMultimap(args.extraHeaders());
              headers.putAll(args.genHeaders());
              headers.put("X-Amz-Meta-Snowball-Auto-Extract", "true");

              if (args.stagingFilename() == null) {
                byte[] data = baos.toByteArray();
                try {
                  return putObjectAsync(
                      args.bucket(),
                      args.region(),
                      args.object(),
                      data,
                      data.length,
                      headers,
                      args.extraQueryParams());
                } catch (InsufficientDataException
                    | InternalException
                    | InvalidKeyException
                    | IOException
                    | NoSuchAlgorithmException
                    | XmlParserException e) {
                  throw new CompletionException(e);
                }
              }

              long length = Paths.get(args.stagingFilename()).toFile().length();
              if (length > ObjectWriteArgs.MAX_OBJECT_SIZE) {
                throw new IllegalArgumentException(
                    "tarball size " + length + " is more than maximum allowed 5TiB");
              }
              try (RandomAccessFile file = new RandomAccessFile(args.stagingFilename(), "r")) {
                return putObjectAsync(
                    args.bucket(),
                    args.region(),
                    args.object(),
                    file,
                    length,
                    headers,
                    args.extraQueryParams());
              } catch (InsufficientDataException
                  | InternalException
                  | InvalidKeyException
                  | IOException
                  | NoSuchAlgorithmException
                  | XmlParserException e) {
                throw new CompletionException(e);
              }
            });
  }

  /**
   * Uploads multiple objects with same content from single stream with optional metadata and tags.
   *
   * <pre>Example:{@code
   * Map<String, String> map = new HashMap<>();
   * map.put("Project", "Project One");
   * map.put("User", "jsmith");
   * CompletableFuture<PutObjectFanOutResponse> future =
   *     minioAsyncClient.putObjectFanOut(
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
   * @return {@link CompletableFuture}&lt;{@link PutObjectFanOutResponse}&gt; object.
   * @throws ErrorResponseException thrown to indicate presigned POST data failure.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<PutObjectFanOutResponse> putObjectFanOut(PutObjectFanOutArgs args)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, IOException, NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    args.validateSse(this.baseUrl);

    return CompletableFuture.supplyAsync(
            () -> {
              // Build POST object data
              String objectName =
                  "pan-out-"
                      + new BigInteger(32, random).toString(32)
                      + "-"
                      + System.currentTimeMillis();
              PostPolicy policy =
                  new PostPolicy(args.bucket(), ZonedDateTime.now().plusMinutes(15));
              policy.addEqualsCondition("key", objectName);
              if (args.sse() != null) {
                for (Map.Entry<String, String> entry : args.sse().headers().entrySet()) {
                  policy.addEqualsCondition(entry.getKey(), entry.getValue());
                }
              }
              if (args.checksum() != null) {
                for (Map.Entry<String, String> entry : args.checksum().headers().entries()) {
                  policy.addEqualsCondition(entry.getKey(), entry.getValue());
                }
              }

              try {
                Map<String, String> formData = this.getPresignedPostFormData(policy);

                // Build MultipartBody
                MultipartBody.Builder multipartBuilder = new MultipartBody.Builder();
                multipartBuilder.setType(MultipartBody.FORM);
                for (Map.Entry<String, String> entry : formData.entrySet()) {
                  multipartBuilder.addFormDataPart(entry.getKey(), entry.getValue());
                }
                multipartBuilder.addFormDataPart("key", objectName);
                multipartBuilder.addFormDataPart("x-minio-fanout-list", args.fanOutList());
                // "file" must be added at last.
                multipartBuilder.addFormDataPart(
                    "file",
                    "fanout-content",
                    new HttpRequestBody(new PartSource(args.stream(), args.size()), null));

                return multipartBuilder.build();
              } catch (JsonProcessingException e) {
                throw new CompletionException(e);
              } catch (ErrorResponseException
                  | InsufficientDataException
                  | InternalException
                  | InvalidKeyException
                  | InvalidResponseException
                  | IOException
                  | NoSuchAlgorithmException
                  | ServerException
                  | XmlParserException e) {
                throw new CompletionException(e);
              }
            })
        .thenCompose(
            body -> {
              try {
                return executePostAsync(args, null, null, body);
              } catch (InsufficientDataException
                  | InternalException
                  | InvalidKeyException
                  | IOException
                  | NoSuchAlgorithmException
                  | XmlParserException e) {
                throw new CompletionException(e);
              }
            })
        .thenApply(
            response -> {
              try {
                JsonFactory jsonFactory = new JsonFactory();
                Iterator<PutObjectFanOutResponse.Result> iterator =
                    objectMapper.readValues(
                        jsonFactory.createParser(response.body().byteStream()),
                        PutObjectFanOutResponse.Result.class);
                List<PutObjectFanOutResponse.Result> results = new LinkedList<>();
                iterator.forEachRemaining(results::add);
                return new PutObjectFanOutResponse(
                    response.headers(), args.bucket(), args.region(), results);
              } catch (IOException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Performs language model inference with the prompt and referenced object as context.
   *
   * @param args {@link PromptObjectArgs} object.
   * @return {@link CompletableFuture}&lt;{@link PromptObjectResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<PromptObjectResponse> promptObject(PromptObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);

    Multimap<String, String> queryParams = newMultimap("lambdaArn", args.lambdaArn());
    Multimap<String, String> headers =
        merge(newMultimap(args.headers()), newMultimap("Content-Type", "application/json"));

    Map<String, Object> promptArgs = args.promptArgs();
    if (promptArgs == null) promptArgs = new HashMap<>();
    promptArgs.put("prompt", args.prompt());
    byte[] data = objectMapper.writeValueAsString(promptArgs).getBytes(StandardCharsets.UTF_8);

    return executePostAsync(args, headers, queryParams, data)
        .thenApply(
            response -> {
              return new PromptObjectResponse(
                  response.headers(),
                  args.bucket(),
                  args.region(),
                  args.object(),
                  response.body().byteStream());
            });
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link MinioClient}. */
  public static final class Builder {
    private HttpUrl baseUrl;
    private String awsS3Prefix;
    private String awsDomainSuffix;
    private boolean awsDualstack;
    private boolean useVirtualStyle;

    private String region;
    private Provider provider;
    private OkHttpClient httpClient;
    private boolean closeHttpClient;
    private ExecutorService executorService = ForkJoinPool.commonPool();

    private void setAwsInfo(String host, boolean https) {
      this.awsS3Prefix = null;
      this.awsDomainSuffix = null;
      this.awsDualstack = false;

      if (!HttpUtils.HOSTNAME_REGEX.matcher(host).find()) return;

      if (HttpUtils.AWS_ELB_ENDPOINT_REGEX.matcher(host).find()) {
        String[] tokens = host.split("\\.elb\\.amazonaws\\.com", 1)[0].split("\\.");
        this.region = tokens[tokens.length - 1];
        return;
      }

      if (!HttpUtils.AWS_ENDPOINT_REGEX.matcher(host).find()) return;

      if (!HttpUtils.AWS_S3_ENDPOINT_REGEX.matcher(host).find()) {
        throw new IllegalArgumentException("invalid Amazon AWS host " + host);
      }

      Matcher matcher = HttpUtils.AWS_S3_PREFIX_REGEX.matcher(host);
      matcher.lookingAt();
      int end = matcher.end();

      this.awsS3Prefix = host.substring(0, end);
      if (this.awsS3Prefix.contains("s3-accesspoint") && !https) {
        throw new IllegalArgumentException("use HTTPS scheme for host " + host);
      }

      String[] tokens = host.substring(end).split("\\.");
      awsDualstack = "dualstack".equals(tokens[0]);
      if (awsDualstack) tokens = Arrays.copyOfRange(tokens, 1, tokens.length);
      String regionInHost = null;
      if (!tokens[0].equals("vpce") && !tokens[0].equals("amazonaws")) {
        regionInHost = tokens[0];
        tokens = Arrays.copyOfRange(tokens, 1, tokens.length);
      }
      this.awsDomainSuffix = String.join(".", tokens);

      if (host.equals("s3-external-1.amazonaws.com")) regionInHost = "us-east-1";
      if (host.equals("s3-us-gov-west-1.amazonaws.com")
          || host.equals("s3-fips-us-gov-west-1.amazonaws.com")) {
        regionInHost = "us-gov-west-1";
      }

      if (regionInHost != null) this.region = regionInHost;
    }

    private void setBaseUrl(HttpUrl url) {
      this.baseUrl = url;
      this.setAwsInfo(url.host(), url.isHttps());
      this.useVirtualStyle = this.awsDomainSuffix != null || url.host().endsWith("aliyuncs.com");
    }

    public Builder endpoint(String endpoint) {
      setBaseUrl(HttpUtils.getBaseUrl(endpoint));
      return this;
    }

    public Builder endpoint(String endpoint, int port, boolean secure) {
      HttpUrl url = HttpUtils.getBaseUrl(endpoint);
      if (port < 1 || port > 65535) {
        throw new IllegalArgumentException("port must be in range of 1 to 65535");
      }
      url = url.newBuilder().port(port).scheme(secure ? "https" : "http").build();

      setBaseUrl(url);
      return this;
    }

    public Builder endpoint(URL url) {
      HttpUtils.validateNotNull(url, "url");
      return endpoint(HttpUrl.get(url));
    }

    public Builder endpoint(HttpUrl url) {
      HttpUtils.validateNotNull(url, "url");
      HttpUtils.validateUrl(url);
      setBaseUrl(url);
      return this;
    }

    public Builder region(String region) {
      if (region != null && !HttpUtils.REGION_REGEX.matcher(region).find()) {
        throw new IllegalArgumentException("invalid region " + region);
      }
      this.region = region;
      return this;
    }

    public Builder credentials(String accessKey, String secretKey) {
      this.provider = new StaticProvider(accessKey, secretKey, null);
      return this;
    }

    public Builder credentialsProvider(Provider provider) {
      this.provider = provider;
      return this;
    }

    public Builder httpClient(OkHttpClient httpClient) {
      HttpUtils.validateNotNull(httpClient, "http client");
      this.httpClient = httpClient;
      return this;
    }

    public Builder httpClient(OkHttpClient httpClient, boolean close) {
      HttpUtils.validateNotNull(httpClient, "http client");
      this.httpClient = httpClient;
      this.closeHttpClient = close;
      return this;
    }

    public Builder executorService(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    public MinioAsyncClient build() {
      HttpUtils.validateNotNull(this.baseUrl, "endpoint");

      if (this.awsDomainSuffix != null
          && this.awsDomainSuffix.endsWith(".cn")
          && !this.awsS3Prefix.endsWith("s3-accelerate.")
          && this.region == null) {
        throw new IllegalArgumentException(
            "Region missing in Amazon S3 China endpoint " + this.baseUrl);
      }

      if (this.httpClient == null) {
        this.closeHttpClient = true;
        this.httpClient =
            HttpUtils.newDefaultHttpClient(
                DEFAULT_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
      }

      return new MinioAsyncClient(
          baseUrl,
          awsS3Prefix,
          awsDomainSuffix,
          awsDualstack,
          useVirtualStyle,
          region,
          provider,
          httpClient,
          closeHttpClient,
          executorService());
    }
  }
}
