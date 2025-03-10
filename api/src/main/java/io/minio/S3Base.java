/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2021 MinIO, Inc.
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.minio.credentials.Credentials;
import io.minio.credentials.Provider;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.CompleteMultipartUpload;
import io.minio.messages.CompleteMultipartUploadResult;
import io.minio.messages.CopyPartResult;
import io.minio.messages.DeleteRequest;
import io.minio.messages.DeleteResult;
import io.minio.messages.ErrorResponse;
import io.minio.messages.InitiateMultipartUploadResult;
import io.minio.messages.Item;
import io.minio.messages.ListAllMyBucketsResult;
import io.minio.messages.ListBucketResultV1;
import io.minio.messages.ListBucketResultV2;
import io.minio.messages.ListMultipartUploadsResult;
import io.minio.messages.ListObjectsResult;
import io.minio.messages.ListPartsResult;
import io.minio.messages.ListVersionsResult;
import io.minio.messages.LocationConstraint;
import io.minio.messages.NotificationRecords;
import io.minio.messages.Part;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/** Core S3 API client. */
public abstract class S3Base implements AutoCloseable {
  static {
    try {
      RequestBody.create(new byte[] {}, null);
    } catch (NoSuchMethodError ex) {
      throw new RuntimeException("Unsupported OkHttp library found. Must use okhttp >= 4.11.0", ex);
    }
  }

  protected static final String NO_SUCH_BUCKET_MESSAGE = "Bucket does not exist";
  protected static final String NO_SUCH_BUCKET = "NoSuchBucket";
  protected static final String NO_SUCH_BUCKET_POLICY = "NoSuchBucketPolicy";
  protected static final String NO_SUCH_OBJECT_LOCK_CONFIGURATION = "NoSuchObjectLockConfiguration";
  protected static final String SERVER_SIDE_ENCRYPTION_CONFIGURATION_NOT_FOUND_ERROR =
      "ServerSideEncryptionConfigurationNotFoundError";
  // maximum allowed bucket policy size is 20KiB
  protected static final int MAX_BUCKET_POLICY_SIZE = 20 * 1024;
  protected final Map<String, String> regionCache = new ConcurrentHashMap<>();
  protected static final Random random = new Random(new SecureRandom().nextLong());
  protected static final ObjectMapper objectMapper =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
          .build();

  private static final String RETRY_HEAD = "RetryHead";
  private static final String END_HTTP = "----------END-HTTP----------";
  private static final String UPLOAD_ID = "uploadId";
  private static final Set<String> TRACE_QUERY_PARAMS =
      ImmutableSet.of("retention", "legal-hold", "tagging", UPLOAD_ID, "acl", "attributes");
  private PrintWriter traceStream;
  private String userAgent = Utils.getDefaultUserAgent();

  protected HttpUrl baseUrl;
  protected String awsS3Prefix;
  protected String awsDomainSuffix;
  protected boolean awsDualstack;
  protected boolean useVirtualStyle;
  protected String region;
  protected Provider provider;
  protected OkHttpClient httpClient;
  protected boolean closeHttpClient;

  protected S3Base(
      HttpUrl baseUrl,
      String awsS3Prefix,
      String awsDomainSuffix,
      boolean awsDualstack,
      boolean useVirtualStyle,
      String region,
      Provider provider,
      OkHttpClient httpClient,
      boolean closeHttpClient) {
    this.baseUrl = baseUrl;
    this.awsS3Prefix = awsS3Prefix;
    this.awsDomainSuffix = awsDomainSuffix;
    this.awsDualstack = awsDualstack;
    this.useVirtualStyle = useVirtualStyle;
    this.region = region;
    this.provider = provider;
    this.httpClient = httpClient;
    this.closeHttpClient = closeHttpClient;
  }

  protected S3Base(S3Base client) {
    this.baseUrl = client.baseUrl;
    this.awsS3Prefix = client.awsS3Prefix;
    this.awsDomainSuffix = client.awsDomainSuffix;
    this.awsDualstack = client.awsDualstack;
    this.useVirtualStyle = client.useVirtualStyle;
    this.region = client.region;
    this.provider = client.provider;
    this.httpClient = client.httpClient;
    this.closeHttpClient = client.closeHttpClient;
  }

  /** Check whether argument is valid or not. */
  protected void checkArgs(BaseArgs args) {
    if (args == null) throw new IllegalArgumentException("null arguments");

    if ((this.awsDomainSuffix != null) && (args instanceof BucketArgs)) {
      String bucketName = ((BucketArgs) args).bucket();
      if (bucketName.startsWith("xn--")
          || bucketName.endsWith("--s3alias")
          || bucketName.endsWith("--ol-s3")) {
        throw new IllegalArgumentException(
            "bucket name '"
                + bucketName
                + "' must not start with 'xn--' and must not end with '--s3alias' or '--ol-s3'");
      }
    }
  }

  /** Throws encapsulated exception wrapped by {@link ExecutionException}. */
  public void throwEncapsulatedException(ExecutionException e)
      throws ErrorResponseException, InsufficientDataException, InternalException,
          InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
          ServerException, XmlParserException {
    if (e == null) return;

    Throwable ex = e.getCause();

    if (ex instanceof CompletionException) {
      ex = ((CompletionException) ex).getCause();
    }

    if (ex instanceof ExecutionException) {
      ex = ((ExecutionException) ex).getCause();
    }

    try {
      throw ex;
    } catch (IllegalArgumentException
        | ErrorResponseException
        | InsufficientDataException
        | InternalException
        | InvalidKeyException
        | InvalidResponseException
        | IOException
        | NoSuchAlgorithmException
        | ServerException
        | XmlParserException exc) {
      throw exc;
    } catch (Throwable exc) {
      throw new RuntimeException(exc.getCause() == null ? exc : exc.getCause());
    }
  }

  private String[] handleRedirectResponse(
      Http.Method method, String bucketName, Response response, boolean retry) {
    String code = null;
    String message = null;

    if (response.code() == 301) {
      code = "PermanentRedirect";
      message = "Moved Permanently";
    } else if (response.code() == 307) {
      code = "Redirect";
      message = "Temporary redirect";
    } else if (response.code() == 400) {
      code = "BadRequest";
      message = "Bad request";
    }

    String region = response.headers().get("x-amz-bucket-region");
    if (message != null && region != null) message += ". Use region " + region;

    if (retry
        && region != null
        && method.equals(Http.Method.HEAD)
        && bucketName != null
        && regionCache.get(bucketName) != null) {
      code = RETRY_HEAD;
      message = null;
    }

    return new String[] {code, message};
  }

  private String buildAwsUrl(
      HttpUrl.Builder builder, String bucketName, boolean enforcePathStyle, String region) {
    String host = this.awsS3Prefix + this.awsDomainSuffix;
    if (host.equals("s3-external-1.amazonaws.com")
        || host.equals("s3-us-gov-west-1.amazonaws.com")
        || host.equals("s3-fips-us-gov-west-1.amazonaws.com")) {
      builder.host(host);
      return host;
    }

    host = this.awsS3Prefix;
    if (this.awsS3Prefix.contains("s3-accelerate")) {
      if (bucketName.contains(".")) {
        throw new IllegalArgumentException(
            "bucket name '" + bucketName + "' with '.' is not allowed for accelerate endpoint");
      }
      if (enforcePathStyle) host = host.replaceFirst("-accelerate", "");
    }

    if (this.awsDualstack) host += "dualstack.";
    if (!this.awsS3Prefix.contains("s3-accelerate")) host += region + ".";
    host += this.awsDomainSuffix;

    builder.host(host);
    return host;
  }

  private String buildListBucketsUrl(HttpUrl.Builder builder, String region) {
    if (this.awsDomainSuffix == null) return null;

    String host = this.awsS3Prefix + this.awsDomainSuffix;
    if (host.equals("s3-external-1.amazonaws.com")
        || host.equals("s3-us-gov-west-1.amazonaws.com")
        || host.equals("s3-fips-us-gov-west-1.amazonaws.com")) {
      builder.host(host);
      return host;
    }

    String s3Prefix = this.awsS3Prefix;
    String domainSuffix = this.awsDomainSuffix;
    if (this.awsS3Prefix.startsWith("s3.") || this.awsS3Prefix.startsWith("s3-")) {
      s3Prefix = "s3.";
      domainSuffix = "amazonaws.com" + (domainSuffix.endsWith(".cn") ? ".cn" : "");
    }

    host = s3Prefix + region + "." + domainSuffix;
    builder.host(host);
    return host;
  }

  /** Build URL for given parameters. */
  protected HttpUrl buildUrl(
      Http.Method method,
      String bucketName,
      String objectName,
      String region,
      Multimap<String, String> queryParamMap)
      throws NoSuchAlgorithmException {
    if (bucketName == null && objectName != null) {
      throw new IllegalArgumentException("null bucket name for object '" + objectName + "'");
    }

    HttpUrl.Builder urlBuilder = this.baseUrl.newBuilder();

    if (queryParamMap != null) {
      for (Map.Entry<String, String> entry : queryParamMap.entries()) {
        urlBuilder.addEncodedQueryParameter(
            Utils.encode(entry.getKey()), Utils.encode(entry.getValue()));
      }
    }

    if (bucketName == null) {
      this.buildListBucketsUrl(urlBuilder, region);
      return urlBuilder.build();
    }

    boolean enforcePathStyle = (
        // use path style for make bucket to workaround "AuthorizationHeaderMalformed" error from
        // s3.amazonaws.com
        (method == Http.Method.PUT && objectName == null && queryParamMap == null)

            // use path style for location query
            || (queryParamMap != null && queryParamMap.containsKey("location"))

            // use path style where '.' in bucketName causes SSL certificate validation error
            || (bucketName.contains(".") && this.baseUrl.isHttps()));

    String host = this.baseUrl.host();
    if (this.awsDomainSuffix != null) {
      host = this.buildAwsUrl(urlBuilder, bucketName, enforcePathStyle, region);
    }

    if (enforcePathStyle || !this.useVirtualStyle) {
      urlBuilder.addEncodedPathSegment(Utils.encode(bucketName));
    } else {
      urlBuilder.host(bucketName + "." + host);
    }

    if (objectName != null) {
      urlBuilder.addEncodedPathSegments(Utils.encodePath(objectName));
    }

    return urlBuilder.build();
  }

  /** Execute HTTP request asynchronously for given parameters. */
  protected CompletableFuture<Response> executeAsync(Http.S3Request s3request, String region)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    HttpUrl url =
        buildUrl(
            s3request.method(),
            s3request.bucket(),
            s3request.object(),
            region,
            s3request.queryParams());
    Credentials credentials = (provider == null) ? null : provider.fetch();
    okhttp3.Request request = s3request.httpRequest(url, credentials);

    StringBuilder traceBuilder = new StringBuilder(s3request.traces());
    PrintWriter traceStream = this.traceStream;
    if (traceStream != null) traceStream.print(s3request.traces());

    OkHttpClient httpClient = this.httpClient;
    if (!s3request.retryFailure()) {
      httpClient = httpClient.newBuilder().retryOnConnectionFailure(false).build();
    }

    CompletableFuture<Response> completableFuture = new CompletableFuture<>();
    httpClient
        .newCall(request)
        .enqueue(
            new Callback() {
              @Override
              public void onFailure(final Call call, IOException e) {
                completableFuture.completeExceptionally(e);
              }

              @Override
              public void onResponse(Call call, final Response response) throws IOException {
                try {
                  onResponse(response);
                } catch (Exception e) {
                  completableFuture.completeExceptionally(e);
                }
              }

              private void onResponse(final Response response) throws IOException {
                String trace =
                    String.format(
                        "%s %d %s\n%s\n\n",
                        response.protocol().toString().toUpperCase(Locale.US),
                        response.code(),
                        response.message(),
                        response.headers().toString());
                traceBuilder.append(trace);
                if (traceStream != null) traceStream.print(trace);

                if (response.isSuccessful()) {
                  if (traceStream != null) {
                    // Trace response body only if the request is not
                    // GetObject/ListenBucketNotification
                    // S3 API.
                    Set<String> keys = s3request.queryParams().keySet();
                    if ((s3request.method() != Http.Method.GET
                            || s3request.object() == null
                            || !Collections.disjoint(keys, TRACE_QUERY_PARAMS))
                        && !(keys.contains("events")
                            && (keys.contains("prefix") || keys.contains("suffix")))) {
                      ResponseBody responseBody = response.peekBody(1024 * 1024);
                      traceStream.println(responseBody.string());
                    }
                    traceStream.println(END_HTTP);
                  }

                  completableFuture.complete(response);
                  return;
                }

                String errorXml = null;
                try (ResponseBody responseBody = response.body()) {
                  errorXml = responseBody.string();
                }

                if (!("".equals(errorXml) && s3request.method().equals(Http.Method.HEAD))) {
                  traceBuilder.append(errorXml);
                  if (traceStream != null) traceStream.print(errorXml);
                  if (!errorXml.endsWith("\n")) {
                    traceBuilder.append("\n");
                    if (traceStream != null) traceStream.println();
                  }
                }
                traceBuilder.append(END_HTTP).append("\n");
                if (traceStream != null) traceStream.println(END_HTTP);

                // Error in case of Non-XML response from server for non-HEAD requests.
                String contentType = response.headers().get("content-type");
                if (!s3request.method().equals(Http.Method.HEAD)
                    && (contentType == null
                        || !Arrays.asList(contentType.split(";")).contains("application/xml"))) {
                  if (response.code() == 304 && response.body().contentLength() == 0) {
                    completableFuture.completeExceptionally(
                        new ServerException(
                            "server failed with HTTP status code " + response.code(),
                            response.code(),
                            traceBuilder.toString()));
                  }

                  completableFuture.completeExceptionally(
                      new InvalidResponseException(
                          response.code(),
                          contentType,
                          errorXml.substring(
                              0, errorXml.length() > 1024 ? 1024 : errorXml.length()),
                          traceBuilder.toString()));
                  return;
                }

                ErrorResponse errorResponse = null;
                if (!"".equals(errorXml)) {
                  try {
                    errorResponse = Xml.unmarshal(ErrorResponse.class, errorXml);
                  } catch (XmlParserException e) {
                    completableFuture.completeExceptionally(e);
                    return;
                  }
                } else if (!s3request.method().equals(Http.Method.HEAD)) {
                  completableFuture.completeExceptionally(
                      new InvalidResponseException(
                          response.code(), contentType, errorXml, traceBuilder.toString()));
                  return;
                }

                if (errorResponse == null) {
                  String code = null;
                  String message = null;
                  switch (response.code()) {
                    case 301:
                    case 307:
                    case 400:
                      String[] result =
                          handleRedirectResponse(
                              s3request.method(), s3request.bucket(), response, true);
                      code = result[0];
                      message = result[1];
                      break;
                    case 404:
                      if (s3request.object() != null) {
                        code = "NoSuchKey";
                        message = "Object does not exist";
                      } else if (s3request.bucket() != null) {
                        code = NO_SUCH_BUCKET;
                        message = NO_SUCH_BUCKET_MESSAGE;
                      } else {
                        code = "ResourceNotFound";
                        message = "Request resource not found";
                      }
                      break;
                    case 501:
                    case 405:
                      code = "MethodNotAllowed";
                      message = "The specified method is not allowed against this resource";
                      break;
                    case 409:
                      if (s3request.bucket() != null) {
                        code = NO_SUCH_BUCKET;
                        message = NO_SUCH_BUCKET_MESSAGE;
                      } else {
                        code = "ResourceConflict";
                        message = "Request resource conflicts";
                      }
                      break;
                    case 403:
                      code = "AccessDenied";
                      message = "Access denied";
                      break;
                    case 412:
                      code = "PreconditionFailed";
                      message = "At least one of the preconditions you specified did not hold";
                      break;
                    case 416:
                      code = "InvalidRange";
                      message = "The requested range cannot be satisfied";
                      break;
                    default:
                      completableFuture.completeExceptionally(
                          new ServerException(
                              "server failed with HTTP status code " + response.code(),
                              response.code(),
                              traceBuilder.toString()));
                      return;
                  }

                  errorResponse =
                      new ErrorResponse(
                          code,
                          message,
                          s3request.bucket(),
                          s3request.object(),
                          request.url().encodedPath(),
                          response.header("x-amz-request-id"),
                          response.header("x-amz-id-2"));
                }

                // invalidate region cache if needed
                if (errorResponse.code().equals(NO_SUCH_BUCKET)
                    || errorResponse.code().equals(RETRY_HEAD)) {
                  regionCache.remove(s3request.bucket());
                }

                ErrorResponseException e =
                    new ErrorResponseException(errorResponse, response, traceBuilder.toString());
                completableFuture.completeExceptionally(e);
              }
            });
    return completableFuture;
  }

  /** Execute HTTP request asynchronously for given args and parameters. */
  protected CompletableFuture<Response> executeAsync(Http.S3Request s3request)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return getRegionAsync(s3request.bucket(), s3request.region())
        .thenCompose(
            location -> {
              try {
                return executeAsync(s3request, location);
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

  /** Returns region of given bucket either from region cache or set in constructor. */
  protected CompletableFuture<String> getRegionAsync(String bucket, String region)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    if (region != null) {
      // Error out if region does not match with region passed via constructor.
      if (this.region != null && !this.region.equals(region)) {
        throw new IllegalArgumentException(
            "region must be " + this.region + ", but passed " + region);
      }
      return CompletableFuture.completedFuture(region);
    }

    if (this.region != null && !this.region.equals("")) {
      return CompletableFuture.completedFuture(this.region);
    }
    if (bucket == null || this.provider == null) {
      return CompletableFuture.completedFuture(Http.US_EAST_1);
    }
    region = regionCache.get(bucket);
    if (region != null) return CompletableFuture.completedFuture(region);

    return getBucketLocationAsync(GetBucketLocationArgs.builder().bucket(bucket).build());
  }

  /** Execute asynchronously GET HTTP request for given parameters. */
  protected CompletableFuture<Response> executeGetAsync(
      BaseArgs args, Multimap<String, String> headers, Multimap<String, String> queryParams)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return executeAsync(
        Http.S3Request.builder()
            .userAgent(userAgent)
            .method(Http.Method.GET)
            .headers(headers)
            .queryParams(queryParams)
            .baseArgs(args)
            .build());
  }

  /** Execute asynchronously HEAD HTTP request for given parameters. */
  protected CompletableFuture<Response> executeHeadAsync(
      BaseArgs args, Multimap<String, String> headers, Multimap<String, String> queryParams)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    Http.S3Request s3request =
        Http.S3Request.builder()
            .userAgent(userAgent)
            .method(Http.Method.HEAD)
            .headers(headers)
            .queryParams(queryParams)
            .baseArgs(args)
            .build();
    return executeAsync(s3request)
        .exceptionally(
            e -> {
              if (e instanceof ErrorResponseException) {
                ErrorResponseException ex = (ErrorResponseException) e;
                if (ex.errorResponse().code().equals(RETRY_HEAD)) {
                  return null;
                }
              }
              throw new CompletionException(e);
            })
        .thenCompose(
            response -> {
              if (response != null) {
                return CompletableFuture.completedFuture(response);
              }

              try {
                return executeAsync(s3request);
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

  /** Execute asynchronously DELETE HTTP request for given parameters. */
  protected CompletableFuture<Response> executeDeleteAsync(
      BaseArgs args, Multimap<String, String> headers, Multimap<String, String> queryParams)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return executeAsync(
            Http.S3Request.builder()
                .userAgent(userAgent)
                .method(Http.Method.DELETE)
                .headers(headers)
                .queryParams(queryParams)
                .baseArgs(args)
                .build())
        .thenApply(
            response -> {
              if (response != null) response.body().close();
              return response;
            });
  }

  /** Execute asynchronously POST HTTP request for given parameters. */
  protected CompletableFuture<Response> executePostAsync(
      BaseArgs args,
      Multimap<String, String> headers,
      Multimap<String, String> queryParams,
      Object data)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return executeAsync(
        Http.S3Request.builder()
            .userAgent(userAgent)
            .method(Http.Method.POST)
            .headers(headers)
            .queryParams(queryParams)
            .body(data, null, null, null, null)
            .baseArgs(args)
            .build());
  }

  /** Execute asynchronously PUT HTTP request for given parameters. */
  protected CompletableFuture<Response> executePutAsync(
      BaseArgs args,
      Multimap<String, String> headers,
      Multimap<String, String> queryParams,
      Object data,
      int length)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return executeAsync(
        Http.S3Request.builder()
            .userAgent(userAgent)
            .method(Http.Method.PUT)
            .headers(headers)
            .queryParams(queryParams)
            .body(data, (long) length, null, null, null)
            .baseArgs(args)
            .build());
  }

  protected CompletableFuture<Integer> calculatePartCountAsync(List<ComposeSource> sources)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    long[] objectSize = {0};
    int index = 0;

    CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(() -> 0);
    for (ComposeSource src : sources) {
      index++;
      final int i = index;
      completableFuture =
          completableFuture.thenCombine(
              statObjectAsync(new StatObjectArgs((ObjectReadArgs) src)),
              (partCount, statObjectResponse) -> {
                src.buildHeaders(statObjectResponse.size(), statObjectResponse.etag());

                long size = statObjectResponse.size();
                if (src.length() != null) {
                  size = src.length();
                } else if (src.offset() != null) {
                  size -= src.offset();
                }

                if (size < ObjectWriteArgs.MIN_MULTIPART_SIZE
                    && sources.size() != 1
                    && i != sources.size()) {
                  throw new IllegalArgumentException(
                      "source "
                          + src.bucket()
                          + "/"
                          + src.object()
                          + ": size "
                          + size
                          + " must be greater than "
                          + ObjectWriteArgs.MIN_MULTIPART_SIZE);
                }

                objectSize[0] += size;
                if (objectSize[0] > ObjectWriteArgs.MAX_OBJECT_SIZE) {
                  throw new IllegalArgumentException(
                      "destination object size must be less than "
                          + ObjectWriteArgs.MAX_OBJECT_SIZE);
                }

                if (size > ObjectWriteArgs.MAX_PART_SIZE) {
                  long count = size / ObjectWriteArgs.MAX_PART_SIZE;
                  long lastPartSize = size - (count * ObjectWriteArgs.MAX_PART_SIZE);
                  if (lastPartSize > 0) {
                    count++;
                  } else {
                    lastPartSize = ObjectWriteArgs.MAX_PART_SIZE;
                  }

                  if (lastPartSize < ObjectWriteArgs.MIN_MULTIPART_SIZE
                      && sources.size() != 1
                      && i != sources.size()) {
                    throw new IllegalArgumentException(
                        "source "
                            + src.bucket()
                            + "/"
                            + src.object()
                            + ": "
                            + "for multipart split upload of "
                            + size
                            + ", last part size is less than "
                            + ObjectWriteArgs.MIN_MULTIPART_SIZE);
                  }
                  partCount += (int) count;
                } else {
                  partCount++;
                }

                if (partCount > ObjectWriteArgs.MAX_MULTIPART_COUNT) {
                  throw new IllegalArgumentException(
                      "Compose sources create more than allowed multipart count "
                          + ObjectWriteArgs.MAX_MULTIPART_COUNT);
                }
                return partCount;
              });
    }

    return completableFuture;
  }

  private abstract class ObjectIterator implements Iterator<Result<Item>> {
    protected Result<Item> error;
    protected Iterator<? extends Item> itemIterator;
    protected Iterator<ListVersionsResult.DeleteMarker> deleteMarkerIterator;
    protected Iterator<ListObjectsResult.Prefix> prefixIterator;
    protected boolean completed = false;
    protected ListObjectsResult listObjectsResult;
    protected String lastObjectName;

    protected abstract void populateResult()
        throws ErrorResponseException, InsufficientDataException, InternalException,
            InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException,
            ServerException, XmlParserException;

    protected synchronized void populate() {
      try {
        populateResult();
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
      }

      if (this.listObjectsResult != null) {
        this.itemIterator = this.listObjectsResult.contents().iterator();
        this.deleteMarkerIterator = this.listObjectsResult.deleteMarkers().iterator();
        this.prefixIterator = this.listObjectsResult.commonPrefixes().iterator();
      } else {
        this.itemIterator = new LinkedList<Item>().iterator();
        this.deleteMarkerIterator = new LinkedList<ListVersionsResult.DeleteMarker>().iterator();
        this.prefixIterator = new LinkedList<ListObjectsResult.Prefix>().iterator();
      }
    }

    @Override
    public boolean hasNext() {
      if (this.completed) return false;

      if (this.error == null
          && this.itemIterator == null
          && this.deleteMarkerIterator == null
          && this.prefixIterator == null) {
        populate();
      }

      if (this.error == null
          && !this.itemIterator.hasNext()
          && !this.deleteMarkerIterator.hasNext()
          && !this.prefixIterator.hasNext()
          && this.listObjectsResult.isTruncated()) {
        populate();
      }

      if (this.error != null) return true;
      if (this.itemIterator.hasNext()) return true;
      if (this.deleteMarkerIterator.hasNext()) return true;
      if (this.prefixIterator.hasNext()) return true;

      this.completed = true;
      return false;
    }

    @Override
    public Result<Item> next() {
      if (this.completed) throw new NoSuchElementException();
      if (this.error == null
          && this.itemIterator == null
          && this.deleteMarkerIterator == null
          && this.prefixIterator == null) {
        populate();
      }

      if (this.error == null
          && !this.itemIterator.hasNext()
          && !this.deleteMarkerIterator.hasNext()
          && !this.prefixIterator.hasNext()
          && this.listObjectsResult.isTruncated()) {
        populate();
      }

      if (this.error != null) {
        this.completed = true;
        return this.error;
      }

      Item item = null;
      if (this.itemIterator.hasNext()) {
        item = this.itemIterator.next();
        item.setEncodingType(this.listObjectsResult.encodingType());
        this.lastObjectName = item.objectName();
      } else if (this.deleteMarkerIterator.hasNext()) {
        item = this.deleteMarkerIterator.next();
      } else if (this.prefixIterator.hasNext()) {
        item = this.prefixIterator.next().toItem();
      }

      if (item != null) {
        item.setEncodingType(this.listObjectsResult.encodingType());
        return new Result<>(item);
      }

      this.completed = true;
      throw new NoSuchElementException();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /** Execute list objects v2. */
  protected Iterable<Result<Item>> listObjectsV2(ListObjectsV2Args args) {
    return new Iterable<Result<Item>>() {
      @Override
      public Iterator<Result<Item>> iterator() {
        return new ObjectIterator() {
          private ListBucketResultV2 result = null;

          @Override
          protected void populateResult()
              throws ErrorResponseException, InsufficientDataException, InternalException,
                  InvalidKeyException, InvalidResponseException, IOException,
                  NoSuchAlgorithmException, ServerException, XmlParserException {
            this.listObjectsResult = null;
            this.itemIterator = null;
            this.prefixIterator = null;

            try {
              ListObjectsV2Response response =
                  listObjectsV2Async(
                          ListObjectsV2Args.builder()
                              .extraHeaders(args.extraHeaders())
                              .extraQueryParams(args.extraQueryParams())
                              .bucket(args.bucket())
                              .region(args.region())
                              .delimiter(args.delimiter())
                              .encodingType(args.encodingType())
                              .maxKeys(args.maxKeys())
                              .prefix(args.prefix())
                              .startAfter(args.startAfter())
                              .continuationToken(
                                  result == null
                                      ? args.continuationToken()
                                      : result.nextContinuationToken())
                              .fetchOwner(args.fetchOwner())
                              .includeUserMetadata(args.includeUserMetadata())
                              .build())
                      .get();
              result = response.result();
              this.listObjectsResult = response.result();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              throwEncapsulatedException(e);
            }
          }
        };
      }
    };
  }

  /** Execute list objects v1. */
  protected Iterable<Result<Item>> listObjectsV1(ListObjectsV1Args args) {
    return new Iterable<Result<Item>>() {
      @Override
      public Iterator<Result<Item>> iterator() {
        return new ObjectIterator() {
          private ListBucketResultV1 result = null;

          @Override
          protected void populateResult()
              throws ErrorResponseException, InsufficientDataException, InternalException,
                  InvalidKeyException, InvalidResponseException, IOException,
                  NoSuchAlgorithmException, ServerException, XmlParserException {
            this.listObjectsResult = null;
            this.itemIterator = null;
            this.prefixIterator = null;

            String nextMarker = (result == null) ? args.marker() : result.nextMarker();
            if (nextMarker == null) nextMarker = this.lastObjectName;

            try {
              ListObjectsV1Response response =
                  listObjectsV1Async(
                          ListObjectsV1Args.builder()
                              .extraHeaders(args.extraHeaders())
                              .extraQueryParams(args.extraQueryParams())
                              .bucket(args.bucket())
                              .region(args.region())
                              .delimiter(args.delimiter())
                              .encodingType(args.encodingType())
                              .maxKeys(args.maxKeys())
                              .prefix(args.prefix())
                              .marker(nextMarker)
                              .build())
                      .get();
              result = response.result();
              this.listObjectsResult = response.result();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              throwEncapsulatedException(e);
            }
          }
        };
      }
    };
  }

  /** Execute list object versions. */
  protected Iterable<Result<Item>> listObjectVersions(ListObjectVersionsArgs args) {
    return new Iterable<Result<Item>>() {
      @Override
      public Iterator<Result<Item>> iterator() {
        return new ObjectIterator() {
          private ListVersionsResult result = null;

          @Override
          protected void populateResult()
              throws ErrorResponseException, InsufficientDataException, InternalException,
                  InvalidKeyException, InvalidResponseException, IOException,
                  NoSuchAlgorithmException, ServerException, XmlParserException {
            this.listObjectsResult = null;
            this.itemIterator = null;
            this.prefixIterator = null;

            try {
              ListObjectVersionsResponse response =
                  listObjectVersionsAsync(
                          ListObjectVersionsArgs.builder()
                              .extraHeaders(args.extraHeaders())
                              .extraQueryParams(args.extraQueryParams())
                              .bucket(args.bucket())
                              .region(args.region())
                              .delimiter(args.delimiter())
                              .encodingType(args.encodingType())
                              .maxKeys(args.maxKeys())
                              .prefix(args.prefix())
                              .keyMarker(result == null ? args.keyMarker() : result.nextKeyMarker())
                              .versionIdMarker(
                                  result == null
                                      ? args.versionIdMarker()
                                      : result.nextVersionIdMarker())
                              .build())
                      .get();
              result = response.result();
              this.listObjectsResult = response.result();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              throwEncapsulatedException(e);
            }
          }
        };
      }
    };
  }

  protected PartReader newPartReader(
      Object data, long objectSize, long partSize, int partCount, Checksum.Algorithm... algorithms)
      throws NoSuchAlgorithmException {
    if (data instanceof RandomAccessFile) {
      return new PartReader((RandomAccessFile) data, objectSize, partSize, partCount, algorithms);
    }

    if (data instanceof InputStream) {
      return new PartReader((InputStream) data, objectSize, partSize, partCount, algorithms);
    }

    return null;
  }

  /** Notification result records representation. */
  protected static class NotificationResultRecords {
    Response response = null;
    Scanner scanner = null;
    ObjectMapper mapper = null;

    public NotificationResultRecords(Response response) {
      this.response = response;
      this.scanner = new Scanner(response.body().charStream()).useDelimiter("\n");
      this.mapper =
          JsonMapper.builder()
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
              .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
              .build();
    }

    /** returns closeable iterator of result of notification records. */
    public CloseableIterator<Result<NotificationRecords>> closeableIterator() {
      return new CloseableIterator<Result<NotificationRecords>>() {
        String recordsString = null;
        NotificationRecords records = null;
        boolean isClosed = false;

        @Override
        public void close() throws IOException {
          if (!isClosed) {
            try {
              response.body().close();
              scanner.close();
            } finally {
              isClosed = true;
            }
          }
        }

        public boolean populate() {
          if (isClosed) return false;
          if (recordsString != null) return true;

          while (scanner.hasNext()) {
            recordsString = scanner.next().trim();
            if (!recordsString.equals("")) break;
          }

          if (recordsString == null || recordsString.equals("")) {
            try {
              close();
            } catch (IOException e) {
              isClosed = true;
            }
            return false;
          }
          return true;
        }

        @Override
        public boolean hasNext() {
          return populate();
        }

        @Override
        public Result<NotificationRecords> next() {
          if (isClosed) throw new NoSuchElementException();
          if ((recordsString == null || recordsString.equals("")) && !populate()) {
            throw new NoSuchElementException();
          }

          try {
            records = mapper.readValue(recordsString, NotificationRecords.class);
            return new Result<>(records);
          } catch (JsonMappingException e) {
            return new Result<>(e);
          } catch (JsonParseException e) {
            return new Result<>(e);
          } catch (IOException e) {
            return new Result<>(e);
          } finally {
            recordsString = null;
            records = null;
          }
        }
      };
    }
  }

  private Multimap<String, String> getCommonListObjectsQueryParams(
      String delimiter, String encodingType, Integer maxKeys, String prefix) {
    Multimap<String, String> queryParams =
        Utils.newMultimap(
            "delimiter",
            (delimiter == null) ? "" : delimiter,
            "max-keys",
            Integer.toString(maxKeys > 0 ? maxKeys : 1000),
            "prefix",
            (prefix == null) ? "" : prefix);
    if (encodingType != null) queryParams.put("encoding-type", encodingType);
    return queryParams;
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
    this.httpClient = Http.setTimeout(this.httpClient, connectTimeout, writeTimeout, readTimeout);
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
    this.httpClient = Http.disableCertCheck(this.httpClient);
  }

  /**
   * Sets application's name/version to user agent. For more information about user agent refer <a
   * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html">#rfc2616</a>.
   *
   * @param name Your application name.
   * @param version Your application version.
   */
  public void setAppInfo(String name, String version) {
    if (name == null || version == null) return;
    this.userAgent = Utils.getDefaultUserAgent() + " " + name.trim() + "/" + version.trim();
  }

  /**
   * Enables HTTP call tracing and written to traceStream.
   *
   * @param traceStream {@link OutputStream} for writing HTTP call tracing.
   * @see #traceOff
   */
  public void traceOn(OutputStream traceStream) {
    if (traceStream == null) throw new IllegalArgumentException("trace stream must be provided");
    this.traceStream =
        new PrintWriter(new OutputStreamWriter(traceStream, StandardCharsets.UTF_8), true);
  }

  /**
   * Disables HTTP call tracing previously enabled.
   *
   * @see #traceOn
   * @throws IOException upon connection error
   */
  public void traceOff() throws IOException {
    this.traceStream = null;
  }

  /** Enables dual-stack endpoint for Amazon S3 endpoint. */
  public void enableDualStackEndpoint() {
    this.awsDualstack = true;
  }

  /** Disables dual-stack endpoint for Amazon S3 endpoint. */
  public void disableDualStackEndpoint() {
    this.awsDualstack = false;
  }

  /** Enables virtual-style endpoint. */
  public void enableVirtualStyleEndpoint() {
    this.useVirtualStyle = true;
  }

  /** Disables virtual-style endpoint. */
  public void disableVirtualStyleEndpoint() {
    this.useVirtualStyle = false;
  }

  /** Sets AWS S3 domain prefix. */
  public void setAwsS3Prefix(@Nonnull String awsS3Prefix) {
    if (awsS3Prefix == null) throw new IllegalArgumentException("null Amazon AWS S3 domain prefix");
    if (!Utils.AWS_S3_PREFIX_REGEX.matcher(awsS3Prefix).find()) {
      throw new IllegalArgumentException("invalid Amazon AWS S3 domain prefix " + awsS3Prefix);
    }
    this.awsS3Prefix = awsS3Prefix;
  }

  /** Execute stat object a.k.a head object S3 API asynchronously. */
  protected CompletableFuture<StatObjectResponse> statObjectAsync(StatObjectArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    args.validateSsec(baseUrl);
    return executeHeadAsync(
            args,
            args.getHeaders(),
            (args.versionId() != null) ? Utils.newMultimap("versionId", args.versionId()) : null)
        .thenApply(
            response ->
                new StatObjectResponse(
                    response.headers(), args.bucket(), args.region(), args.object()));
  }

  public CompletableFuture<String> getBucketLocationAsync(GetBucketLocationArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeAsync(
            Http.S3Request.builder()
                .userAgent(userAgent)
                .method(Http.Method.GET)
                .baseArgs(args)
                .queryParams(Utils.newMultimap("location", null))
                .build(),
            Http.US_EAST_1)
        .thenApply(
            response -> {
              String location;
              try (ResponseBody body = response.body()) {
                LocationConstraint lc = Xml.unmarshal(LocationConstraint.class, body.charStream());
                if (lc.location() == null || lc.location().equals("")) {
                  location = Http.US_EAST_1;
                } else if (lc.location().equals("EU") && this.awsDomainSuffix != null) {
                  location = "eu-west-1"; // eu-west-1 is also referred as 'EU'.
                } else {
                  location = lc.location();
                }
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              }

              regionCache.put(args.bucket(), location);
              return location;
            });
  }

  /**
   * Do <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html">AbortMultipartUpload
   * S3 API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Region of the bucket.
   * @param objectName Object name in the bucket.
   * @param uploadId Upload ID.
   * @param extraHeaders Extra headers (Optional).
   * @param extraQueryParams Extra query parameters (Optional).
   * @return {@link CompletableFuture}&lt;{@link AbortMultipartUploadResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<AbortMultipartUploadResponse> abortMultipartUploadAsync(
      AbortMultipartUploadArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executeDeleteAsync(
            args,
            null,
            Utils.mergeMultimap(
                args.extraQueryParams(), Utils.newMultimap(UPLOAD_ID, args.uploadId())))
        .thenApply(
            response -> {
              try {
                return new AbortMultipartUploadResponse(
                    response.headers(), args.bucket(), region, args.object(), args.uploadId());
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html">CompleteMultipartUpload
   * S3 API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Region of the bucket.
   * @param objectName Object name in the bucket.
   * @param uploadId Upload ID.
   * @param parts List of parts.
   * @param extraHeaders Extra headers (Optional).
   * @param extraQueryParams Extra query parameters (Optional).
   * @return {@link CompletableFuture}&lt;{@link ObjectWriteResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ObjectWriteResponse> completeMultipartUploadAsync(
      CompleteMultipartUploadArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePostAsync(
            args,
            null,
            Utils.mergeMultimap(
                args.extraQueryParams(), Utils.newMultimap(UPLOAD_ID, args.uploadId())),
            new CompleteMultipartUpload(args.parts()))
        .thenApply(
            response -> {
              try {
                String bodyContent = response.body().string();
                bodyContent = bodyContent.trim();
                if (!bodyContent.isEmpty()) {
                  try {
                    if (Xml.validate(ErrorResponse.class, bodyContent)) {
                      ErrorResponse errorResponse = Xml.unmarshal(ErrorResponse.class, bodyContent);
                      throw new CompletionException(
                          new ErrorResponseException(errorResponse, response, null));
                    }
                  } catch (XmlParserException e) {
                    // As it is not <Error> message, fallback to parse CompleteMultipartUploadOutput
                    // XML.
                  }

                  try {
                    CompleteMultipartUploadResult result =
                        Xml.unmarshal(CompleteMultipartUploadResult.class, bodyContent);
                    return new ObjectWriteResponse(
                        response.headers(),
                        result.bucket(),
                        result.location(),
                        result.object(),
                        result.etag(),
                        response.header("x-amz-version-id"),
                        result);
                  } catch (XmlParserException e) {
                    // As this CompleteMultipartUpload REST call succeeded, just log it.
                    Logger.getLogger(S3Base.class.getName())
                        .warning(
                            "S3 service returned unknown XML for CompleteMultipartUpload REST API. "
                                + bodyContent);
                  }
                }

                return new ObjectWriteResponse(
                    response.headers(),
                    args.bucket(),
                    region,
                    args.object(),
                    null,
                    response.header("x-amz-version-id"));
              } catch (IOException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html">CreateMultipartUpload
   * S3 API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Region name of buckets in S3 service.
   * @param objectName Object name in the bucket.
   * @param headers Request headers.
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link CreateMultipartUploadResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadAsync(
      CreateMultipartUploadArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePostAsync(
            args,
            null,
            Utils.mergeMultimap(args.extraQueryParams(), Utils.newMultimap("uploads", "")),
            null)
        .thenApply(
            response -> {
              try {
                InitiateMultipartUploadResult result =
                    Xml.unmarshal(
                        InitiateMultipartUploadResult.class, response.body().charStream());
                return new CreateMultipartUploadResponse(
                    response.headers(), args.bucket(), region, args.object(), result);
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html">DeleteObjects S3
   * API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Region of the bucket (Optional).
   * @param objectList List of object names.
   * @param quiet Quiet flag.
   * @param bypassGovernanceMode Bypass Governance retention mode.
   * @param extraHeaders Extra headers for request (Optional).
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link DeleteObjectsResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  protected CompletableFuture<DeleteObjectsResponse> deleteObjectsAsync(DeleteObjectsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePostAsync(
            args,
            Utils.mergeMultimap(
                args.extraHeaders(),
                args.bypassGovernanceMode()
                    ? Utils.newMultimap("x-amz-bypass-governance-retention", "true")
                    : null),
            Utils.mergeMultimap(args.extraQueryParams(), Utils.newMultimap("delete", "")),
            new DeleteRequest(args.objects(), args.quiet()))
        .thenApply(
            response -> {
              try {
                String bodyContent = response.body().string();
                try {
                  if (Xml.validate(DeleteResult.Error.class, bodyContent)) {
                    DeleteResult.Error error = Xml.unmarshal(DeleteResult.Error.class, bodyContent);
                    DeleteResult result = new DeleteResult(error);
                    return new DeleteObjectsResponse(
                        response.headers(), args.bucket(), args.region(), result);
                  }
                } catch (XmlParserException e) {
                  // Ignore this exception as it is not <Error> message,
                  // but parse it as <DeleteResult> message below.
                }

                DeleteResult result = Xml.unmarshal(DeleteResult.class, bodyContent);
                return new DeleteObjectsResponse(
                    response.headers(), args.bucket(), args.region(), result);
              } catch (IOException | XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">ListObjects
   * version 2 S3 API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Region of the bucket (Optional).
   * @param delimiter Delimiter (Optional).
   * @param encodingType Encoding type (Optional).
   * @param startAfter Fetch listing after this key (Optional).
   * @param maxKeys Maximum object information to fetch (Optional).
   * @param prefix Prefix (Optional).
   * @param continuationToken Continuation token (Optional).
   * @param fetchOwner Flag to fetch owner information (Optional).
   * @param includeUserMetadata MinIO extension flag to include user metadata (Optional).
   * @param extraHeaders Extra headers for request (Optional).
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link ListObjectsV2Response}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  protected CompletableFuture<ListObjectsV2Response> listObjectsV2Async(ListObjectsV2Args args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);

    Multimap<String, String> queryParams =
        Utils.mergeMultimap(
            args.extraQueryParams(),
            getCommonListObjectsQueryParams(
                args.delimiter(), args.encodingType(), args.maxKeys(), args.prefix()));
    if (args.startAfter() != null) queryParams.put("start-after", args.startAfter());
    if (args.continuationToken() != null)
      queryParams.put("continuation-token", args.continuationToken());
    if (args.fetchOwner()) queryParams.put("fetch-owner", "true");
    if (args.includeUserMetadata()) queryParams.put("metadata", "true");
    queryParams.put("list-type", "2");

    return executeGetAsync(args, args.extraHeaders(), queryParams)
        .thenApply(
            response -> {
              try {
                ListBucketResultV2 result =
                    Xml.unmarshal(ListBucketResultV2.class, response.body().charStream());
                return new ListObjectsV2Response(
                    response.headers(), args.bucket(), args.region(), result);
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">ListObjects
   * version 1 S3 API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Region of the bucket (Optional).
   * @param delimiter Delimiter (Optional).
   * @param encodingType Encoding type (Optional).
   * @param marker Marker (Optional).
   * @param maxKeys Maximum object information to fetch (Optional).
   * @param prefix Prefix (Optional).
   * @param extraHeaders Extra headers for request (Optional).
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link ListObjectsV1Response}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  protected CompletableFuture<ListObjectsV1Response> listObjectsV1Async(ListObjectsV1Args args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);

    Multimap<String, String> queryParams =
        Utils.mergeMultimap(
            args.extraQueryParams(),
            getCommonListObjectsQueryParams(
                args.delimiter(), args.encodingType(), args.maxKeys(), args.prefix()));
    if (args.marker() != null) queryParams.put("marker", args.marker());

    return executeGetAsync(args, args.extraHeaders(), queryParams)
        .thenApply(
            response -> {
              try {
                ListBucketResultV1 result =
                    Xml.unmarshal(ListBucketResultV1.class, response.body().charStream());
                return new ListObjectsV1Response(
                    response.headers(), args.bucket(), args.region(), result);
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html">ListObjectVersions
   * API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Region of the bucket (Optional).
   * @param delimiter Delimiter (Optional).
   * @param encodingType Encoding type (Optional).
   * @param keyMarker Key marker (Optional).
   * @param maxKeys Maximum object information to fetch (Optional).
   * @param prefix Prefix (Optional).
   * @param versionIdMarker Version ID marker (Optional).
   * @param extraHeaders Extra headers for request (Optional).
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link ListObjectVersionsResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  protected CompletableFuture<ListObjectVersionsResponse> listObjectVersionsAsync(
      ListObjectVersionsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);

    Multimap<String, String> queryParams =
        Utils.mergeMultimap(
            args.extraQueryParams(),
            getCommonListObjectsQueryParams(
                args.delimiter(), args.encodingType(), args.maxKeys(), args.prefix()));
    if (args.keyMarker() != null) queryParams.put("key-marker", args.keyMarker());
    if (args.versionIdMarker() != null) {
      queryParams.put("version-id-marker", args.versionIdMarker());
    }
    queryParams.put("versions", "");

    return executeGetAsync(args, args.extraHeaders(), queryParams)
        .thenApply(
            response -> {
              try {
                ListVersionsResult result =
                    Xml.unmarshal(ListVersionsResult.class, response.body().charStream());
                return new ListObjectVersionsResponse(
                    response.headers(), args.bucket(), args.region(), result);
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  private Part[] uploadParts(
      PutObjectBaseArgs args, String uploadId, PartReader partReader, PartSource firstPartSource)
      throws InterruptedException, ExecutionException, InsufficientDataException, InternalException,
          InvalidKeyException, IOException, NoSuchAlgorithmException, XmlParserException {
    Part[] parts = new Part[ObjectWriteArgs.MAX_MULTIPART_COUNT];
    int partNumber = 0;
    PartSource partSource = firstPartSource;
    while (true) {
      partNumber++;

      Multimap<String, String> ssecHeaders = null;
      // set encryption headers in the case of SSE-C.
      if (args.sse() != null && args.sse() instanceof ServerSideEncryption.CustomerKey) {
        ssecHeaders = Multimaps.forMap(args.sse().headers());
      }

      UploadPartResponse response =
          uploadPartAsync(
                  UploadPartArgs.builder()
                      .bucket(args.bucket())
                      .region(args.region())
                      .object(args.object())
                      .buffer(partSource, partSource.size())
                      .partNumber(partNumber)
                      .uploadId(uploadId)
                      .headers(ssecHeaders)
                      .build())
              .get();
      parts[partNumber - 1] = new Part(partNumber, response.etag());

      partSource = partReader.getPart();
      if (partSource == null) break;
    }

    return parts;
  }

  private CompletableFuture<ObjectWriteResponse> putMultipartObjectAsync(
      PutObjectBaseArgs args,
      Multimap<String, String> headers,
      PartReader partReader,
      PartSource firstPartSource)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    return CompletableFuture.supplyAsync(
        () -> {
          String uploadId = null;
          ObjectWriteResponse response = null;
          try {
            CreateMultipartUploadResponse createMultipartUploadResponse =
                createMultipartUploadAsync(
                        CreateMultipartUploadArgs.builder()
                            .extraQueryParams(args.extraQueryParams())
                            .bucket(args.bucket())
                            .region(args.region())
                            .object(args.object())
                            .headers(headers)
                            .build())
                    .get();
            uploadId = createMultipartUploadResponse.result().uploadId();
            Part[] parts = uploadParts(args, uploadId, partReader, firstPartSource);
            response =
                completeMultipartUploadAsync(
                        CompleteMultipartUploadArgs.builder()
                            .bucket(args.bucket())
                            .region(args.region())
                            .object(args.object())
                            .uploadId(uploadId)
                            .parts(parts)
                            .build())
                    .get();
          } catch (InsufficientDataException
              | InternalException
              | InvalidKeyException
              | IOException
              | NoSuchAlgorithmException
              | XmlParserException
              | InterruptedException
              | ExecutionException e) {
            Throwable throwable = e;
            if (throwable instanceof ExecutionException) {
              throwable = ((ExecutionException) throwable).getCause();
            }
            if (throwable instanceof CompletionException) {
              throwable = ((CompletionException) throwable).getCause();
            }
            if (uploadId == null) {
              throw new CompletionException(throwable);
            }
            try {
              abortMultipartUploadAsync(
                      AbortMultipartUploadArgs.builder()
                          .bucket(args.bucket())
                          .region(args.region())
                          .object(args.object())
                          .uploadId(uploadId)
                          .build())
                  .get();
            } catch (InsufficientDataException
                | InternalException
                | InvalidKeyException
                | IOException
                | NoSuchAlgorithmException
                | XmlParserException
                | InterruptedException
                | ExecutionException ex) {
              throwable = ex;
              if (throwable instanceof ExecutionException) {
                throwable = ((ExecutionException) throwable).getCause();
              }
              if (throwable instanceof CompletionException) {
                throwable = ((CompletionException) throwable).getCause();
              }
            }
            throw new CompletionException(throwable);
          }
          return response;
        });
  }

  /**
   * Execute put object asynchronously from object data from {@link RandomAccessFile} or {@link
   * InputStream}.
   *
   * @param args {@link PutObjectBaseArgs}.
   * @param data {@link RandomAccessFile} or {@link InputStream}.
   * @param objectSize object size.
   * @param partSize part size for multipart upload.
   * @param partCount Number of parts for multipart upload.
   * @param contentType content-type of object.
   * @return {@link CompletableFuture}&lt;{@link ObjectWriteResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  protected CompletableFuture<ObjectWriteResponse> putObjectAsync(
      PutObjectBaseArgs args,
      Object data,
      long objectSize,
      long partSize,
      int partCount,
      String contentType)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    PartReader partReader = newPartReader(data, objectSize, partSize, partCount);
    if (partReader == null) {
      throw new IllegalArgumentException("data must be RandomAccessFile or InputStream");
    }

    Multimap<String, String> headers = Utils.newMultimap(args.extraHeaders());
    headers.putAll(args.genHeaders());
    if (!headers.containsKey("Content-Type")) headers.put("Content-Type", contentType);

    return CompletableFuture.supplyAsync(
            () -> {
              try {
                return partReader.getPart();
              } catch (NoSuchAlgorithmException | IOException e) {
                throw new CompletionException(e);
              }
            })
        .thenCompose(
            partSource -> {
              try {
                if (partReader.partCount() == 1) {
                  return putObjectAsync(
                      args.bucket(),
                      args.region(),
                      args.object(),
                      partSource,
                      headers,
                      args.extraQueryParams());
                } else {
                  return putMultipartObjectAsync(args, headers, partReader, partSource);
                }
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
   * Do <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">PutObject S3
   * API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param objectName Object name in the bucket.
   * @param data Object data must be InputStream, RandomAccessFile, byte[] or String.
   * @param length Length of object data.
   * @param headers Additional headers.
   * @param extraQueryParams Additional query parameters if any.
   * @return {@link CompletableFuture}&lt;{@link ObjectWriteResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  protected CompletableFuture<ObjectWriteResponse> putObjectAsync(PutObjectAPIArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Long length = args.length();
    Object data = args.file();
    if (args.file() == null) data = args.buffer();
    if (args.buffer() == null) data = args.data();
    return executePutAsync(
            args,
            Utils.mergeMultimap(args.extraHeaders(), args.headers()),
            args.extraQueryParams(),
            data,
            length)
        .thenApply(
            response -> {
              try {
                return new ObjectWriteResponse(
                    response.headers(),
                    args.bucket(),
                    args.region(),
                    args.object(),
                    response.header("ETag").replaceAll("\"", ""),
                    response.header("x-amz-version-id"));
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html">ListMultipartUploads
   * S3 API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Region of the bucket (Optional).
   * @param delimiter Delimiter (Optional).
   * @param encodingType Encoding type (Optional).
   * @param keyMarker Key marker (Optional).
   * @param maxUploads Maximum upload information to fetch (Optional).
   * @param prefix Prefix (Optional).
   * @param uploadIdMarker Upload ID marker (Optional).
   * @param extraHeaders Extra headers for request (Optional).
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link ListMultipartUploadsResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ListMultipartUploadsResponse> listMultipartUploadsAsync(
      ListMultipartUploadsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Multimap<String, String> queryParams =
        Utils.mergeMultimap(
            args.extraQueryParams(),
            Utils.newMultimap(
                "uploads",
                "",
                "delimiter",
                (args.delimiter() != null) ? args.delimiter() : "",
                "max-uploads",
                (args.maxUploads() != null) ? args.maxUploads().toString() : "1000",
                "prefix",
                (args.prefix() != null) ? args.prefix() : ""));
    if (args.encodingType() != null) queryParams.put("encoding-type", args.encodingType());
    if (args.keyMarker() != null) queryParams.put("key-marker", args.keyMarker());
    if (args.uploadIdMarker() != null) queryParams.put("upload-id-marker", args.uploadIdMarker());

    return executeGetAsync(args, args.extraHeaders(), queryParams)
        .thenApply(
            response -> {
              try {
                ListMultipartUploadsResult result =
                    Xml.unmarshal(ListMultipartUploadsResult.class, response.body().charStream());
                return new ListMultipartUploadsResponse(
                    response.headers(), bucketName, region, result);
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html">ListParts S3
   * API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Name of the bucket (Optional).
   * @param objectName Object name in the bucket.
   * @param maxParts Maximum parts information to fetch (Optional).
   * @param partNumberMarker Part number marker (Optional).
   * @param uploadId Upload ID.
   * @param extraHeaders Extra headers for request (Optional).
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link ListPartsResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<ListPartsResponse> listPartsAsync(ListPartsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    Multimap<String, String> queryParams =
        Utils.mergeMultimap(
            args.extraQueryParams(),
            Utils.newMultimap(
                UPLOAD_ID,
                args.uploadId(),
                "max-parts",
                (args.maxParts() != null) ? args.maxParts().toString() : "1000"));
    if (args.partNumberMarker() != null) {
      queryParams.put("part-number-marker", args.partNumberMarker().toString());
    }

    return executeGetAsync(args, args.extraHeaders(), queryParams)
        .thenApply(
            response -> {
              try {
                ListPartsResult result =
                    Xml.unmarshal(ListPartsResult.class, response.body().charStream());
                return new ListPartsResponse(
                    response.headers(), bucketName, region, objectName, result);
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html">UploadPart S3
   * API</a> asynchronously.
   *
   * @param bucketName Name of the bucket.
   * @param region Region of the bucket (Optional).
   * @param objectName Object name in the bucket.
   * @param data Object data must be InputStream, RandomAccessFile, byte[] or String.
   * @param length Length of object data.
   * @param uploadId Upload ID.
   * @param partNumber Part number.
   * @param extraHeaders Extra headers for request (Optional).
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link UploadPartResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<UploadPartResponse> uploadPartAsync(UploadPartArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    Long length = args.length();
    Object data = args.file();
    if (args.file() == null) data = args.buffer();
    if (args.buffer() == null) data = args.data();
    return executePutAsync(
            args,
            args.extraHeaders(),
            Utils.mergeMultimap(
                args.extraQueryParams(),
                Utils.newMultimap(
                    "partNumber",
                    Integer.toString(args.partNumber()),
                    "uploadId",
                    args.uploadId())),
            data,
            length)
        .thenApply(
            response -> {
              try {
                return new UploadPartResponse(
                    response.headers(),
                    args.bucket(),
                    args.region(),
                    args.object(),
                    args.uploadId(),
                    args.partNumber(),
                    response.header("ETag").replaceAll("\"", ""));
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html">UploadPartCopy
   * S3 API</a>.
   *
   * @param bucketName Name of the bucket.
   * @param region Region of the bucket (Optional).
   * @param objectName Object name in the bucket.
   * @param uploadId Upload ID.
   * @param partNumber Part number.
   * @param headers Request headers with source object definitions.
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link UploadPartCopyResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  public CompletableFuture<UploadPartCopyResponse> uploadPartCopyAsync(UploadPartCopyArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);
    return executePutAsync(
            args,
            Utils.mergeMultimap(args.extraHeaders(), args.headers()),
            Utils.mergeMultimap(
                args.extraQueryParams(),
                Utils.newMultimap(
                    "partNumber",
                    Integer.toString(args.partNumber()),
                    "uploadId",
                    args.uploadId())),
            null,
            0)
        .thenApply(
            response -> {
              try {
                CopyPartResult result =
                    Xml.unmarshal(CopyPartResult.class, response.body().charStream());
                return new UploadPartCopyResponse(
                    response.headers(),
                    args.bucket(),
                    args.region(),
                    args.object(),
                    args.uploadId(),
                    args.partNumber(),
                    result);
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  /**
   * Do <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html">ListBuckets
   * S3 API</a>.
   *
   * @param bucketRegion Fetch buckets from the region (Optional).
   * @param maxBuckets Maximum buckets to be fetched (Optional).
   * @param prefix Bucket name prefix (Optional).
   * @param continuationToken continuation token (Optional).
   * @param extraHeaders Extra headers for request (Optional).
   * @param extraQueryParams Extra query parameters for request (Optional).
   * @return {@link CompletableFuture}&lt;{@link ListBucketsResponse}&gt; object.
   * @throws InsufficientDataException thrown to indicate not enough data available in InputStream.
   * @throws InternalException thrown to indicate internal library error.
   * @throws InvalidKeyException thrown to indicate missing of HMAC SHA-256 library.
   * @throws IOException thrown to indicate I/O error on S3 operation.
   * @throws NoSuchAlgorithmException thrown to indicate missing of MD5 or SHA-256 digest library.
   * @throws XmlParserException thrown to indicate XML parsing error.
   */
  protected CompletableFuture<ListBucketsResponse> listBucketsAsync(ListBucketsArgs args)
      throws InsufficientDataException, InternalException, InvalidKeyException, IOException,
          NoSuchAlgorithmException, XmlParserException {
    checkArgs(args);

    Multimap<String, String> queryParams = Utils.newMultimap(args.extraQueryParams());
    if (args.bucketRegion() != null) queryParams.put("bucket-region", args.bucketRegion());
    queryParams.put(
        "max-buckets", Integer.toString(args.maxBuckets() > 0 ? args.maxBuckets() : 10000));
    if (args.prefix() != null) queryParams.put("prefix", args.prefix());
    if (args.continuationToken() != null) {
      queryParams.put("continuation-token", args.continuationToken());
    }

    return executeGetAsync(args, args.extraHeaders(), queryParams)
        .thenApply(
            response -> {
              try {
                ListAllMyBucketsResult result =
                    Xml.unmarshal(ListAllMyBucketsResult.class, response.body().charStream());
                return new ListBucketsResponse(response.headers(), result);
              } catch (XmlParserException e) {
                throw new CompletionException(e);
              } finally {
                response.close();
              }
            });
  }

  @Override
  public void close() throws Exception {
    if (closeHttpClient) {
      httpClient.dispatcher().executorService().shutdown();
      httpClient.connectionPool().evictAll();
    }
  }
}
