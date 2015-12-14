/*
 * Minio Java Library for Amazon S3 Compatible Cloud Storage, (C) 2015 Minio, Inc.
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

import io.minio.acl.Acl;
import io.minio.errors.*;
import io.minio.messages.*;
import io.minio.http.*;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import org.xmlpull.v1.XmlPullParserException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import com.google.common.io.BaseEncoding;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;


/**
 * <p>
 * This class implements a simple cloud storage client. This client consists
 * of a useful subset of S3 compatible functionality.
 * </p>
 * <h2>Service</h2>
 * <ul>
 * <li>Creating a bucket</li>
 * <li>Listing buckets</li>
 * </ul>
 * <h2>Bucket</h2>
 * <ul>
 * <li> Creating an object, including automatic upload resuming for large objects.</li>
 * <li> Listing objects in a bucket</li>
 * <li> Listing active multipart uploads</li>
 * <li> Setting canned ACLs on buckets</li>
 * </ul>
 * <h2>Object</h2>
 * <ul>
 * <li>Removing an active multipart upload for a specific object and uploadId</li>
 * <li>Read object metadata</li>
 * <li>Reading an object</li>
 * <li>Reading a range of bytes of an object</li>
 * <li>Deleting an object</li>
 * </ul>
 * <p>
 * Optionally, users can also provide access/secret keys. If keys are provided, all requests by the
 * client will be signed using AWS Signature Version 4.
 * </p>
 * For examples on using this library, please see
 * <a href="https://github.com/minio/minio-java/tree/master/src/test/java/io/minio/examples"></a>.
 */
@SuppressWarnings({"SameParameterValue", "WeakerAccess"})
public final class MinioClient {
  private static final DateTimeFormatter AMZ_DATE_FORMAT = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss'Z'")
      .withZoneUTC();
  // default multipart upload size is 5MiB, maximum is 5GiB
  private static final int MIN_MULTIPART_SIZE = 5 * 1024 * 1024;
  private static final int MAX_MULTIPART_SIZE = 5 * 1024 * 1024 * 1024;
  // default expiration for a presigned URL is 7 days in seconds
  private static final int DEFAULT_EXPIRY_TIME = 7 * 24 * 3600;
  private static final String DEFAULT_USER_AGENT = "Minio (" + System.getProperty("os.arch") + "; "
      + System.getProperty("os.arch") + ") minio-java/" + MinioProperties.INSTANCE.getVersion();

  // the current client instance's base URL.
  private HttpUrl baseUrl;
  // access key to sign all requests with
  private String accessKey;
  // Secret key to sign all requests with
  private String secretKey;

  // default Transport
  private final OkHttpClient transport = new OkHttpClient();

  // logger which is set only on enableLogger. Atomic reference is used to prevent multiple loggers
  // from being instantiated
  private final AtomicReference<Logger> logger = new AtomicReference<Logger>();
  private String userAgent = DEFAULT_USER_AGENT;


  public MinioClient(String endpoint) throws InvalidEndpointException, InvalidPortException {
    this(endpoint, 0, null, null, false);
  }


  public MinioClient(URL url) throws NullPointerException, InvalidEndpointException, InvalidPortException {
    this(url.toString(), 0, null, null, false);
  }


  public MinioClient(String endpoint, String accessKey, String secretKey)
    throws InvalidEndpointException, InvalidPortException {
    this(endpoint, 0, accessKey, secretKey, false);
  }


  public MinioClient(URL url, String accessKey, String secretKey)
    throws NullPointerException, InvalidEndpointException, InvalidPortException {
    this(url.toString(), 0, accessKey, secretKey, false);
  }


  public MinioClient(String endpoint, int port, String accessKey, String secretKey)
    throws InvalidEndpointException, InvalidPortException {
    this(endpoint, port, accessKey, secretKey, false);
  }


  public MinioClient(String endpoint, String accessKey, String secretKey, boolean insecure)
    throws InvalidEndpointException, InvalidPortException {
    this(endpoint, 0, accessKey, secretKey, insecure);
  }


  /**
   * Create a new client.
   *
   * @param endpoint  request endpoint.  Valid endpoint is an URL, domain name, IPv4 or IPv6 address.
   *                  Valid endpoints:
   *                  * https://s3.amazonaws.com
   *                  * https://s3.amazonaws.com/
   *                  * https://play.minio.io:9000
   *                  * http://play.minio.io:9010/
   *                  * localhost
   *                  * localhost.localdomain
   *                  * play.minio.io
   *                  * 127.0.0.1
   *                  * 192.168.1.60
   *                  * ::1
   * @param port      valid port.  It should be in between 1 and 65535.  Unused if endpoint is an URL.
   * @param accessKey access key to access service in endpoint.
   * @param secretKey secret key to access service in endpoint.
   * @param insecure  to access endpoint, use HTTP if true else HTTPS.
   *
   * @see #MinioClient(String endpoint)
   * @see #MinioClient(URL url)
   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
   * @see #MinioClient(URL url, String accessKey, String secretKey)
   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean insecure)
   */
  public MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean insecure)
    throws InvalidEndpointException, InvalidPortException {
    if (endpoint == null) {
      throw new InvalidEndpointException("(null)", "null endpoint");
    }

    // for valid URL endpoint, port and insecure are ignored
    HttpUrl url = HttpUrl.parse(endpoint);
    if (url != null) {
      if (!"/".equals(url.encodedPath())) {
        throw new InvalidEndpointException(endpoint, "no path allowed in endpoint");
      }

      // treat Amazon S3 host as special case
      String amzHost = url.host();
      if (amzHost.endsWith(".amazonaws.com") && !amzHost.equals("s3.amazonaws.com")) {
        throw new InvalidEndpointException(endpoint, "for Amazon S3, host should be 's3.amazonaws.com' in endpoint");
      }

      this.baseUrl = url;
      this.accessKey = accessKey;
      this.secretKey = secretKey;

      return;
    }

    // endpoint may be a valid hostname, IPv4 or IPv6 address
    if (!this.isValidEndpoint(endpoint)) {
      throw new InvalidEndpointException(endpoint, "invalid host");
    }

    // treat Amazon S3 host as special case
    if (endpoint.endsWith(".amazonaws.com") && !endpoint.equals("s3.amazonaws.com")) {
      throw new InvalidEndpointException(endpoint, "for amazon S3, host should be 's3.amazonaws.com'");
    }

    if (port < 0 || port > 65535) {
      throw new InvalidPortException(port, "port must be in range of 1 to 65535");
    }

    Scheme scheme = Scheme.HTTPS;
    if (insecure) {
      scheme = Scheme.HTTP;
    }

    if (port == 0) {
      this.baseUrl = new HttpUrl.Builder()
          .scheme(scheme.toString())
          .host(endpoint)
          .build();
    } else {
      this.baseUrl = new HttpUrl.Builder()
          .scheme(scheme.toString())
          .host(endpoint)
          .port(port)
          .build();
    }
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }


  private boolean isValidEndpoint(String endpoint) {
    if (InetAddressValidator.getInstance().isValid(endpoint)) {
      return true;
    }

    // endpoint may be a hostname
    // refer https://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_host_names
    // why checks are done like below
    if (endpoint.length() < 1 || endpoint.length() > 253) {
      return false;
    }

    for (String label : endpoint.split("\\.")) {
      if (label.length() < 1 || label.length() > 63) {
        return false;
      }

      if (!(label.matches("^[a-zA-Z0-9][a-zA-Z0-9-]*") && endpoint.matches(".*[a-zA-Z0-9]$"))) {
        return false;
      }
    }

    return true;
  }


  private void checkBucketName(String name) throws InvalidBucketNameException {
    if (name == null) {
      throw new InvalidBucketNameException("(null)", "null bucket name");
    }

    if (name.length() < 3 || name.length() > 63) {
      String msg = "bucket name must be at least 3 and no more than 63 characters long";
      throw new InvalidBucketNameException(name, msg);
    }

    if (name.indexOf(".") != -1) {
      String msg = "bucket name with '.' is not allowed due to SSL cerificate verification error.  "
          + "For more information refer http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html";
      throw new InvalidBucketNameException(name, msg);
    }

    if (!name.matches("^[a-z0-9][a-z0-9\\-]+[a-z0-9]$")) {
      String msg = "bucket name does not follow Amazon S3 standards.  For more information refer "
          + "http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html";
      throw new InvalidBucketNameException(name, msg);
    }
  }


  private Request getRequest(Method method, String bucketName, String objectName, byte[] data,
                             Map<String,String> headerMap, Map<String,String> queryParamMap)
    throws InvalidBucketNameException {
    if (bucketName == null && objectName != null) {
      throw new InvalidBucketNameException("(null)", "null bucket name for object '" + objectName + "'");
    }

    HttpUrl.Builder urlBuilder = this.baseUrl.newBuilder();

    if (bucketName != null) {
      checkBucketName(bucketName);
      urlBuilder.addPathSegment(bucketName);
    }

    if (objectName != null) {
      urlBuilder.addPathSegment(objectName);
    }

    if (queryParamMap != null) {
      for (Map.Entry<String,String> entry : queryParamMap.entrySet()) {
        urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
      }
    }

    RequestBody requestBody = null;
    if (data != null) {
      requestBody = RequestBody.create(null, data);
    }

    Request.Builder requestBuilder = new Request.Builder();
    requestBuilder.url(urlBuilder.build());
    requestBuilder.method(method.toString(), requestBody);
    if (headerMap != null) {
      for (Map.Entry<String,String> entry : headerMap.entrySet()) {
        requestBuilder.header(entry.getKey(), entry.getValue());
      }
    }
    requestBuilder.header("User-Agent", this.userAgent);

    DateTime date = new DateTime();
    this.transport.setFollowRedirects(false);
    this.transport.interceptors().add(new RequestSigner(data, accessKey, secretKey, date));
    requestBuilder.header("x-amz-date", date.toString(AMZ_DATE_FORMAT));

    return requestBuilder.build();
  }


  private HttpResponse execute(Method method, String bucketName, String objectName,
                               byte[] data, Map<String,String> headerMap, Map<String,String> queryParamMap)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    Request request = getRequest(method, bucketName, objectName, data, headerMap, queryParamMap);

    Response response = this.transport.newCall(request).execute();
    if (response == null) {
      throw new NoResponseException();
    }

    ResponseHeader header = new ResponseHeader();
    HeaderParser.set(response.headers(), header);

    if (response.isSuccessful()) {
      return new HttpResponse(header, response.body());
    }

    try {
      if (header.getContentLength() > 0) {
        throw new ErrorResponseException(new ErrorResponse(response.body().charStream()));
      }
    } finally {
      response.body().close();
    }

    ErrorCode ec;
    switch (response.code()) {
      case 404:
        if (objectName != null) {
          ec = ErrorCode.NO_SUCH_KEY;
        } else if (bucketName != null) {
          ec = ErrorCode.NO_SUCH_BUCKET;
        } else {
          ec = ErrorCode.RESOURCE_NOT_FOUND;
        }
        break;
      case 501:
      case 405:
        ec = ErrorCode.METHOD_NOT_ALLOWED;
        break;
      case 409:
        if (bucketName != null) {
          ec = ErrorCode.NO_SUCH_BUCKET;
        } else {
          ec = ErrorCode.RESOURCE_CONFLICT;
        }
        break;
      case 403:
        ec = ErrorCode.ACCESS_DENIED;
        break;
      default:
        throw new InternalException("unhandled HTTP code " + response.code() + ".  Please report this issue at "
                                    + "https://github.com/minio/minio-java/issues");
    }

    throw new ErrorResponseException(new ErrorResponse(ec.code(), ec.message(), bucketName, objectName,
                                                       request.httpUrl().encodedPath(), header.getXamzRequestId(),
                                                       header.getXamzId2()));
  }


  private HttpResponse executeGet(String bucketName, String objectName, Map<String,String> headerMap,
                                  Map<String,String> queryParamMap)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    return execute(Method.GET, bucketName, objectName, null, headerMap, queryParamMap);
  }


  private HttpResponse executeHead(String bucketName, String objectName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    return execute(Method.HEAD, bucketName, objectName, null, null, null);
  }


  private HttpResponse executeDelete(String bucketName, String objectName, Map<String,String> queryParamMap)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    return execute(Method.DELETE, bucketName, objectName, null, null, queryParamMap);
  }


  private HttpResponse executePost(String bucketName, String objectName, byte[] data, Map<String,String> queryParamMap)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    return execute(Method.POST, bucketName, objectName, data, null, queryParamMap);
  }


  private HttpResponse executePut(String bucketName, String objectName, byte[] data, Map<String,String> headerMap,
                                  Map<String,String> queryParamMap)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    return execute(Method.PUT, bucketName, objectName, data, headerMap, queryParamMap);
  }


  /**
   * Set application info to user agent - see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
   *
   * @param name     your application name
   * @param version  your application version
   */
  @SuppressWarnings("unused")
  public void setAppInfo(String name, String version) {
    if (name == null || version == null) {
      // nothing to do
      return;
    }

    this.userAgent = DEFAULT_USER_AGENT + " " + name.trim() + "/" + version.trim();
  }


  /**
   * Returns metadata of given object.
   *
   * @param bucketName Bucket name.
   * @param objectName Object name in the bucket.
   *
   * @return Populated object metadata.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @see ObjectStat
   */
  public ObjectStat statObject(String bucketName, String objectName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    HttpResponse response = executeHead(bucketName, objectName);
    ResponseHeader header = response.header();
    return new ObjectStat(bucketName, objectName, header.getLastModified(), header.getContentLength(),
                          header.getEtag(), header.getContentType());
  }


  /**
   * Returns an InputStream containing the object. The InputStream must be closed when
   * complete or the connection will remain open.
   *
   * @param bucketName Bucket name
   * @param objectName Object name in the bucket
   *
   * @return an InputStream containing the object. Close the InputStream when done.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public InputStream getObject(String bucketName, String objectName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    HttpResponse response = executeGet(bucketName, objectName, null, null);
    return response.body().byteStream();
  }


  /** Returns an presigned URL containing the object.
   *
   * @param bucketName  Bucket name
   * @param objectName  Object name in the bucket
   * @param expires     object expiration
   *
   * @throws InvalidBucketNameException   upon an invalid bucket name
   * @throws InvalidKeyException          upon an invalid access key or secret key
   * @throws IOException                  upon signature calculation failure
   * @throws NoSuchAlgorithmException     upon requested algorithm was not found during signature calculation
   * @throws InvalidExpiresRangeException upon input expires is out of range
   */
  public String presignedGetObject(String bucketName, String objectName, Integer expires)
    throws InvalidBucketNameException, InvalidKeyException, IOException, NoSuchAlgorithmException,
           InvalidExpiresRangeException {
    if (expires < 1 || expires > DEFAULT_EXPIRY_TIME) {
      throw new InvalidExpiresRangeException(expires, "expires must be in range of 1 to " + DEFAULT_EXPIRY_TIME);
    }

    Request request = getRequest(Method.GET, bucketName, objectName, null, null, null);
    RequestSigner signer = new RequestSigner(null, this.accessKey, this.secretKey, new DateTime());
    return signer.preSignV4(request, expires);
  }


  /** Returns an presigned URL containing the object.
   *
   * @param bucketName  Bucket name
   * @param objectName  Object name in the bucket
   *
   * @throws IOException     upon connection error
   * @throws NoSuchAlgorithmException upon requested algorithm was not found during signature calculation
   * @throws InvalidExpiresRangeException upon input expires is out of range
   */
  public String presignedGetObject(String bucketName, String objectName)
    throws IOException, NoSuchAlgorithmException, InvalidExpiresRangeException, InvalidKeyException,
           InvalidBucketNameException {
    return presignedGetObject(bucketName, objectName, DEFAULT_EXPIRY_TIME);
  }


  /** Returns an presigned URL for PUT.
   *
   * @param bucketName  Bucket name
   * @param objectName  Object name in the bucket
   * @param expires     object expiration
   *
   * @throws InvalidBucketNameException   upon an invalid bucket name
   * @throws InvalidKeyException          upon an invalid access key or secret key
   * @throws IOException                  upon signature calculation failure
   * @throws NoSuchAlgorithmException     upon requested algorithm was not found during signature calculation
   * @throws InvalidExpiresRangeException upon input expires is out of range
   */
  public String presignedPutObject(String bucketName, String objectName, Integer expires)
    throws InvalidBucketNameException, InvalidKeyException, IOException, NoSuchAlgorithmException,
           InvalidExpiresRangeException {
    if (expires < 1 || expires > DEFAULT_EXPIRY_TIME) {
      throw new InvalidExpiresRangeException(expires, "expires must be in range of 1 to " + DEFAULT_EXPIRY_TIME);
    }

    Request request = getRequest(Method.PUT, bucketName, objectName, "".getBytes("UTF-8"), null, null);
    RequestSigner signer = new RequestSigner(null, this.accessKey, this.secretKey, new DateTime());
    return signer.preSignV4(request, expires);
  }


  /** Returns an presigned URL for PUT.
   *
   * @param bucketName  Bucket name
   * @param objectName  Object name in the bucket
   *
   * @throws IOException     upon connection error
   * @throws NoSuchAlgorithmException upon requested algorithm was not found during signature calculation
   * @throws InvalidExpiresRangeException upon input expires is out of range
   */
  public String presignedPutObject(String bucketName, String objectName)
    throws IOException, NoSuchAlgorithmException, InvalidExpiresRangeException, InvalidKeyException,
           InvalidBucketNameException {
    return presignedPutObject(bucketName, objectName, DEFAULT_EXPIRY_TIME);
  }


  /** Returns an Map for POST form data.
   *
   * @param policy new PostPolicy
   *
   * @throws NoSuchAlgorithmException upon requested algorithm was not found during signature calculation
   * @throws InvalidExpiresRangeException upon input expires is out of range
   * @throws UnsupportedEncodingException upon unsupported Encoding error
   */
  public Map<String, String> presignedPostPolicy(PostPolicy policy) throws UnsupportedEncodingException,
                                                                           NoSuchAlgorithmException,
                                                                           InvalidKeyException {
    DateTime date = new DateTime();
    RequestSigner signer = new RequestSigner(null, this.accessKey, this.secretKey, date);
    String region = Regions.INSTANCE.getRegion(this.baseUrl.uri().getHost());
    policy.setAlgorithm("AWS4-HMAC-SHA256");
    policy.setCredential(this.accessKey + "/" + signer.getScope(region, date));
    policy.setDate(date);

    String policybase64 = policy.base64();
    String signature = signer.postPreSignV4(policybase64, date, region);
    policy.setPolicy(policybase64);
    policy.setSignature(signature);
    return policy.getFormData();
  }


  /** Returns an InputStream containing a subset of the object. The InputStream must be
   *  closed or the connection will remain open.
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param offsetStart Offset from the start of the object.
   *
   * @return an InputStream containing the object. Close the InputStream when done.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws InvalidRangeException       upon invalid offsetStart and/or length
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public InputStream getPartialObject(String bucketName, String objectName, long offsetStart)
    throws InvalidRangeException, InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    ObjectStat stat = statObject(bucketName, objectName);
    long length = stat.getLength() - offsetStart;
    return getPartialObject(bucketName, objectName, offsetStart, length);
  }


  /**
   * Returns an InputStream containing a subset of the object. The InputStream must be
   * closed or the connection will remain open.
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param offsetStart Offset from the start of the object.
   * @param length      Length of bytes to retrieve.
   *
   * @return an InputStream containing the object. Close the InputStream when done.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws InvalidRangeException       upon invalid offsetStart and/or length
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public InputStream getPartialObject(String bucketName, String objectName, long offsetStart, long length)
    throws InvalidRangeException, InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    if (offsetStart < 0 || length <= 0) {
      throw new InvalidRangeException();
    }

    Map<String,String> headerMap = new Hashtable<String,String>();
    headerMap.put("Range", "bytes=" + offsetStart + "-" + (offsetStart + length - 1));

    HttpResponse response = executeGet(bucketName, objectName, headerMap, null);
    return response.body().byteStream();
  }


  /** Remove an object from a bucket.
   *
   * @param bucketName Bucket name
   * @param objectName Object name in the bucket
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public void removeObject(String bucketName, String objectName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    executeDelete(bucketName, objectName, null);
  }


  /**
   * listObjects is a wrapper around listObjects(bucketName, null)
   *
   * @param bucketName Bucket name
   *
   * @return an iterator of Items.
   * @see #listObjects(String, String, boolean)
   */
  public Iterator<Result<Item>> listObjects(final String bucketName) throws XmlPullParserException {
    return listObjects(bucketName, null);
  }


  /**
   * listObjects is a wrapper around listObjects(bucketName, prefix, true)
   *
   * @param bucketName Bucket name
   * @param prefix     Prefix string.  List objects whose name starts with `prefix`
   *
   * @return an iterator of Items.
   * @see #listObjects(String, String, boolean)
   */
  public Iterator<Result<Item>> listObjects(final String bucketName, final String prefix)
    throws XmlPullParserException {
    // list all objects recursively
    return listObjects(bucketName, prefix, true);
  }


  /**
   * @param bucketName Bucket name
   * @param prefix     Prefix string.  List objects whose name starts with `prefix`
   * @param recursive when false, emulates a directory structure where each listing returned is either a full object
   *                  or part of the object's key up to the first '/'. All objects wit the same prefix up to the first
   *                  '/' will be merged into one entry.
   *
   * @return an iterator of Items.
   */
  public Iterator<Result<Item>> listObjects(final String bucketName, final String prefix, final boolean recursive)
    throws XmlPullParserException {
    return new MinioIterator<Result<Item>>() {
      private String marker = null;
      private boolean isComplete = false;

      @Override
      protected List<Result<Item>> populate() throws XmlPullParserException {
        if (!isComplete) {
          String delimiter = null;
          // set delimiter  to '/' if not recursive to emulate directories
          if (!recursive) {
            delimiter = "/";
          }
          ListBucketResult listBucketResult;
          List<Result<Item>> items = new LinkedList<Result<Item>>();
          try {
            listBucketResult = listObjects(bucketName, marker, prefix, delimiter, 1000);
            for (Item item : listBucketResult.getContents()) {
              items.add(new Result<Item>(item, null));
              if (listBucketResult.isTruncated()) {
                marker = item.getKey();
              }
            }
            for (Prefix prefix : listBucketResult.getCommonPrefixes()) {
              Item item = new Item();
              item.setKey(prefix.getPrefix());
              item.setIsDir(true);
              items.add(new Result<Item>(item, null));
            }
            if (listBucketResult.isTruncated() && delimiter != null) {
              marker = listBucketResult.getNextMarker();
            } else if (!listBucketResult.isTruncated()) {
              isComplete = true;
            }
          } catch (IOException e) {
            items.add(new Result<Item>(null, e));
            isComplete = true;
            return items;
          } catch (MinioException e) {
            items.add(new Result<Item>(null, e));
            isComplete = true;
            return items;
          }
          return items;
        }
        return new LinkedList<Result<Item>>();
      }
    };
  }


  private ListBucketResult listObjects(String bucketName, String marker, String prefix, String delimiter, int maxKeys)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    if (maxKeys < 0 || maxKeys > 1000) {
      maxKeys = 1000;
    }

    Map<String,String> queryParamMap = new HashMap<String,String>();
    queryParamMap.put("max-keys", Integer.toString(maxKeys));
    queryParamMap.put("marker", marker);
    queryParamMap.put("prefix", prefix);
    queryParamMap.put("delimiter", delimiter);

    HttpResponse response = executeGet(bucketName, null, null, queryParamMap);

    ListBucketResult result = new ListBucketResult();
    result.parseXml(response.body().charStream());
    return result;
  }


  /**
   * List buckets owned by the current user.
   *
   * @return an iterator of Bucket type.
   *
   * @throws NoResponseException     upon no response from server
   * @throws IOException             upon connection error
   * @throws XmlPullParserException  upon parsing response xml
   * @throws ErrorResponseException  upon unsuccessful execution
   * @throws InternalException       upon internal library error
   */
  public Iterator<Bucket> listBuckets()
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    HttpResponse response = executeGet(null, null, null, null);
    ListAllMyBucketsResult result = new ListAllMyBucketsResult();
    result.parseXml(response.body().charStream());
    return result.getBuckets().iterator();
  }


  /**
   * Test whether a bucket exists and the user has at least read access.
   *
   * @param bucketName Bucket name
   *
   * @return true if the bucket exists and the user has at least read access
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public boolean bucketExists(String bucketName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    try {
      executeHead(bucketName, null);
      return true;
    } catch (ErrorResponseException e) {
      if (e.getErrorCode() != ErrorCode.NO_SUCH_BUCKET) {
        throw e;
      }
    }

    return false;
  }


  /**
   * Create a bucket with default ACL.
   *
   * @param bucketName Bucket name
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public void makeBucket(String bucketName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    this.makeBucket(bucketName, null);
  }


  /**
   * Create a bucket with a given ACL.
   *
   * @param bucketName Bucket name
   * @param acl        Canned ACL
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public void makeBucket(String bucketName, Acl acl)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    byte[] data = null;
    Map<String,String> headerMap = new HashMap<String,String>();

    String region = Regions.INSTANCE.getRegion(this.baseUrl.host());
    if ("us-east-1".equals(region)) {
      // for 'us-east-1', location constraint is not required.  for more info
      // http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
      data = "".getBytes("UTF-8");
    } else {
      CreateBucketConfiguration config = new CreateBucketConfiguration();
      config.setLocationConstraint(region);
      data = config.toString().getBytes("UTF-8");

      byte[] md5sum = calculateMd5sum(data);
      if (md5sum != null) {
        headerMap.put("Content-MD5", BaseEncoding.base64().encode(md5sum));
      }
    }

    if (acl == null) {
      headerMap.put("x-amz-acl", Acl.PRIVATE.toString());
    } else {
      headerMap.put("x-amz-acl", acl.toString());
    }

    executePut(bucketName, null, data, headerMap, null);
  }


  /**
   * Remove a bucket with a given name.
   * <p>
   * NOTE: -
   * All objects (including all object versions and delete markers) in the bucket
   * must be deleted prior, this API will not recursively delete objects
   * </p>
   *
   * @param bucketName Bucket name
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public void removeBucket(String bucketName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    executeDelete(bucketName, null, null);
  }


  /**
   * Get the bucket's ACL.
   *
   * @param bucketName Bucket name
   *
   * @return Acl type
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public Acl getBucketAcl(String bucketName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    AccessControlPolicy policy = this.getAccessPolicy(bucketName);
    Acl acl = Acl.PRIVATE;
    List<Grant> accessControlList = policy.getAccessControlList();
    switch (accessControlList.size()) {
      case 1:
        for (Grant grant : accessControlList) {
          if (grant.getGrantee().getUri() == null && "FULL_CONTROL".equals(grant.getPermission())) {
            acl = Acl.PRIVATE;
            break;
          }
        }
        break;
      case 2:
        for (Grant grant : accessControlList) {
          if ("http://acs.amazonaws.com/groups/global/AuthenticatedUsers".equals(grant.getGrantee().getUri())
              &&
              "READ".equals(grant.getPermission())) {
            acl = Acl.AUTHENTICATED_READ;
            break;
          }
          if ("http://acs.amazonaws.com/groups/global/AllUsers".equals(grant.getGrantee().getUri())
              &&
              "READ".equals(grant.getPermission())) {
            acl = Acl.PUBLIC_READ;
            break;
          }
        }
        break;
      case 3:
        for (Grant grant : accessControlList) {
          if ("http://acs.amazonaws.com/groups/global/AllUsers".equals(grant.getGrantee().getUri())
              &&
              "WRITE".equals(grant.getPermission())) {
            acl = Acl.PUBLIC_READ_WRITE;
            break;
          }
        }
        break;
      default:
        throw new InternalException("Invalid control flow.  Please report this issue at "
                                      + "https://github.com/minio/minio-java/issues");
    }
    return acl;
  }


  private AccessControlPolicy getAccessPolicy(String bucketName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    Map<String,String> queryParamMap = new HashMap<String,String>();
    queryParamMap.put("acl", "");

    HttpResponse response = executeGet(bucketName, null, null, queryParamMap);

    AccessControlPolicy result = new AccessControlPolicy();
    result.parseXml(response.body().charStream());
    return result;
  }


  /**
   * Set the bucket's ACL.
   *
   * @param bucketName Bucket name
   * @param acl        Canned ACL
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws InvalidAclNameException     upon invalid ACL is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public void setBucketAcl(String bucketName, Acl acl)
    throws InvalidAclNameException, InvalidBucketNameException, NoResponseException, IOException,
           XmlPullParserException, ErrorResponseException, InternalException {
    if (acl == null) {
      throw new InvalidAclNameException();
    }

    Map<String,String> queryParamMap = new HashMap<String,String>();
    queryParamMap.put("acl", "");

    Map<String,String> headerMap = new HashMap<String,String>();
    headerMap.put("x-amz-acl", acl.toString());

    executePut(bucketName, null, "".getBytes("UTF-8"), headerMap, queryParamMap);
  }


  /**
   * Create an object.
   * <p>
   * If the object is larger than 5MB, the client will automatically use a multipart session.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again. The client will examine all parts of any current upload session
   * and attempt to reuse the session automatically. If a mismatch is discovered, the upload will fail
   * before uploading any more data. Otherwise, it will resume uploading where the session left off.
   * </p>
   * <p>
   * If the multipart session fails, the user is responsible for resuming or removing the session.
   * </p>
   *
   * @param bucketName  Bucket name
   * @param objectName  Object name to create in the bucket
   * @param contentType Content type to set this object to
   * @param size        Size of all the data that will be uploaded.
   * @param body        Data to upload
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public void putObject(String bucketName, String objectName, String contentType, long size, InputStream body)
    throws MinioException, UnexpectedShortReadException, InputSizeMismatchException,
           InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    boolean isMultipart = false;
    boolean newUpload = true;
    int partSize = 0;
    String uploadId = null;

    if (contentType == null || "".equals(contentType.trim())) {
      contentType = "application/octet-stream";
    }

    if (size > MIN_MULTIPART_SIZE) {
      // check if multipart exists
      Iterator<Result<Upload>> multipartUploads = listIncompleteUploads(bucketName, objectName);
      while (multipartUploads.hasNext()) {
        Upload upload = multipartUploads.next().getResult();
        if (upload.getKey().equals(objectName)) {
          uploadId = upload.getUploadId();
          newUpload = false;
        }
      }

      isMultipart = true;
      partSize = calculatePartSize(size);
    }

    if (!isMultipart) {
      Data data = readData((int) size, body);
      if (data.getData().length != size || destructiveHasMore(body)) {
        throw new UnexpectedShortReadException();
      }
      putObject(bucketName, objectName, contentType, data.getData(), data.getMD5());
      return;
    }
    long totalSeen = 0;
    List<Part> parts = new LinkedList<Part>();
    int partNumber = 1;
    Iterator<Part> existingParts = new LinkedList<Part>().iterator();
    if (newUpload) {
      uploadId = newMultipartUpload(bucketName, objectName);
    } else {
      existingParts = listObjectParts(bucketName, objectName, uploadId);
    }
    while (true) {
      Data data = readData(partSize, body);
      if (data.getData().length == 0) {
        break;
      }
      if (data.getData().length < partSize) {
        long expectedSize = size - totalSeen;
        if (expectedSize != data.getData().length) {
          throw new UnexpectedShortReadException();
        }
      }
      if (!newUpload && existingParts.hasNext()) {
        Part existingPart = existingParts.next();
        if (existingPart.getPartNumber() == partNumber
            &&
            existingPart.getETag().toLowerCase().equals(BaseEncoding.base16().encode(data.getMD5()).toLowerCase())) {
          partNumber++;
          continue;
        }
      }
      String etag = putObject(bucketName, objectName, contentType, data.getData(),
                              data.getMD5(), uploadId, partNumber);
      totalSeen += data.getData().length;

      Part part = new Part();
      part.setPartNumber(partNumber);
      part.setETag(etag);
      parts.add(part);
      partNumber++;
    }
    if (totalSeen != size) {
      throw new InputSizeMismatchException();
    }

    completeMultipart(bucketName, objectName, uploadId, parts);
  }


  private void putObject(String bucketName, String objectName, String contentType, byte[] data, byte[] md5sum)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    putObject(bucketName, objectName, contentType, data, md5sum, "", 0);
  }


  private String putObject(String bucketName, String objectName, String contentType, byte[] data, byte[] md5sum,
                           String uploadId, int partId)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    Map<String,String> headerMap = new HashMap<String,String>();
    headerMap.put("Content-MD5", BaseEncoding.base64().encode(md5sum));

    Map<String,String> queryParamMap = null;
    if (partId > 0 && uploadId != null && !"".equals(uploadId.trim())) {
      queryParamMap = new HashMap<String,String>();
      queryParamMap.put("partNumber", Integer.toString(partId));
      queryParamMap.put("uploadId", uploadId);
    }

    HttpResponse response = executePut(bucketName, objectName, data, headerMap, queryParamMap);
    return response.header().getEtag();
  }


  /**
   * listIncompleteUploads is a wrapper around listIncompleteUploads(bucketName, null, true)
   *
   * @param bucketName Bucket name
   *
   * @return an iterator of Upload.
   * @see #listIncompleteUploads(String, String, boolean)
   */
  public Iterator<Result<Upload>> listIncompleteUploads(String bucketName) throws XmlPullParserException {
    return listIncompleteUploads(bucketName, null, true);
  }


  /**
   * listIncompleteUploads is a wrapper around listIncompleteUploads(bucketName, prefix, true)
   *
   * @param bucketName Bucket name
   * @param prefix filters the list of uploads to include only those that start with prefix
   *
   * @return an iterator of Upload.
   * @see #listIncompleteUploads(String, String, boolean)
   */
  public Iterator<Result<Upload>> listIncompleteUploads(String bucketName, String prefix)
    throws XmlPullParserException {
    return listIncompleteUploads(bucketName, prefix, true);
  }


  /**
   * @param bucketName  Bucket name
   * @param prefix      Prefix string.  List objects whose name starts with `prefix`
   * @param recursive when false, emulates a directory structure where each listing returned is either a full object
   *                  or part of the object's key up to the first '/'. All uploads with the same prefix up to the first
   *                  '/' will be merged into one entry.
   *
   * @return an iterator of Upload.
   */
  public Iterator<Result<Upload>> listIncompleteUploads(final String bucketName, final String prefix,
                                                        final boolean recursive) throws XmlPullParserException {

    return new MinioIterator<Result<Upload>>() {
      private boolean isComplete = false;
      private String keyMarker = null;
      private String uploadIdMarker;

      @Override
      protected List<Result<Upload>> populate() throws XmlPullParserException {
        List<Result<Upload>> ret = new LinkedList<Result<Upload>>();
        if (!isComplete) {
          ListMultipartUploadsResult uploadResult;
          String delimiter = null;
          // set delimiter  to '/' if not recursive to emulate directories
          if (!recursive) {
            delimiter = "/";
          }
          try {
            uploadResult = listIncompleteUploads(bucketName, keyMarker,
                                                 uploadIdMarker, prefix,
                                                 delimiter, 1000);
            if (uploadResult.isTruncated()) {
              keyMarker = uploadResult.getNextKeyMarker();
              uploadIdMarker = uploadResult.getNextUploadIdMarker();
            } else {
              isComplete = true;
            }
            List<Upload> uploads = uploadResult.getUploads();
            for (Upload upload : uploads) {
              ret.add(new Result<Upload>(upload, null));
            }
          } catch (IOException e) {
            ret.add(new Result<Upload>(null, e));
            isComplete = true;
          } catch (MinioException e) {
            ret.add(new Result<Upload>(null, e));
            isComplete = true;
          }
        }
        return ret;
      }
    };
  }


  private ListMultipartUploadsResult listIncompleteUploads(String bucketName, String keyMarker, String uploadIdMarker,
                                                           String prefix, String delimiter, int maxUploads)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    if (maxUploads < 0 || maxUploads > 1000) {
      maxUploads = 1000;
    }

    Map<String,String> queryParamMap = new HashMap<String,String>();
    queryParamMap.put("uploads", "");
    queryParamMap.put("max-uploads", Integer.toString(maxUploads));
    queryParamMap.put("prefix", prefix);
    queryParamMap.put("key-marker", keyMarker);
    queryParamMap.put("upload-id-marker", uploadIdMarker);
    queryParamMap.put("delimiter", delimiter);

    HttpResponse response = executeGet(bucketName, null, null, queryParamMap);

    ListMultipartUploadsResult result = new ListMultipartUploadsResult();
    result.parseXml(response.body().charStream());
    return result;
  }


  private String newMultipartUpload(String bucketName, String objectName)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    Map<String,String> queryParamMap = new HashMap<String,String>();
    queryParamMap.put("uploads", "");

    HttpResponse response = executePost(bucketName, objectName, "".getBytes("UTF-8"), queryParamMap);

    InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
    result.parseXml(response.body().charStream());
    return result.getUploadId();
  }


  private void completeMultipart(String bucketName, String objectName, String uploadId, List<Part> parts)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    Map<String,String> queryParamMap = new HashMap<String,String>();
    queryParamMap.put("uploadId", uploadId);

    CompleteMultipartUpload completeManifest = new CompleteMultipartUpload();
    completeManifest.setParts(parts);

    executePost(bucketName, objectName, completeManifest.toString().getBytes("UTF-8"), queryParamMap);
  }


  private Iterator<Part> listObjectParts(final String bucketName, final String objectName,
                                         final String uploadId) throws XmlPullParserException {
    return new MinioIterator<Part>() {
      public int marker;
      private boolean isComplete = false;

      @Override
      protected List<Part> populate()
        throws InvalidArgumentException, InvalidBucketNameException, NoResponseException, IOException,
        XmlPullParserException, ErrorResponseException, InternalException {
        if (!isComplete) {
          ListPartsResult result;
          result = listObjectParts(bucketName, objectName, uploadId, marker);
          if (result.isTruncated()) {
            marker = result.getNextPartNumberMarker();
          } else {
            isComplete = true;
          }
          return result.getParts();
        }
        return new LinkedList<Part>();
      }
    };
  }


  private ListPartsResult listObjectParts(String bucketName, String objectName, String uploadId, int partNumberMarker)
    throws InvalidArgumentException, InvalidBucketNameException, NoResponseException, IOException,
           XmlPullParserException, ErrorResponseException, InternalException {
    if (partNumberMarker <= 0) {
      throw new InvalidArgumentException("part number marker should be greater than 0");
    }

    Map<String,String> queryParamMap = new HashMap<String,String>();
    queryParamMap.put("uploadId", uploadId);
    queryParamMap.put("part-number-marker", Integer.toString(partNumberMarker));

    HttpResponse response = executeGet(bucketName, objectName, null, queryParamMap);

    ListPartsResult result = new ListPartsResult();
    result.parseXml(response.body().charStream());
    return result;
  }


  private void abortMultipartUpload(String bucketName, String objectName, String uploadId)
    throws InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    Map<String,String> queryParamMap = new HashMap<String,String>();
    queryParamMap.put("uploadId", uploadId);
    executeDelete(bucketName, objectName, queryParamMap);
  }


  /**
   * Remove active incomplete multipart upload of an object.
   *
   * @param bucketName Bucket name
   * @param objectName Object name in the bucket
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoResponseException         upon no response from server
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   */
  public void removeIncompleteUpload(String bucketName, String objectName)
    throws MinioException, InvalidBucketNameException, NoResponseException, IOException, XmlPullParserException,
           ErrorResponseException, InternalException {
    Iterator<Result<Upload>> uploads = listIncompleteUploads(bucketName, objectName);
    while (uploads.hasNext()) {
      Upload upload = uploads.next().getResult();
      if (objectName.equals(upload.getKey())) {
        abortMultipartUpload(bucketName, objectName, upload.getUploadId());
        return;
      }
    }
  }


  private int calculatePartSize(long size) {
    // 9999 is used instead of 10000 to cater for the last part being too small
    int partSize = (int) (size / 9999);
    if (partSize > MIN_MULTIPART_SIZE) {
      if (partSize > MAX_MULTIPART_SIZE) {
        return MAX_MULTIPART_SIZE;
      }
      return partSize;
    }
    return MIN_MULTIPART_SIZE;
  }


  private byte[] calculateMd5sum(byte[] data) {
    byte[] md5sum;
    try {
      MessageDigest md5Digest = MessageDigest.getInstance("MD5");
      md5sum = md5Digest.digest(data);
    } catch (NoSuchAlgorithmException e) {
      // we should never see this, unless the underlying JVM is broken.
      // Throw a runtime exception if we run into this, the environment
      // is not sane
      System.err.println("MD5 message digest type not found, the current JVM is likely broken.");
      throw new RuntimeException(e);
    }
    return md5sum;
  }


  private Data readData(int size, InputStream data) throws IOException {
    int amountRead = 0;
    byte[] fullData = new byte[size];
    while (amountRead != size) {
      byte[] buf = new byte[size - amountRead];
      int curRead = data.read(buf);
      if (curRead == -1) {
        break;
      }
      buf = Arrays.copyOf(buf, curRead);
      System.arraycopy(buf, 0, fullData, amountRead, curRead);
      amountRead += curRead;
    }
    fullData = Arrays.copyOfRange(fullData, 0, amountRead);
    Data d = new Data();
    d.setData(fullData);
    d.setMD5(calculateMd5sum(fullData));
    return d;
  }


  private boolean destructiveHasMore(InputStream data) {
    try {
      return data.read() > -1;
    } catch (IOException e) {
      return false;
    }
  }


  /**
   * Enable logging to a java logger for debugging purposes. This will enable logging for all http requests.
   */
  @SuppressWarnings("unused")
  public void enableLogging() {
    if (this.logger.get() == null) {
      this.logger.set(Logger.getLogger(OkHttpClient.class.getName()));
      this.logger.get().setLevel(Level.CONFIG);
      this.logger.get().addHandler(new Handler() {

        @Override
        public void close() throws SecurityException {
        }

        @Override
        public void flush() {
        }

        @Override
        public void publish(LogRecord record) {
          // default ConsoleHandler will print >= INFO to System.err
          if (record.getLevel().intValue() < Level.INFO.intValue()) {
            System.out.println(record.getMessage());
          }
        }
      });
    } else {
      this.logger.get().setLevel(Level.CONFIG);
    }
  }


  /**
   * Disable logging http requests.
   */
  @SuppressWarnings("unused")
  public void disableLogging() {
    if (this.logger.get() != null) {
      this.logger.get().setLevel(Level.OFF);
    }
  }


  public URL getUrl() {
    return this.baseUrl.url();
  }
}
