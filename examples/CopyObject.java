/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2017 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.minio.CopyObjectArgs;
import io.minio.CopySource;
import io.minio.MinioClient;
import io.minio.ServerSideEncryption;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.KeyGenerator;

public class CopyObject {
  /** MinioClient.copyObject() example. */
  public static void main(String[] args)
      throws IOException, NoSuchAlgorithmException, InvalidKeyException {
    try {
      /* play.min.io for test and development. */
      MinioClient minioClient =
          MinioClient.builder()
              .endpoint("https://play.min.io")
              .credentials("Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
              .build();

      /* Amazon S3: */
      // MinioClient minioClient =
      //     MinioClient.builder()
      //         .endpoint("https://s3.amazonaws.com")
      //         .credentials("YOUR-ACCESSKEY", "YOUR-SECRETACCESSKEY")
      //         .build();

      KeyGenerator keyGen = KeyGenerator.getInstance("AES");
      keyGen.init(256);
      ServerSideEncryption.CustomerKey ssec =
          new ServerSideEncryption.CustomerKey(keyGen.generateKey());

      Map<String, String> myContext = new HashMap<>();
      myContext.put("key1", "value1");
      ServerSideEncryption sseKms = new ServerSideEncryption.KMS("Key-Id", myContext);

      ServerSideEncryption sseS3 = new ServerSideEncryption.S3();

      Map<String, String> headers = new HashMap<>();
      headers.put("Content-Type", "application/json");
      headers.put("x-amz-meta-my-project", "Project One");

      String etag = "9855d05ab7a1cfd5ea304f0547c24496";

      {
        // Create object "my-objectname" in bucket "my-bucketname" by copying from object
        // "my-objectname" in bucket "my-source-bucketname".
        minioClient.copyObject(
            CopyObjectArgs.builder()
                .bucket("my-bucketname")
                .object("my-objectname")
                .source(
                    CopySource.builder()
                        .bucket("my-source-bucketname")
                        .object("my-objectname")
                        .build())
                .build());
        System.out.println(
            "my-source-bucketname/my-objectname copied "
                + "to my-bucketname/my-objectname successfully");
      }

      {
        // Create object "my-objectname" in bucket "my-bucketname" by copying from object
        // "my-source-objectname" in bucket "my-source-bucketname".
        minioClient.copyObject(
            CopyObjectArgs.builder()
                .bucket("my-bucketname")
                .object("my-objectname")
                .source(
                    CopySource.builder()
                        .bucket("my-source-bucketname")
                        .object("my-source-objectname")
                        .build())
                .build());
        System.out.println(
            "my-source-bucketname/my-source-objectname copied "
                + "to my-bucketname/my-objectname successfully");
      }

      {
        // Create object "my-objectname" in bucket "my-bucketname" with SSE-KMS server-side
        // encryption by copying from object "my-objectname" in bucket "my-source-bucketname".
        minioClient.copyObject(
            CopyObjectArgs.builder()
                .bucket("my-bucketname")
                .object("my-objectname")
                .source(
                    CopySource.builder()
                        .bucket("my-source-bucketname")
                        .object("my-objectname")
                        .build())
                .sse(sseKms) // Replace with actual key.
                .build());
        System.out.println(
            "my-source-bucketname/my-objectname copied "
                + "to my-bucketname/my-objectname successfully");
      }

      {
        // Create object "my-objectname" in bucket "my-bucketname" with SSE-S3 server-side
        // encryption by copying from object "my-objectname" in bucket "my-source-bucketname".
        minioClient.copyObject(
            CopyObjectArgs.builder()
                .bucket("my-bucketname")
                .object("my-objectname")
                .source(
                    CopySource.builder()
                        .bucket("my-source-bucketname")
                        .object("my-objectname")
                        .build())
                .sse(sseS3) // Replace with actual key.
                .build());
        System.out.println(
            "my-source-bucketname/my-objectname copied "
                + "to my-bucketname/my-objectname successfully");
      }

      {
        // Create object "my-objectname" in bucket "my-bucketname" with SSE-C server-side encryption
        // by copying from object "my-objectname" in bucket "my-source-bucketname".
        minioClient.copyObject(
            CopyObjectArgs.builder()
                .bucket("my-bucketname")
                .object("my-objectname")
                .source(
                    CopySource.builder()
                        .bucket("my-source-bucketname")
                        .object("my-objectname")
                        .build())
                .sse(ssec) // Replace with actual key.
                .build());
        System.out.println(
            "my-source-bucketname/my-objectname copied "
                + "to my-bucketname/my-objectname successfully");
      }

      {
        // Create object "my-objectname" in bucket "my-bucketname" by copying from SSE-C encrypted
        // object "my-source-objectname" in bucket "my-source-bucketname".
        minioClient.copyObject(
            CopyObjectArgs.builder()
                .bucket("my-bucketname")
                .object("my-objectname")
                .source(
                    CopySource.builder()
                        .bucket("my-source-bucketname")
                        .object("my-source-objectname")
                        .ssec(ssec) // Replace with actual key.
                        .build())
                .build());
        System.out.println(
            "my-source-bucketname/my-source-objectname copied "
                + "to my-bucketname/my-objectname successfully");
      }

      {
        // Create object "my-objectname" in bucket "my-bucketname" with custom headers conditionally
        // by copying from object "my-objectname" in bucket "my-source-bucketname".
        minioClient.copyObject(
            CopyObjectArgs.builder()
                .bucket("my-bucketname")
                .object("my-objectname")
                .source(
                    CopySource.builder()
                        .bucket("my-source-bucketname")
                        .object("my-objectname")
                        .matchETag(etag) // Replace with actual etag.
                        .build())
                .headers(headers) // Replace with actual headers.
                .build());
        System.out.println(
            "my-source-bucketname/my-objectname copied "
                + "to my-bucketname/my-objectname successfully");
      }
    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
    }
  }
}
