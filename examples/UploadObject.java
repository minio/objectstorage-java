/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2015 MinIO, Inc.
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

import io.minio.MinioClient;
import io.minio.ServerSideEncryption;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.KeyGenerator;

public class UploadObject {
  /** MinioClient.putObject() example. */
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

      {
        // Upload 'my-filename' as object 'my-objectname' in 'my-bucketname'.
        minioClient.uploadObject(
            UploadObjectArgs.builder()
                .bucket("my-bucketname")
                .object("my-objectname")
                .filename("my-filename")
                .build());
        System.out.println("my-filename is uploaded to my-objectname successfully");
      }

      {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256);
        ServerSideEncryption.CustomerKey ssec =
            new ServerSideEncryption.CustomerKey(keyGen.generateKey());

        // Upload 'my-filename' as object encrypted 'my-objectname' in 'my-bucketname'.
        minioClient.uploadObject(
            UploadObjectArgs.builder()
                .bucket("my-bucketname")
                .object("my-objectname")
                .filename("my-filename")
                .sse(ssec)
                .build());
        System.out.println("my-filename is uploaded to my-objectname successfully");
      }
    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
    }
  }
}
