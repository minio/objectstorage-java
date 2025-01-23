/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2019 MinIO, Inc.
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
import io.minio.SetBucketLifecycleArgs;
import io.minio.errors.MinioException;
import io.minio.messages.Filter;
import io.minio.messages.LifecycleConfiguration;
import io.minio.messages.Status;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

public class SetBucketLifecycle {
  /** MinioClient.SetBucketLifecycle() example. */
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

      List<LifecycleConfiguration.Rule> rules = new LinkedList<>();
      rules.add(
          new LifecycleConfiguration.Rule(
              Status.ENABLED,
              null,
              new LifecycleConfiguration.Expiration((ZonedDateTime) null, 365, null),
              new Filter("logs/"),
              "rule2",
              null,
              null,
              null));
      LifecycleConfiguration config = new LifecycleConfiguration(rules);

      minioClient.setBucketLifecycle(
          SetBucketLifecycleArgs.builder().bucket("my-bucketname").config(config).build());
    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
    }
  }
}
