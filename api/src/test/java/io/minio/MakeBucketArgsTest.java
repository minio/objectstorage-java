/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2020 MinIO, Inc.
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

import org.junit.Assert;
import org.junit.Test;

public class MakeBucketArgsTest {
  @Test(expected = NullPointerException.class)
  public void testEmptyBuild() {
    MakeBucketArgs.builder().build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyBucketBuild1() {
    MakeBucketArgs.builder().objectLock(false).build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyBucketBuild2() {
    MakeBucketArgs.builder().bucket(null).build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyBucketBuild3() {
    MakeBucketArgs.builder().bucket("mybucket").bucket(null).build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyRegionBuild() {
    MakeBucketArgs.builder().region("").build();
    Assert.fail("exception should be thrown");
  }

  @Test
  public void testBuild() {
    MakeBucketArgs args =
        MakeBucketArgs.builder().objectLock(true).bucket("mybucket").region("myregion").build();
    Assert.assertEquals("mybucket", args.bucket());
    Assert.assertEquals("myregion", args.region());
    Assert.assertEquals(true, args.objectLock());

    args = MakeBucketArgs.builder().bucket("mybucket").build();
    Assert.assertEquals("mybucket", args.bucket());
    Assert.assertEquals(null, args.region());
    Assert.assertEquals(false, args.objectLock());
  }
}
