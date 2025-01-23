/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2025 MinIO, Inc.
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MultiOutputStream extends OutputStream implements AutoCloseable {
  private final List<OutputStream> outputStreams;
  private final ExecutorService executorService;
  private final boolean useParallel;

  public MultiOutputStream(List<OutputStream> outputStreams, boolean useParallel) {
    this.outputStreams = outputStreams;
    this.useParallel = useParallel && outputStreams.size() > 2;
    this.executorService =
        this.useParallel ? Executors.newFixedThreadPool(outputStreams.size()) : null;
  }

  private void handleExceptions(List<IOException> exceptions) throws IOException {
    if (exceptions.isEmpty()) return;
    if (exceptions.size() == 1) throw exceptions.get(0);
    IOException combinedException = new IOException("Multiple IOExceptions occurred");
    for (IOException e : exceptions) combinedException.addSuppressed(e);
    throw combinedException;
  }

  private void writeToStreamsParallel(byte[] buffer, int off, int len) throws IOException {
    List<IOException> exceptions = new CopyOnWriteArrayList<>();
    List<Future<?>> futures = new ArrayList<>();

    for (OutputStream outputStream : outputStreams) {
      futures.add(
          executorService.submit(
              () -> {
                try {
                  outputStream.write(buffer, off, len);
                  outputStream.flush();
                } catch (IOException e) {
                  exceptions.add(e);
                }
              }));
    }

    for (Future<?> future : futures) {
      try {
        future.get(5, TimeUnit.SECONDS); // Timeout to prevent indefinite blocking
      } catch (TimeoutException e) {
        exceptions.add(new IOException("Timeout occurred while writing in parallel", e));
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          exceptions.add((IOException) e.getCause());
        } else {
          exceptions.add(new IOException("Unexpected error in parallel execution", e.getCause()));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        exceptions.add(new IOException("Thread interrupted during parallel execution", e));
      }
    }

    handleExceptions(exceptions);
  }

  private void writeToStreamsSequential(byte[] buffer, int off, int len) throws IOException {
    List<IOException> exceptions = new ArrayList<>();
    for (OutputStream outputStream : outputStreams) {
      try {
        outputStream.write(buffer, off, len);
        outputStream.flush();
      } catch (IOException e) {
        exceptions.add(e);
      }
    }
    handleExceptions(exceptions);
  }

  private void writeToStreams(byte[] buffer, int off, int len) throws IOException {
    if (useParallel) {
      writeToStreamsParallel(buffer, off, len);
    } else {
      writeToStreamsSequential(buffer, off, len);
    }
  }

  @Override
  public void write(int b) throws IOException {
    writeToStreams(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  public void write(byte[] buffer) throws IOException {
    writeToStreams(buffer, 0, buffer.length);
  }

  @Override
  public void write(byte[] buffer, int off, int len) throws IOException {
    writeToStreams(buffer, off, len);
  }

  @Override
  public void close() throws IOException {
    List<IOException> exceptions = new ArrayList<>();
    for (OutputStream outputStream : outputStreams) {
      try {
        outputStream.close();
      } catch (IOException e) {
        exceptions.add(e);
      }
    }
    if (executorService != null) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    handleExceptions(exceptions);
  }
}
