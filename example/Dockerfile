FROM ubuntu:22.04

# Avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    unzip \
    sqlite3 \
    libsqlite3-dev \
    protobuf-compiler \
    python3 \
    python3-protobuf

# Install Bazel (using a binary download)
# We use version 6.x as it's generally compatible
RUN curl -Lo /usr/local/bin/bazel https://github.com/bazelbuild/bazel/releases/download/6.4.0/bazel-6.4.0-linux-x86_64 && \
    chmod +x /usr/local/bin/bazel

WORKDIR /workspace

# Copy project files
# Note: We assume the build context is the root directory
COPY . .

# Build upb amalgamation
RUN cd upb/archive && bazel build //:gen_amalgamation

# Copy generated files to upb_out
RUN mkdir -p upb_out && \
    cp upb/archive/bazel-bin/upb.c upb_out/ && \
    cp upb/archive/bazel-bin/upb.h upb_out/ && \
    HEADER_PATH=$(find /root/.cache/ -name utf8_range.h | head -n 1) && \
    if [ -n "$HEADER_PATH" ]; then \
        DIR_PATH=$(dirname "$HEADER_PATH"); \
        cp $DIR_PATH/* upb_out/ || true; \
    fi && \
    rm -f upb_out/main.c

# Build the extension
RUN make

# Move to example directory
WORKDIR /workspace/example

# Generate descriptor set and python bindings
RUN protoc --descriptor_set_out=ecommerce.pb ecommerce.proto
RUN protoc --python_out=. ecommerce.proto

# Default command runs the example
CMD ["python3", "ecommerce_example.py"]
