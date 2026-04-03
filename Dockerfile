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
    python3-protobuf \
    valgrind \
    lcov

# Install Bazel (using a binary download)
# We use version 6.x as it's generally compatible
RUN curl -Lo /usr/local/bin/bazel https://github.com/bazelbuild/bazel/releases/download/6.4.0/bazel-6.4.0-linux-x86_64 && \
    chmod +x /usr/local/bin/bazel

WORKDIR /workspace

# We will mount the source or copy it?
# For a reproducible build in Docker, copying is good.
# But for development, mounting is better.
# Let's copy for now to ensure it builds standalone.
# We will use .dockerignore to avoid copying large build artifacts if any.

# Copy project files
COPY . .

# Initialize submodules if not already done (though copying might copy them if they are in the context)
# If .git is copied, we can update it.
# RUN git submodule update --init --recursive

# Build upb amalgamation
# We need to run it in the directory where WORKSPACE is.
# Based on previous listing, it's in upb/archive
RUN cd upb/archive && bazel build //:gen_amalgamation

# Copy generated files to upb_out
# We assume they are in bazel-bin
# We also search for utf8_range.h which is a dependency
RUN mkdir -p upb_out && \
    cp upb/archive/bazel-bin/upb.c upb_out/ && \
    cp upb/archive/bazel-bin/upb.h upb_out/ && \
    HEADER_PATH=$(find /root/.cache/ -name utf8_range.h | head -n 1) && \
    if [ -n "$HEADER_PATH" ]; then \
        DIR_PATH=$(dirname "$HEADER_PATH"); \
        cp $DIR_PATH/* upb_out/ || true; \
    fi && \
    rm -f upb_out/main.c

# Create a stub lite_proto.c if it doesn't exist so `make` doesn't fail
RUN touch lite_proto.c

# Build the extension
RUN make

# Build the test program
RUN gcc -o test_upb test_upb.c upb_out/*.c -I. -msse4.1

# Run the test to verify
RUN ./test_upb

# Verify extension can be loaded
RUN sqlite3 :memory: "SELECT load_extension('./lite_proto.so', 'sqlite3_liteproto_init');"

# Generate descriptor set
RUN protoc --descriptor_set_out=person.pb person.proto
RUN protoc --descriptor_set_out=ecommerce.pb ecommerce.proto
RUN protoc --python_out=. ecommerce.proto
RUN protoc --python_out=. person.proto
RUN protoc --descriptor_set_out=all_types.pb all_types.proto
RUN protoc --python_out=. all_types.proto

# Build Phase 3 test
RUN gcc -o test_phase3 test_phase3.c upb_out/*.c -I. -msse4.1

# Run Phase 3 test
RUN ./test_phase3

# Verify virtual table creation
RUN sqlite3 :memory: "SELECT load_extension('./lite_proto.so', 'sqlite3_liteproto_init'); CREATE VIRTUAL TABLE person USING lite_proto('person.pb', 'Person');"

# Create a text file with data
RUN echo 'name: "Alice"\nid: 123\nemail: "alice@example.com"\naddress {\n  city: "Wonderland"\n  zip: "12345"\n}\nphone_numbers: "123-456"\nphone_numbers: "789-012"\nactive: true\nscore: 95.5' > person.txt

# Encode to binary
RUN protoc --encode=Person person.proto < person.txt > person.bin

# Run tests with coverage
RUN make coverage && \
    grep '#####' lite_proto.c.gcov | head -n 100 && \
    lcov --capture --directory . --output-file coverage.info --ignore-errors gcov,source && \
    lcov --remove coverage.info '/usr/*' '*/upb_out/*' --output-file coverage.info --ignore-errors gcov,source && \
    lcov --list coverage.info

# Run leak analysis
RUN make leaks

# Default command (runs benchmark)
CMD ["python3", "benchmark.py"]
