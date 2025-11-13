# Argument for Java version, defaulting to 11 (must be before first FROM)
ARG JAVA_VERSION=11

# Build stage - compile updated aws-lambda-rie to fix CVE-2025-58186
FROM golang:1.24-alpine AS rie-builder
RUN apk add --no-cache git gcc musl-dev && \
    git clone --depth 1 https://github.com/aws/aws-lambda-runtime-interface-emulator.git /src && \
    cd /src && \
    go build -ldflags="-s -w" -o /aws-lambda-rie ./cmd/aws-lambda-rie

# Runtime stage - use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Replace the vulnerable aws-lambda-rie binary with the patched version (CVE-2025-58186 fix)
COPY --from=rie-builder /aws-lambda-rie /usr/local/bin/aws-lambda-rie

# Update critical packages to fix CVE-2025-8058
# Vulnerable packages: glibc, glibc-all-langpacks, glibc-common, glibc-minimal-langpack, libcrypt. (minimal approach - only update what's necessary)
RUN yum update-minimal -y --setopt=tsflags=nodocs glibc glibc-common glibc-minimal-langpack libcrypt curl libcurl && \
    yum autoremove -y && \
    yum clean all && \
    rm -rf /var/cache/yum /var/lib/yum/yumdb/* /tmp/* /var/tmp/*

# Argument for Java tool options, defaulting to an empty string
ARG JAVA_TOOL_OPTIONS=""
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17
ENV JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-dynamodb-2022.47.1.jar ${LAMBDA_TASK_ROOT}
# Unpack the jar
RUN jar xf athena-dynamodb-2022.47.1.jar

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
# No need to specify here (already defined in .yaml file)