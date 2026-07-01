# Argument for Java version, defaulting to 11
ARG JAVA_VERSION=11
FROM public.ecr.aws/lambda/java:${JAVA_VERSION} AS builder

ARG JAR_VERSION=2022.47.1
COPY target/athena-dynamodb-${JAR_VERSION}.jar /app/
WORKDIR /app
RUN jar xf athena-dynamodb-${JAR_VERSION}.jar && rm -f athena-dynamodb-${JAR_VERSION}.jar

# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Fix CVE-2025-14087: glib2 buffer underflow
# Fix CVE-2025-64720 and CVE-2025-64505: libpng vulnerabilities
RUN yum update -y glib2 libpng && \
    yum clean all && \
    rm -rf /var/cache/yum

# Argument for Java tool options, defaulting to an empty string
ARG JAVA_TOOL_OPTIONS=""
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17
ENV JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}

COPY --from=builder /app/ ${LAMBDA_TASK_ROOT}/
