# Argument for Java version, defaulting to 17
ARG JAVA_VERSION=17
# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Fix CVE-2025-14087: glib2 buffer underflow
# Fix CVE-2025-64720 and CVE-2025-64505: libpng vulnerabilities
RUN yum update -y glib2 libpng && \
    yum clean all && \
    rm -rf /var/cache/yum

# Argument for Java tool options, defaulting to an empty string
ARG JAVA_TOOL_OPTIONS=""
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17.
# Additional JVM options (e.g., for JDK 25: -Darrow.allocation.manager.type=Unsafe)
# can be passed and will be included along with these defaults.
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED -XX:-TieredCompilation ${JAVA_TOOL_OPTIONS}"

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-dynamodb-2022.47.1.jar ${LAMBDA_TASK_ROOT}
# Unpack the jar
RUN jar xf athena-dynamodb-2022.47.1.jar
