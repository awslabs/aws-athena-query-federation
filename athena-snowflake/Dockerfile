# Argument for Java version, defaulting to 11.al2023-x86_64
ARG JAVA_VERSION=11.al2023-x86_64
# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION} AS builder

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-snowflake.zip /app/

# Unpack the archive and then remove it
WORKDIR /app
RUN jar xf athena-snowflake.zip && rm -f athena-snowflake.zip

# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Argument for Java tool options, defaulting to an empty string
ARG JAVA_TOOL_OPTIONS=""
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17
ENV JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}

# Copy the extracted contents from the builder stage to the Lambda task root directory
COPY --from=builder /app/ ${LAMBDA_TASK_ROOT}/

# Command can be overwritten by providing a different command in the template directly.
# No need to specify here (already defined in .yaml file because legacy and connections use different)
