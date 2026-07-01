# Argument for Java version, defaulting to 11
ARG JAVA_VERSION=11
# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION} AS builder

# Argument for JAR version
ARG JAR_VERSION=2022.47.1
# Copy function code and runtime dependencies from Maven layout
COPY target/athena-postgresql-${JAR_VERSION}.jar /app/

# Unpack the JAR and then remove it
WORKDIR /app
RUN jar xf athena-postgresql-${JAR_VERSION}.jar && rm -f athena-postgresql-${JAR_VERSION}.jar

# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Argument for Java tool options, defaulting to an empty string
ARG JAVA_TOOL_OPTIONS=""
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17
ENV JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}

# Copy the extracted contents from the builder stage to the Lambda task root directory
COPY --from=builder /app/ ${LAMBDA_TASK_ROOT}/

# Command can be overwritten by providing a different command in the template directly.
# No need to specify here (already defined in athena-postgresql.yaml because has two different handlers)