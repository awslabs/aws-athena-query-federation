# Argument for Java version, defaulting to 11
ARG JAVA_VERSION=11
FROM public.ecr.aws/lambda/java:${JAVA_VERSION} AS builder

ARG JAR_VERSION=2022.47.1
COPY target/athena-hbase-${JAR_VERSION}.jar /app/
WORKDIR /app
RUN jar xf athena-hbase-${JAR_VERSION}.jar && rm -f athena-hbase-${JAR_VERSION}.jar

# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Argument for Java tool options, defaulting to an empty string
ARG JAVA_TOOL_OPTIONS=""
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17
ENV JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}

COPY --from=builder /app/ ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
# No need to specify here (already defined in .yaml file)