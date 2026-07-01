# Argument for Java version, defaulting to 11
ARG JAVA_VERSION=11
FROM public.ecr.aws/lambda/java:${JAVA_VERSION} AS builder

ARG JAR_VERSION=2022.47.1
COPY target/athena-mysql-${JAR_VERSION}.jar /app/
WORKDIR /app
RUN jar xf athena-mysql-${JAR_VERSION}.jar && rm -f athena-mysql-${JAR_VERSION}.jar

# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Argument for Java tool options, defaulting to an empty string
ARG JAVA_TOOL_OPTIONS=""
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17
ENV JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}

COPY --from=builder /app/ ${LAMBDA_TASK_ROOT}/

# Command can be overwritten by providing a different command in the template directly.
# No need to specify here (already defined in .yaml file because legacy and connections use different)
