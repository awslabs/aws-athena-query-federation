# Argument for Java version, defaulting to 11
ARG JAVA_VERSION=11
# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Argument for Java tool options, defaulting to an empty string
ARG JAVA_TOOL_OPTIONS=""
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17
ENV JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}

# Copy pom.xml and JAR file
COPY pom.xml target/athena-snowflake-*.jar /tmp/

# Extract version and setup (remove JAR after extraction)
RUN VERSION=$(grep -oP '(?<=<version>)[^<]+' /tmp/pom.xml | head -1) && \
    echo "Extracted version: $VERSION" && \
    jar xf /tmp/athena-snowflake-${VERSION}.jar && \
    rm -f /tmp/pom.xml
# Command can be overwritten by providing a different command in the template directly.
# No need to specify here (already defined in .yaml file because legacy and connections use different)
