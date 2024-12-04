FROM public.ecr.aws/lambda/java:11

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

#copy java default cacerts
COPY src/main/resources/cacerts /usr/lib/jvm/java-11-openjdk/lib/security/cacerts

# Add server certificates to the default trust store
COPY src/main/resources/oracle-server.crt /tmp/
RUN keytool -import -trustcacerts -file /tmp/oracle-server.crt \
    -alias orclcert -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit -noprompt

# Add your application jar
COPY target/athena-oracle-2022.47.1.jar ${LAMBDA_TASK_ROOT}
ENV JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/usr/lib/jvm/java-11-openjdk/lib/security/cacerts -Djavax.net.ssl.trustStorePassword=changeit"

# Unpack the jar
RUN jar xf athena-oracle-2022.47.1.jar