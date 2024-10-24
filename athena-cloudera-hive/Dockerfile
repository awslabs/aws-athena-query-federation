FROM public.ecr.aws/lambda/java:11

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-cloudera-hive-2024.42.3.jar ${LAMBDA_TASK_ROOT}
# Unpack the jar
RUN jar xf athena-cloudera-hive-2024.42.3.jar

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "com.amazonaws.athena.connectors.cloudera.HiveMuxCompositeHandler" ]