FROM public.ecr.aws/lambda/java:11

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-datalakegen2-2024.46.2.jar ${LAMBDA_TASK_ROOT}
# Unpack the jar
RUN jar xf athena-datalakegen2-2024.46.2.jar

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2MuxCompositeHandler" ]