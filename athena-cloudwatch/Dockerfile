FROM public.ecr.aws/lambda/java:17

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-cloudwatch-2022.47.1.jar ${LAMBDA_TASK_ROOT}
# Unpack the jar
RUN jar xf athena-cloudwatch-2022.47.1.jar

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "com.amazonaws.athena.connectors.cloudwatch.CloudwatchCompositeHandler" ]