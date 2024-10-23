FROM public.ecr.aws/lambda/java:17

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-snowflake.zip ${LAMBDA_TASK_ROOT}
# Unpack the jar
RUN jar xf athena-snowflake.zip

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "com.amazonaws.athena.connectors.snowflake.SnowflakeMuxCompositeHandler" ]