FROM public.ecr.aws/lambda/java:11

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-snowflake.zip ${LAMBDA_TASK_ROOT}
# Unpack the jar
RUN jar xf athena-snowflake.zip

# Command can be overwritten by providing a different command in the template directly.
# No need to specify here (already defined in .yaml file because legacy and connections use different)