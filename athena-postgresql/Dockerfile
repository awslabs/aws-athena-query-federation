FROM public.ecr.aws/lambda/java:11

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-postgresql-2025.2.2.jar ${LAMBDA_TASK_ROOT}
# Unpack the jar
RUN jar xf athena-postgresql-2025.2.2.jar

# Command can be overwritten by providing a different command in the template directly.
# No need to specify here (already defined in athena-postgresql.yaml because has two different handlers)