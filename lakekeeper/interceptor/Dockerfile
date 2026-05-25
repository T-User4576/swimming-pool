# Pre-built image for the Lakekeeper role-sync receiver.
#
#
FROM python:3.12-slim AS build

# pyjwt[crypto] pulls in cryptography, which compiles C extensions ->
# build-essential only in the build stage, not in the runtime image.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

# Install deps into a separate prefix so the runtime stage can copy them
# without dragging build-essential along.
RUN pip install --no-cache-dir --prefix=/install \
      requests \
      "pyjwt[crypto]"


FROM python:3.12-slim AS runtime

# Non-root user with a fixed UID. Sufficient for vanilla K8s/RKE and
# restricted PSS. (OpenShift would be its own pattern with group-0
# permissions — deliberately not done here; we run on RKE.)
RUN useradd -u 10001 -M -s /sbin/nologin role-sync

# Copy site-packages from the build stage.
COPY --from=build /install /usr/local

# Script into the image. Rebuild + bump the tag on code changes.
COPY interceptor.py /app/interceptor.py

USER 10001
EXPOSE 8200

# Unbuffered stdout for clean kubectl logs.
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "/app/interceptor.py"]
