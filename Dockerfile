# ==================================================
# Global build arguments
# ==================================================
ARG TRADING_RUNTIME_COMMIT


# ==================================================
# Dependency + test stage
# ==================================================
FROM python:3.11.14-slim-trixie AS build

ARG TRADING_RUNTIME_COMMIT

ENV TRADING_RUNTIME_COMMIT=${TRADING_RUNTIME_COMMIT}
ENV PATH="/install/bin:${PATH}"
ENV PYTHONPATH="/install/lib/python3.11/site-packages"

WORKDIR /workspaces/trading-runtime

# System dependencies for building Python packages & running tests
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
        build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (maximizes Docker cache)
COPY requirements.txt .
COPY requirements-dev.txt .

RUN pip install --upgrade pip && \
    pip install --prefix=/install -r requirements.txt && \
    pip install --prefix=/install -r requirements-dev.txt

# Copy project files
COPY pyproject.toml .
COPY scripts/check.sh .
COPY trading_runtime/ trading_runtime/
COPY tests/ tests/

# Install the package itself
RUN pip install --prefix=/install .

# Run test & quality checks
RUN chmod +x check.sh && ./check.sh


# ==================================================
# Runtime stage (clean production image)
# ==================================================
FROM python:3.11.14-slim-trixie AS runtime

ARG GIT_COMMIT
ARG GIT_BRANCH
ARG GIT_DIRTY

ENV GIT_COMMIT=${GIT_COMMIT} \
    GIT_BRANCH=${GIT_BRANCH} \
    GIT_DIRTY=${GIT_DIRTY} \
    PYTHONUNBUFFERED=1

# Minimal runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user for production runtime
RUN adduser --disabled-password --gecos '' appuser

# Copy only installed Python artifacts from build stage
COPY --from=build /install /usr/local

# Application directory (mostly symbolic now — no source code needed)
WORKDIR /app

# Drop privileges
USER appuser
