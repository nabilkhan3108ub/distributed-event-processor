# ==========================================
# EventFlow Dockerfile — Multi-Stage Build
# ==========================================
#
# WHAT IS A DOCKERFILE?
# A Dockerfile is a recipe for building a Docker image.
# Each instruction creates a layer. The final image contains
# everything needed to run our application.
#
# MULTI-STAGE BUILD:
# We use two stages:
# 1. BUILD stage: uses a large image with Maven + JDK to compile our code
# 2. RUN stage: uses a small image with only JRE to run the compiled JAR
#
# WHY MULTI-STAGE?
# The Maven + JDK image is ~800MB (includes compiler, build tools).
# The JRE-only image is ~200MB.
# Our final image only needs the JRE, so we save 600MB.
# Smaller images = faster deployment, less storage, smaller attack surface.

# ==================== STAGE 1: BUILD ====================
# "AS build" names this stage so we can reference it later
FROM maven:3.9-eclipse-temurin-17 AS build

# Set working directory inside the container
WORKDIR /app

# Copy pom.xml first (for dependency caching)
# Docker caches each layer. If pom.xml hasn't changed,
# Docker skips re-downloading dependencies. This speeds up
# rebuilds when only Java code changes.
COPY pom.xml .

# Download all dependencies (cached if pom.xml unchanged)
RUN mvn dependency:go-offline -B

# Now copy the source code
COPY src ./src

# Build the JAR file, skipping tests (tests run in CI/CD separately)
# -DskipTests: we run tests in GitHub Actions, not during Docker build
# The JAR goes to target/eventflow-1.0.0.jar
RUN mvn package -DskipTests -B

# ==================== STAGE 2: RUN ====================
# Use a slim JRE image (no compiler, no Maven, no build tools)
# We use the full JRE (not Alpine) to ensure all native libraries work
# (Alpine uses musl libc instead of glibc, which breaks some Java libraries)
FROM eclipse-temurin:17-jre-jammy

# Add a non-root user for security
# Running as root inside a container is a security risk —
# if an attacker exploits our app, they'd have root access
# to the container's filesystem.
RUN groupadd -r eventflow && useradd -r -g eventflow eventflow

WORKDIR /app

# Copy ONLY the compiled JAR from the build stage
# Everything else (source code, Maven cache) is discarded
COPY --from=build /app/target/eventflow-*.jar app.jar

# Switch to non-root user
USER eventflow

# Document which port the app uses
# This doesn't actually open the port — it's documentation.
# The actual port mapping happens in docker-compose.yml
EXPOSE 8080

# Health check: Docker uses this to determine container health
HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Start the application
# JVM FLAGS:
# -XX:+UseContainerSupport: respect Docker memory limits
# -XX:MaxRAMPercentage=75: use at most 75% of container's RAM for heap
# -XX:+UseG1GC: use the G1 garbage collector (good for low-latency)
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-XX:+UseG1GC", \
  "-jar", "app.jar"]
