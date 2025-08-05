# Backend Dockerfile
# Stage 1: Base image with Node and Python
FROM node:20-bullseye AS base
RUN apt-get update && apt-get install -y python3 python3-pip
WORKDIR /app

# Stage 2: Dependencies installation
FROM base AS dependencies
COPY package*.json ./
RUN npm install --production
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

# Stage 3: Build application
FROM base AS build
COPY --from=dependencies /app/node_modules ./node_modules
COPY . .
RUN npm run build --if-present

# Stage 4: Production image
FROM base AS production
COPY --from=build /app ./

# Health check using the /status endpoint
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/status || exit 1

# Resource constraints
ENV NODE_OPTIONS="--max-old-space-size=512"
ENV PYTHONUNBUFFERED=1

EXPOSE 8080
CMD ["node", "server.js"]