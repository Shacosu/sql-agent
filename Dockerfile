# Multi-stage Dockerfile for NestJS (ESM, pnpm) optimized for production

# 1) Base image with Node 20 and corepack (pnpm)
FROM node:20-alpine AS base
WORKDIR /app
ENV PNPM_HOME=/root/.local/share/pnpm
ENV PATH=$PNPM_HOME:$PATH
RUN corepack enable

# 2) Dependencies (install all deps including dev for building)
FROM base AS deps
COPY package.json pnpm-lock.yaml ./
# If you later add .npmrc, copy it too to respect registry configs
# COPY .npmrc ./
RUN pnpm install --frozen-lockfile

# 3) Build (compile TS -> dist)
FROM base AS build
COPY --from=deps /app/node_modules ./node_modules
COPY . .
# Ensure Nest CLI is available via devDependencies
RUN pnpm run build

# 4) Production image (small, only prod deps + dist)
FROM node:20-alpine AS prod
WORKDIR /app
ENV NODE_ENV=production
ENV PNPM_HOME=/root/.local/share/pnpm
ENV PATH=$PNPM_HOME:$PATH
RUN corepack enable

# Copy package manifests and install only production dependencies
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --prod --frozen-lockfile

# Copy built artifacts
COPY --from=build /app/dist ./dist

# Expose Nest default port
EXPOSE 3000

# Environment variables (override via docker-compose or runtime)
ENV PORT=3000

# Start the server
CMD ["node", "dist/main.js"]
