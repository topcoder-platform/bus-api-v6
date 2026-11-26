# ---- Base Stage ----
FROM node:26.13.1-alpine3.24 AS base
RUN apk upgrade --no-cache && apk add --no-cache "zlib>=1.4-r0"
WORKDIR /usr/src/app

# ---- Tooling Stage ----
FROM base AS tooling
RUN npm install -g pnpm@11.15.1

# ---- Dependencies Stage ----
FROM tooling AS deps
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY tools ./tools
RUN pnpm install --frozen-lockfile

# ---- Build Stage ----
FROM deps AS build
COPY . .
RUN pnpm lint && pnpm build && pnpm test && pnpm test:dependency-compat

# ---- Production Dependencies Stage ----
FROM tooling AS prod-deps
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY tools ./tools
RUN pnpm install --prod --frozen-lockfile --ignore-scripts

# ---- Production Stage ----
FROM alpine:3.24 AS production
RUN apk upgrade --no-cache \
    && apk add --no-cache ca-certificates 'nodejs-current~26.13.1' 'libcrypto3>=3.5.9-r1' 'libssl3>=3.5.9-r1' 'zlib>=1.4-r0' \
    && addgroup -S -g 1000 node \
    && adduser -S -D -u 1000 -G node node
WORKDIR /usr/src/app
ENV NODE_ENV=production
COPY --from=build --chown=node:node /usr/src/app/dist ./dist
COPY --from=prod-deps --chown=node:node /usr/src/app/node_modules ./node_modules
EXPOSE 3000
USER node
CMD ["node", "dist/main.js"]
