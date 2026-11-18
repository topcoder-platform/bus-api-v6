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
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a AS production
# Use Alpine's dynamically linked Node so apk updates the OpenSSL used at runtime.
RUN apk upgrade --no-cache \
    && apk add --no-cache 'nodejs-current~26' 'nodejs-current>=26.13.1-r0' \
        'libssl3>=3.5.10-r0' 'libcrypto3>=3.5.10-r0' 'zlib>=1.4-r0' ca-certificates icu-data-full \
    && node -e "const libs = process.report.getReport().sharedObjects; if (!['/usr/lib/libssl.so.3', '/usr/lib/libcrypto.so.3'].every(p => libs.includes(p))) process.exit(1)" \
    && addgroup -g 1000 node \
    && adduser -D -u 1000 -G node node \
    && ln -s /usr/bin/node /usr/local/bin/node
COPY --from=prod-deps /usr/local/bin/docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
WORKDIR /usr/src/app
ENV NODE_ENV=production
COPY --from=build --chown=node:node /usr/src/app/dist ./dist
COPY --from=prod-deps --chown=node:node /usr/src/app/node_modules ./node_modules
EXPOSE 3000
USER node
CMD ["node", "dist/main.js"]
