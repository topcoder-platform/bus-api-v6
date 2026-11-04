# ---- Base Stage ----
FROM node:26.12.1-alpine3.24 AS base
RUN apk upgrade --no-cache
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
FROM base AS production
ENV NODE_ENV=production
RUN rm -rf /usr/local/lib/node_modules /opt/yarn-v* \
  && rm -f /usr/local/bin/npm /usr/local/bin/npx
COPY --from=build --chown=node:node /usr/src/app/dist ./dist
COPY --from=prod-deps --chown=node:node /usr/src/app/node_modules ./node_modules
EXPOSE 3000
USER node
CMD ["node", "dist/main.js"]
