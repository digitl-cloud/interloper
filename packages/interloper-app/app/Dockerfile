FROM node:22-alpine AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml ./
COPY patches/ patches/
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY . ./
RUN pnpm exec nuxt prepare && pnpm build

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
RUN addgroup -S app && adduser -S app -G app
COPY --from=build --chown=app:app /app/.output/ ./
USER app
EXPOSE 3000
CMD ["node", "/app/server/index.mjs"]
