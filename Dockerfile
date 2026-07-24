FROM node:22-alpine AS frontend

WORKDIR /frontend
COPY app/package*.json ./
RUN npm ci
COPY app ./
RUN npm run build

FROM node:22-slim

WORKDIR /app

# Debian (glibc) runtime so the Python deps install from prebuilt manylinux wheels.
# confluent-kafka ships NO musl wheel (any version), so on Alpine it source-compiles
# and OOMs/times out under `railway up` (no build-layer cache). glibc wheels need no
# build toolchain or -dev libraries, so the whole apk build-base/*-dev set is dropped.
# Versions pinned to the currently-resolved releases for reproducible builds.
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-venv python3-pip tzdata ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && python3 -m venv /opt/rssvenv \
  && /opt/rssvenv/bin/pip install --upgrade pip setuptools wheel \
  && /opt/rssvenv/bin/pip install --no-cache-dir \
       pymongo==4.17.0 feedparser==6.0.12 requests==2.34.2 curl_cffi==0.15.0 \
       python-dotenv==1.2.2 beautifulsoup4==4.15.0 "psycopg[binary]==3.3.4" \
       confluent-kafka==2.15.0 redis==8.0.1

COPY Infrastructure/server/package*.json ./
RUN npm ci --omit=dev

COPY Infrastructure/server ./
COPY Infrastructure/pipeline ./Infrastructure/pipeline
COPY Infrastructure/kafka ./Infrastructure/kafka
COPY 1_News ./1_News
COPY 2_Screener ./2_Screener
COPY chart-service/finviz_auth.py ./chart-service/finviz_auth.py
COPY 5_Social ./5_Social
COPY config ./config
COPY scripts ./scripts
COPY scripts /scripts
COPY --from=frontend /frontend/dist ./public

ENV PYTHONUNBUFFERED=1
ENV RSS_COOLDOWN_SECONDS=0
ENV RSS_STATE_FILE=/tmp/feedflash_rss_fetch_state.json
ENV NODE_ENV=production

EXPOSE 3001

CMD ["npm", "run", "start"]
