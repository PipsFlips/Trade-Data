FROM node:22-slim
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY server.js indicator.html edgeful-baselines.json ib-strategy.js orb-strategy.js mes-failure-strategy.js vwap-strategy.js history-coverage.js backtest-history.js trade-audit-engine.js trade-audit-service.js ./
RUN mkdir -p /data
ENV DATA_DIR=/data
EXPOSE 8080
CMD ["npm","start"]
