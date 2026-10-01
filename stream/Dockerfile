FROM node:22-slim
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY server.js indicator.html edgeful-baselines.json ib-strategy.js orb-strategy.js history-coverage.js ./
RUN mkdir -p /data
ENV DATA_DIR=/data
EXPOSE 8080
CMD ["npm","start"]
