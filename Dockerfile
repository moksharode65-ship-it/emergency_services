FROM node:22-alpine

WORKDIR /app

COPY server/package*.json ./
RUN npm install --omit=dev

COPY server/ ./

ENV NODE_ENV=production
EXPOSE 5000 5001 5002 5003

CMD ["node", "start-all.js"]
