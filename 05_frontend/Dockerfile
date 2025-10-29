FROM node:20-alpine

WORKDIR /app

COPY 05_frontend/package.json .

RUN npm install

RUN npm i -g serve

COPY 05_frontend/. .

RUN npm run build

EXPOSE 3000

CMD ["serve", "-s", "dist"]