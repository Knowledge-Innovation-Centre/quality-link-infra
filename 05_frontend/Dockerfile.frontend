FROM node:20-alpine

WORKDIR /app

COPY package*.json ./

RUN npm install

RUN npm i -g serve

COPY . .

ARG VITE_RECAPTCHA_SITE_KEY
ENV VITE_RECAPTCHA_SITE_KEY=$VITE_RECAPTCHA_SITE_KEY

RUN npm run build

EXPOSE 3000

CMD ["serve", "-s", "dist"]