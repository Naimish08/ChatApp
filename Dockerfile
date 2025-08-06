FROM node:20-slim

WORKDIR /usr/src/app

COPY package*.json ./

RUN npm install

COPY . .

RUN mkdir -p uploads

EXPOSE 3000

CMD ["node", "index.js"]