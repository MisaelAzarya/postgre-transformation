FROM node:latest
WORKDIR /app
COPY package.json /app
RUN npm install
COPY . /app
CMD sleep 2; node index.js