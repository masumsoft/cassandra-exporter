FROM node:8.5.0-alpine

ADD package.json package.json
RUN npm install

ADD index.js index.js
ADD import.js import.js

VOLUME /data

ENTRYPOINT node import.js
