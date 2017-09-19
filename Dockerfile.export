FROM node:8.5.0-alpine

ADD package.json package.json
RUN npm install

ADD index.js index.js
ADD export.js export.js

VOLUME /data

ENTRYPOINT node export.js
