FROM node:16
WORKDIR /app
COPY *.json ./
COPY src ./src
COPY config ./config
RUN npm ci
RUN npm run build
EXPOSE 7001
EXPOSE 7004
EXPOSE 37001
ENTRYPOINT [ "node", "dist/src/singularity.js" ]
