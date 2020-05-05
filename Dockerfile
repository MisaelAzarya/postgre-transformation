FROM node:latest
WORKDIR /app
ENV HOST="172.17.0.2" USER="postgres" PASSWORD="mysecretpassword" PORT="5432" DATABASE="staging_ingestion" 
ENV HOST2="172.17.0.2" USER2="postgres" PASSWORD2="mysecretpassword" PORT2="5432" DATABASE2="staging_transformation"
COPY package.json /app
RUN npm install
COPY . /app
CMD sleep 2; node index.js