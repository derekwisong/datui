# Run this container to test the apt repository installation

FROM ubuntu:latest

RUN apt-get update && apt-get install -y curl && apt-get clean

COPY install.sh /install.sh

CMD ["/install.sh"]
