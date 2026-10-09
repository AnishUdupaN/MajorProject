FROM debian:latest
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 python3 ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /project
