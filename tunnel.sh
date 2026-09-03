#!/bin/zsh

# Find the first local HTTPS server
PORT=""

for port in $(lsof -nP -iTCP -sTCP:LISTEN 2>/dev/null | \
              awk 'NR>1 {split($9,a,":"); print a[length(a)]}' | sort -nu)
do
    if curl -sk --connect-timeout 1 "https://localhost:$port" >/dev/null 2>&1; then
        PORT="$port"
        break
    fi
done

if [[ -z "$PORT" ]]; then
    echo "No HTTPS server found."
    exit 1
fi

echo "HTTPS server found on port: $PORT"
echo "Starting Cloudflare Tunnel..."

cloudflared tunnel --url "https://localhost:$PORT" \
    --no-tls-verify \
    --http-host-header localhost
