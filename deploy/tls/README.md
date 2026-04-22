# TLS certificates

Place these files here before starting the `nginx` service:

- `fullchain.pem` — TLS certificate chain
- `privkey.pem`   — private key (chmod 600)

For Let's Encrypt:

```bash
sudo certbot certonly --standalone -d engram.example.com
sudo cp /etc/letsencrypt/live/engram.example.com/fullchain.pem deploy/tls/
sudo cp /etc/letsencrypt/live/engram.example.com/privkey.pem   deploy/tls/
sudo chown $(id -u):$(id -g) deploy/tls/*
```

For development, generate a self-signed cert (browsers will warn):

```bash
openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes \
  -subj "/CN=engram.localhost" \
  -keyout deploy/tls/privkey.pem -out deploy/tls/fullchain.pem
```
