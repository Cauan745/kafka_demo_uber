# Trabalho 4 - Kafka

Simulação do sistema do Uber usando Kafka pra comunicação entre serviços. O sistema rastreia motoristas em tempo real e mostra no mapa via WebSocket.

## Stack

- Go (backend, producer, consumer)
- Apache Kafka (mensageria)
- PostgreSQL (banco de dados)
- HTML/CSS/JS (frontend)
- Docker Compose (orquestração)

## Como rodar

```bash
docker compose up
```

Acesse `http://localhost:80` pra criar uma conta e depois fazer login.

A página do mapa fica em `http://localhost:80/app/` (precisa estar logado).

Pra simular motoristas sem o docker, o `QUANTITY` define quantos motoristas cada producer vai simular:

```bash
make producer QUANTITY=5
```

Pra rodar 50 producers em paralelo (50 x 3 = 150 motoristas):

```bash
make run-bunch-producers QUANTITY=3
```
