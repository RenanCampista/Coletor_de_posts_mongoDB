# Coletor de posts do MongoDB

Esse script foi desenvolvido para pegar dados do MongoDB a partir de um intervalo de tempo e transformar em um arquivo CSV.

## Instalação

Para instalar as dependências do projeto, execute o comando abaixo:

```bash
pip install -r requirements.txt
```

## Configuração

Para configurar o script, é necessário criar um arquivo `.env` na raiz do projeto. As variáveis ​​necessárias estão listadas no arquivo .env.example.
Para configurar o projeto:

1. Copie o arquivo `.env.example` para um novo arquivo chamado `.env`:
  
```bash
  cp .env.example .env
```

2. Abra o arquivo `.env` e preencha as variáveis ​​de ambiente com seus valores:
  
```bash
SSH_HOST="IP do servidor"
SSH_USER="Usuário do servidor"
SSH_PRIVATE_KEY="Caminho para a chave privada"
SSH_PASSPHRASE="Frase secreta da chave privada"
MONGO_CONNECTION_STRING="String de conexão do MongoDB"
MONGO_PORT="Porta do MongoDB"
MONGO_DATABASE="Nome do banco de dados"
```

Note que é necessário ter o arquivo de chave privada para acessar o servidor via SSH. O caminho deve ser colocado na variável `SSH_PRIVATE_KEY`. Portanto, se o arquivo estiver na mesma pasta do script, bastará colocar o nome do arquivo.

## Utilização

Para utilizar o script, execute o comando abaixo:

```bash
python download_extractions.py <SocialNetwork> --inicio AAAA-MM-DD --fim AAAA-MM-DD
```
Onde:
- `<SocialNetwork>`: Rede social que deseja baixar os dados. Ex: `twitter`, `facebook`, `instagram`, `tiktok`.
- `--inicio`: Data de início do intervalo de tempo que deseja baixar os dados.
- `--fim`: Data de fim do intervalo de tempo que deseja baixar os dados.

## Exemplo

```bash
python download_extractions.py twitter --inicio 2024-06-24 --fim 2024-06-27
```
O comando acima irá baixar os dados do Twitter no intervalo de tempo de 24/06/2024 a 27/06/2024. Note que a data deve ser no formato AAAA-MM-DD e a data de início deve ser menor que a data de fim.