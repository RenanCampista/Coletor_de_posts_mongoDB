# Coletor de posts do MongoDB

Este script foi desenvolvido para baixar dados do MongoDB a partir de um intervalo de tempo e transformá-los em arquivos CSV. 

## Instalação

Para instalar as dependências do projeto, em um terminal execute o comando abaixo:

```bash
pip install -r requirements.txt
```

Obs: É necessário ter o Python instalado na máquina. Caso não tenha, acesse o link [Python](https://www.python.org/downloads/) para baixar e instalar.

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
PROJECT="Nome do projeto para baixar os dados"
```

Note que é necessário ter o arquivo de chave privada para acessar o servidor via SSH. O caminho deve ser colocado na variável `SSH_PRIVATE_KEY`. Portanto, se o arquivo estiver na mesma pasta do script, bastará colocar o nome do arquivo.

## Utilização

Para utilizar o script, execute o comando abaixo:

```bash
python download_extractions.py <SocialNetwork> --inicio AAAA-MM-DD --fim AAAA-MM-DD
```
Onde:
- `<SocialNetwork>`: Rede social que deseja baixar os dados. Ex: `twitter`, `facebook`, `instagram`, `tiktok`, `youtube`.
- `--inicio`: Data de início do intervalo de tempo que deseja baixar os dados.
- `--fim`: Data de fim do intervalo de tempo que deseja baixar os dados.

## Exemplo

```bash
python download_extractions.py twitter --inicio 2024-06-24 --fim 2024-06-27
```
O comando acima irá baixar os dados do Twitter no intervalo de tempo de 24/06/2024 a 27/06/2024. Note que a data deve ser no formato AAAA-MM-DD e a data de início deve ser menor que a data de fim.

## Pegar comentários do Twitter
É possível baixar comentários do banco de dados do Twitter. Nesse caso, não é possível filtrar por tema ou termo, pois os dados estão associados apenas ao post original. Para baixar os comentários, use a flag `--get_comments`. Ex:

```bash
python download_extractions.py twitter --inicio 2024-07-24 --fim 2024-07-27 --get_comments
```
O comando acima irá baixar os comentários do Twitter no intervalo de tempo de 24/07/2024 a 27/07/2024.

## Adicionar filtros de busca
É possível retornar postagens a partir de um tema ou termo específico. Os dois parâmetros abaixo são opcionais e podem ser utilizados separadamente ou em conjunto.

### Filtro por tema
Para filtrar as postagens por tema, adicione o parâmetro `--tema` seguido do tema desejado. Ex:

```bash
python download_extractions.py twitter --inicio 2024-06-24 --fim 2024-06-27 --tema 'clima'
```
O comando acima irá baixar os dados do Twitter no intervalo de tempo de 24/06/2024 a 27/06/2024 que contenham o tema "clima".

### Filtro por termo
Para filtrar as postagens por termo, adicione o parâmetro `--termo` seguido do termo desejado. Ex:

```bash
python download_extractions.py twitter --inicio 2024-06-24 --fim 2024-06-27 --termo 'chuva'
```
O comando acima irá baixar os dados do Twitter no intervalo de tempo de 24/06/2024 a 27/06/2024 que contenham o termo "chuva".


## Usando os dois filtros juntos
Para usar os dois filtros juntos, basta adicionar os dois parâmetros ao comando. Ex:

```bash
python download_extractions.py twitter --inicio 2024-06-24 --fim 2024-06-27 --tema 'clima' --termo 'chuva'
```
O comando acima irá baixar os dados do Twitter no intervalo de tempo de 24/06/2024 a 27/06/2024 que contenham o tema "clima" e o termo "chuva".

Obs: Certifique-se de que o tema ou termo estão entre aspas simples. Além disso, quando usar os dois filtros juntos, garanta que o termo faz parte do tema.
