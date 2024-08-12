from pymongo import MongoClient, errors
from dotenv import load_dotenv
from datetime import datetime
from enum import Enum
import pexpect
import argparse
import time
import json
import csv
import os


def establish_ssh_tunnel(ssh_command: str, ssh_passphrase: str) -> pexpect.spawn:
    """Establishes an SSH tunnel using a given command and passphrase with pexpect."""
    
    try:
        print("Estabelecendo túnel SSH com pexpect...")
        ssh_process = pexpect.spawn(ssh_command, timeout=30)
        ssh_process.expect("Enter passphrase for key .*:")
        print("Informando a passphrase...")
        ssh_process.sendline(ssh_passphrase)
        time.sleep(3)
        print("Conexão SSH estabelecida.")
        return ssh_process
    except pexpect.exceptions.TIMEOUT:
        print("O túnel SSH não pôde ser estabelecido. O tempo limite expirou.")
        ssh_process.kill(9)
        raise
    

def connect_to_mongodb(connection_string: str) -> MongoClient:
    """Connects to a MongoDB database using a given connection string."""
    
    print("Conectando ao MongoDB...")
    client = MongoClient(connection_string, serverSelectionTimeoutMS=20000)
    client.admin.command('ping')  # Force a server selection to check the connection
    print("Conexão com o MongoDB estabelecida.")
    return client


def query_mongodb(client: MongoClient, database: str, collection: str, query: dict) -> list:
    """Queries a MongoDB collection and returns the results."""
    
    return list(client[database][collection].find(query))


class SocialNetwork(Enum):
    """Enum class to represent the social network to be converted."""
    
    TWITTER = "twitter"
    TIKTOK = "tiktok"
    INSTAGRAM = "instagram"
    FACEBOOK = "facebook"
    
    def get_posts(self, new_row_base, content) -> dict:
        body, metadata = content['body'], content.get('metadata', {})
        new_row = new_row_base.copy()
        timestamp = body.get('timestamp', datetime.now())
        if not isinstance(timestamp, datetime):
            timestamp = datetime.now()
        
        if self == SocialNetwork.TWITTER:
            new_row.update({
                'Name': body.get('authorName', ''),
                'Username': body.get('authorNickName', ''),
                'Tweet ID (click to view url)': body.get('statusId', ''),
                'Retweets': metadata.get('stats', {}).get('share', 0),
                'Comments': metadata.get('stats', {}).get('comment', 0),
                'Favorites': metadata.get('stats', {}).get('like', 0),
                'Is Retweet?': body.get('isRetweet', False),
                'Date': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'Tweet Text': body.get('text', ''),
                'Author Followers': body.get('authorFollowers', 0),
                'Author Friends': body.get('authorFriendsCount', 0), # Apparently this is what the 'Favorites' field does
                'Author Favorites': metadata.get('stats', {}).get('like', 0),
                'Author Statuses': body.get('statuses', 0),
                'Author Bio': (body.get('authorBio') or '').replace('\n', ' '),
                'Author Image': body.get('authorImage', ''),
                'Author Location': body.get('locationName', ''),
                'Author Verified': 'no', # not found in the data
                'Tweet Source': body.get('source', ''),
                'Status URL': body.get('postUrl', '') or metadata.get('collect', {}).get('commentUrl', ''),
            })
        elif self == SocialNetwork.TIKTOK:
            new_row.update({
                'Name (click to view profile)': body.get('authorName', ''),
                'Unique ID': body.get('authorNickName', ''),
                'Date': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'Description': body.get('text', ''),
                'Digg count':'', 
                'Share count': metadata.get('stats', {}).get('share', 0),
                'Play count': metadata.get('stats', {}).get('seen', 0),
                'Comment count': metadata.get('stats', {}).get('comment', 0),
                'Music': body.get('musicName', ''),
                'Author Following count': body.get('authorFollowing', 0),
                'Author Follower count': body.get('authorFollowers', 0),
                'Author Heart count': body.get('authorHeart', 0),
                'Author Video count': body.get('authorVideo', 0),
                'Author Digg count':body.get('authorDigg', 0),
                'Video link': body.get('postUrl', ''),
                'Author URL': body.get('authorUrl', ''),
            })
        elif self == SocialNetwork.INSTAGRAM:
            new_row.update({
                'Shortcode': body.get('shortcode', ''),
                'Username': body.get('authorNickName', ''),
                'Profile ID': body.get('authorId', ''),
                'Media Type': body.get('productType', ''),
                'Video View Count': metadata.get('stats', {}).get('seen', 0),
                'Likes': metadata.get('stats', {}).get('like', 0),
                'Comments': metadata.get('stats', {}).get('comment', 0),
                'Date created': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'Caption': (body.get('text') or '').replace('\n', ' '),
                'URL': body.get('postUrl', ''),
            })    
        elif self == SocialNetwork.FACEBOOK:
            new_row.update({
                'Name (click to view profile)': body.get('authorName', ''),
                'Profile ID': body.get('authorId', ''),
                'Date': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'Likes': metadata.get('stats', {}).get('like', 0),
                'Stars': '', # not found in the data
                'Comment': body.get('text', ''),
                'URL': body.get('postUrl', ''),
                'Profile Image':'', # not found in the data
            })
        else:
            raise ValueError(f"Rede social não suportada: {self.value}")
        return new_row

    def get_comments(self, comments, count) -> list:
        return [self.get_posts({'': count, ' ': ''}, comment) for count, comment in enumerate(comments, start=count)]


def organize_data(posts: dict, social_network: SocialNetwork, get_comments: bool = False) -> tuple[list, int]:
    """Organize data to be saved in a csv file"""
    
    data = []
    count = 0
    for post in posts:
        if get_comments:
            content = post['comments']
            new_row = social_network.get_comments(content, count)
            data += new_row
            count += len(new_row)
        else:
            new_row_base = {
                '': count,
                ' ': '',
            }
            if social_network == SocialNetwork.INSTAGRAM: new_row_base.update({'ID': post['postId']})    
            if social_network == SocialNetwork.TIKTOK: new_row_base.update({'Video ID': post['postId']})        
            content = post['postHistory'][-1]
            new_row = social_network.get_posts(new_row_base, content)
            data.append(new_row)
            count += 1
    return data, count


def save_to_json(data: list, filename: str) -> None:
    """Saves a list of data to a JSON file."""
    
    with open(filename, "w") as file:
        json.dump(data, file, indent=4, default=str)
    print("Dados salvos em JSON.")


def save_to_csv(data: list, file_name: str):
    """Write data to a csv file using csv module."""
    
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(
            f=file, 
            fieldnames=data[0].keys(), 
            quotechar='"', 
            quoting=csv.QUOTE_NONNUMERIC,
        )
        
        writer.writeheader()
        
        for row in data:
            writer.writerow(row)
            
    print(f"Arquivo {os.path.basename(file_name)} salvo com sucesso.")


def env_variable(var_name: str) -> str:
    """Gets an environment variable or raises an exception if it is not set."""
    
    if not (var_value := os.getenv(var_name)):
        raise ValueError(f"Variável de ambiente {var_name} não definida. Verifique o arquivo .env.")
    return var_value


def main(social_network: SocialNetwork, since_date_str: str, until_date_str: str, get_comments: bool):
    """Main function to download data from MongoDB and save it to a csv file."""
    
    since_date = datetime.strptime(since_date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    until_date = datetime.strptime(until_date_str + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    
    if since_date > until_date:
        raise ValueError("Start date must be before end date.")
    
    load_dotenv()
    SSH_COMMAND = f"sudo ssh -f -N -o TCPKeepAlive=yes -o ServerAliveInterval=60 -L {env_variable('MONGO_PORT')}:localhost:{env_variable('MONGO_PORT')} -i {env_variable('SSH_PRIVATE_KEY')} {env_variable('SSH_USER')}@{env_variable('SSH_HOST')}"
    MONGO_COLLECTION = f"{social_network.value}_{'comment' if get_comments else 'postsV2'}"
    QUERY = {
        "createdAt": {
            "$gte": since_date,
            "$lte": until_date
        }
    }
    if args.tema and isinstance(args.tema, str) and not args.get_comments:
        QUERY["postHistory.metadata.collect.theme"] = args.tema
    if args.termo and isinstance(args.termo, str) and not args.get_comments:
        QUERY["postHistory.metadata.collect.terms"] = {"$all": [args.termo]}
    
    try:
        ssh_process = establish_ssh_tunnel(SSH_COMMAND, env_variable("SSH_PASSPHRASE"))
        client = connect_to_mongodb(env_variable("MONGO_CONNECTION_STRING"))
        print(f"Iniciando consulta ao MongoDB para a rede social {social_network.value}...")
        data = query_mongodb(client, env_variable("MONGO_DATABASE"), MONGO_COLLECTION, QUERY)
        if len(data) == 0:
            failed_message = f"Nenhum post encontrado no intervalo de {since_date_str} a {until_date_str}."
            if args.tema:
                failed_message += f" Tema: {args.tema}."
            if args.termo:
                failed_message += f" Termo: {args.termo}."
            print(failed_message)
            return
        
        data, num_results = organize_data(data, args.social_network, args.get_comments)   
        sucess_message = f"Encontrado {num_results}"
        if args.get_comments:
            sucess_message += f" comentários no intervalo no intervalo de {since_date_str} a {until_date_str}."
        else:
            sucess_message += f" posts no intervalo de {since_date_str} a {until_date_str}."
            if args.tema:
                sucess_message += f" Tema: {args.tema}."
                file_name += f"_tema_{args.tema}"
            if args.termo:
                sucess_message += f" Termo: {args.termo}."
                file_name += f"_termo_{args.termo}"
        file_name = f"{social_network.value}_{since_date_str}_{until_date_str}"
        file_name += ".csv"
        print(sucess_message)        
             
        social_network_folder = args.social_network.value
        folder_path = os.path.join(os.getcwd(), social_network_folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        file_path = os.path.join(folder_path, file_name)
        save_to_csv(data, file_path)
    except errors.ServerSelectionTimeoutError as err:
        print(f"Erro de seleção de servidor ao conectar ao MongoDB: {err}")
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        print("Fechando o túnel SSH...")
        ssh_process.terminate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Baixa dados de uma rede social de um banco de dados MongoDB e salva em um arquivo CSV.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "social_network",
        type=SocialNetwork,
        choices=SocialNetwork,
        help="Rede social que deseja baixar os dados. Opções: twitter, tiktok, instagram, facebook."
    )
    parser.add_argument(
        "--inicio",
        type=str,
        help="Data de início no formato AAAA-MM-DD",
        required=True
    )
    parser.add_argument(
        "--fim",
        type=str,
        help="Data de fim no formato AAAA-MM-DD",
        required=True
    )
    parser.add_argument(
        "--get_comments",
        action="store_true",
        help="Pegar comentários ao invés de posts",
        default=False
    )
    parser.add_argument(
        "--tema",
        type=str,
        help="Tema da pesquisa",
        default=None
    )
    parser.add_argument(
        "--termo",
        type=str,
        help="Termo da pesquisa",
        default=None
    )
    args = parser.parse_args()
    main(args.social_network, args.inicio, args.fim, args.get_comments)
    