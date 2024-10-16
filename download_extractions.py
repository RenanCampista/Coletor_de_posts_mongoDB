"""Script to download data from MongoDB and save it to a csv file. """
from pymongo import MongoClient, errors
from dotenv import load_dotenv
from datetime import datetime
from enum import Enum
import pexpect
import time
import json
import csv
import os
import re


########################################### DATABASE FUNCTIONS ###########################################
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
    try:
        print("Conectando ao MongoDB...")
        client = MongoClient(connection_string, serverSelectionTimeoutMS=20000)
        client.admin.command('ping')  # Force a server selection to check the connection
        print("Conexão com o MongoDB estabelecida.")
        return client
    except errors.ServerSelectionTimeoutError as err:
        print(f"Erro de seleção de servidor ao conectar ao MongoDB: {err}")
        raise
    except Exception as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        raise


def query_mongodb(client: MongoClient, database: str, collection: str, query: dict) -> list:
    """Queries a MongoDB collection and returns the results."""
    try:
        return list(client[database][collection].find(query))
    except Exception as e:
        print(f"Erro ao realizar consulta ao MongoDB: {e}")
        raise


def build_query(start_date: datetime, end_date: datetime, theme: str) -> dict:
    """Builds a query to filter data by date and theme."""
    query = {
        "createdAt": {
            "$gte": start_date,
            "$lte": end_date
        }
    }
    if theme:
        query['themes'] = { "$in": theme}
    return query



########################################### ENUMS ###########################################
class TypeExport(Enum):
    """Enum class to represent the types of data to be exported."""
    POSTS = "postsV2"
    COMMENTS = "comment"
    
    
class Project(Enum):
    """Enum class to represent the projects with available data."""
    MMA = "Raw"
    VACINA = "Vacinal_Raw"
    RSF = "ReportersSF_Raw"


class SocialNetwork(Enum):
    """Enum class to represent the social network to be converted."""
    TWITTER = "twitter"
    TIKTOK = "tiktok"
    INSTAGRAM = "instagram"
    FACEBOOK = "facebook"
    YOUTUBE = "youtube"
    
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
                'Caption': re.sub(r'\n', ' ', body.get('text') or ''),
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
        elif self == SocialNetwork.YOUTUBE:
            def _get_video_id(post_url: str) -> str:
                if 'v=' in post_url:
                    start = post_url.find('v=') + 2
                    end = post_url.find('&', start)
                    if end == -1:
                        end = len(post_url)
                    return post_url[start:end]
                return ''
            
            new_row.update({
                'channelId': body.get('authorId', ''),
                'channelTitle': body.get('authorName', ''),
                'videoId': _get_video_id(body.get('postUrl', '')),
                'videoUrl': body.get('postUrl', ''),
                'publishedAt': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'videoTitle': body.get('title', ''),
                'videoDescription': re.sub(r'\s+', ' ', body.get('text') or ''),
                'tags': body.get('tags', ''),
                'videoCategoryId': body.get('categoryId', ''),
                'duration': body.get('details',{}).get('duration', ''),
                'dimension':body.get('details',{}).get('dimension', ''),
                'definition': body.get('details',{}).get('definition', ''),
                'caption': body.get('details',{}).get('caption', ''),
                'defaultLanguage': body.get('defaultAudioLanguage', ''),
                'thumbnail_maxres': body.get('thumbnails', {}).get('maxres', {}).get('url', ''),
                'licensedContent': body.get('details',{}).get('licensedContent', ''),
                'viewCount': metadata.get('stats', {}).get('seen', 0),
                'LikeCount': metadata.get('stats', {}).get('like', 0),
                'dislikeCount':'', # not found in the data
                'favoriteCount': metadata.get('stats', {}).get('favorite', 0),
                'commentCount': metadata.get('stats', {}).get('comment')
            })
        else:
            raise ValueError(f"Rede social não suportada: {self.value}")
        return new_row

    def get_comments(self, comments, count) -> list:
        return [self.get_posts({'': count, ' ': ''}, comment) for count, comment in enumerate(comments, start=count)]



########################################### UTILS ###########################################
def select_enum(enum_class):
    """Prompts the user to select a value from an Enum class."""
    choices = {str(index): enum_member for index, enum_member in enumerate(enum_class, start=1)}
    print(f"\nSelecione uma opção de {enum_class.__name__}:")
    for index, enum_member in choices.items():
        print(f"{index} - {enum_member.name}")
    
    while True:
        choice = input("Digite o número da opção desejada: ")
        if choice in choices:
            return choices[choice]
        else:
            print("Opção inválida, tente novamente.")


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
    """Retrieves an environment variable and raises an error if not found."""
    var_value = os.getenv(var_name)
    if not var_value:
        raise EnvironmentError(f"Variável de ambiente '{var_name}' não encontrada.")
    return var_value


def filter_data_by_date(data, reference_date):
    """Filtra os dados e retorna apenas aqueles com a data superior à data fornecida."""
    filtered_data = []
    for item in data:
        item_date_str = item.get('Date', '')
        if item_date_str:
            item_date = datetime.strptime(item_date_str, '%Y-%m-%d %H:%M:%S')
            if item_date > reference_date:
                filtered_data.append(item)
    return filtered_data


def organize_data(posts: dict, social_network: SocialNetwork, get_comments: bool = False) -> tuple[list, int]:
    """Organize data to be saved in a csv file"""
    def add_url_and_shortcode(rows, url, shortcode):
        """Add URL and shortcode to each row in the list."""
        for row in rows:
            row['Post URL'] = url
            row['SourceShortCode'] = shortcode

    def create_new_row_base(post, social_network):
        """Create a base dictionary for new rows based on the social network."""
        new_row_base = {}
        if social_network == SocialNetwork.INSTAGRAM:
            new_row_base["SourceShortCode"] = ''
            new_row_base['type'] = 'comment' if get_comments else 'post'
            new_row_base["SourceLink"] = ''
        elif social_network == SocialNetwork.TIKTOK:
            new_row_base['Video ID'] = post['postId']
        elif social_network == SocialNetwork.TWITTER:
            new_row_base['lineid'] = post['terms']
            new_row_base[''] = ''
        return new_row_base

    data = []
    count = 0

    for post in posts:
        if get_comments:
            comments = post['comments']
            new_rows = social_network.get_comments(comments, count)
            if social_network in {SocialNetwork.INSTAGRAM, SocialNetwork.TIKTOK}:
                add_url_and_shortcode(new_rows, post['postUrl'], post['postShortcode'])
            data.extend(new_rows)
            count += len(new_rows)
        else:
            new_row_base = create_new_row_base(post, social_network)
            content = post['postHistory'][-1]
            new_row = social_network.get_posts(new_row_base, content)
            data.append(new_row)
            count += 1
    return data, count



########################################### MAIN FUNCTIONS ###########################################
def main():
    """Main function to download data from MongoDB and save it to a csv file."""
    social_network = select_enum(SocialNetwork)
    project = select_enum(Project)
    type_export = select_enum(TypeExport)
    theme = input("Informe um tema (digite '*' para todos): ")
    if theme == '*': theme = ''

    start_date_str = input("Informe a data de início (formato: AAAA-MM-DD): ")
    end_date_str = input("Informe a data de término (formato: AAAA-MM-DD): ")
    start_date = datetime.strptime(start_date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end_date_str + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    if start_date > end_date:
        raise ValueError("A data de início não pode ser maior que a data de término.")
    
    query = build_query(start_date, end_date, theme)
    
    load_dotenv()
    ssh_command = f"""sudo ssh -f -N -o TCPKeepAlive=yes -o ServerAliveInterval=60 -L 
        {env_variable('MONGO_PORT')}:localhost:{env_variable('MONGO_PORT')} -i 
        {env_variable('SSH_PRIVATE_KEY')} {env_variable('SSH_USER')}@{env_variable('SSH_HOST')}"""
    mongo_collection = f"{social_network.value}_{type_export.value}"
    
    ssh_process = establish_ssh_tunnel(ssh_command, env_variable("SSH_PASSPHRASE"))
    client = connect_to_mongodb(env_variable("MONGO_CONNECTION_STRING"))
    print(f"Iniciando consulta ao MongoDB para a rede social {social_network.value}...")
    data = query_mongodb(client, project.value, mongo_collection, query)
    
    if len(data) == 0:
        print(f"Nenhum resultado encontrado no intervalo de {start_date} a {end_date} com o tema: {'*' if not theme else theme}.")
        return
    
    data, num_results = organize_data(data, social_network, type_export == TypeExport.COMMENTS)
    data = filter_data_by_date(data, start_date)
    print(f"Foram coletados {num_results} dados no intervalo de {start_date} a {end_date} com o tema: {theme}.")
    print(f"{len(data)} dados criados a partir da data {start_date}.")

    file_name = f"{social_network.value}_{type_export.value}_{start_date_str}_{end_date_str}.csv"
    social_network_folder = social_network.value
    folder_path = os.path.join(os.getcwd(), social_network_folder)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    file_path = os.path.join(folder_path, file_name)
    save_to_csv(data, file_path)

    ssh_process.terminate()
    print(f"Processo concluído com sucesso. Arquivo salvo em {file_path}.")
    
    
if __name__ == "__main__":
    main()