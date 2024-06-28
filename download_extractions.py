from pymongo import MongoClient, errors
from dotenv import load_dotenv
import datetime
from enum import Enum
import subprocess
import argparse
import time
import json
import csv
import os


def establish_ssh_tunnel(ssh_command: list, ssh_passphrase: str) -> subprocess.Popen:
    """Establishes an SSH tunnel using a given command and passphrase. """
    
    print("Establishing SSH tunnel...")
    #ssh_process = subprocess.Popen(ssh_command, stdin=subprocess.PIPE)
    ssh_process = subprocess.Popen(ssh_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ssh_process.communicate(input=ssh_passphrase.encode())
    time.sleep(5)  # Wait to ensure the tunnel is ready
    print("SSH tunnel established.")
    return ssh_process


def connect_to_mongodb(connection_string: str) -> MongoClient:
    """Connects to a MongoDB database using a given connection string."""
    
    print("Connecting to MongoDB...")
    client = MongoClient(connection_string, serverSelectionTimeoutMS=20000)
    client.admin.command('ping')  # Force a server selection to check the connection
    print("MongoDB connection established.")
    return client


def query_mongodb(client: MongoClient, database: str, collection: str, query: dict) -> list:
    """Queries a MongoDB collection and returns the results."""
    
    db = client[database]
    collection = db[collection]
    results = collection.find(query)
    return [document for document in results]


def save_data_to_json(data: list, filename: str) -> None:
    """Saves a list of data to a JSON file."""
    
    with open(filename, "w") as file:
        json.dump(data, file, indent=4, default=str)
    print("Data successfully downloaded!")


class SocialNetwork(Enum):
    """Enum class to represent the social network to be converted."""

    TWITTER = "twitter"
    TIKTOK = "tiktok"
    INSTAGRAM = "instagram"
    FACEBOOK = "facebook"

    def __str__(self):
        return self.value
    
    def get_data(self, new_row_base, history) -> dict:
        body = history['body']
        metadata = history.get('metadata', {})
        new_row = new_row_base.copy()

        # Verify if the timestamp is a datetime object
        timestamp = body.get('timestamp', datetime.datetime.now())
        if not isinstance(timestamp, datetime.datetime):
            timestamp = datetime.datetime.now()
            
        if self == self.__class__.TWITTER:
            new_row.update({
                'Name': body.get('authorName', ''),
                'Username': body.get('authorNickName', ''),
                'Tweet ID (click to view url)': body.get('statusId', ''),
                'Retweets': metadata.get('stats', {}).get('share', 0),
                'Comments': metadata.get('stats', {}).get('comment', 0),
                'Favorites': metadata.get('stats', {}).get('like', 0),
                'Is Retweet?': 'no',  # not found in the data
                'Date': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'Tweet Text': body.get('text', ''),
                'Author Followers': body.get('authorFollowers', 0),
                'Author Friends': body.get('authorFriendsCount', 0),  # Apparently this is what the 'Favorites' field does.
                'Author Favorites': metadata.get('stats', {}).get('like', 0),
                'Author Statuses': body.get('statuses', 0),
                'Author Bio': (body.get('authorBio') or '').replace('\n', ' '),
                'Author Image': body.get('authorImage', ''),
                'Author Location': body.get('locationName', ''),
                'Author Verified': 'no',  # not found in the data
                'Tweet Source': body.get('source', ''),
                'authorUrl': body.get('authorUrl', ''),
                'authorId': body.get('authorId', ''),
                'Status URL': body.get('postUrl', ''),
            })
            return new_row
        if self == self.__class__.TIKTOK:
            new_row.update({
                'postUrl': body.get('postUrl', ''),
                'authorName': body.get('authorName', ''),
                'authorUrl': body.get('authorUrl', ''),
                'authorId': body.get('authorId', ''),
                'timestamp': body.get('timestamp', {}).get('$date', ''),
                'text': body.get('text', ''),
                'tagId': body.get('tagId', ''),
                'tagName': body.get('tagName', ''),
                'musicId': body.get('musicId', ''),
                'musicName': body.get('musicName', ''),
                'musicAuthor': body.get('musicAuthor', ''),
                'authorFollowing': body.get('authorFollowing', 0),
                'authorFollowers': body.get('authorFollowers', 0),
                'authorHeart': body.get('authorHeart', 0),
                'authorVideo': body.get('authorVideo', 0),
                'authorDigg': body.get('authorDigg', 0),
                'authorBio': (body.get('authorBio') or '').replace('\n', ' '),
                'authorVerified': body.get('authorVerified', False),
                'commentUrl': body.get('commentUrl', ''),
                'commentCount': metadata.get('stats', {}).get('comment', 0),
                'likeCount': metadata.get('stats', {}).get('like', 0),
                'seenCount': metadata.get('stats', {}).get('seen', 0),
                'shareCount': metadata.get('stats', {}).get('share', 0),
            })
            return new_row
        if self == self.__class__.INSTAGRAM:
            new_row.update({
                'postUrl': body.get('postUrl', ''),
                'authorName': body.get('authorName', ''),
                'authorNickName': body.get('authorNickName', ''),
                'authorUrl': body.get('authorUrl', ''),
                'authorId': body.get('authorId', ''),
                'timestamp': body.get('timestamp', {}).get('$date', ''),
                'text': (body.get('text') or '').replace('\n', ' '),
                "authorImage": body.get('authorImage', ''),
                "reply": body.get('reply', ''),
                "shortcode": body.get('shortcode', ''),
                "isVideo": body.get('isVideo', False),
                'productType': body.get('productType', ''),
                'isSponsored': body.get('isSponsored', False),
                'locationName': body.get('locationName', ''),
                'media': body.get('media', ''),
                'commentCount': metadata.get('stats', {}).get('comment', 0),
                'likeCount': metadata.get('stats', {}).get('like', 0),
                'seenCount': metadata.get('stats', {}).get('seen', 0),
            })    
            return new_row
        if self == self.__class__.FACEBOOK:
            new_row.update({
                'postUrl': body.get('postUrl', ''),
                'authorName': body.get('authorName', ''),
                'authorUrl': body.get('authorUrl', ''),
                'authorId': body.get('authorId', ''),
                'timestamp': body.get('timestamp', {}).get('$date', ''),
                'text': body.get('text', ''),
                'commentCount': metadata.get('stats', {}).get('comment', 0),
                'likeCount': metadata.get('stats', {}).get('like', 0),
                'seenCount': metadata.get('stats', {}).get('seen', 0),
                'shareCount': metadata.get('stats', {}).get('share', 0),
                'reactionCount': metadata.get('stats', {}).get('reaction', 0),
            })
            return new_row
        raise ValueError(f"Invalid social network {self}.")


def organize_data(posts: dict, social_network: SocialNetwork) -> list:
    """Organize data to be saved in a csv file"""
    
    data = []
    count = 0
    for post in posts:
        new_row_base = {
            'id': count,
        }
        history = post['postHistory'][-1]
        new_row = social_network.get_data(new_row_base, history)
        data.append(new_row)
        count += 1
        
    return data


def save_to_csv(data: list, social_network: SocialNetwork):
    """Write data to a csv file using csv module."""
    
    filename = f'{social_network.value}.csv'
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def env_variable(var_name: str) -> str:
    """Gets an environment variable or raises an exception if it is not set."""
    
    if not (var_value := os.getenv(var_name)):
        raise ValueError(f"Environment variable {var_name} is required")
    return var_value


def main(since_date_str: str, until_date_str: str):
    """Main function to download data from MongoDB and save it to a csv file."""
    
    since_date = datetime.datetime.strptime(since_date_str, "%Y-%m-%d")
    until_date = datetime.datetime.strptime(until_date_str, "%Y-%m-%d")
    
    if since_date > until_date:
        raise ValueError("Start date must be before end date.")
    
    since_date = since_date.replace(hour=0, minute=0, second=0)
    until_date = until_date.replace(hour=23, minute=59, second=59)
    
    load_dotenv()

    SSH_HOST = env_variable("SSH_HOST")
    SSH_USER = env_variable("SSH_USER")
    SSH_PRIVATE_KEY = env_variable("SSH_PRIVATE_KEY")
    SSH_PASSPHRASE = env_variable("SSH_PASSPHRASE")
    MONGO_CONNECTION_STRING = env_variable("MONGO_CONNECTION_STRING")
    MONGO_DATABASE = env_variable("MONGO_DATABASE")
    MONGO_COLLECTION = env_variable("MONGO_COLLECTION")

    SSH_COMMAND = [
        "ssh",
        "-f", "-N",
        "-o", "TCPKeepAlive=yes",
        "-o", "ServerAliveInterval=60",
        "-L", f"27018:localhost:27018",
        "-i", SSH_PRIVATE_KEY,
        f"{SSH_USER}@{SSH_HOST}"
    ]
    
    #sudo ssh -f -N -o TCPKeepAlive=yes -o ServerAliveInterval=60 -L 27018:localhost:27018 -i ssh_rsa_api_03_04_2024.key root@159.89.254.129

    since_date = datetime.datetime.strptime(since_date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    until_date = datetime.datetime.strptime(until_date_str + " 23:59:59", "%Y-%m-%d %H:%M:%S")

    QUERY = {
        "createdAt": {
            "$gte": since_date,
            "$lte": until_date
        }
    }

    try:
        ssh_process = establish_ssh_tunnel(SSH_COMMAND, SSH_PASSPHRASE)
        client = connect_to_mongodb(MONGO_CONNECTION_STRING)
        data = query_mongodb(client, MONGO_DATABASE, MONGO_COLLECTION, QUERY)
        print(f"Found {len(data)} posts.")
        data = organize_data(data, args.social_network)
        save_to_csv(data, args.social_network)
    except errors.ServerSelectionTimeoutError as err:
        print(f"Erro de seleção de servidor ao conectar ao MongoDB: {err}")
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        print("Fechando o túnel SSH.")
        ssh_process.terminate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download data from MongoDB and save it to a csv file.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    
    parser.add_argument(
        "social_network",
        type=SocialNetwork,
        choices=SocialNetwork,
        help="""Social network to convert file\n\n
        Currently supported: twitter, tiktok, instagram, facebook""",
    )
    
    parser.add_argument(
        "--since",
        type=str,
        help="Start date in the format YYYY-MM-DD",
        required=True
    )

    parser.add_argument(
        "--until",
        type=str,
        help="End date in the format YYYY-MM-DD",
        required=True
    )
    
    args = parser.parse_args()
    main(args.since, args.until)