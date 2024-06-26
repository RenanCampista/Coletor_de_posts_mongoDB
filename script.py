import pandas as pd
import json
import sys
import argparse
from enum import Enum


class SocialNetwork(Enum):
    """Enum class to represent the social network to be converted."""

    TWITTER = "twitter"
    TIKTOK = "tiktok"
    INSTAGRAM = "instagram"
    FACEBOOK = "facebook"

    def __str__(self):
        return self.value
    
    def get_data(self, new_row_base, history) -> dict:
        if self == self.__class__.TWITTER:
            body = history['body']
            metadata = history.get('metadata', {})
            new_row = new_row_base.copy()
            new_row.update({
                'postUrl': body.get('postUrl', ''),
                'authorName': body.get('authorName', ''),
                'authorNickName': body.get('authorNickName', ''),
                'authorUrl': body.get('authorUrl', ''),
                'authorId': body.get('authorId', ''),
                'timestamp': body.get('timestamp', {}).get('$date', ''),
                'text': body.get('text', ''),
                'authorImage': body.get('authorImage', ''),
                'locationName': body.get('locationName', ''),
                'authorBio': body.get('authorBio', ''),
                'authorFriendsCount': body.get('authorFriendsCount', 0),
                'statuses': body.get('statuses', 0),
                'statusId': body.get('statusId', ''),
                'source': body.get('source', ''),
                'replyToUserId': body.get('replyToUserId', ''),
                'replyToStatusId': body.get('replyToStatusId', ''),
                'authorFollowers': body.get('authorFollowers', 0),
                'authorFollowing': body.get('authorFollowing', 0),
                'commentCount': metadata.get('stats', {}).get('comment', 0),
                'likeCount': metadata.get('stats', {}).get('like', 0),
                'seenCount': metadata.get('stats', {}).get('seen', 0),
                'shareCount': metadata.get('stats', {}).get('share', 0),
            })
            return new_row
        if self == self.__class__.TIKTOK:
            body = history['body']
            metadata = history.get('metadata', {})
            new_row = new_row_base.copy()
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
                'authorBio': body.get('authorBio', ''),
                'authorVerified': body.get('authorVerified', False),
                'commentUrl': body.get('commentUrl', ''),
                'commentCount': metadata.get('stats', {}).get('comment', 0),
                'likeCount': metadata.get('stats', {}).get('like', 0),
                'seenCount': metadata.get('stats', {}).get('seen', 0),
                'shareCount': metadata.get('stats', {}).get('share', 0),
            })
            return new_row
        if self == self.__class__.INSTAGRAM:
            body = history['body']
            metadata = history.get('metadata', {})
            new_row = new_row_base.copy()
            new_row.update({
                'postUrl': body.get('postUrl', ''),
                'authorName': body.get('authorName', ''),
                'authorNickName': body.get('authorNickName', ''),
                'authorUrl': body.get('authorUrl', ''),
                'authorId': body.get('authorId', ''),
                'timestamp': body.get('timestamp', {}).get('$date', ''),
                'text': body.get('text', ''),
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
            body = history['body']
            metadata = history.get('metadata', {})
            new_row = new_row_base.copy()
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


def read_json_file(file_path: str) -> dict:
    """Reads a JSON file and returns its content as a dictionary."""
    
    with open(file_path, 'r', encoding='utf-8') as json_file:
        return json.load(json_file)


def organize_data(posts: dict, social_network: SocialNetwork):
    """Organize data to be saved in a csv file"""
    
    data = []
    for post in posts:
        new_row_base = {
            'postId': post['postId'],
        }
        history = post['postHistory'][-1]
        new_row = social_network.get_data(new_row_base, history)
        data.append(new_row)
        
    return data


def save_to_csv(data: list, SocialNetwork: SocialNetwork):
    """Write data to a csv file."""

    df = pd.DataFrame(data)
    df.to_csv(f'{SocialNetwork.value}.csv', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Converts JSON extraction data to csv format.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    
    parser.add_argument(
        "path",
        type=str,
        help="Path to the JSON file to be converted.",
    )
    
    parser.add_argument(
        "social_network",
        type=SocialNetwork,
        choices=SocialNetwork,
        help="""Social network to convert file\n\n
        Currently supported: twitter, tiktok, instagram, facebook""",
    )
    
    
    args = parser.parse_args()
    posts = read_json_file(args.path)
    data = organize_data(posts, args.social_network)
    save_to_csv(data, args.social_network)
    