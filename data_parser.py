import pydgraph
import pandas as pd
import json
import logging
from typing import Dict


class DGraphCSVLoader:
    def __init__(self, client):
        """Initialize with a pydgraph client"""
        self.client = client
        self.logger = logging.getLogger(__name__)
        
    def set_schema(self):
        """Set up the schema for the social network"""
        schema = """
            # Type definitions
            type User {
                username: string @index(exact) .
                email: string @index(exact) .
                bio: string @index(fulltext) .
                joinDate: datetime .
                isAdmin: bool .
                influenceScore: float @index(float) .
                location: geo @index(geo) .
                follows: [uid] @reverse .
                posts: [uid] .
                comments: [uid] .
                communities: [uid] .
            }

            type Post {
                content: string @index(fulltext) .
                timestamp: datetime .
                viewCount: int @index(int) .
                engagementScore: float @index(float) .
                author: uid .
                likedBy: [uid] .
                hashtags: [uid] .
                comments: [uid] .
            }

            type Comment {
                content: string @index(fulltext) .
                timestamp: datetime .
                sentimentScore: float @index(float) .
                replyCount: int .
                author: uid .
                post: uid .
            }

            type Community {
                name: string @index(exact) .
                category: string @index(exact) .
                memberCount: int @index(int) .
                members: [uid] @reverse .
            }

            type Hashtag {
                name: string @index(exact) .
                useCount: int @index(int) .
                trendScore: float @index(float) .
                posts: [uid] .
            }

            # Predicate definitions
            username: string @index(exact) .
            email: string @index(exact) .
            bio: string @index(fulltext) .
            joinDate: datetime .
            isAdmin: bool .
            influenceScore: float @index(float) .
            location: geo @index(geo) .
            content: string @index(fulltext) .
            timestamp: datetime .
            viewCount: int @index(int) .
            engagementScore: float @index(float) .
            sentimentScore: float @index(float) .
            replyCount: int .
            name: string @index(exact) .
            category: string @index(exact) .
            memberCount: int @index(int) .
            useCount: int @index(int) .
            trendScore: float @index(float) .
        """
        return self.client.alter(pydgraph.Operation(schema=schema))
    
    def load_data(self, data_dir: str):
        """Load all CSV files from the directory"""
        try:
            # Process base nodes first
            users = self._load_users(f"{data_dir}/users.csv")
            communities = self._load_communities(f"{data_dir}/communities.csv")
            posts = self._load_posts(f"{data_dir}/posts.csv")
            hashtags = self._load_hashtags(f"{data_dir}/hashtags.csv")
            comments = self._load_comments(f"{data_dir}/comments.csv")
            
            # Process relationships
            self._load_follows(f"{data_dir}/user_follows.csv")
            self._load_community_members(f"{data_dir}/community_members.csv")
            self._load_post_hashtags(f"{data_dir}/post_hashtags.csv")
            self._load_post_likes(f"{data_dir}/post_likes.csv")
            
            print("Data loaded successfully")
            print(f"Users: {len(users)}")
            print(f"Communities: {len(communities)}")
            print(f"Posts: {len(posts)}")
            print(f"Hashtags: {len(hashtags)}")
            print(f"Comments: {len(comments)}")
            print("Relationships loaded")
            
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise
        
    
    def _create_mutation(self, data: Dict) -> Dict:
        """Create a mutation object for pydgraph"""
        txn = self.client.txn()
        try:
            mutation = json.dumps(data)
            assigned = txn.mutate(set_obj=data)
            txn.commit()
            return assigned
        finally:
            txn.discard()
            
            
    def _load_users(self, file_path: str) -> Dict[str, str]:
        """Load users from CSV and return uid mapping"""
        df = pd.read_csv(file_path)
        uid_map = {}
        
        for _, row in df.iterrows():
            # Create user node
            user_data = {
                "dgraph.type": "User",
                "username": row['username'],
                "email": row['email'],
                "bio": row['bio'],
                "joinDate": row['join_date'],
                "isAdmin": row['is_admin'].lower() == 'true',
                "influenceScore": float(row['influence_score'])
            }
            
            # Handle location if present
            if pd.notna(row['location']):
                lat, lon = map(float, row['location'].split(','))
                user_data["location"] = json.dumps({
                    "type": "Point",
                    "coordinates": [lon, lat]
                })
            
            user_uid = row['user_id']
            user_data["uid"] = json.dumps({
                "user_id": user_uid
            })
            
            # Create mutation and store mapping
            assigned = self._create_mutation(user_data)
            uid_map[row['user_id']] = assigned.uids[list(assigned.uids.keys())[0]]
            
        return uid_map
    
    def _load_communities(self, file_path: str) -> Dict[str, str]:
        """Load communities from CSV"""
        df = pd.read_csv(file_path)
        uid_map = {}
        
        for _, row in df.iterrows():
            community_data = {
                "dgraph.type": "Community",
                "name": row['name'],
                "category": row['category'],
                "memberCount": int(row['member_count']),
                "community_id": row['community_id']
            }
            
            assigned = self._create_mutation(community_data)
            uid_map[row['community_id']] = assigned.uids[list(assigned.uids.keys())[0]]
            
        return uid_map

    def _load_posts(self, file_path: str) -> Dict[str, str]:
        """Load posts from CSV"""
        df = pd.read_csv(file_path)
        uid_map = {}
        
        for _, row in df.iterrows():
            post_data = {
                "dgraph.type": "Post",
                "post_id": row['post_id'],
                "content": row['content'],
                "timestamp": row['timestamp'],
                "viewCount": int(row['view_count']),
                "engagementScore": float(row['engagement_score']),
                "author": row['author_id']
            }
            
            assigned = self._create_mutation(post_data)
            uid_map[row['post_id']] = assigned.uids[list(assigned.uids.keys())[0]]
            
        return uid_map
    
    def _load_hashtags(self, file_path: str) -> Dict[str, str]:
        """Load hashtags from CSV"""
        df = pd.read_csv(file_path)
        uid_map = {}
        
        for _, row in df.iterrows():
            hashtag_data = {
                "dgraph.type": "Hashtag",
                "hashtag_id": row['hashtag_id'],
                "name": row['name'],
                "useCount": int(row['use_count']),
                "trendScore": float(row['trend_score'])
            }
            
            assigned = self._create_mutation(hashtag_data)
            uid_map[row['hashtag_id']] = assigned.uids[list(assigned.uids.keys())[0]]
            
        return uid_map
    
    
    def _load_comments(self, file_path: str) -> Dict[str, str]:
        """Load comments from CSV"""
        df = pd.read_csv(file_path)
        uid_map = {}
        
        for _, row in df.iterrows():
            comment_data = {
                "dgraph.type": "Comment",
                "comment_id": row['comment_id'],
                "content": row['content'],
                "timestamp": row['timestamp'],
                "sentimentScore": float(row['sentiment_score']),
                "replyCount": int(row['reply_count']),
                "author": row['author_id'],
                "post": row['post_id']
            }
            
            assigned = self._create_mutation(comment_data)
            uid_map[row['comment_id']] = assigned.uids[list(assigned.uids.keys())[0]]
            
        return uid_map
    
    
    def _load_follows(self, file_path: str):
        """Load user follows relationships"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            follow_data = {
                "follower_id": row['follower_id'],
                "followed_id": row['followed_id']
            }
            
            assigned = self._create_mutation(follow_data)
            
        return assigned
        
    
    def _load_community_members(self, file_path: str):
        """Load community members relationships"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            member_data = {
                "community_id": row['community_id'],
                "user_id": row['user_id']
            }
            
            assigned = self._create_mutation(member_data)
            
        return assigned
        
    def _load_post_hashtags(self, file_path: str):
        """Load post hashtags relationships"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            post_hashtag_data = {
                "post_id": row['post_id'],
                "hashtag_id": row['hashtag_id']
            }
            
            assigned = self._create_mutation(post_hashtag_data)
            
        return assigned
        
    
    def _load_post_likes(self, file_path: str):
        """Load post likes relationships"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            post_like_data = {
                "post_id": row['post_id'],
                "user_id": row['user_id']
            }
            
            assigned = self._create_mutation(post_like_data)
            
        return assigned
    
    
    def delete_all_data(self):
        """Drop all data from the graph"""
        return self.client.alter(pydgraph.Operation(drop_all=True))
    
    