import pydgraph
import pandas as pd
import json
import logging
from typing import Dict

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class DGraphCSVLoader:
    def __init__(self, client):
        """Initialize with a pydgraph client"""
        self.client = client
        self.logger = logging.getLogger(__name__)
        self.user_uid_map = {}
        self.community_uid_map = {}
        self.post_uid_map = {}
        self.hashtag_uid_map = {}
        self.comments_map = {}
        
    def set_schema(self):
        """Set up the schema for the social network"""
        schema = """
            # Type definitions
            type User {
                username
                email
                bio
                joinDate
                isAdmin
                influenceScore
                location
                follows: [uid]
                posts: [uid]
                comments: [uid]
                communities: [uid]
            }

            type Post {
                content
                timestamp
                viewCount
                engagementScore
                author: uid
                likedBy: [uid] 
                hashtags: [uid] 
                comments: [uid] 
            }

            type Comment {
                content
                timestamp
                sentimentScore
                replyCount
                author: uid 
                post: uid 
            }

            type Community {
                name
                category
                memberCount
                members
            }

            type Hashtag {
                name
                useCount
                trendScore
                posts: [uid] 
            }
            
            type Follows {
                follower: uid 
                followed: uid 
            }
            
            type CommunityMember {
                community: uid 
                member: uid 
            }
            
            type PostHashtag {
                post: uid 
                hashtag: uid 
            }
            
            type PostLike {
                post: uid 
                user: uid 
            }
            

            # Predicate definitions
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
            content: string @index(fulltext) .
            timestamp: datetime .
            viewCount: int @index(int) .
            engagementScore: float @index(float) .
            author: uid .
            likedBy: [uid] .
            hashtags: [uid] .
            sentimentScore: float @index(float) .
            replyCount: int .
            post: uid .
            name: string @index(hash) .
            category: string @index(exact) .
            memberCount: int @index(int) .
            members: [uid] @reverse .
            useCount: int @index(int) .
            trendScore: float @index(float) .
            follower: uid .
            followed: uid .
            community: uid .
            member: uid .
            hashtag: uid .
            user: uid .
        """
        return self.client.alter(pydgraph.Operation(schema=schema))
    
    def load_data(self, data_dir: str):
        """Load all CSV files from the directory"""
        try:
                # Process base nodes first
            self.user_uid_map = self._load_users(f"{data_dir}/user.csv")
            self.logger.info(f"Loaded {len(self.user_uid_map)} users")
            
            self.community_uid_map = self._load_communities(f"{data_dir}/communities.csv")
            self.logger.info(f"Loaded {len(self.community_uid_map)} communities")
            
            self.post_uid_map = self._load_posts(f"{data_dir}/post.csv", self.user_uid_map)
            self.logger.info(f"Loaded {len(self.post_uid_map)} posts")
            
            self.hashtag_uid_map = self._load_hashtags(f"{data_dir}/hashtags.csv")
            self.logger.info(f"Loaded {len(self.hashtag_uid_map)} hashtags")
            
            self.comments_map = self._load_comments(f"{data_dir}/comments.csv", self.user_uid_map, self.post_uid_map)
            self.logger.info(f"Loaded {len(self.comments_map)} comments")
            
            # Process relationships
            self._load_follows(f"{data_dir}/user_follows.csv")
            self._load_community_members(f"{data_dir}/community_members.csv")
            self._load_post_hashtags(f"{data_dir}/post_hashtags.csv")
            self._load_post_likes(f"{data_dir}/post_likes.csv")
            
            print("Data loaded successfully")
            print(f"Users: {len(self.user_uid_map)}")
            print(f"Communities: {len(self.community_uid_map)}")
            print(f"Posts: {len(self.post_uid_map)}")
            print(f"Hashtags: {len(self.hashtag_uid_map)}")
            print(f"Comments: {len(self.comments_map)}")
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
        
        self.logger.info(f"Loading {len(df)} users from {file_path}")
        
        for _, row in df.iterrows():
            try:
                # Create user node
                user_data = {
                    "dgraph.type": "User",
                    "username": row['username'],
                    "email": row['email'],
                    "bio": row['bio'],
                    "joinDate": row['join_date'],
                    "isAdmin": bool(row['is_admin']),  # Ensure boolean type
                    "influenceScore": float(row['influence_score'])
                    # Removed user_id as it shouldn't be stored in dgraph
                }
                
                # Handle location if present
                if pd.notna(row['location']):
                    try:
                        lat, lon = map(float, row['location'].split(','))
                        user_data["location"] = {
                            "type": "Point",
                            "coordinates": [lon, lat]
                        }
                    except ValueError as e:
                        self.logger.warning(f"Invalid location format for user {row['user_id']}: {e}")
                
                # Create mutation and store mapping
                assigned = self._create_mutation(user_data)
                
                # Get the assigned UID
                if assigned.uids:
                    uid = assigned.uids[list(assigned.uids.keys())[0]]
                    uid_map[row['user_id']] = uid
                    self.logger.debug(f"Created user {row['username']} (ID: {row['user_id']}) with UID: {uid}")
                else:
                    self.logger.error(f"No UID assigned for user {row['user_id']}")
                    continue
                    
            except Exception as e:
                self.logger.error(f"Error creating user {row['user_id']}: {str(e)}")
                continue
        
        self.logger.info(f"Successfully loaded {len(uid_map)} users")
        if len(uid_map) < len(df):
            self.logger.warning(f"Failed to load {len(df) - len(uid_map)} users")
        
        # Debug output of the UID mapping
        self.logger.debug("User ID to UID mapping:")
        for user_id, uid in uid_map.items():
            self.logger.debug(f"{user_id} -> {uid}")
        
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

    def _load_posts(self, file_path: str, user_uid_map: Dict[str, str]) -> Dict[str, str]:
        """Load posts from CSV"""
        df = pd.read_csv(file_path)
        uid_map = {}
        
        for _, row in df.iterrows():
            # Check if author exists in user_uid_map
            author_id = row['author_id']
            if author_id not in user_uid_map:
                self.logger.warning(f"Author ID {author_id} not found in user UID map. Skipping post.")
                continue
                
            # Create post node with proper author UID reference
            post_data = {
                "dgraph.type": "Post",
                "content": row['content'],
                "timestamp": row['timestamp'],
                "viewCount": int(row['view_count']),
                "engagementScore": float(row['engagement_score']),
                "author": {"uid": user_uid_map[author_id]}  # Proper UID reference format
            }
            
            # Create mutation and store mapping
            try:
                assigned = self._create_mutation(post_data)
                uid = assigned.uids[list(assigned.uids.keys())[0]]
                uid_map[row['post_id']] = uid
            except Exception as e:
                self.logger.error(f"Error creating post: {str(e)}")
                continue
        
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
    
    
    def _load_comments(self, file_path: str, user_uid_map: Dict[str, str], post_uid_map: Dict[str, str]) -> Dict[str, str]:
        """Load comments from CSV"""
        df = pd.read_csv(file_path)
        uid_map = {}
        
        self.logger.info(f"Loading {len(df)} comments from {file_path}")
        
        for _, row in df.iterrows():
            try:
                # Check if author exists in user_uid_map
                author_id = row['author_id']
                post_id = row['post_id']
                
                if author_id not in user_uid_map:
                    self.logger.warning(f"Author ID {author_id} not found in user UID map. Skipping comment.")
                    continue
                    
                if post_id not in post_uid_map:
                    self.logger.warning(f"Post ID {post_id} not found in post UID map. Skipping comment.")
                    continue
                
                # Create comment node with proper UID references
                comment_data = {
                    "dgraph.type": "Comment",
                    "content": row['content'],
                    "timestamp": row['timestamp'],
                    "sentimentScore": float(row['sentiment_score']),
                    "replyCount": int(row['reply_count']),
                    "author": {"uid": user_uid_map[author_id]},  # Proper UID reference format
                    "post": {"uid": post_uid_map[post_id]}      # Proper UID reference format
                }
                
                # Create mutation and store mapping
                assigned = self._create_mutation(comment_data)
                uid = assigned.uids[list(assigned.uids.keys())[0]]
                uid_map[row['comment_id']] = uid
                self.logger.debug(f"Created comment for post {post_id} by author {author_id} with UID: {uid}")
                
            except Exception as e:
                self.logger.error(f"Error creating comment: {str(e)}")
                continue
        
        self.logger.info(f"Successfully loaded {len(uid_map)} comments")
        if len(uid_map) < len(df):
            self.logger.warning(f"Failed to load {len(df) - len(uid_map)} comments")
            
        return uid_map
    
    
    def _load_follows(self, file_path: str):
        """Load user follows relationships"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            try:
                # Get UIDs for both users
                follower_uid = self.user_uid_map[row['follower_id']]
                followed_uid = self.user_uid_map[row['followed_id']]
                
                # Create the follows relationship using proper edge format
                follow_data = {
                    "uid": follower_uid,
                    "follows": {
                        "uid": followed_uid
                    }
                }
                
                self._create_mutation(follow_data)
                
            except KeyError as e:
                self.logger.error(f"Missing UID mapping for user: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error creating follows relationship: {str(e)}")
        
    
    def _load_community_members(self, file_path: str):
        """Load community members relationships"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            try:
                # Get UIDs for community and user
                community_uid = self.community_uid_map[row['community_id']]
                member_uid = self.user_uid_map[row['user_id']]
                
                # Create the membership relationship
                member_data = {
                    "uid": community_uid,
                    "members": {
                        "uid": member_uid
                    }
                }
                
                self._create_mutation(member_data)
                
            except KeyError as e:
                self.logger.error(f"Missing UID mapping: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error creating community membership: {str(e)}")
        
        
    def _load_post_hashtags(self, file_path: str):
        """Load post hashtags relationships"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            try:
                # Get UIDs for post and hashtag
                post_uid = self.post_uid_map[row['post_id']]
                hashtag_uid = self.hashtag_uid_map[row['hashtag_id']]
                
                # Create the hashtag relationship
                hashtag_data = {
                    "uid": post_uid,
                    "hashtags": {
                        "uid": hashtag_uid
                    }
                }
                
                self._create_mutation(hashtag_data)
                
            except KeyError as e:
                self.logger.error(f"Missing UID mapping: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error creating post-hashtag relationship: {str(e)}")
        
    
    def _load_post_likes(self, file_path: str):
        """Load post likes relationships"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            try:
                # Get UIDs for post and user
                post_uid = self.post_uid_map[row['post_id']]
                user_uid = self.user_uid_map[row['user_id']]
                
                # Create the like relationship
                like_data = {
                    "uid": post_uid,
                    "likedBy": {
                        "uid": user_uid
                    }
                }
                
                self._create_mutation(like_data)
                
            except KeyError as e:
                self.logger.error(f"Missing UID mapping: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error creating post-like relationship: {str(e)}")
    
    
    def delete_all_data(self):
        """Drop all data from the graph"""
        return self.client.alter(pydgraph.Operation(drop_all=True))
    
    