import requests
import json
import mysql.connector
from mysql.connector import pooling
import time
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import jieba
import jieba.posseg as pseg
from urllib.parse import urlparse
import oss2
import signal

# 加载环境变量
load_dotenv()

# 配置项
APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
OSS_ENDPOINT = os.getenv("OSS_ENDPOINT")
OSS_ACCESS_KEY = os.getenv("OSS_ACCESS_KEY")
OSS_SECRET_KEY = os.getenv("OSS_SECRET_KEY")
OSS_BUCKET = os.getenv("OSS_BUCKET")
TARGET_REGION = "天津 河东"
MAX_RETRIES = 3
RETRY_DELAY = 5
HEARTBEAT_INTERVAL = 3600

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/social_monitoring.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 全局资源初始化
class GracefulKiller:
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True

# 数据库连接池
try:
    db_pool = pooling.MySQLConnectionPool(
        pool_name="weibo_pool",
        pool_size=5,
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        connect_timeout=30
    )
    logger.info("数据库连接池初始化成功")
except mysql.connector.Error as e:
    logger.error(f"数据库连接池初始化失败: {e}")
    raise

# OSS客户端
try:
    auth = oss2.Auth(OSS_ACCESS_KEY, OSS_SECRET_KEY)
    bucket = oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)
    logger.info("OSS客户端初始化成功")
except Exception as e:
    logger.error(f"OSS客户端初始化失败: {e}")
    raise

# OAuth 2.0 授权管理
class WeiboOAuth:
    def __init__(self, app_key, app_secret, redirect_uri):
        self.app_key = app_key
        self.app_secret = app_secret
        self.redirect_uri = redirect_uri
        self.access_token = None
        self.refresh_token = None
        self.expires_at = 0

    def get_authorization_url(self):
        """生成授权URL"""
        return f"https://api.weibo.com/oauth2/authorize?client_id={self.app_key}&redirect_uri={self.redirect_uri}&response_type=code"

    def get_access_token(self, code):
        """使用授权码换取 access_token"""
        token_url = "https://api.weibo.com/oauth2/access_token"
        data = {
            "client_id": self.app_key,
            "client_secret": self.app_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri
        }
        try:
            response = requests.post(token_url, data=data, timeout=10)
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data["access_token"]
            self.refresh_token = token_data.get("refresh_token")
            self.expires_at = time.time() + token_data["expires_in"]
            logger.info("成功获取 access_token")
            return self.access_token
        except Exception as e:
            logger.error(f"获取 access_token 失败: {e}")
            raise

    def refresh_access_token(self):
        """刷新 access_token"""
        if not self.refresh_token:
            raise ValueError("没有 refresh_token，无法刷新")
        refresh_url = "https://api.weibo.com/oauth2/access_token"
        data = {
            "client_id": self.app_key,
            "client_secret": self.app_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token
        }
        try:
            response = requests.post(refresh_url, data=data, timeout=10)
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data["access_token"]
            self.refresh_token = token_data.get("refresh_token")
            self.expires_at = time.time() + token_data["expires_in"]
            logger.info("成功刷新 access_token")
            return self.access_token
        except Exception as e:
            logger.error(f"刷新 access_token 失败: {e}")
            raise

    def get_token(self):
        """获取有效的 access_token"""
        if not self.access_token or time.time() >= self.expires_at - 60:
            self.refresh_access_token()
        return self.access_token

# 核心函数
def get_db_connection():
    for _ in range(3):
        try:
            return db_pool.get_connection()
        except mysql.connector.Error as e:
            logger.error(f"获取数据库连接失败: {e}")
            time.sleep(5)
    return None

def fetch_weibo_data(oauth):
    """获取微博数据（使用动态 access_token）"""
    url = "https://api.weibo.com/2/statuses/public_timeline.json"
    access_token = oauth.get_token()
    params = {"access_token": access_token, "count": 100}
    
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 403:
            logger.warning("API访问被限制，请检查访问频率")
            return []
        return json.loads(response.text).get("statuses", [])
    except Exception as e:
        logger.error(f"API请求异常: {e}")
        return []

def upload_to_oss(file_url, prefix):
    if not file_url:
        return None
    
    try:
        filename = os.path.basename(urlparse(file_url).path)
        oss_path = f"{prefix}/{int(time.time())}_{filename}"
        
        with requests.get(file_url, stream=True, timeout=15) as r:
            r.raise_for_status()
            result = bucket.put_object(oss_path, r.raw)
            if result.status == 200:
                return f"https://{OSS_BUCKET}.{OSS_ENDPOINT}/{oss_path}"
        return None
    except Exception as e:
        logger.error(f"OSS上传失败: {e}")
        return None

def parse_geo(geo):
    if geo and geo.get("type") == "Point":
        coordinates = geo.get("coordinates", [])
        if len(coordinates) == 2:
            return coordinates[0], coordinates[1]
    return None, None

def extract_keywords(text):
    words = pseg.cut(text)
    keywords = [w.word for w in words if w.flag in ["ns", "n"] and ("天津" in w.word or "河东" in w.word)]
    return json.dumps(keywords, ensure_ascii=False) if keywords else None

def filter_by_region(post):
    user_location = post["user"].get("location", "")
    text = post["text"]
    geo_lat, geo_lon = parse_geo(post.get("geo"))
    
    if TARGET_REGION in user_location:
        return True
    if extract_keywords(text):
        return True
    if geo_lat and geo_lon:
        if 39.08 <= geo_lat <= 39.15 and 117.20 <= geo_lon <= 117.30:
            return True
    return False

def bulk_insert(conn, posts):
    if not posts:
        return
    cursor = conn.cursor()
    insert_data = []
    for post in posts:
        if filter_by_region(post):
            lat, lon = parse_geo(post.get("geo"))
            keywords = extract_keywords(post["text"])
            image_urls = post.get("pic_urls", [])
            image_url = None
            if image_urls:
                image_url = upload_to_oss(
                    image_urls[0].get("thumbnail_pic").replace("thumbnail", "large"),
                    "images"
                )
            video_url = post.get("video_url")
            video_oss_url = upload_to_oss(video_url, "videos") if video_url else None
            
            insert_data.append((
                int(post["idstr"]),
                post["text"],
                datetime.strptime(post["created_at"], "%a %b %d %H:%M:%S %z %Y"),
                lat,
                lon,
                keywords,
                image_url,
                video_oss_url
            ))
    if insert_data:
        sql = """
        INSERT IGNORE INTO weibo_data (post_id, text, created_at, latitude, longitude, keywords, image_url, video_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.executemany(sql, insert_data)
            conn.commit()
            logger.info(f"插入 {len(insert_data)} 条数据")
        except mysql.connector.Error as e:
            logger.error(f"数据库插入错误: {e}")
            conn.rollback()

def heartbeat():
    logger.info(f"[Heartbeat] System is alive at {datetime.now().isoformat()}")

# 主程序
def main():
    killer = GracefulKiller()
    last_heartbeat = time.time()
    
    # 初始化OAuth
    oauth = WeiboOAuth(APP_KEY, APP_SECRET, REDIRECT_URI)
    
    # 获取授权码（手动步骤）
    print(f"请访问以下URL进行授权：{oauth.get_authorization_url()}")
    code = input("请输入授权后获取的code：")
    oauth.get_access_token(code)
    
    while not killer.kill_now:
        try:
            if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                heartbeat()
                last_heartbeat = time.time()
            
            conn = get_db_connection()
            if not conn:
                time.sleep(30)
                continue
                
            posts = fetch_weibo_data(oauth)
            if not posts:
                time.sleep(300)
                continue
                
            bulk_insert(conn, posts)
            
        except Exception as e:
            logger.error(f"主循环异常: {e}", exc_info=True)
            time.sleep(60)
        finally:
            if 'conn' in locals() and conn:
                conn.close()
                
        time.sleep(600)
    
    logger.info("收到终止信号，优雅退出")

if __name__ == "__main__":
    main()