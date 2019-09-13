from bs4 import BeautifulSoup
import logging
import requests
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

###############################################################################
# SETTINGS

URL_KDNUGGETS = 'https://www.kdnuggets.com'
URL_MEDIUM = 'https://towardsdatascience.com'
METADATA_PATH = 'articles.db'
DATATAU_API_URL = 'https://datatau.net/api/post'
DATATAU_API_KEY = '7h-o33hDiZ8b90p_pGt9'

###############################################################################
# LOGGING


def get_logger():
    logger = logging.getLogger('populator_logger')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


logger = get_logger()

###############################################################################
# CLASSES

Base = declarative_base()


class Article(Base):
    __tablename__ = 'articles'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    url = Column(String)

    def __repr__(self):
        return f"<Article(title={self.title}, url={self.url})>"


###############################################################################
# FUNCTIONS

def get_posts_kdnuggets():

    logger.info("getting top posts from KDnuggets...")
    r = requests.get(URL_KDNUGGETS)
    if r.status_code != 200:
        logger.error("error fetching KDnuggets posts...")
        raise ConnectionError("error retrieving KDnuggets html...")

    soup = BeautifulSoup(r.content, 'html.parser')
    articles_raw = soup.find('ol', class_='three_ol').find_all('li')
    articles = [Article(title=article.b.text, url=URL_KDNUGGETS + article.a['href']) for article in articles_raw]

    return articles


###############################################################################
# MAIN SCRIPT

if __name__ == '__main__':

    logger.info("starting DataTau 'populator' bot")

    # connect to database
    logger.info("connecting to metadata database...")
    engine = create_engine('sqlite:///' + METADATA_PATH)
    Session = sessionmaker(bind=engine)

    Base.metadata.create_all(engine)  # will not create already existent tables
    session = Session()

    # retrieve posts
    articles = get_posts_kdnuggets()

    # go through articles, checking if already posted against metadata, if not posted, post and break

    for article in articles:

        published = session.query(Article).filter_by(url=article.url).scalar()

        if not published:
            logger.info(f"article '{article.title}' is not in DataTau!, posting...")
            r = requests.post(DATATAU_API_URL,
                              json={'api_key': DATATAU_API_KEY,
                                    'title': article.title,
                                    'url': article.url})

            if r.status_code != 200:
                logger.error("error posting to DataTau using API...")
                raise ConnectionError('unable to post to DataTau...')

            logger.info("adding article to metadata database to not post it again...")
            session.add(article)
            session.commit()

            logger.info("only one article per day, exiting loop!")
            break

        else:
            logger.info(f"article '{article.title}' already published... moving to the next one!")
            continue

    session.close_all()
    logger.info("finished process...")
