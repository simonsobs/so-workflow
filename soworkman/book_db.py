"""This script setups a database for observation books and files"""

import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()
Session = sessionmaker()


class Observations(Base):
    """
    Attributes
    ----------
    obs_id: observation id
    """
    __tablename__ = 'observations'
    obs_id = db.Column(db.String, primary_key=True)
    bid = db.Column(db.String, db.ForeignKey("books.bid"))
    book = relationship("Books", back_populates='obs')


class Books(Base):
    """
    Attributes
    ----------
    bid: book id
    observations: list of observations within the book
    status: integer, stage of processing, 0 is unprocessed,
        1 is being processed, 2 is completed,
    """
    __tablename__ = 'books'
    bid = db.Column(db.String, primary_key=True)
    # one to many
    obs = relationship("Observations", back_populates='book')
    status = db.Column(db.Integer)


class BookDB:
    def __init__(self, db_path, echo=False):
        self.engine = db.create_engine(f"sqlite:///{db_path}", echo=echo)
        Session.configure(bind=self.engine)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def add_book(self, session, bid, obs_list, commit=True):
        """add book to database"""
        observations = [Observations(obs_id=obs_id) for obs_id in obs_list]
        book = Books(bid=bid, obs=observations, status=0)
        session.add(book)
        if commit: session.commit()

    def add_books(self, session, bids, obs_lists, commit=True):
        """add multiple books to database.

        Parameters
        ----------
        session: BookDB session
        bid: a list of book ids
        obs_lists: a list of list where the smaller list consists of
            different observations and the outer loop goes through
            different books
        """
        for bid, obs_list in zip(bids, obs_lists):
            self.add_book(session, bid, obs_list, commit=False)
        if commit: session.commit()
