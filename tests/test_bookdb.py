from soworkman.book_db import BookDB, Observations, Books
import os, os.path as op
import pytest

@pytest.fixture
def db_session():
    # remove existing database if needed
    if os.path.exists("test.db"): os.system("rm test.db")
    # create new db
    db = BookDB("test.db")
    session = db.Session()
    return (db, session)

def test_add_book(db_session):
    db, session = db_session
    db.add_book(session, "bid1", ["obs1", "obs2"], commit=True)
    assert len(session.query(Observations).all()) == 2
    assert len(session.query(Books).all()) == 1
    session.commit()

def test_add_books(db_session):
    db, session = db_session
    db.add_books(session, ["bid2", "bid3"], [["obs3", "obs4"], ["obs5", "obs6"]])
    assert len(session.query(Books).all()) == 2
    assert len(session.query(Observations).all()) == 4
