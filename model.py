import classifier
from core.model import SessionManager

# Add to Work
    # One Work may participate in many WorkGenre assignments.
    genres = association_proxy('work_genres', 'genre',
                               creator=WorkGenre.from_genre)

    work_genres = relationship("WorkGenre", backref="work",
                               cascade="all, delete-orphan")


class CirculationSessionManager(SessionManager):

    @classmethod
    def initialize_data(cls, session):
        SessionManager.initialize_data(session)
        # Create all genres.
        for g in classifier.genres.values():
            Genre.lookup(session, g, autocreate=True)

class Genre(Base):

    # TODO: this code has gone missing, pick it up from elsewhere

    # TODO: class methods of Work that need to be ported
