from sqlalchemy import Column, BigInteger, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, backref
from geoalchemy2 import Geography


Base = declarative_base()


class Chapter(Base):
    """Entity representing youthmapper chapters, their location in the world, and members of their chapter. """
    __tablename__ = 'chapter'
    name = Column(String)
    institution = Column(String, primary_key=True)
    city = Column(String)
    country = Column(String)
    rank = Column(String)
    location = Column(Geography(geometry_type='POINT', srid=4326))
    ## one - to - many relationship for members of a Youthmapper chapter
    members = relationship("User")


class User(Base):
    """Entity representing any account that has contributed to OSM data related to Youthmapper Africa Campaigns."""
    __tablename__ = 'user'
    name = Column(String, primary_key=True)
    uid = Column(BigInteger, default=-1)
    is_youthmapper = Column(Boolean, default=False)
    institution = Column(String, ForeignKey('chapter.institution'))

    affiliation = relationship("Chapter", back_populates="members")



class Changeset(Base):
    __tablename__ = 'changeset'

    id = Column(BigInteger, primary_key=True)
    author = Column(String, ForeignKey('user.name'))
    timestamp = Column(DateTime)
    bbox = Column(Geography(geometry_type='POLYGON', srid=4326))

    # elements = relationship('Element', backref=backref('changeset', lazy = 'dynamic'))
    # creator = relationship('User', backref=backref('changesets', lazy='dynamic'))


class Element(Base):
    __tablename__ = 'element'
    osm_id = Column(BigInteger, primary_key=True)
    timestamp = Column(DateTime)
    changeset_id = Column(BigInteger, ForeignKey('changeset.id'))
    type = Column(String)
    author_name = Column(String, ForeignKey('user.name'))
    author = relationship("User", backref='mappings')


    __mapper_args__ = {
        'polymorphic_identity': 'element',
        'polymorphic_on': type
    }


class Node(Element):
    __tablename__ = 'node'
    osm_id = Column(BigInteger, ForeignKey('element.osm_id'), primary_key=True)
    location = Column(Geography(geometry_type='POINT', srid=4326))

    __mapper_args__ = {
        'polymorphic_identity':'node',
    }


class Way(Element):
    __tablename__ = 'way'
    osm_id = Column(BigInteger, ForeignKey('element.osm_id'), primary_key=True)

    # nodes = association_proxy('way_X_node', 'node')

    __mapper_args__ = {
        'polymorphic_identity': 'way',
    }


class Version(Base):
    __tablename__ = 'version'

    version_num = Column(Integer, primary_key=True)
    element_id = Column(BigInteger, ForeignKey('element.osm_id'), primary_key=True)
    is_current = Column(Boolean, default=False)

    element = relationship('Element', backref=backref('versions', lazy='dynamic'))


class WayXNode(Base):
    __tablename__ = 'way_X_node'

    node_id = Column(BigInteger, ForeignKey('node.osm_id'), primary_key=True)
    way_id = Column(BigInteger, ForeignKey('way.osm_id'), primary_key=True)
    way_changeset = Column(BigInteger,ForeignKey('changeset.id'), primary_key=True)
    position = Column(Integer, primary_key=True)




class ElementTag(Base):
    __tablename__ = 'element_tags'

    key = Column(String, primary_key=True)
    value = Column(String, primary_key=True)
    osm_id = Column(BigInteger, ForeignKey('element.osm_id'), primary_key=True)

class ChangesetTag(Base):
    __tablename__ = 'changeset_tags'

    key = Column(String, primary_key=True)
    value = Column(String, primary_key=True)
    changeset_id = Column(BigInteger, ForeignKey('changeset.id'), primary_key=True)

    # changeset = relationship("Changeset", backref('tags', lazy='dynamic'))











