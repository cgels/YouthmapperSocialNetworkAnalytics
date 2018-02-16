from etl.models import *
import pickle as pkl
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from etl.db_helpers import make_point_geom, make_bbox_polygon
from sqlalchemy.exc import IntegrityError
import re
import gzip


### CHANGE CONNECTION STRING TO SUIT YOUR DATABASE

eng = create_engine("postgresql://postgres:postgres@localhost/geocenterdev")


metadata = MetaData()
metadata.create_all(eng)

Session = sessionmaker(eng)
session = Session()


def teardown_db():
    ChangesetTag.__table__.drop(eng)
    ElementTag.__table__.drop(eng)
    Version.__table__.drop(eng)
    WayXNode.__table__.drop(eng)
    Way.__table__.drop(eng)
    Node.__table__.drop(eng)
    Element.__table__.drop(eng)
    Changeset.__table__.drop(eng)
    User.__table__.drop(eng)
    Chapter.__table__.drop(eng)

def init_tables(reset = False):
    if reset:
        teardown_db()

    try:
        Chapter.__table__.create(eng)
        User.__table__.create(eng)
        Changeset.__table__.create(eng)
        Element.__table__.create(eng)
        Node.__table__.create(eng)
        Way.__table__.create(eng)
        WayXNode.__table__.create(eng)
        Version.__table__.create(eng)
        ElementTag.__table__.create(eng)
        ChangesetTag.__table__.create(eng)
    except Exception as e:
        print(e)

wtf = ""

def load_chapters():
    with open("./data/chapter_data.pkl", "rb+") as f:
        data = pkl.load(f)

    for d in data:
        typos = {'univeristy': 'university'}
        institution = re.sub( '\s+', ' ', d.get('Institution').lower().strip()).lower().strip()
        for t in typos:
            if t in institution:
                institution = institution.replace(t, typos[t])

        session.add(Chapter(institution = institution,
                            name=d.get('Chapter').lower().strip(),
                            city=d.get('City').lower().strip(),
                            country=d.get('Country').lower().strip(),
                            rank=d.get('Rank'),
                            location=make_point_geom(d.get('long'), d.get('lat')),
                            ))

    session.add(Chapter(institution = "youth mappers africa",
                            name="youth mappers africa",
                            city="unknown",
                            country="unknown",
                            rank="unknown",
                            location=make_point_geom(0.0, 1.0),
                            ))

    session.add(Chapter(institution = "unknown",
                            name="unaffiliated",
                            city="unknown",
                            country="unknown",
                            rank="unknown",
                            location=make_point_geom(1.0, 0.0),
                            ))
    session.commit()


def load_users():
    with open("./data/youthmappers.pkl", "rb+") as f:
        data = pkl.load(f)

    for d in data:
        typos = {"butisema university" : "busitema university",
                "jomo kenyatta university of agriculture and technology." : "jomo kenyatta university of agriculture and technology",
                 "kumi univeristy" : "kumi university",
                 "universidad distrital francisco josé de caldas" : "universidad distrital francisco jose de caldas",
                 "insititue d' enseignement superieur de ruhengeri" : "insititue d' enseignement superieur de ruhengeri"}
        inst = re.sub("\s+", ' ', d.get('Institution').lower().strip())

        if inst in typos.keys():
            inst = typos[inst].lower().strip()

        session.merge(User(name=d.get('user').lower().strip(),
                         is_youthmapper=True,
                         institution=inst))
        session.commit()


def load_changesets():
    with gzip.open("./data/changeset_data.pklz", "rb+") as f:
        data = pkl.load(f)

    print(len(data))
    uids = set()
    for d in data:
        uid = d.get('uid')
        usr = d.get('user')
        usr, uid, uids = process_author(uid, uids, usr)

        bbox_str = make_bbox_polygon(d.get('min_lat'),d.get('min_lon'), d.get('max_lat'), d.get('max_lon'))
        cs = Changeset(id = d.get('id'),
                  timestamp=d.get('created_at'),
                  author=usr,
                  bbox=bbox_str
                  )
        session.add(cs)
        session.commit()


def process_author(uid, uids, usr):
    if usr == None:
        usr = "user_info_not_provided"
        uid = 0
    usr = usr.lower().strip()
    ## add uid to users
    if uid not in uids:
        existing = session.query(User).get(usr)
        if existing != None:
            if existing.uid == -1:
                existing.uid = uid
        else:
            session.add(User(name=usr, uid=uid, institution="unknown"))
        uids.add(uid)

        session.commit()
    return usr, uid, uids


def load_nodes():
    with gzip.open("./data/node_data.pklz", "rb+") as f:
        data = pkl.load(f)

    usrs = set()
    nids = set()
    cs_ids = set()
    for d in data:
        usr = d.get('user')

        if usr == None:
            usr = 'unknown'
        usr.lower().strip()

        cs = d.get('changeset_id')
        if cs not in cs_ids:
            changeset = session.query(Changeset).get(cs)
            if changeset == None:
                session.add(Changeset(id=cs,
                                      author=usr))
                session.commit()
            else:
                if changeset.author != usr:
                    usr = changeset.author


        if usr not in usrs:
            u = session.query(User).get(usr)
            if u == None:
                session.add(User(name = 'unknown',
                                 institution = 'unknown'))
                session.commit()


        if d.get('id') not in nids:
            e = Node(osm_id = d.get('id'),
                     timestamp = d.get('timestamp'),
                     changeset_id = d.get('changeset_id'),
                     author_name = usr,
                     location = make_point_geom(d.get('lat'), d.get('lon'))
                     )
            session.merge(e)

        v = Version(version_num=d.get('version'),
                    element_id=d.get('id'))

        session.add(v)
        session.commit()


def load_ways():
    with gzip.open("./data/way_data.pklz", "rb+") as f:
        data = pkl.load(f)

    print(data[0])
    usrs = set()
    wids = set()
    cs_ids = set()
    for d in data:
        # ensure we have a user name - default to 'unknown' if not present in dict.
        usr = d.get('user')
        if usr == None:
            usr = 'unknown'
        usr.lower().strip()

        # ensure changeset exists in db for referential integrity if it has not been encountered yet in this call.
        cs = d.get('changeset_id')
        if cs not in cs_ids:
            changeset = session.query(Changeset).get(cs)
            if changeset == None:
                session.add(Changeset(id=cs, author=usr))
                session.commit()
            else:
                # if changeset already exists, overwrite author user to prevent possible 'unknown' others.
                # OSM wiki states that 1-1 correspondence between an OSM element and its changeset. (author of changeset must be author of element)
                if changeset.author != usr:
                    usr = changeset.author

        # ensure author name exists in DBfor referential integrity
        if usr not in usrs:
            u = session.query(User).get(usr)
            if u == None:
                session.add(User(name='unknown',
                                 institution='unknown'))
                session.commit()

        isDuplicate = False
        wid = d.get('id')
        if d.get('id') not in wids:
            w = Way(osm_id=wid,
                     timestamp=d.get('timestamp'),
                     changeset_id=d.get('changeset_id'),
                     author_name=usr,
                     )
            try:
                session.add(w)
                session.commit()
            except IntegrityError as e:
                session.rollback()
                isDuplicate = True

        # add way version
        try:
            v = Version(version_num=d.get('version'),
                    element_id=wid)
            session.add(v)
            session.commit()
        except IntegrityError as e:
            session.rollback()

        ## add associations between nodes and ways
        if not isDuplicate:
            nds = set()
            for i, n in enumerate(d.get('nd')):
                nid, nver = n['node_id'], n['node_version']
                key = (nid, nver)
                session.add(WayXNode(node_id=nid, way_id=wid, way_changeset=cs, position=i))
                ## add any new node versions to version table.
                if key not in nds:
                    nv = Version(version_num=nver,
                                element_id=nid)
                    session.merge(nv)
                    nds.add(key)
            session.commit()

def load_changeset_tags():
    with gzip.open("./data/changeset_data.pklz", "rb+") as f:
        data = pkl.load(f)

    print(data[0])
    data = (d for d in data if d.get('tag') != {})

    for d in data:
        cs_id = d.get('id')
        tags = d.get('tag')
        for t in tags:
            session.add(ChangesetTag(key = t, value = tags[t], changeset_id=cs_id))
        session.commit()


def load_node_tags():
    with gzip.open("./data/node_tags.pklz", "rb+") as f:
        data = pkl.load(f)

    print(data[0])

    tags = set()
    prevEle = None
    for d in data:
        t = (d.get('name'), d.get('val'), d.get('id'))
        curEle = t[2]
        if t not in tags:
            session.add(ElementTag(key=d.get('name'), value=d.get('val'), osm_id=d.get('id')))
            tags.add(t)

        if curEle != prevEle:
            session.commit()
        prevEle = curEle


def load_way_tags():
    with gzip.open("./data/way_tags.pklz", "rb+") as f:
        data = pkl.load(f)

    tags = set()
    prevEle = None
    for d in data:
        t = (d.get('name'), d.get('val'), d.get('id'))
        curEle = t[2]
        if t not in tags:
            session.add(ElementTag(key=d.get('name'), value=d.get('val'), osm_id=d.get('id')))
            tags.add(t)

        # commit all the tags for an element
        if prevEle != None and curEle != prevEle:
            session.commit()
        prevEle = curEle


# init_tables(True)
# load_chapters()
# load_users()
# load_changesets()
# load_nodes()
# load_ways()
# load_changeset_tags()
# load_node_tags()
# load_way_tags()
