#!/usr/bin/env python
from json import dumps
import logging
from venv import create

from flask import (
    Flask,
    g,
    request,
    Response,
)
from neo4j import (
    GraphDatabase,
    Record,
    basic_auth,
)

app = Flask(__name__, static_url_path="/static/")
url = "neo4j://localhost:7687"
username = "neo4j"
password = "cbdneo4j"
neo4j_version = "4"
database = "neo4j"

port = 8080

driver = GraphDatabase.driver(url, auth=basic_auth(username, password))


def get_db():
    if not hasattr(g, "neo4j_db"):
        if neo4j_version.startswith("4"):
            g.neo4j_db = driver.session(database=database)
        else:
            g.neo4j_db = driver.session()
    return g.neo4j_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, "neo4j_db"):
        g.neo4j_db.close()


@app.route("/")
def get_index():
    return app.send_static_file("index.html")

@app.route("/characters")
def get_characters():
    return app.send_static_file("characters.html")

@app.route("/stats")
def get_stats():
    return app.send_static_file("stats.html")

def serialize_house(house):
    return {
        "id": house["id"],
        "name": house["name"],
        "coatOfArms": house["coatOfArms"],
        "words": house["words"],
        "founded": house["founded"],
        "titles": house["titles"],
        "ancestralWeapons": house["ancestralWeapons"],
        "region": house["region"],
        "seats": house["seats"]
    }


def serialize_region(region):
    return {
        "id": region["id"],
        "name": region["name"]
    }

def serialize_seat(seat):
    return {
        "id": seat["id"],
        "name": seat["name"]
    }

def serialize_person(person):
    return {
        "id": person["id"],
        "name": person["name"],
        "aliases": person["aliases"],
        "born": person["born"],
        "books": person["books"],
        "tvSeries": person["tvSeries"],
        "playedBy": person["playedBy"],
        "isFemale": person["isFemale"],
        "culture": person["culture"],
        "died": person["died"],
        "titles": person["titles"],
    }

@app.route("/list")
def get_list():
    def work(tx):
        return list(tx.run(
            "MATCH (n:Person)-[r:ALLIED_WITH]->(h:House) "
            "RETURN h AS house, SUM(SIZE(n.tvSeries)) "
            "ORDER BY SUM(SIZE(n.tvSeries)) DESC "
            "LIMIT 10"
        ))
    db = get_db()
    results = db.read_transaction(work)
    return Response(
        dumps([serialize_house(record["house"]) for record in results]),
        mimetype="application/json"
    )

@app.route("/searchHouse")
def get_searchHouse():
    def work(tx, q_):
        return list(tx.run(
            "MATCH (house:House) "
            "WHERE toLower(house.name) CONTAINS toLower($name) "
            "RETURN house AS house LIMIT 20",
            {"name": q_}
        ))

    try:
        q = request.args["q"]
    except KeyError:
        return []
    else:
        db = get_db()
        results = db.read_transaction(work, q)
        return Response(
            dumps([serialize_house(record["house"]) for record in results]),
            mimetype="application/json"
        )

@app.route("/searchCharacter")
def get_searchCharacter():
    def work(tx, q_):
        return list(tx.run(
            "MATCH (person:Person) "
            "WHERE toLower(person.name) CONTAINS toLower($name) "
            "RETURN person AS person LIMIT 20",
            {"name": q_}
        ))

    try:
        q = request.args["q"]
    except KeyError:
        return []
    else:
        db = get_db()
        results = db.read_transaction(work, q)
        return Response(
            dumps([(serialize_person(record["person"]),record["person"].id) for record in results]),
            mimetype="application/json"
        )

@app.route("/allies/<house_id>", methods=["GET"])
def get_allies(house_id):
    def work(tx):
        return list(tx.run(
            "MATCH (p:Person)-[r:ALLIED_WITH]->(h:House) "
            "WHERE h.id = toInteger($house) "
            "RETURN COUNT(p) AS num_allies",
            {"house": house_id}
        ))
    db = get_db()
    results = db.read_transaction(work)
    return Response(
        dumps(results[0]["num_allies"]),
        mimetype="application/json"
    )

@app.route("/foundedBy/<house_id>", methods=["GET"])
def get_foundedBy(house_id):
    def work(tx):
        return list(tx.run(
            "MATCH (h:House)-[r:FOUNDED_BY]->(p:Person) "
            "WHERE h.id = toInteger($house) "
            "RETURN p as person",
            {"house": house_id}
        ))
    db = get_db()
    results = db.read_transaction(work)
    return Response(
        dumps(serialize_person(results[0]["person"])["name"] if results else "None"),
        mimetype="application/json"
    )


@app.route("/createCharacter")
def createCharacter():
    def work(tx, name_,isFemale_,playedBy_,culture_):
        result = tx.run(
            "CREATE (person:Person {name:$name, isFemale:$isFemale,playedBy:$playedBy,culture:$culture}) RETURN person AS person ,id(person) AS id",
            {"name": name_,"isFemale": isFemale_,"playedBy":playedBy_,"culture":culture_}
        )
        record = result.single()
        return serialize_person(record["person"]), record["id"]

    try:
        name = request.args["name"]
        if(request.args["isFemale"]=="true"):
            isFemale = True
        else:
            isFemale = False
        playedBy = request.args["playedBy"]
        culture = request.args["culture"]

    except KeyError:
        return ""
    else:
        db = get_db()
        result = db.write_transaction(work, name, isFemale,playedBy, culture)
        return Response(
            dumps(result),
            mimetype="application/json"
        )

@app.route("/createRel")
def createRel():
    def work(tx, houseid_,characterid_):
        result = tx.run(
            "MATCH (p:Person)-[r:ALLIED_WITH]->(h:House) WHERE h.id=toInteger($houseid) AND id(p)= toInteger($characterid) RETURN COUNT(p) AS match",
            {"houseid": houseid_,"characterid": characterid_}
        )

        match = result.data()[0]["match"]

        if not match:
            tx.run(
                "MATCH (h:House),(p:Person) WHERE h.id=toInteger($houseid) AND id(p)= toInteger($characterid) "
                "CREATE (p)-[r:ALLIED_WITH]->(h)",
                {"houseid": houseid_,"characterid": characterid_}
            )
            return True
        else:
            return False
    try:
        houseid = request.args["houseid"]
        characterid = request.args["characterid"]
    except KeyError:
        return ""
    else:
        db = get_db()
        response = db.write_transaction(work, houseid, characterid)
        return Response(
            dumps(response),
            mimetype="application/json"
        )

@app.route("/searchRegion")
def get_searchRegion():
    def work(tx, q_):
        res = tx.run(
            "MATCH (s:Seat)-[so:SEAT_OF]->(h:House)-[ir:IN_REGION]->(r:Region) "
            "WHERE id(r) = toInteger($id) "
            "RETURN COUNT(DISTINCT s) AS num",
            {"id": q_}
        )
        return res.data()[0]["num"]

    try:
        q = request.args["q"]
    except KeyError:
        return []
    else:
        db = get_db()
        results = db.read_transaction(work, q)
        return Response(
            dumps(results),
            mimetype="application/json"
        )

@app.route("/regions")
def get_regions():
    def work(tx):
        return list(tx.run(
            "MATCH (r:Region) "
            "RETURN r AS region, id(r) AS id"
        ))

    db = get_db()
    results = db.read_transaction(work)
    return Response(
        dumps([(serialize_region(record["region"]),record["id"]) for record in results]),
        mimetype="application/json"
    )

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)
    logging.info("Starting on port %d, database is at %s", port, url)
    app.run(port=port, use_reloader=True)